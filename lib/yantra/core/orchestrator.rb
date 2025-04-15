# lib/yantra/core/orchestrator.rb

require 'set'
# require 'thread' # Only if using Queue for find_all_descendants
require_relative 'state_machine'
require_relative '../errors'
require_relative '../worker/retry_handler' # Required by job_failed

module Yantra
  module Core
    # Responsible for driving workflow execution based on job lifecycle events
    # and managing job state transitions via the repository.
    class Orchestrator
      attr_reader :repository, :worker_adapter

      # @param repository An object implementing Yantra::Persistence::RepositoryInterface.
      # @param worker_adapter An object implementing Yantra::Worker::EnqueuingInterface.
      def initialize(repository: Yantra.repository, worker_adapter: Yantra.worker_adapter)
        unless repository && worker_adapter
            raise ArgumentError, "Repository and Worker Adapter must be provided."
        end
        @repository = repository
        @worker_adapter = worker_adapter
        # @event_notifier = ActiveSupport::Notifications # Or your chosen notifier
      end

      # --- Workflow Lifecycle ---

      # Starts a workflow if it's pending.
      # Finds initial jobs and enqueues them.
      # @param workflow_id [String]
      # @return [Boolean] true if successfully started, false otherwise.
      def start_workflow(workflow_id)
        puts "INFO: [Orchestrator] Attempting to start workflow #{workflow_id}"
        workflow = repository.find_workflow(workflow_id)
        unless workflow
          puts "ERROR: [Orchestrator] Workflow #{workflow_id} not found for starting."
          return false
        end
        current_state = workflow.state.to_sym
        unless current_state == StateMachine::PENDING
           puts "WARN: [Orchestrator] Workflow #{workflow_id} cannot start; already in state :#{current_state}."
           return false
        end

        StateMachine.validate_transition!(current_state, StateMachine::RUNNING) # Let it raise

        attributes = { state: StateMachine::RUNNING.to_s, started_at: Time.current }
        unless repository.update_workflow_attributes(workflow_id, attributes, expected_old_state: current_state)
           puts "WARN: [Orchestrator] Failed to update workflow #{workflow_id} state to RUNNING (maybe state changed concurrently?)."
           return false
        end

        puts "INFO: [Orchestrator] Workflow #{workflow_id} state set to RUNNING."
        # TODO: Emit workflow.started event

        find_and_enqueue_ready_jobs(workflow_id)
        true
      rescue Errors::InvalidStateTransition => e
        puts "ERROR: [Orchestrator] Invalid initial state transition for workflow #{workflow_id}. #{e.message}"
        false
      # TODO: Consider rescuing PersistenceError from find_workflow/update_workflow_attributes
      end

      # --- Job Lifecycle Callbacks (Called by Worker Job e.g., AsyncJob) ---

      # Called by the worker before executing user perform method.
      # Validates state and updates it to :running.
      # @param job_id [String]
      # @return [Boolean] true if okay to proceed with execution, false otherwise.
      def job_starting(job_id)
         puts "INFO: [Orchestrator] Starting job #{job_id}"
         job_record = repository.find_job(job_id)
         unless job_record
           puts "ERROR: [Orchestrator] Job record #{job_id} not found when starting."
           return false
         end

         current_state = job_record.state.to_sym
         target_state = StateMachine::RUNNING

         # Allow if enqueued (first run) or already running (retry)
         unless [StateMachine::ENQUEUED, StateMachine::RUNNING].include?(current_state)
           puts "WARN: [Orchestrator] Job #{job_id} expected state :enqueued or :running to start but found :#{current_state}."
           return false
         end

         # Only update state if moving from enqueued
         if current_state == StateMachine::ENQUEUED
           begin
             StateMachine.validate_transition!(current_state, target_state)
           rescue Errors::InvalidStateTransition => e
             puts "WARN: [Orchestrator] Cannot start job #{job_id}. #{e.message}"
             return false
           end

           update_attrs = { state: target_state.to_s, started_at: Time.current }
           unless repository.update_job_attributes(job_id, update_attrs, expected_old_state: :enqueued)
              puts "WARN: [Orchestrator] Failed to update job #{job_id} state to running (maybe already changed?). Aborting execution."
              return false
           end
           # TODO: Emit yantra.job.started event
         end
         true # Okay to proceed
      end

      # Called by the worker after user perform method completes successfully.
      # Updates state, records output, and triggers downstream checks.
      # @param job_id [String]
      # @param result [Object] The return value of the user's perform method.
      def job_succeeded(job_id, result)
         puts "INFO: [Orchestrator] Job #{job_id} succeeded."
         # Update state to succeeded and record output
         final_attrs = {
           state: StateMachine::SUCCEEDED.to_s,
           finished_at: Time.current,
           output: result
         }
         update_success = repository.update_job_attributes(job_id, final_attrs, expected_old_state: :running)
         # TODO: Emit yantra.job.succeeded event

         if update_success
            # Trigger downstream logic now that final state is set
            job_finished(job_id)
         else
            puts "WARN: [Orchestrator] Failed to update job #{job_id} state to succeeded (maybe already changed?)."
         end
      end

      # Called by the worker after user perform method raises an error.
      # Delegates retry/failure logic to the RetryHandler.
      # The RetryHandler will either re-raise (for backend retry) or call
      # mark_job_failed_permanently if retries are exhausted.
      # @param job_id [String]
      # @param error [StandardError] The exception object raised.
      # @param executions [Integer] The current execution attempt number.
      def job_failed(job_id, error, executions)
         puts "ERROR: [Orchestrator] Handling failure for job #{job_id} on attempt #{executions}."
         job_record = repository.find_job(job_id)
         unless job_record
           puts "ERROR: [Orchestrator] Job record #{job_id} not found when handling failure."
           return # Or raise? If job existed for job_starting, should exist now.
         end

         # Ensure klass is loaded for RetryHandler
         user_job_klass = Object.const_get(job_record.klass) rescue Yantra::Job

         # Delegate decision to RetryHandler
         # TODO: Make retry_handler_class configurable
         retry_handler_class = Yantra.configuration.try(:retry_handler_class) || Yantra::Worker::RetryHandler
         handler = retry_handler_class.new(
           repository: repository,
           orchestrator: self, # Pass self for callback on permanent failure
           job_record: job_record,
           error: error,
           executions: executions,
           user_job_klass: user_job_klass
         )

         # handle_error! will either re-raise the error (for backend retry)
         # or call self.mark_job_failed_permanently if max attempts reached.
         handler.handle_error!
      end

      # Called by RetryHandler ONLY when a job fails permanently after retries.
      # Updates state, records error, sets flag, calls job_finished.
      # @param job_id [String]
      # @param error [StandardError]
      def mark_job_failed_permanently(job_id, error)
        puts "INFO: [Orchestrator] Marking job #{job_id} as failed permanently."
        job_record = repository.find_job(job_id) # Fetch fresh record for workflow_id
        return unless job_record

        final_attrs = {
          state: StateMachine::FAILED.to_s,
          finished_at: Time.current
        }
        # Use expected_old_state :running for optimistic lock
        update_success = repository.update_job_attributes(job_id, final_attrs, expected_old_state: :running)

        if update_success
           repository.record_job_error(job_id, error)
           repository.set_workflow_has_failures_flag(job_record.workflow_id)
           # TODO: Emit yantra.job.failed event (permanent)
           job_finished(job_id) # Trigger downstream checks AFTER final failure is recorded
        else
           puts "WARN: [Orchestrator] Failed to update job #{job_id} state to failed (maybe already changed?)."
        end
      end


      # --- Core Logic (Triggered after final state set via job_succeeded/mark_job_failed_permanently) ---

      # Central point called AFTER a job has reached a final state (succeeded, failed, cancelled).
      # Handles checking dependents and workflow completion.
      # NOTE: This method assumes the job's final state is already persisted.
      # @param job_id [String] ID of the job that reached a final state.
      def job_finished(job_id)
        puts "INFO: [Orchestrator] Handling post-finish logic for job #{job_id}."
        finished_job = repository.find_job(job_id)
        unless finished_job
          puts "ERROR: [Orchestrator] Job #{job_id} not found when handling post-finish."
          return
        end

        workflow_id = finished_job.workflow_id
        finished_state = finished_job.state.to_sym

        # Trigger appropriate next steps based on the final state
        if finished_state == StateMachine::SUCCEEDED
          dependent_job_ids = repository.get_job_dependents(job_id)
          check_and_enqueue_dependents(dependent_job_ids)
        elsif finished_state == StateMachine::FAILED || finished_state == StateMachine::CANCELLED
          # Job reached final failed/cancelled state, initiate downstream cancellation
          cancel_downstream_jobs(job_id)
        end

        # Always check workflow completion after a job reaches a final state
        check_workflow_completion(workflow_id)
      end

      private

      # --- Helper methods ---

      # (find_and_enqueue_ready_jobs remains the same)
      def find_and_enqueue_ready_jobs(workflow_id)
        ready_job_ids = repository.find_ready_jobs(workflow_id)
        puts "INFO: [Orchestrator] Found ready jobs for workflow #{workflow_id}: #{ready_job_ids}" unless ready_job_ids.empty?
        ready_job_ids.each { |id| enqueue_job(id) }
      end

      # (check_and_enqueue_dependents remains the same)
      def check_and_enqueue_dependents(dependent_job_ids)
        dependent_job_ids.each do |dep_job_id|
          dep_job = repository.find_job(dep_job_id)
          next unless dep_job && dep_job.state.to_sym == StateMachine::PENDING
          prerequisite_ids = repository.get_job_dependencies(dep_job_id)
          all_deps_succeeded = prerequisite_ids.all? do |prereq_id|
            prereq_job = repository.find_job(prereq_id)
            prereq_job && prereq_job.state.to_sym == StateMachine::SUCCEEDED
          end
          enqueue_job(dep_job_id) if all_deps_succeeded
        end
      end

      # (enqueue_job remains the same)
      def enqueue_job(job_id)
        job = repository.find_job(job_id)
        return unless job
        current_state = job.state.to_sym
        target_state = StateMachine::ENQUEUED
        begin
          StateMachine.validate_transition!(current_state, target_state)
        rescue Errors::InvalidStateTransition => e
           puts "WARN: [Orchestrator] Cannot enqueue job #{job_id}. #{e.message}"
           return
        end
        attributes = { state: target_state.to_s, enqueued_at: Time.current }
        if repository.update_job_attributes(job_id, attributes, expected_old_state: current_state)
          queue_to_use = job.queue || 'default'
          worker_adapter.enqueue(job_id, job.workflow_id, job.klass, queue_to_use)
        else
           puts "WARN: [Orchestrator] Failed to update job #{job_id} state to #{target_state} before enqueuing."
        end
      end

      # (cancel_downstream_jobs remains the same)
      def cancel_downstream_jobs(failed_or_cancelled_job_id)
        descendant_ids_set = find_all_descendants(failed_or_cancelled_job_id)
        return if descendant_ids_set.empty?
        descendant_ids = descendant_ids_set.to_a
        puts "INFO: [Orchestrator] Attempting to bulk cancel #{descendant_ids.size} downstream jobs: #{descendant_ids}."
        if repository.respond_to?(:cancel_jobs_bulk)
           updated_count = repository.cancel_jobs_bulk(descendant_ids)
           puts "INFO: [Orchestrator] Bulk cancellation request processed for #{updated_count} jobs."
        else
           puts "ERROR: [Orchestrator] Repository does not support #cancel_jobs_bulk."
        end
      end

      # (find_all_descendants remains the same)
      def find_all_descendants(start_job_id)
         puts "DEBUG: [Orchestrator] Finding all descendants for job #{start_job_id}"
         descendants = Set.new
         queue = repository.get_job_dependents(start_job_id).uniq
         queue.each { |id| descendants.add(id) }
         head = 0
         while head < queue.length
           current_job_id = queue[head]; head += 1
           next_dependents = repository.get_job_dependents(current_job_id)
           next_dependents.each { |next_dep_id| queue << next_dep_id if descendants.add?(next_dep_id) }
         end
         puts "DEBUG: [Orchestrator] Found descendants for job #{start_job_id}: #{descendants.to_a}"
         descendants
      end

      # (check_workflow_completion remains the same)
      def check_workflow_completion(workflow_id)
        running_count = repository.running_job_count(workflow_id)
        puts "DEBUG: [Orchestrator] Checking workflow completion for #{workflow_id}. Running count: #{running_count}"
        return if running_count > 0
        workflow = repository.find_workflow(workflow_id)
        return unless workflow && workflow.state.to_sym == StateMachine::RUNNING
        has_failures = repository.workflow_has_failures?(workflow_id)
        final_state = has_failures ? StateMachine::FAILED : StateMachine::SUCCEEDED
        begin
          StateMachine.validate_transition!(StateMachine::RUNNING, final_state)
        rescue Errors::InvalidStateTransition => e
          puts "ERROR: [Orchestrator] Invalid final state transition calculation for workflow #{workflow_id}. #{e.message}"
          return
        end
        attributes = { state: final_state.to_s, finished_at: Time.current }
        puts "INFO: [Orchestrator] Workflow #{workflow_id} finished. Setting state to #{final_state}."
        if repository.update_workflow_attributes(workflow_id, attributes, expected_old_state: StateMachine::RUNNING)
          puts "INFO: [Orchestrator] Workflow #{workflow_id} successfully set to #{final_state}."
        else
          puts "WARN: [Orchestrator] Failed to update workflow #{workflow_id} final state to #{final_state} (maybe already changed?)."
        end
      end

    end # class Orchestrator
  end # module Core
end # module Yantra

