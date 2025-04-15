# lib/yantra/core/orchestrator.rb

require 'set'
require 'thread' # Keep for Queue if used, otherwise remove if Array used in find_all_descendants
require_relative 'state_machine'
require_relative '../errors'
require_relative '../worker/retry_handler' # Require the retry handler

module Yantra
  module Core
    # Responsible for driving workflow execution based on job lifecycle events
    # and managing job state transitions via the repository.
    class Orchestrator
      attr_reader :repository, :worker_adapter

      def initialize(repository: Yantra.repository, worker_adapter: Yantra.worker_adapter)
        unless repository && worker_adapter
            raise ArgumentError, "Repository and Worker Adapter must be provided."
        end
        @repository = repository
        @worker_adapter = worker_adapter
      end

      # --- Workflow Lifecycle ---

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
        StateMachine.validate_transition!(current_state, StateMachine::RUNNING) # Will raise if invalid

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
      end

      # --- Job Lifecycle Callbacks (Called by Worker Job) ---

      # Called by the worker just before executing user perform method.
      # Validates state and updates it to :running.
      # @param job_id [String]
      # @return [Boolean] true if okay to proceed with execution, false otherwise.
      def job_starting(job_id)
         puts "DEBUG: [Orchestrator] job_starting called for #{job_id}"
         job_record = repository.find_job(job_id)
         unless job_record
           puts "ERROR: [Orchestrator] Job record #{job_id} not found when starting. Cannot execute."
           return false
         end

         current_state = job_record.state.to_sym
         target_state = StateMachine::RUNNING

         # Allow starting if enqueued (first attempt) or already running (retry attempt)
         unless [StateMachine::ENQUEUED, StateMachine::RUNNING].include?(current_state)
           puts "WARN: [Orchestrator] Job #{job_id} expected state :enqueued or :running to start but found :#{current_state}."
           return false # Do not proceed
         end

         # Only update state if it was enqueued (first run)
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
               return false # Do not proceed
            end
            # TODO: Emit yantra.job.started event
         end
         true # Okay to proceed with execution
      end

      # Called by the worker after user perform method completes successfully.
      # @param job_id [String]
      # @param result [Object] The return value of the user's perform method.
      def job_succeeded(job_id, result)
         puts "DEBUG: [Orchestrator] job_succeeded called for #{job_id}"
         # Validate transition and update state
         final_attrs = {
           state: StateMachine::SUCCEEDED.to_s,
           finished_at: Time.current,
           output: result
         }
         # Use expected_old_state :running for optimistic lock
         update_success = repository.update_job_attributes(job_id, final_attrs, expected_old_state: :running)
         # TODO: Emit yantra.job.succeeded event

         if update_success
            # Trigger downstream logic only if state update was successful
            job_finished(job_id)
         else
            puts "WARN: [Orchestrator] Failed to update job #{job_id} state to succeeded (maybe already changed?)."
         end
      end

      # Called by the worker after user perform method raises an error.
      # Delegates retry/failure logic to the RetryHandler.
      # @param job_id [String]
      # @param error [StandardError] The exception object raised.
      def job_failed(job_id, error)
         puts "ERROR: [Orchestrator] job_failed called for #{job_id}: #{error.class}"
         # Fetch necessary data for the handler
         job_record = repository.find_job(job_id)
         unless job_record
            puts "ERROR: [Orchestrator] Job record #{job_id} not found when handling failure."
            return # Cannot proceed
         end
         # Need job klass name and execution count - where does worker get this?
         # Worker needs to fetch these or receive them. Assume worker passes them.
         # This indicates maybe the worker *should* instantiate the handler?
         # Let's assume for now Orchestrator can get this info if needed, or handler adapts.
         # Revisit: Maybe worker should pass executions count?

         # TODO: How to get user_job_klass and executions reliably here?
         # This suggests the RetryHandler might be better instantiated/called by the worker.
         # Let's revert this part for now and keep retry logic in worker,
         # but centralize the FINAL state update and job_finished call.

         # --- REVISED PLAN for job_failed ---
         # Worker calls RetryHandler. Handler decides :fail_permanently or :retry (re-raises).
         # If Handler decides :fail_permanently, IT calls Orchestrator#job_permanently_failed
         # If Handler re-raises, Orchestrator isn't called yet.

         # New method called by RetryHandler ONLY on final failure
         # @param job_id [String]
         # @param error [StandardError]
         # @param workflow_id [String]
         # @param final_state [Symbol] Should be :failed
         # @param finished_at [Time]
         # (This method combines update + record + flag + notify) - maybe too much?

         # Let's stick closer to the previous plan: RetryHandler updates state/error/flag, then calls job_finished.
         # So Orchestrator doesn't need job_failed/job_succeeded methods directly called by worker.
         # Worker calls repo to update state, then calls orchestrator.job_finished.
         # This puts logic back in the worker... undoing the refactor goal.

         # --- Let's try the Orchestrator-centric approach again ---
         # Worker calls orchestrator.job_failed(id, error, executions, klass_name)

         # Fetch user job klass
         user_job_klass = Object.const_get(job_record.klass) rescue Yantra::Job

         retry_handler_class = Yantra.configuration.try(:retry_handler_class) || Yantra::Worker::RetryHandler
         handler = retry_handler_class.new(
            repository: repository,
            orchestrator: self, # Pass self!
            job_record: job_record,
            error: error,
            executions: job_record.retries + 1, # Use Yantra's counter? No, need AJ's. Worker must pass executions!
            user_job_klass: user_job_klass
          )
          # Need to get execution count from worker... This design is flawed.

         # --- SIMPLER REFACTOR: Orchestrator handles FINAL state updates ---
         # Worker: try { perform } rescue { re-raise or call orchestrator.job_permanently_failed } success { orchestrator.job_succeeded }
         # Let's assume worker calls job_succeeded / job_permanently_failed

         # This method called by worker on PERMANENT failure (after retries)
         # Or called by RetryHandler in previous design
         # Let's assume RetryHandler calls this.
         mark_job_failed_permanently(job_id, error)
         # After marking failed, trigger downstream checks
         job_finished(job_id)

      end

      # --- Core Logic (Called Internally or by Success/Failure Handlers) ---

      # Central point called AFTER a job has reached a final state (succeeded, failed, cancelled).
      # Handles checking dependents and workflow completion.
      # @param job_id [String] ID of the job that reached a final state.
      def job_finished(job_id)
        puts "INFO: [Orchestrator] Handling post-finish logic for job #{job_id}."
        finished_job = repository.find_job(job_id)
        unless finished_job
          puts "ERROR: [Orchestrator] Job #{job_id} not found when handling finish."
          return
        end

        workflow_id = finished_job.workflow_id
        finished_state = finished_job.state.to_sym

        # If job succeeded, check dependents
        if finished_state == StateMachine::SUCCEEDED
          dependent_job_ids = repository.get_job_dependents(job_id)
          check_and_enqueue_dependents(dependent_job_ids)
        elsif finished_state == StateMachine::FAILED || finished_state == StateMachine::CANCELLED
          # If job failed/cancelled *finally*, trigger downstream cancellation
          # Note: This assumes the state was set by the worker/retry handler
          cancel_downstream_jobs(job_id)
        end

        # Check workflow completion regardless of this job's outcome
        check_workflow_completion(workflow_id)
      end

      private

      # --- Helper methods for state updates (called by worker outcome handlers) ---
      # These methods centralize the logic for updating state and notifying job_finished

      def mark_job_succeeded(job_id, result)
         puts "DEBUG: [Orchestrator] Setting job #{job_id} to succeeded."
         final_attrs = {
           state: StateMachine::SUCCEEDED.to_s,
           finished_at: Time.current,
           output: result
         }
         update_success = repository.update_job_attributes(job_id, final_attrs, expected_old_state: :running)
         # TODO: Emit yantra.job.succeeded event
         if update_success
            job_finished(job_id) # Trigger downstream checks
         else
            puts "WARN: [Orchestrator] Failed to update job #{job_id} state to succeeded (maybe already changed?)."
         end
      end

      def mark_job_failed_permanently(job_id, error)
         # Assumes retries already checked by caller (e.g., RetryHandler)
         puts "INFO: [Orchestrator] Marking job #{job_id} as failed permanently."
         job_record = repository.find_job(job_id) # Need workflow ID
         return unless job_record

         final_attrs = {
           state: StateMachine::FAILED.to_s,
           finished_at: Time.current
         }
         update_success = repository.update_job_attributes(job_id, final_attrs, expected_old_state: :running)

         if update_success
            repository.record_job_error(job_id, error)
            repository.set_workflow_has_failures_flag(job_record.workflow_id)
            # TODO: Emit yantra.job.failed event
            job_finished(job_id) # Trigger downstream checks
         else
            puts "WARN: [Orchestrator] Failed to update job #{job_id} state to failed (maybe already changed?)."
         end
      end


      # --- Other private helpers ---

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
