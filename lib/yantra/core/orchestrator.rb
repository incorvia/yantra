# lib/yantra/core/orchestrator.rb

require 'set'
require_relative 'state_machine'
require_relative '../errors'
require_relative '../worker/retry_handler'

module Yantra
  module Core
    class Orchestrator
      attr_reader :repository, :worker_adapter

      def initialize(repository: Yantra.repository, worker_adapter: Yantra.worker_adapter)
        # ... (remains the same) ...
        unless repository && worker_adapter
            raise ArgumentError, "Repository and Worker Adapter must be provided."
        end
        @repository = repository
        @worker_adapter = worker_adapter
      end

      # --- Workflow Lifecycle ---
      def start_workflow(workflow_id)
        # ... (remains the same) ...
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
      end

      # --- Job Lifecycle Callbacks ---
      def job_starting(job_id)
        # ... (remains the same) ...
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

      def job_succeeded(job_id, result)
        # ... (remains the same) ...
         puts "INFO: [Orchestrator] Job #{job_id} succeeded."
         final_attrs = {
           state: StateMachine::SUCCEEDED.to_s,
           finished_at: Time.current,
           output: result
         }
         update_success = repository.update_job_attributes(job_id, final_attrs, expected_old_state: :running)
         # TODO: Emit yantra.job.succeeded event

         if update_success
            job_finished(job_id)
         else
            puts "WARN: [Orchestrator] Failed to update job #{job_id} state to succeeded (maybe already changed?)."
         end
      end

      # --- MODIFIED: job_finished ---
      def job_finished(job_id)
        # ... (check for finished_job remains the same) ...
        puts "INFO: [Orchestrator] Handling post-finish logic for job #{job_id}."
        finished_job = repository.find_job(job_id)
        unless finished_job
          puts "ERROR: [Orchestrator] Job #{job_id} not found when handling post-finish."
          return
        end

        workflow_id = finished_job.workflow_id
        finished_state = finished_job.state.to_sym

        # --- Check if workflow is already cancelled ---
        workflow = repository.find_workflow(workflow_id)
        if workflow && workflow.state.to_sym == StateMachine::CANCELLED
          puts "INFO: [Orchestrator] Workflow #{workflow_id} is cancelled. Halting further processing for job #{job_id}."
          return
        end
        # --- End Check ---

        # ... (logic for handling success/failure/cancellation and dependents remains the same) ...
        if finished_state == StateMachine::SUCCEEDED
          dependent_job_ids = repository.get_job_dependents(job_id)
          check_and_enqueue_dependents(dependent_job_ids)
        elsif finished_state == StateMachine::FAILED
          cancel_downstream_jobs(job_id)
        elsif finished_state == StateMachine::CANCELLED
          cancel_downstream_jobs(job_id)
        end

        # Check workflow completion AFTER handling dependents
        check_workflow_completion(workflow_id)
      end
      # --- END MODIFIED: job_finished ---


      # --- Private methods ---
      # ... (find_and_enqueue_ready_jobs, etc. remain the same) ...
      private

      def find_and_enqueue_ready_jobs(workflow_id)
        # ... (implementation remains the same) ...
        ready_job_ids = repository.find_ready_jobs(workflow_id)
        puts "INFO: [Orchestrator] Found ready jobs for workflow #{workflow_id}: #{ready_job_ids}" unless ready_job_ids.empty?
        ready_job_ids.each { |id| enqueue_job(id) }
      end

      def check_and_enqueue_dependents(dependent_job_ids)
        # ... (implementation remains the same) ...
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

      def enqueue_job(job_id)
        # ... (implementation remains the same) ...
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

      def cancel_downstream_jobs(failed_or_cancelled_job_id)
        # ... (implementation remains the same) ...
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

      def find_all_descendants(start_job_id)
        # ... (implementation remains the same) ...
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


      # --- MODIFIED: check_workflow_completion ---
      # Checks if a workflow is complete and updates its state.
      # A workflow is complete if no jobs are running AND no jobs are enqueued.
      def check_workflow_completion(workflow_id)
        # Fetch counts for running and enqueued jobs
        running_count = repository.running_job_count(workflow_id)
        enqueued_count = repository.enqueued_job_count(workflow_id) # <<< Use new repo method

        puts "DEBUG: [Orchestrator] Checking workflow completion for #{workflow_id}. Running: #{running_count}, Enqueued: #{enqueued_count}"

        # Only proceed if nothing is running AND nothing is enqueued
        return if running_count > 0 || enqueued_count > 0

        # If we reach here, no jobs are running or waiting in the queue.
        # Now check the workflow's current state and failure status.
        workflow = repository.find_workflow(workflow_id)

        # Only attempt to finalize if the workflow is currently marked as running
        return unless workflow && workflow.state.to_sym == StateMachine::RUNNING

        # Determine final state based on failures
        has_failures = repository.workflow_has_failures?(workflow_id)
        final_state = has_failures ? StateMachine::FAILED : StateMachine::SUCCEEDED

        # Validate the transition (should be valid from running -> succeeded/failed)
        begin
          StateMachine.validate_transition!(StateMachine::RUNNING, final_state)
        rescue Errors::InvalidStateTransition => e
          puts "ERROR: [Orchestrator] Invalid final state transition calculation for workflow #{workflow_id}. #{e.message}"
          return
        end

        # Update the workflow state
        attributes = { state: final_state.to_s, finished_at: Time.current }
        puts "INFO: [Orchestrator] Workflow #{workflow_id} appears complete. Setting state to #{final_state}."
        if repository.update_workflow_attributes(workflow_id, attributes, expected_old_state: StateMachine::RUNNING)
          puts "INFO: [Orchestrator] Workflow #{workflow_id} successfully set to #{final_state}."
          # TODO: Emit workflow.succeeded or workflow.failed event
        else
          # This could happen if the state changed concurrently between checks
          puts "WARN: [Orchestrator] Failed to update workflow #{workflow_id} final state to #{final_state} (maybe already changed?)."
        end
      end
      # --- END MODIFIED: check_workflow_completion ---

    end # class Orchestrator
  end # module Core
end # module Yantra

