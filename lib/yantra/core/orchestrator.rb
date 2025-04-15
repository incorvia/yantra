# lib/yantra/core/orchestrator.rb

require 'set' # Needed for Set in find_all_descendants
# Note: Using Array as queue instead of require 'thread' for Queue
require_relative 'state_machine'
require_relative '../errors'
# Assuming Yantra module provides access to configured adapters
# require_relative '../yantra'
# Assuming event system is available, e.g.:
# require 'active_support/notifications'

module Yantra
  module Core
    # Responsible for driving workflow execution based on job lifecycle events.
    # It interacts with the persistence layer (via Repository) and the
    # background job system (via Worker Adapter).
    class Orchestrator
      attr_reader :repository, :worker_adapter

      # @param repository An object implementing Yantra::Persistence::RepositoryInterface.
      # @param worker_adapter An object implementing Yantra::Worker::EnqueuingInterface.
      def initialize(repository: Yantra.repository, worker_adapter: Yantra.worker_adapter) # Assumes Yantra.worker_adapter is defined
        unless repository && worker_adapter
            raise ArgumentError, "Repository and Worker Adapter must be provided."
        end
        @repository = repository
        @worker_adapter = worker_adapter
        # @event_notifier = ActiveSupport::Notifications # Or your chosen notifier
      end

      # Attempts to start a workflow.
      # - Sets workflow state to running.
      # - Finds and enqueues initially ready jobs (those with no dependencies).
      #
      # @param workflow_id [String] The ID of the workflow to start.
      # @return [Boolean] true if successfully started, false otherwise.
      def start_workflow(workflow_id)
        puts "INFO: [Orchestrator] Attempting to start workflow #{workflow_id}"
        # TODO: Consider fetching workflow with pessimistic locking if using SQL
        workflow = repository.find_workflow(workflow_id)

        unless workflow
          puts "ERROR: [Orchestrator] Workflow #{workflow_id} not found for starting."
          # TODO: Raise specific error?
          return false
        end

        current_state = workflow.state.to_sym # Assumes state attribute exists

        # Only start if pending
        unless current_state == StateMachine::PENDING
           puts "WARN: [Orchestrator] Workflow #{workflow_id} cannot start; already in state :#{current_state}."
           return false
        end

        # Validate transition PENDING -> RUNNING for the workflow
        # Note: Workflow might only become RUNNING when first job starts.
        #       For v1 simplicity, we set it here. Revisit if needed.
        begin
          StateMachine.validate_transition!(current_state, StateMachine::RUNNING)
        rescue Errors::InvalidStateTransition => e
          puts "ERROR: [Orchestrator] Invalid initial state transition for workflow #{workflow_id}. #{e.message}"
          return false
        end

        # Update workflow state in persistence
        attributes = { state: StateMachine::RUNNING, started_at: Time.current }
        unless repository.update_workflow_attributes(workflow_id, attributes, expected_old_state: current_state)
           puts "WARN: [Orchestrator] Failed to update workflow #{workflow_id} state to RUNNING (maybe state changed concurrently?)."
           return false # Avoid proceeding if state update failed
        end

        puts "INFO: [Orchestrator] Workflow #{workflow_id} state set to RUNNING."
        # TODO: Emit workflow.started event: @event_notifier.instrument("workflow.started.yantra", workflow_id: workflow_id)

        # Find and enqueue jobs ready to run (initially, those with no dependencies)
        find_and_enqueue_ready_jobs(workflow_id)
        true
      end

      # Handles the completion (success or failure) of a single job.
      # - Finds dependents of the finished job.
      # - If success: Checks dependents and enqueues any that are now ready.
      # - If failure/cancelled: Initiates efficient downstream cancellation.
      # - Checks if the entire workflow is now complete.
      #
      # @param job_id [String] The ID of the job that finished.
      def job_finished(job_id)
        puts "INFO: [Orchestrator] Handling finish for job #{job_id}."
        finished_job = repository.find_job(job_id) # Assumes find_job is implemented

        unless finished_job
          puts "ERROR: [Orchestrator] Job #{job_id} not found when handling finish."
          return
        end

        workflow_id = finished_job.workflow_id # Assumes job object has workflow_id
        finished_state = finished_job.state.to_sym # Assumes job object has state

        # --- Handle Dependents ---
        if finished_state == StateMachine::SUCCEEDED
          dependent_job_ids = repository.get_job_dependents(job_id) # Assumes get_job_dependents is implemented
          puts "DEBUG: [Orchestrator] Job #{job_id} succeeded. Checking dependents: #{dependent_job_ids}" unless dependent_job_ids.empty?
          check_and_enqueue_dependents(dependent_job_ids) # Existing method to check/enqueue ready jobs
        elsif finished_state == StateMachine::FAILED || finished_state == StateMachine::CANCELLED
          # If job failed or was cancelled, cancel everything downstream efficiently
          puts "INFO: [Orchestrator] Job #{job_id} finished as #{finished_state}. Initiating downstream cancellation."
          cancel_downstream_jobs(job_id) # Calls the optimized bulk cancellation logic
        end

        # --- Check Workflow Completion ---
        # This check runs after every job finishes. Consider optimizations later if needed.
        check_workflow_completion(workflow_id)
      end


      private

      # Finds pending jobs in the workflow whose dependencies are all met (succeeded).
      # @param workflow_id [String]
      def find_and_enqueue_ready_jobs(workflow_id)
        # Use the repository method which should encapsulate the complex query/logic
        ready_job_ids = repository.find_ready_jobs(workflow_id)
        puts "INFO: [Orchestrator] Found ready jobs for workflow #{workflow_id}: #{ready_job_ids}" unless ready_job_ids.empty?

        ready_job_ids.each do |ready_job_id|
          enqueue_job(ready_job_id)
        end
      end

      # Given a list of job IDs, check if each one is ready to run (all deps succeeded)
      # and enqueue it if so.
      # @param dependent_job_ids [Array<String>]
      def check_and_enqueue_dependents(dependent_job_ids)
        # TODO: Optimize - Fetch dependent jobs and prerequisite states in bulk
        dependent_job_ids.each do |dep_job_id|
          dep_job = repository.find_job(dep_job_id)
          # Only proceed if the dependent job is still pending
          next unless dep_job && dep_job.state.to_sym == StateMachine::PENDING

          prerequisite_ids = repository.get_job_dependencies(dep_job_id)

          # Check if all prerequisites are met
          # TODO: Optimize this - could be N+1 queries for job states. Need bulk state lookup.
          all_deps_succeeded = prerequisite_ids.all? do |prereq_id|
            prereq_job = repository.find_job(prereq_id)
            # If a prerequisite job doesn't exist (e.g., deleted?), treat as not succeeded.
            prereq_job && prereq_job.state.to_sym == StateMachine::SUCCEEDED
          end

          if all_deps_succeeded
             puts "INFO: [Orchestrator] All dependencies met for job #{dep_job_id}. Enqueuing."
             enqueue_job(dep_job_id)
          else
             puts "DEBUG: [Orchestrator] Dependencies not yet met for job #{dep_job_id}."
             # Optional: Proactively cancel if a prerequisite failed/cancelled.
             # This might be redundant if the failed/cancelled job already triggered cancellation.
          end
        end
      end

      # Enqueues a specific job if it's in a valid state to be enqueued.
      # @param job_id [String]
      def enqueue_job(job_id)
        job = repository.find_job(job_id)
        return unless job # Job might have been deleted/cancelled concurrently

        current_state = job.state.to_sym
        target_state = StateMachine::ENQUEUED

        # Validate transition (e.g., PENDING -> ENQUEUED)
        begin
          StateMachine.validate_transition!(current_state, target_state)
        rescue Errors::InvalidStateTransition => e
           puts "WARN: [Orchestrator] Cannot enqueue job #{job_id}. #{e.message}"
           return
        end

        # Update state in DB first
        attributes = { state: target_state.to_s, enqueued_at: Time.current }
        if repository.update_job_attributes(job_id, attributes, expected_old_state: current_state)
          puts "INFO: [Orchestrator] Enqueuing job #{job_id} (Class: #{job.klass}, Queue: #{job.queue_name})..."
          # Actually enqueue using the worker adapter
          # Assumes worker adapter interface defines enqueue like this:
          worker_adapter.enqueue(job_id, job.workflow_id, job.klass, job.queue_name)
          # TODO: Emit job.enqueued event
        else
           puts "WARN: [Orchestrator] Failed to update job #{job_id} state to #{target_state} before enqueuing (maybe already changed?)."
        end
      end

      # Initiates cancellation for all jobs downstream from a failed/cancelled job.
      # Finds all descendants and triggers a bulk cancellation via the repository.
      # @param failed_or_cancelled_job_id [String] The ID of the job that failed/was cancelled.
      def cancel_downstream_jobs(failed_or_cancelled_job_id)
        descendant_ids_set = find_all_descendants(failed_or_cancelled_job_id)
        if descendant_ids_set.empty?
           puts "INFO: [Orchestrator] No downstream jobs found to cancel for #{failed_or_cancelled_job_id}."
           return
        end

        descendant_ids = descendant_ids_set.to_a
        puts "INFO: [Orchestrator] Attempting to bulk cancel #{descendant_ids.size} downstream jobs: #{descendant_ids}."

        # Use the bulk repository method (ensure it exists in interface/adapters)
        if repository.respond_to?(:cancel_jobs_bulk)
           updated_count = repository.cancel_jobs_bulk(descendant_ids)
           puts "INFO: [Orchestrator] Bulk cancellation request processed, updated #{updated_count} jobs."
           # TODO: Emit job.cancelled events for each job_id in descendant_ids? Or one bulk event?
        else
           puts "ERROR: [Orchestrator] Repository does not support #cancel_jobs_bulk. Cannot perform efficient cancellation."
           # If absolutely necessary, could fall back to a less efficient one-by-one here,
           # but ideally the bulk method should be implemented in all relevant adapters.
        end
      end

      # Performs a breadth-first search (BFS) using the repository
      # to find all unique descendant job IDs starting from the immediate
      # dependents of a given job ID.
      # @param start_job_id [String] The ID of the job whose descendants we need.
      # @return [Set<String>] A set of unique descendant job IDs.
      def find_all_descendants(start_job_id)
         puts "DEBUG: [Orchestrator] Finding all descendants for job #{start_job_id}"
         descendants = Set.new
         # Use an Array as a queue for BFS
         queue = repository.get_job_dependents(start_job_id).uniq
         queue.each { |id| descendants.add(id) } # Add initial dependents to set

         head = 0 # Pointer to the current item in the queue array
         while head < queue.length
           current_job_id = queue[head]
           head += 1 # Move head pointer

           # Find dependents of the current job
           # TODO: Consider a bulk get_job_dependents method for optimization if needed
           next_dependents = repository.get_job_dependents(current_job_id)
           next_dependents.each do |next_dep_id|
             # If successfully added (i.e., not seen before), add to queue for further traversal
             if descendants.add?(next_dep_id)
               queue << next_dep_id # Add to end of array for BFS
             end
           end
         end
         puts "DEBUG: [Orchestrator] Found descendants for job #{start_job_id}: #{descendants.to_a}"
         descendants
      end

      # Checks if the workflow has any running jobs. If not, determines the final
      # workflow state (succeeded/failed) based on the has_failures flag and updates it.
      # @param workflow_id [String]
      def check_workflow_completion(workflow_id)
        # Use efficient repository check first
        running_count = repository.running_job_count(workflow_id)
        puts "DEBUG: [Orchestrator] Checking workflow completion for #{workflow_id}. Running count: #{running_count}"
        return if running_count > 0

        # If no jobs are running, determine final state
        # Need to re-fetch workflow to get current state for validation, consider locking
        workflow = repository.find_workflow(workflow_id)
        # Only proceed if workflow exists and is currently marked as running
        # This prevents trying to complete an already finished/cancelled workflow again
        return unless workflow && workflow.state.to_sym == StateMachine::RUNNING

        has_failures = repository.workflow_has_failures?(workflow_id)
        final_state = has_failures ? StateMachine::FAILED : StateMachine::SUCCEEDED

        begin
          StateMachine.validate_transition!(StateMachine::RUNNING, final_state)
        rescue Errors::InvalidStateTransition => e
          # Should not happen if logic is correct, but log defensively
          puts "ERROR: [Orchestrator] Invalid final state transition calculation for workflow #{workflow_id}. #{e.message}"
          return
        end

        # Update workflow state to its final status
        attributes = { state: final_state.to_s, finished_at: Time.current } # Convert state to string for persistence
        puts "INFO: [Orchestrator] Workflow #{workflow_id} finished. Setting state to #{final_state}."
        if repository.update_workflow_attributes(workflow_id, attributes, expected_old_state: StateMachine::RUNNING)
          puts "INFO: [Orchestrator] Workflow #{workflow_id} successfully set to #{final_state}."
          # TODO: Emit workflow.completed or workflow.failed event
          # TODO: Trigger final stat calculation/persistence if implemented
        else
          # This could happen if another process already marked it finished concurrently
          puts "WARN: [Orchestrator] Failed to update workflow #{workflow_id} final state to #{final_state} (maybe already changed?)."
        end
      end

    end # class Orchestrator
  end # module Core
end # module Yantra

