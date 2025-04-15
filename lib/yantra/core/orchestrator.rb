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
          return false
        end

        current_state = workflow.state.to_sym # Assumes state attribute exists

        unless current_state == StateMachine::PENDING
           puts "WARN: [Orchestrator] Workflow #{workflow_id} cannot start; already in state :#{current_state}."
           return false
        end

        begin
          StateMachine.validate_transition!(current_state, StateMachine::RUNNING)
        rescue Errors::InvalidStateTransition => e
          puts "ERROR: [Orchestrator] Invalid initial state transition for workflow #{workflow_id}. #{e.message}"
          return false
        end

        attributes = { state: StateMachine::RUNNING.to_s, started_at: Time.current } # Use String for state persistence
        unless repository.update_workflow_attributes(workflow_id, attributes, expected_old_state: current_state)
           puts "WARN: [Orchestrator] Failed to update workflow #{workflow_id} state to RUNNING (maybe state changed concurrently?)."
           return false # Avoid proceeding if state update failed
        end

        puts "INFO: [Orchestrator] Workflow #{workflow_id} state set to RUNNING."
        # TODO: Emit workflow.started event

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
        finished_job = repository.find_job(job_id)

        unless finished_job
          puts "ERROR: [Orchestrator] Job #{job_id} not found when handling finish."
          return
        end

        workflow_id = finished_job.workflow_id
        finished_state = finished_job.state.to_sym

        # --- Handle Dependents ---
        if finished_state == StateMachine::SUCCEEDED
          dependent_job_ids = repository.get_job_dependents(job_id)
          puts "DEBUG: [Orchestrator] Job #{job_id} succeeded. Checking dependents: #{dependent_job_ids}" unless dependent_job_ids.empty?
          check_and_enqueue_dependents(dependent_job_ids)
        elsif finished_state == StateMachine::FAILED || finished_state == StateMachine::CANCELLED
          puts "INFO: [Orchestrator] Job #{job_id} finished as #{finished_state}. Initiating downstream cancellation."
          cancel_downstream_jobs(job_id)
        end

        # --- Check Workflow Completion ---
        check_workflow_completion(workflow_id)
      end


      private

      # Finds pending jobs in the workflow whose dependencies are all met (succeeded).
      # @param workflow_id [String]
      def find_and_enqueue_ready_jobs(workflow_id)
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
          next unless dep_job && dep_job.state.to_sym == StateMachine::PENDING

          prerequisite_ids = repository.get_job_dependencies(dep_job_id)
          # TODO: Optimize this - could be N+1 queries for job states. Need bulk state lookup.
          all_deps_succeeded = prerequisite_ids.all? do |prereq_id|
            prereq_job = repository.find_job(prereq_id)
            prereq_job && prereq_job.state.to_sym == StateMachine::SUCCEEDED
          end

          if all_deps_succeeded
             puts "INFO: [Orchestrator] All dependencies met for job #{dep_job_id}. Enqueuing."
             enqueue_job(dep_job_id)
          else
             puts "DEBUG: [Orchestrator] Dependencies not yet met for job #{dep_job_id}."
          end
        end
      end

      # Enqueues a specific job if it's in a valid state to be enqueued.
      # @param job_id [String]
      def enqueue_job(job_id)
        puts "DEBUG: [Orchestrator] Attempting to enqueue job #{job_id}..." # DEBUG
        job = repository.find_job(job_id)
        unless job
           puts "WARN: [Orchestrator] Cannot enqueue job #{job_id}. Job not found." # DEBUG
           return
        end

        current_state = job.state.to_sym
        target_state = StateMachine::ENQUEUED
        puts "DEBUG: [Orchestrator] Job #{job_id} current state :#{current_state}. Attempting transition to :#{target_state}." # DEBUG

        begin
          StateMachine.validate_transition!(current_state, target_state)
          puts "DEBUG: [Orchestrator] Transition valid for job #{job_id}." # DEBUG
        rescue Errors::InvalidStateTransition => e
           puts "WARN: [Orchestrator] Cannot enqueue job #{job_id}. #{e.message}"
           return
        end

        # Update state in DB first
        attributes = { state: target_state.to_s, enqueued_at: Time.current } # Use String for state persistence
        puts "DEBUG: [Orchestrator] Attempting to update job #{job_id} attributes: #{attributes.inspect} (expected old state: #{current_state})" # DEBUG
        update_result = repository.update_job_attributes(job_id, attributes, expected_old_state: current_state)
        puts "DEBUG: [Orchestrator] Update job attributes result for #{job_id}: #{update_result.inspect}" # DEBUG

        if update_result
          # Use the 'queue' attribute from the persisted job record
          queue_to_use = job.queue || 'default' # Fallback if queue column is nil
          puts "INFO: [Orchestrator] Enqueuing job #{job_id} (Class: #{job.klass}, Queue: #{queue_to_use}) via worker adapter..." # DEBUG
          # Pass attributes from the persisted record to the worker adapter
          worker_adapter.enqueue(job_id, job.workflow_id, job.klass, queue_to_use) # <<< FIXED: Use job.queue
          # TODO: Emit job.enqueued event
        else
           puts "WARN: [Orchestrator] Failed to update job #{job_id} state to #{target_state} before enqueuing (maybe already changed?). NOT ENQUEUING." # DEBUG
        end
      end

      # Initiates cancellation for all jobs downstream from a failed/cancelled job.
      # @param failed_or_cancelled_job_id [String]
      def cancel_downstream_jobs(failed_or_cancelled_job_id)
        descendant_ids_set = find_all_descendants(failed_or_cancelled_job_id)
        return if descendant_ids_set.empty?

        descendant_ids = descendant_ids_set.to_a
        puts "INFO: [Orchestrator] Attempting to bulk cancel #{descendant_ids.size} downstream jobs: #{descendant_ids}."

        if repository.respond_to?(:cancel_jobs_bulk)
           updated_count = repository.cancel_jobs_bulk(descendant_ids)
           puts "INFO: [Orchestrator] Bulk cancellation request processed for #{updated_count} jobs."
           # TODO: Emit job.cancelled events
        else
           puts "ERROR: [Orchestrator] Repository does not support #cancel_jobs_bulk. Cannot perform efficient cancellation."
        end
      end

      # Performs BFS to find all unique descendant job IDs starting from immediate dependents.
      # @param start_job_id [String]
      # @return [Set<String>]
      def find_all_descendants(start_job_id)
         # ... (implementation remains same) ...
         puts "DEBUG: [Orchestrator] Finding all descendants for job #{start_job_id}"
         descendants = Set.new
         queue = repository.get_job_dependents(start_job_id).uniq
         queue.each { |id| descendants.add(id) }
         head = 0
         while head < queue.length
           current_job_id = queue[head]
           head += 1
           next_dependents = repository.get_job_dependents(current_job_id)
           next_dependents.each do |next_dep_id|
             if descendants.add?(next_dep_id)
               queue << next_dep_id
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
        # ... (implementation remains same) ...
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
          # TODO: Emit workflow.completed or workflow.failed event
          # TODO: Trigger final stat calculation/persistence if implemented
        else
          puts "WARN: [Orchestrator] Failed to update workflow #{workflow_id} final state to #{final_state} (maybe already changed?)."
        end
      end

    end # class Orchestrator
  end # module Core
end # module Yantra

