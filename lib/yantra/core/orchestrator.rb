# lib/yantra/core/orchestrator.rb

require_relative 'state_machine'
require_relative '../errors'
# Assuming Yantra module provides access to configured adapters
# require_relative '../yantra'
# Assuming event system is available, e.g.:
# require 'active_support/notifications'

module Yantra
  module Core
    # Orchestrates the execution of workflows based on job lifecycle events.
    # It interacts with the persistence layer (via Repository) and the
    # background job system (via Worker Adapter).
    class Orchestrator
      attr_reader :repository, :worker_adapter

      # @param repository An object implementing Yantra::Persistence::RepositoryInterface.
      # @param worker_adapter An object implementing Yantra::Worker::EnqueuingInterface.
      def initialize(repository: Yantra.repository, worker_adapter: Yantra.worker_adapter) # Assumes Yantra.worker_adapter is defined
        @repository = repository
        @worker_adapter = worker_adapter
        # @event_notifier = ActiveSupport::Notifications # Or your chosen notifier
      end

      # Attempts to start a workflow.
      # - Sets workflow state to running.
      # - Finds and enqueues initially ready jobs (those with no dependencies).
      #
      # @param workflow_id [String] The ID of the workflow to start.
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
      # - If failure: Cancels dependents (basic v1 logic).
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

        # Get IDs of jobs that depend on the one that just finished
        dependent_job_ids = repository.get_job_dependents(job_id)
        puts "DEBUG: [Orchestrator] Job #{job_id} finished as #{finished_state}. Dependents: #{dependent_job_ids}" unless dependent_job_ids.empty?

        # --- Handle Dependents ---
        if finished_state == StateMachine::SUCCEEDED
          check_and_enqueue_dependents(dependent_job_ids)
        elsif finished_state == StateMachine::FAILED
          # TODO: Implement retry logic before cancelling dependents
          puts "INFO: [Orchestrator] Job #{job_id} failed. Cancelling dependents: #{dependent_job_ids}"
          cancel_dependents(dependent_job_ids) # Basic: cancel downstream jobs on failure
        elsif finished_state == StateMachine::CANCELLED
          # If a job is cancelled, its dependents should also be cancelled
          puts "INFO: [Orchestrator] Job #{job_id} was cancelled. Cancelling dependents: #{dependent_job_ids}"
          cancel_dependents(dependent_job_ids)
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
        dependent_job_ids.each do |dep_job_id|
          dep_job = repository.find_job(dep_job_id)
          # Only proceed if the dependent job is still pending
          next unless dep_job && dep_job.state.to_sym == StateMachine::PENDING

          prerequisite_ids = repository.get_job_dependencies(dep_job_id)

          # Check if all prerequisites are met
          # TODO: Optimize this - could be N+1 queries. Need bulk state lookup.
          #       For v1, proceed with simpler logic.
          all_deps_succeeded = prerequisite_ids.all? do |prereq_id|
            prereq_job = repository.find_job(prereq_id)
            prereq_job && prereq_job.state.to_sym == StateMachine::SUCCEEDED
          end

          if all_deps_succeeded
             puts "INFO: [Orchestrator] All dependencies met for job #{dep_job_id}. Enqueuing."
             enqueue_job(dep_job_id)
          else
             puts "DEBUG: [Orchestrator] Dependencies not yet met for job #{dep_job_id}."
             # Optional: Check if any prerequisite failed/cancelled to proactively cancel this job
             # prerequisite_states = prerequisite_ids.map { |id| repository.find_job(id)&.state&.to_sym }
             # if prerequisite_states.any? { |state| state == StateMachine::FAILED || state == StateMachine::CANCELLED }
             #   cancel_job(dep_job_id)
             # end
          end
        end
      end

      # Cancels a list of dependent jobs and their downstream dependents recursively.
      # @param job_ids_to_cancel [Array<String>]
      def cancel_dependents(job_ids_to_cancel)
         job_ids_to_cancel.each do |job_id|
           cancel_job(job_id) # cancel_job handles recursion via get_job_dependents
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
        attributes = { state: target_state, enqueued_at: Time.current }
        if repository.update_job_attributes(job_id, attributes, expected_old_state: current_state)
          puts "INFO: [Orchestrator] Enqueuing job #{job_id} (Class: #{job.klass}, Queue: #{job.queue_name})..."
          # Actually enqueue using the worker adapter
          # TODO: Ensure job object passed to enqueue has necessary info if not re-fetched by worker
          worker_adapter.enqueue(job_id, job.workflow_id, job.klass, job.queue_name) # Assumes enqueue signature
          # TODO: Emit job.enqueued event
        else
           puts "WARN: [Orchestrator] Failed to update job #{job_id} state to #{target_state} before enqueuing (maybe already changed?)."
        end
      end

      # Cancels a specific job if it's in a valid state to be cancelled.
      # Also recursively cancels jobs that depend on it.
      # @param job_id [String]
      def cancel_job(job_id)
        job = repository.find_job(job_id)
        return unless job

        current_state = job.state.to_sym
        target_state = StateMachine::CANCELLED

        # Avoid cancelling already finished/cancelled jobs
        return if StateMachine.terminal?(current_state) || current_state == target_state

        begin
          StateMachine.validate_transition!(current_state, target_state)
        rescue Errors::InvalidStateTransition => e
           puts "WARN: [Orchestrator] Cannot cancel job #{job_id}. #{e.message}"
           return
        end

        attributes = { state: target_state, finished_at: Time.current }
        if repository.update_job_attributes(job_id, attributes, expected_old_state: current_state)
          puts "INFO: [Orchestrator] Cancelled job #{job_id}."
          # TODO: Emit job.cancelled event

          # Recursively cancel jobs that depend on this cancelled job
          dependent_job_ids = repository.get_job_dependents(job_id)
          cancel_dependents(dependent_job_ids) unless dependent_job_ids.empty?
        else
           puts "WARN: [Orchestrator] Failed to update job #{job_id} state to #{target_state} during cancellation (maybe already changed?)."
        end
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
        attributes = { state: final_state, finished_at: Time.current }
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

    end
  end
end

