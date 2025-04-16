# lib/yantra/client.rb

require 'securerandom'
require_relative 'workflow'
require_relative 'job'
require_relative 'errors'
require_relative 'core/orchestrator' # Require Orchestrator
require_relative 'core/state_machine' # Require StateMachine for constants

module Yantra
  # Public interface for interacting with the Yantra workflow system.
  # Provides methods for creating, starting, and inspecting workflows and jobs.
  class Client

    # Creates and persists a new workflow instance along with its defined
    # jobs and their dependencies.
    def self.create_workflow(workflow_klass, *args, **kwargs)
      # ... (implementation remains the same) ...
      # 1. Validate Input
      unless workflow_klass.is_a?(Class) && workflow_klass < Yantra::Workflow
        raise ArgumentError, "#{workflow_klass} must be a Class inheriting from Yantra::Workflow"
      end

      # 2. Instantiate Workflow & Build Graph & Calculate Terminal Status
      puts "INFO: Instantiating workflow #{workflow_klass}..."
      wf_instance = workflow_klass.new(*args, **kwargs)
      puts "INFO: Workflow instance created and processed with #{wf_instance.jobs.count} jobs and #{wf_instance.dependencies.keys.count} jobs having dependencies."

      # 3. Get the configured repository adapter instance
      repo = Yantra.repository

      # --- Persistence ---
      # TODO: Consider wrapping these steps in a transaction

      # 4. Persist the Workflow record itself
      puts "INFO: Persisting workflow record (ID: #{wf_instance.id})..."
      unless repo.persist_workflow(wf_instance) # Pass instance directly
         raise Yantra::Errors::PersistenceError, "Failed to persist workflow record for ID: #{wf_instance.id}"
      end
      puts "INFO: Workflow record persisted."

      # 5. Persist the Jobs (using bulk method)
      jobs_to_persist = wf_instance.jobs
      if jobs_to_persist.any?
        puts "INFO: Persisting #{jobs_to_persist.count} job records via bulk..."
        unless repo.persist_jobs_bulk(jobs_to_persist)
          raise Yantra::Errors::PersistenceError, "Failed to bulk persist job records for workflow ID: #{wf_instance.id}"
        end
        puts "INFO: Job records persisted."
      end

      # 6. Persist the Dependencies (using bulk method)
      dependencies_hash = wf_instance.dependencies
      if dependencies_hash.any?
        links_array = dependencies_hash.flat_map do |job_id, dep_ids|
          dep_ids.map { |dep_id| { job_id: job_id, depends_on_job_id: dep_id } }
        end
        puts "INFO: Persisting #{links_array.count} dependency links via bulk..."
        unless repo.add_job_dependencies_bulk(links_array)
           raise Yantra::Errors::PersistenceError, "Failed to persist dependency links for workflow ID: #{wf_instance.id}"
        end
        puts "INFO: Dependency links persisted."
      end

      # 7. Return the Workflow ID
      puts "INFO: Workflow #{wf_instance.id} created successfully."
      wf_instance.id
    end

    # Starts a previously created workflow.
    def self.start_workflow(workflow_id)
      # ... (implementation remains the same) ...
      puts "INFO: [Client] Requesting start for workflow #{workflow_id}..."
      orchestrator = Core::Orchestrator.new
      orchestrator.start_workflow(workflow_id)
    end

    # Finds a workflow by its ID using the configured repository.
    def self.find_workflow(workflow_id)
      # ... (implementation remains the same) ...
      puts "INFO: [Client] Finding workflow #{workflow_id}..."
      Yantra.repository.find_workflow(workflow_id)
    end

    # Finds a job by its ID using the configured repository.
    def self.find_job(job_id)
      # ... (implementation remains the same) ...
      puts "INFO: [Client] Finding job #{job_id}..."
      Yantra.repository.find_job(job_id)
    end

    # Retrieves jobs associated with a workflow, optionally filtered by status.
    def self.get_workflow_jobs(workflow_id, status: nil)
      # ... (implementation remains the same) ...
      puts "INFO: [Client] Getting jobs for workflow #{workflow_id} (status: #{status})..."
      Yantra.repository.get_workflow_jobs(workflow_id, status: status)
    end

    # --- NEW METHOD: cancel_workflow ---
    # Attempts to cancel a workflow.
    # - Marks the workflow state as 'cancelled'.
    # - Cancels all associated jobs that are currently 'pending' or 'enqueued'.
    # - Allows currently 'running' jobs to complete naturally.
    # @param workflow_id [String] The UUID of the workflow to cancel.
    # @return [Boolean] true if cancellation was successfully initiated, false otherwise
    #   (e.g., workflow not found, already finished, or failed to update state).
    def self.cancel_workflow(workflow_id)
      puts "INFO: [Client.cancel_workflow] Attempting to cancel workflow #{workflow_id}..."
      repo = Yantra.repository
      workflow = repo.find_workflow(workflow_id)

      # 1. Validate Workflow State
      unless workflow
        puts "WARN: [Client.cancel_workflow] Workflow #{workflow_id} not found."
        return false
      end

      current_wf_state = workflow.state.to_sym
      if Core::StateMachine.terminal?(current_wf_state) || current_wf_state == Core::StateMachine::CANCELLED
        puts "WARN: [Client.cancel_workflow] Workflow #{workflow_id} is already in a terminal or cancelled state (#{current_wf_state})."
        return false # Already finished or cancelled
      end

      # 2. Mark Workflow as Cancelled
      # Use expected_old_state for optimistic locking
      # We might be able to cancel from pending or running
      possible_previous_states = [Core::StateMachine::PENDING, Core::StateMachine::RUNNING]
      update_success = false

      possible_previous_states.each do |prev_state|
         # Try updating from each possible previous state
         update_success = repo.update_workflow_attributes(
           workflow_id,
           { state: Core::StateMachine::CANCELLED.to_s, finished_at: Time.current },
           expected_old_state: prev_state
         )
         break if update_success # Stop trying if update succeeded
      end

      unless update_success
         # Reload to check the actual current state if update failed
         latest_state = repo.find_workflow(workflow_id)&.state || 'unknown'
         puts "WARN: [Client.cancel_workflow] Failed to update workflow #{workflow_id} state to cancelled (maybe state changed concurrently? Current state: #{latest_state})."
         return false
      end

      puts "INFO: [Client.cancel_workflow] Workflow #{workflow_id} marked as cancelled in repository."
      # TODO: Emit workflow.cancelled event here

      # 3. Find Pending/Enqueued Jobs for this Workflow
      cancellable_job_states = [Core::StateMachine::PENDING, Core::StateMachine::ENQUEUED]
      jobs_to_cancel = repo.get_workflow_jobs(workflow_id)
                           .select { |j| cancellable_job_states.include?(j.state.to_sym) }

      # 4. Bulk Cancel Pending/Enqueued Jobs
      if jobs_to_cancel.any?
        job_ids_to_cancel = jobs_to_cancel.map(&:id)
        puts "INFO: [Client.cancel_workflow] Cancelling #{job_ids_to_cancel.size} pending/enqueued jobs: #{job_ids_to_cancel}."
        begin
          # Use the bulk cancellation method from the repository interface
          cancelled_count = repo.cancel_jobs_bulk(job_ids_to_cancel)
          puts "INFO: [Client.cancel_workflow] Repository confirmed cancellation update for #{cancelled_count} jobs."
          # TODO: Emit job.cancelled events (might be hard in bulk)
        rescue Yantra::Errors::PersistenceError => e
          # Log error but continue, workflow state is already cancelled
          puts "ERROR: [Client.cancel_workflow] Failed during bulk job cancellation for workflow #{workflow_id}: #{e.message}"
        end
      else
        puts "INFO: [Client.cancel_workflow] No pending or enqueued jobs found to cancel for workflow #{workflow_id}."
      end

      # 5. Running jobs are left untouched and will finish naturally.
      #    The Orchestrator#job_finished method needs modification to handle this.

      true # Cancellation process initiated successfully
    rescue StandardError => e
      # Catch unexpected errors during the process
      puts "ERROR: [Client.cancel_workflow] Unexpected error for workflow #{workflow_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}"
      # Optionally re-raise wrapped in a Yantra::Error
      # raise Yantra::Error, "Cancellation failed: #{e.message}"
      false
    end
    # --- END cancel_workflow ---


    # --- Placeholder for Other Client Methods ---

    # def self.list_workflows(status: nil, limit: 50, offset: 0) ... end
    # def self.retry_failed_jobs(workflow_id) ... end
    # def self.retry_job(job_id) ... end
    # ... etc ...

  end
end

