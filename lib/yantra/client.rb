# lib/yantra/client.rb

require 'securerandom'
require_relative 'workflow'
require_relative 'step'
require_relative 'errors'
require_relative 'core/orchestrator'
require_relative 'core/state_machine'
# --- Require the new service class ---
require_relative 'core/workflow_retry_service'
# --- ---

module Yantra
  # Public interface for interacting with the Yantra workflow system.
  class Client

    # Creates and persists a new workflow instance along with its defined
    # jobs and their dependencies.
    def self.create_workflow(workflow_klass, *args, **kwargs)
      # 1. Validate Input
      unless workflow_klass.is_a?(Class) && workflow_klass < Yantra::Workflow
        raise ArgumentError, "#{workflow_klass} must be a Class inheriting from Yantra::Workflow"
      end

      # 2. Instantiate Workflow & Build Graph & Calculate Terminal Status
      puts "INFO: Instantiating workflow #{workflow_klass}..."
      wf_instance = workflow_klass.new(*args, **kwargs)
      puts "INFO: Workflow instance created and processed with #{wf_instance.steps.count} jobs and #{wf_instance.dependencies.keys.count} jobs having dependencies."

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
      jobs_to_persist = wf_instance.steps
      if jobs_to_persist.any?
        puts "INFO: Persisting #{jobs_to_persist.count} job records via bulk..."
        unless repo.persist_steps_bulk(jobs_to_persist)
          raise Yantra::Errors::PersistenceError, "Failed to bulk persist job records for workflow ID: #{wf_instance.id}"
        end
        puts "INFO: Job records persisted."
      end

      # 6. Persist the Dependencies (using bulk method)
      dependencies_hash = wf_instance.dependencies
      if dependencies_hash.any?
        links_array = dependencies_hash.flat_map do |step_id, dep_ids|
          dep_ids.map { |dep_id| { step_id: step_id, depends_on_step_id: dep_id } }
        end
        puts "INFO: Persisting #{links_array.count} dependency links via bulk..."
        unless repo.add_step_dependencies_bulk(links_array)
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
      puts "INFO: [Client] Requesting start for workflow #{workflow_id}..."
      orchestrator = Core::Orchestrator.new
      orchestrator.start_workflow(workflow_id)
    end

    # Finds a workflow by its ID using the configured repository.
    def self.find_workflow(workflow_id)
      puts "INFO: [Client] Finding workflow #{workflow_id}..."
      Yantra.repository.find_workflow(workflow_id)
    end

    # Finds a job by its ID using the configured repository.
    def self.find_step(step_id)
      puts "INFO: [Client] Finding step #{step_id}..."
      Yantra.repository.find_step(step_id)
    end

    # Retrieves jobs associated with a workflow, optionally filtered by status.
    def self.get_workflow_steps(workflow_id, status: nil)
      puts "INFO: [Client] Getting steps for workflow #{workflow_id} (status: #{status})..."
      Yantra.repository.get_workflow_steps(workflow_id, status: status)
    end

    # Attempts to cancel a workflow.
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
      possible_previous_states = [Core::StateMachine::PENDING, Core::StateMachine::RUNNING]
      update_success = false
      possible_previous_states.each do |prev_state|
         update_success = repo.update_workflow_attributes(
           workflow_id,
           { state: Core::StateMachine::CANCELLED.to_s, finished_at: Time.current },
           expected_old_state: prev_state
         )
         break if update_success
      end

      unless update_success
         latest_state = repo.find_workflow(workflow_id)&.state || 'unknown'
         puts "WARN: [Client.cancel_workflow] Failed to update workflow #{workflow_id} state to cancelled (maybe state changed concurrently? Current state: #{latest_state})."
         return false
      end

      puts "INFO: [Client.cancel_workflow] Workflow #{workflow_id} marked as cancelled in repository."
      # TODO: Emit workflow.cancelled event here

      # 3. Find Pending/Enqueued Jobs for this Workflow
      cancellable_step_states = [Core::StateMachine::PENDING, Core::StateMachine::ENQUEUED]
      steps_to_cancel = repo.get_workflow_steps(workflow_id)
                           .select { |j| cancellable_step_states.include?(j.state.to_sym) }

      # 4. Bulk Cancel Pending/Enqueued Jobs
      if steps_to_cancel.any?
        step_ids_to_cancel = steps_to_cancel.map(&:id)
        puts "INFO: [Client.cancel_workflow] Cancelling #{step_ids_to_cancel.size} pending/enqueued jobs: #{step_ids_to_cancel}."
        begin
          cancelled_count = repo.cancel_steps_bulk(step_ids_to_cancel)
          puts "INFO: [Client.cancel_workflow] Repository confirmed cancellation update for #{cancelled_count} jobs."
        rescue Yantra::Errors::PersistenceError => e
          puts "ERROR: [Client.cancel_workflow] Failed during bulk job cancellation for workflow #{workflow_id}: #{e.message}"
        end
      else
        puts "INFO: [Client.cancel_workflow] No pending or enqueued jobs found to cancel for workflow #{workflow_id}."
      end

      true # Cancellation process initiated successfully
    rescue StandardError => e
      puts "ERROR: [Client.cancel_workflow] Unexpected error for workflow #{workflow_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}"
      false
    end


    # --- REFACTORED METHOD: retry_failed_steps ---
    # Attempts to retry all failed jobs within a failed workflow.
    # Delegates the core re-enqueuing logic to WorkflowRetryService.
    # @param workflow_id [String] The UUID of the workflow to retry.
    # @return [Integer, false] The number of jobs re-enqueued for retry, or false if the
    #   workflow was not found, not in a failed state, or failed to reset state.
    def self.retry_failed_steps(workflow_id)
      puts "INFO: [Client.retry_failed_steps] Attempting to retry failed jobs for workflow #{workflow_id}..."
      repo = Yantra.repository
      worker = Yantra.worker_adapter # Get worker adapter instance

      # 1. Validate Workflow State
      workflow = repo.find_workflow(workflow_id)
      unless workflow
        puts "WARN: [Client.retry_failed_steps] Workflow #{workflow_id} not found."
        return false
      end
      unless workflow.state.to_sym == Core::StateMachine::FAILED
        puts "WARN: [Client.retry_failed_steps] Workflow #{workflow_id} is not in 'failed' state (current: #{workflow.state}). Cannot retry."
        return false
      end

      # 2. Reset Workflow State
      # Atomically update state back to running and clear has_failures flag,
      # ensuring it was previously 'failed'.
      workflow_update_success = repo.update_workflow_attributes(
        workflow_id,
        {
          state: Core::StateMachine::RUNNING.to_s,
          has_failures: false,
          finished_at: nil # Clear finished_at timestamp
        },
        expected_old_state: Core::StateMachine::FAILED
      )

      unless workflow_update_success
        # Reload to check the actual current state if update failed
        latest_state = repo.find_workflow(workflow_id)&.state || 'unknown'
        puts "WARN: [Client.retry_failed_steps] Failed to reset workflow #{workflow_id} state to running (maybe state changed concurrently? Current state: #{latest_state})."
        return false
      end
      puts "INFO: [Client.retry_failed_steps] Workflow #{workflow_id} state reset to running."
      # TODO: Emit workflow.retrying event?

      # 3. Delegate Job Re-enqueuing to Service Class
      # Ensure the service class is required at the top of the file
      retry_service = Core::WorkflowRetryService.new(
        workflow_id: workflow_id,
        repository: repo,
        worker_adapter: worker
      )
      reenqueued_count = retry_service.call

      puts "INFO: [Client.retry_failed_steps] Finished. Service reported #{reenqueued_count} jobs re-enqueued."
      reenqueued_count # Return the count from the service

    rescue StandardError => e
      # Catch unexpected errors during the process
      puts "ERROR: [Client.retry_failed_steps] Unexpected error for workflow #{workflow_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}"
      false
    end
    # --- END REFACTORED retry_failed_steps ---


    # --- Placeholder for Other Client Methods ---

    # def self.list_workflows(status: nil, limit: 50, offset: 0) ... end
    # def self.retry_job(step_id) ... end # Might call retry_failed_steps internally after finding workflow?
    # ... etc ...

  end
end

