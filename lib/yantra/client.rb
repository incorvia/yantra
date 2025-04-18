# lib/yantra/client.rb

require 'securerandom'
require_relative 'workflow'
require_relative 'step'
require_relative 'errors'
require_relative 'core/orchestrator'
require_relative 'core/state_machine'
require_relative 'core/workflow_retry_service' # Keep this require

module Yantra
  # Public interface for interacting with the Yantra workflow system.
  class Client

    # Creates and persists a new workflow instance along with its defined
    # jobs and their dependencies.
    def self.create_workflow(workflow_klass, *args, **kwargs)
      # ... (create_workflow method remains unchanged) ...
      # 1. Validate Input
      unless workflow_klass.is_a?(Class) && workflow_klass < Yantra::Workflow
        raise ArgumentError, "#{workflow_klass} must be a Class inheriting from Yantra::Workflow"
      end

      # 2. Instantiate Workflow & Build Graph & Calculate Terminal Status
      wf_instance = workflow_klass.new(*args, **kwargs)

      # 3. Get the configured repository adapter instance
      repo = Yantra.repository

      # --- Persistence ---
      # TODO: Consider wrapping these steps in a transaction

      # 4. Persist the Workflow record itself
      unless repo.persist_workflow(wf_instance) # Pass instance directly
        raise Yantra::Errors::PersistenceError, "Failed to persist workflow record for ID: #{wf_instance.id}"
      end

      # 5. Persist the Jobs (using bulk method)
      jobs_to_persist = wf_instance.steps
      if jobs_to_persist.any?
        unless repo.persist_steps_bulk(jobs_to_persist)
          raise Yantra::Errors::PersistenceError, "Failed to bulk persist job records for workflow ID: #{wf_instance.id}"
        end
      end

      # 6. Persist the Dependencies (using bulk method)
      dependencies_hash = wf_instance.dependencies
      if dependencies_hash.any?
        links_array = dependencies_hash.flat_map do |step_id, dep_ids|
          dep_ids.map { |dep_id| { step_id: step_id, depends_on_step_id: dep_id } }
        end

        unless repo.add_step_dependencies_bulk(links_array)
          raise Yantra::Errors::PersistenceError, "Failed to persist dependency links for workflow ID: #{wf_instance.id}"
        end
      end

      # 7. Return the Workflow ID
      wf_instance.id
    end

    # Starts a previously created workflow.
    def self.start_workflow(workflow_id)
      # ... (start_workflow method remains unchanged) ...
      orchestrator = Core::Orchestrator.new
      orchestrator.start_workflow(workflow_id)
    end

    # Finds a workflow by its ID using the configured repository.
    def self.find_workflow(workflow_id)
      # ... (find_workflow method remains unchanged) ...
      Yantra.repository.find_workflow(workflow_id)
    end

    # Finds a job by its ID using the configured repository.
    def self.find_step(step_id)
      # ... (find_step method remains unchanged) ...
      Yantra.repository.find_step(step_id)
    end

    # Retrieves jobs associated with a workflow, optionally filtered by status.
    def self.get_workflow_steps(workflow_id, status: nil)
      # ... (get_workflow_steps method remains unchanged) ...
      Yantra.repository.get_workflow_steps(workflow_id, status: status)
    end

    # Attempts to cancel a workflow and its pending/enqueued steps.
    # Returns true if cancellation was successfully initiated, false otherwise.
    def self.cancel_workflow(workflow_id)
      log_method = Yantra.logger ? ->(level, msg) { Yantra.logger.send(level, "[Client.cancel_workflow] #{msg}") } : ->(level, msg) { puts "#{level.upcase}: [Client.cancel_workflow] #{msg}" }

      log_method.call(:info, "Attempting to cancel workflow #{workflow_id}...")

      repo = Yantra.repository
      notifier = Yantra.notifier
      workflow = repo.find_workflow(workflow_id)
      finished_at_time = Time.current # Capture time once

      # --- Workflow Validation ---
      unless workflow
        log_method.call(:warn, "Workflow #{workflow_id} not found.")
        return false
      end

      current_wf_state = workflow.state.to_sym
      if Core::StateMachine.terminal?(current_wf_state) || current_wf_state == Core::StateMachine::CANCELLED
        log_method.call(:warn, "Workflow #{workflow_id} is already in a terminal or cancelled state (#{current_wf_state}).")
        return false
      end

      # --- Update Workflow State ---
      # Try updating from PENDING or RUNNING to CANCELLED atomically.
      possible_previous_states = [Core::StateMachine::PENDING, Core::StateMachine::RUNNING]
      update_success = false
      possible_previous_states.each do |prev_state|
        update_success = repo.update_workflow_attributes(
          workflow_id,
          { state: Core::StateMachine::CANCELLED.to_s, finished_at: finished_at_time },
          expected_old_state: prev_state
        )
        break if update_success
      end

      unless update_success
        latest_state = repo.find_workflow(workflow_id)&.state || 'unknown'
        log_method.call(:warn, "Failed to update workflow #{workflow_id} state to cancelled (maybe state changed concurrently? Current state: #{latest_state}).")
        return false
      end

      log_method.call(:info, "Workflow #{workflow_id} marked as cancelled in repository.")

      # --- Publish Workflow Cancelled Event ---
      begin
        payload = {
          workflow_id: workflow_id,
          klass: workflow.klass,
          state: Core::StateMachine::CANCELLED, # Send symbol or string based on notifier needs
          finished_at: finished_at_time
        }
        notifier&.publish('yantra.workflow.cancelled', payload)
        log_method.call(:info, "Published yantra.workflow.cancelled event for #{workflow_id}.")
      rescue => e
        log_method.call(:error, "Failed to publish yantra.workflow.cancelled event for #{workflow_id}: #{e.message}")
      end

      # --- Cancel Pending/Enqueued Steps ---
      # Find steps after marking workflow cancelled.
      cancellable_step_states = [Core::StateMachine::PENDING.to_s, Core::StateMachine::ENQUEUED.to_s]
      steps_to_cancel = repo.get_workflow_steps(workflow_id)
                             .select { |j| cancellable_step_states.include?(j.state.to_s) } # Compare strings

      step_ids_to_cancel = steps_to_cancel.map(&:id)
      log_method.call(:debug, "Found step IDs to cancel: #{step_ids_to_cancel.inspect}")

      if step_ids_to_cancel.any?
        log_method.call(:info, "Cancelling #{step_ids_to_cancel.size} pending/enqueued jobs in DB: #{step_ids_to_cancel}.")
        begin
          # Bulk update steps in DB.
          cancelled_count = repo.cancel_steps_bulk(step_ids_to_cancel)
          log_method.call(:debug, "cancel_steps_bulk returned count: #{cancelled_count.inspect}")
          log_method.call(:info, "Repository confirmed DB cancellation update for #{cancelled_count} jobs.")

          # --- Publish Step Cancelled Events ---
          # Publish event for each step identified for cancellation, regardless of bulk update result count.
          log_method.call(:info, "Publishing yantra.step.cancelled events for steps: #{step_ids_to_cancel}...")
          step_ids_to_cancel.each do |step_id|
            log_method.call(:debug, "Publishing step.cancelled for step_id: #{step_id}")
            begin
              payload = { step_id: step_id, workflow_id: workflow_id } # Minimal payload
              notifier&.publish('yantra.step.cancelled', payload)
            rescue => e
              log_method.call(:error, "Failed to publish yantra.step.cancelled event for step #{step_id}: #{e.message}")
            end
          end
          # --- END Publish Step Cancelled Events ---

        rescue Yantra::Errors::PersistenceError => e
          log_method.call(:error, "Failed during bulk job cancellation for workflow #{workflow_id}: #{e.message}")
          # Continue, workflow is marked cancelled, but steps might not be.
        end
      else
        log_method.call(:info, "No pending or enqueued jobs found to cancel for workflow #{workflow_id}.")
      end

      true # Indicate cancellation process was initiated successfully

    # --- General Error Rescue ---
    rescue StandardError => e
      log_method.call(:error, "Unexpected error during cancel_workflow for #{workflow_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      false # Indicate failure
    end

    # Attempts to retry all failed jobs within a failed workflow.
    # Returns the number of jobs re-enqueued or false on failure.
    def self.retry_failed_steps(workflow_id)
      # ... (retry_failed_steps method remains unchanged) ...
      log_method = Yantra.logger ? ->(level, msg) { Yantra.logger.send(level, "[Client.retry_failed_steps] #{msg}") } : ->(level, msg) { puts "#{level.upcase}: [Client.retry_failed_steps] #{msg}" }

      repo = Yantra.repository
      worker = Yantra.worker_adapter # Get worker adapter instance
      notifier = Yantra.notifier     # Get notifier instance

      # 1. Validate Workflow State
      workflow = repo.find_workflow(workflow_id)
      unless workflow
        log_method.call(:warn, "Workflow #{workflow_id} not found.")
        return false
      end
      unless workflow.state.to_sym == Core::StateMachine::FAILED
        log_method.call(:warn, "Workflow #{workflow_id} is not in FAILED state (current: #{workflow.state}). Cannot retry.")
        return false
      end

      # 2. Reset Workflow State
      # Atomically update state back to running and clear has_failures flag,
      # ensuring it was previously 'failed'.
      log_method.call(:info, "Attempting to reset workflow #{workflow_id} to RUNNING state.")
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
        log_method.call(:error, "Failed to reset workflow #{workflow_id} state to RUNNING (expected FAILED, found #{latest_state}).")
        return false
      end
      log_method.call(:info, "Workflow #{workflow_id} state successfully reset to RUNNING.")

      # TODO: Emit workflow.retrying event? Consider adding:
      # notifier&.publish('yantra.workflow.retrying', { workflow_id: workflow_id, klass: workflow.klass })

      # 3. Delegate Job Re-enqueuing to Service Class
      # Ensure the service class is required at the top of the file
      retry_service = Core::WorkflowRetryService.new(
        workflow_id: workflow_id,
        repository: repo,
        notifier: notifier,
        worker_adapter: worker
      )
      reenqueued_count = retry_service.call
      log_method.call(:info, "WorkflowRetryService re-enqueued #{reenqueued_count} steps for workflow #{workflow_id}.")

      reenqueued_count # Return the count from the service

    # --- General Error Rescue ---
    rescue StandardError => e
      # Catch unexpected errors during the process
      log_method.call(:error, "Unexpected error during retry_failed_steps for #{workflow_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      false # Indicate failure
    end

    # --- Placeholder for Other Client Methods ---
    # ...

  end # class Client
end # module Yantra
