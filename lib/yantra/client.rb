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
      # ... (start_workflow method remains unchanged) ...
      puts "INFO: [Client] Requesting start for workflow #{workflow_id}..."
      orchestrator = Core::Orchestrator.new
      orchestrator.start_workflow(workflow_id)
    end

    # Finds a workflow by its ID using the configured repository.
    def self.find_workflow(workflow_id)
      # ... (find_workflow method remains unchanged) ...
      puts "INFO: [Client] Finding workflow #{workflow_id}..."
      Yantra.repository.find_workflow(workflow_id)
    end

    # Finds a job by its ID using the configured repository.
    def self.find_step(step_id)
      # ... (find_step method remains unchanged) ...
      puts "INFO: [Client] Finding step #{step_id}..."
      Yantra.repository.find_step(step_id)
    end

    # Retrieves jobs associated with a workflow, optionally filtered by status.
    def self.get_workflow_steps(workflow_id, status: nil)
      # ... (get_workflow_steps method remains unchanged) ...
      puts "INFO: [Client] Getting steps for workflow #{workflow_id} (status: #{status})..."
      Yantra.repository.get_workflow_steps(workflow_id, status: status)
    end

    # Attempts to cancel a workflow.
    def self.cancel_workflow(workflow_id)
      Yantra.logger.info { "[Client.cancel_workflow] Attempting to cancel workflow #{workflow_id}..." } if Yantra.logger
      repo = Yantra.repository
      notifier = Yantra.notifier
      workflow = repo.find_workflow(workflow_id)
      finished_at_time = Time.current

      unless workflow
        Yantra.logger.warn { "[Client.cancel_workflow] Workflow #{workflow_id} not found." } if Yantra.logger
        return false
      end

      current_wf_state = workflow.state.to_sym
      if Core::StateMachine.terminal?(current_wf_state) || current_wf_state == Core::StateMachine::CANCELLED
        Yantra.logger.warn { "[Client.cancel_workflow] Workflow #{workflow_id} is already in a terminal or cancelled state (#{current_wf_state})." } if Yantra.logger
        return false
      end

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
         Yantra.logger.warn { "[Client.cancel_workflow] Failed to update workflow #{workflow_id} state to cancelled (maybe state changed concurrently? Current state: #{latest_state})." } if Yantra.logger
         return false
      end

      Yantra.logger.info { "[Client.cancel_workflow] Workflow #{workflow_id} marked as cancelled in repository." } if Yantra.logger

      # Publish workflow.cancelled event
      begin
        payload = { workflow_id: workflow_id, klass: workflow.klass, state: Core::StateMachine::CANCELLED, finished_at: finished_at_time }
        notifier.publish('yantra.workflow.cancelled', payload)
        Yantra.logger.info { "[Client.cancel_workflow] Published yantra.workflow.cancelled event for #{workflow_id}." } if Yantra.logger
      rescue => e
        Yantra.logger.error { "[Client.cancel_workflow] Failed to publish yantra.workflow.cancelled event for #{workflow_id}: #{e.message}" } if Yantra.logger
      end

      # Find Pending/Enqueued Jobs for this Workflow *after* marking workflow cancelled
      cancellable_step_states = [Core::StateMachine::PENDING.to_s, Core::StateMachine::ENQUEUED.to_s]
      steps_to_cancel = repo.get_workflow_steps(workflow_id)
                           .select { |j| cancellable_step_states.include?(j.state.to_s) } # Compare strings

      # Bulk Cancel Pending/Enqueued Jobs in DB
      step_ids_to_cancel = steps_to_cancel.map(&:id)
      Yantra.logger.debug { "[Client.cancel_workflow] Found step IDs to cancel: #{step_ids_to_cancel.inspect}"} if Yantra.logger

      if step_ids_to_cancel.any?
        Yantra.logger.info { "[Client.cancel_workflow] Cancelling #{step_ids_to_cancel.size} pending/enqueued jobs in DB: #{step_ids_to_cancel}." } if Yantra.logger
        begin
          cancelled_count = repo.cancel_steps_bulk(step_ids_to_cancel)
          Yantra.logger.debug { "[Client.cancel_workflow] cancel_steps_bulk returned count: #{cancelled_count.inspect}"} if Yantra.logger
          Yantra.logger.info { "[Client.cancel_workflow] Repository confirmed DB cancellation update for #{cancelled_count} jobs." } if Yantra.logger

          # --- ADDED: Publish step.cancelled event for each cancelled step ---
          # Publish event even if DB count was 0, based on IDs found *before* bulk update
          Yantra.logger.info { "[Client.cancel_workflow] Publishing yantra.step.cancelled events for steps: #{step_ids_to_cancel}..." } if Yantra.logger
          step_ids_to_cancel.each do |step_id|
             Yantra.logger.debug { "[Client.cancel_workflow] Publishing step.cancelled for step_id: #{step_id}"} if Yantra.logger
             begin
                payload = { step_id: step_id, workflow_id: workflow_id }
                notifier.publish('yantra.step.cancelled', payload)
             rescue => e
                Yantra.logger.error { "[Client.cancel_workflow] Failed to publish yantra.step.cancelled event for step #{step_id}: #{e.message}" } if Yantra.logger
             end
          end
          # --- END ADDED SECTION ---

        rescue Yantra::Errors::PersistenceError => e
          Yantra.logger.error { "[Client.cancel_workflow] Failed during bulk job cancellation for workflow #{workflow_id}: #{e.message}" } if Yantra.logger
        end
      else
        Yantra.logger.info { "[Client.cancel_workflow] No pending or enqueued jobs found to cancel for workflow #{workflow_id}." } if Yantra.logger
      end

      true # Indicate cancellation process was initiated
    rescue StandardError => e
      Yantra.logger.error { "[Client.cancel_workflow] Unexpected error for workflow #{workflow_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}" } if Yantra.logger
      false
    end

    # Attempts to retry all failed jobs within a failed workflow.
    def self.retry_failed_steps(workflow_id)
      # ... (retry_failed_steps method remains unchanged) ...
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

    # --- Placeholder for Other Client Methods ---
    # ...

  end
end

