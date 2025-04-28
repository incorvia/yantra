# lib/yantra/client.rb

require 'securerandom'
require_relative 'workflow'
require_relative 'step'
require_relative 'errors'
require_relative 'core/orchestrator'
require_relative 'core/state_machine'
require_relative 'core/workflow_retry_service'

module Yantra
  # Provides the public API for interacting with the Yantra workflow system.
  class Client
    # Creates and persists a new workflow instance, its steps, and dependencies.
    #
    # @param workflow_klass [Class] The workflow class (must subclass Yantra::Workflow).
    # @param args [Array] Positional arguments for the workflow constructor.
    # @param kwargs [Hash] Keyword arguments for the workflow constructor.
    # @return [String] The unique ID of the created workflow.
    # @raise [ArgumentError] If workflow_klass is not a valid Yantra::Workflow subclass.
    # @raise [Yantra::Errors::PersistenceError] If persistence fails at any stage.
    def self.create_workflow(workflow_klass, *args, **kwargs)
      unless workflow_klass.is_a?(Class) && workflow_klass < Yantra::Workflow
        raise ArgumentError, "#{workflow_klass} must be a subclass of Yantra::Workflow"
      end

      wf_instance = workflow_klass.new(*args, **kwargs)
      repo = Yantra.repository

      # Persist the main workflow record
      unless repo.create_workflow(wf_instance)
        raise Yantra::Errors::PersistenceError, "Failed to persist workflow record for ID: #{wf_instance.id}"
      end

      # Persist the steps if any are defined
      if wf_instance.steps.any?
        unless repo.create_steps_bulk(wf_instance.steps)
          raise Yantra::Errors::PersistenceError, "Failed to persist step records for workflow ID: #{wf_instance.id}"
        end
      end

      # Persist dependencies if any are defined
      if wf_instance.dependencies.any?
        links_array = wf_instance.dependencies.flat_map do |step_id, dep_ids|
          dep_ids.map { |dep_id| { step_id: step_id, depends_on_step_id: dep_id } }
        end

        unless repo.add_step_dependencies_bulk(links_array)
          raise Yantra::Errors::PersistenceError, "Failed to persist dependency links for workflow ID: #{wf_instance.id}"
        end
      end

      wf_instance.id
    end

    # Initiates the execution of a previously created workflow.
    #
    # @param workflow_id [String] The ID of the workflow to start.
    # @return Result of the orchestrator's start process (specifics depend on implementation).
    def self.start_workflow(workflow_id)
      Core::Orchestrator.new.start_workflow(workflow_id)
    end

    # Retrieves workflow status details from the repository.
    #
    # @param workflow_id [String] The ID of the workflow to find.
    # @return [Yantra::WorkflowStatus, nil] Workflow status object or nil if not found.
    def self.find_workflow(workflow_id)
      Yantra.repository.find_workflow(workflow_id)
    end

    # Retrieves step status details from the repository.
    #
    # @param step_id [String] The ID of the step to find.
    # @return [Yantra::StepStatus, nil] Step status object or nil if not found.
    def self.find_step(step_id)
      Yantra.repository.find_step(step_id)
    end

    # Retrieves all steps associated with a specific workflow.
    #
    # @param workflow_id [String] The ID of the workflow.
    # @param status [String, Symbol, nil] Optional filter by step status.
    # @return [Array<Yantra::StepStatus>] A list of step status objects.
    def self.list_steps(workflow_id:, status: nil)
      Yantra.repository.list_steps(workflow_id:, status: status)
    end

    # Attempts to cancel a running or pending workflow and its eligible steps.
    #
    # @param workflow_id [String] The ID of the workflow to cancel.
    # @return [Boolean] true if cancellation was initiated successfully, false otherwise.
    def self.cancel_workflow(workflow_id)
      log = log_method_for("cancel_workflow")
      log.call(:info, "Attempting to cancel workflow #{workflow_id}...")

      repo = Yantra.repository
      notifier = Yantra.notifier
      workflow = repo.find_workflow(workflow_id)

      unless workflow
        log.call(:warn, "Workflow #{workflow_id} not found.")
        return false
      end

      current_state = workflow.state.to_sym
      if Core::StateMachine.terminal?(current_state) || current_state == Core::StateMachine::CANCELLED
        log.call(:warn, "Workflow already in terminal/cancelled state (#{current_state}). Cannot cancel.")
        return false
      end

      # Try to atomically update state from PENDING or RUNNING to CANCELLED
      finished_at_time = Time.current # Use Time.current if ActiveSupport is available
      update_success = [Core::StateMachine::PENDING, Core::StateMachine::RUNNING].any? do |prev_state|
        repo.update_workflow_attributes(
          workflow_id,
          { state: Core::StateMachine::CANCELLED.to_s, finished_at: finished_at_time },
          expected_old_state: prev_state
        )
      end

      unless update_success
        latest_state = repo.find_workflow(workflow_id)&.state || 'unknown'
        log.call(:warn, "Failed to update workflow state to cancelled. Current state: #{latest_state}.")
        return false
      end

      log.call(:info, "Workflow #{workflow_id} marked as cancelled in repository.")

      # Publish workflow cancelled event
      begin
        payload = {
          workflow_id: workflow_id,
          klass: workflow.klass,
          state: Core::StateMachine::CANCELLED,
          finished_at: finished_at_time
        }
        notifier&.publish('yantra.workflow.cancelled', payload)
        log.call(:info, "Published yantra.workflow.cancelled event.")
      rescue => e
        log.call(:error, "Failed to publish workflow cancelled event: #{e.message}")
        # Continue with step cancellation even if notification fails
      end

      # Cancel associated pending/enqueued steps
      steps_to_cancel = repo.list_steps(workflow_id:)
                            .select { |j| [Core::StateMachine::PENDING.to_s, Core::StateMachine::ENQUEUED.to_s].include?(j.state.to_s) }

      step_ids = steps_to_cancel.map(&:id)
      log.call(:debug, "Steps eligible for cancellation: #{step_ids.inspect}")

      if step_ids.any?
        log.call(:info, "Attempting to cancel #{step_ids.size} steps in repository.")
        begin
          cancelled_count = repo.bulk_cancel_steps(step_ids)
          log.call(:info, "Repository confirmed cancellation of #{cancelled_count} steps.")

          # Publish step cancelled events
          step_ids.each do |step_id|
            begin
              notifier&.publish('yantra.step.cancelled', { step_id: step_id, workflow_id: workflow_id })
            rescue => e
              log.call(:error, "Failed to publish step.cancelled event for step #{step_id}: #{e.message}")
            end
          end
        rescue Yantra::Errors::PersistenceError => e
          log.call(:error, "Persistence error during step cancellation: #{e.message}")
          # Depending on requirements, might return false here or just log
        rescue => e # Catch unexpected errors during bulk cancel or notification loop
          log.call(:error, "Unexpected error during step cancellation processing: #{e.class} - #{e.message}")
        end
      else
        log.call(:info, "No pending/enqueued steps found to cancel for workflow #{workflow_id}.")
      end

      true # Indicate cancellation process was successfully initiated
    rescue => e # Catch unexpected errors in the main flow
      log.call(:error, "Unexpected error during cancel_workflow: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      false
    end

    # Resets a FAILED workflow to RUNNING and re-enqueues its FAILED steps.
    #
    # @param workflow_id [String] The ID of the FAILED workflow to retry.
    # @return [Integer, Boolean] The number of steps re-enqueued, or false on failure.
    def self.retry_failed_steps(workflow_id)
      log = log_method_for("retry_failed_steps")

      repo = Yantra.repository
      worker = Yantra.worker_adapter
      notifier = Yantra.notifier
      workflow = repo.find_workflow(workflow_id)

      unless workflow
        log.call(:warn, "Workflow #{workflow_id} not found for retry.")
        return false
      end

      unless workflow.state.to_sym == Core::StateMachine::FAILED
        log.call(:warn, "Workflow #{workflow_id} is not in FAILED state (current: #{workflow.state}). Cannot retry.")
        return false
      end

      log.call(:info, "Resetting workflow #{workflow_id} state to RUNNING for retry.")
      success = repo.update_workflow_attributes(
        workflow_id,
        {
          state: Core::StateMachine::RUNNING.to_s,
          has_failures: false,
          finished_at: nil # Clear finished timestamp
        },
        expected_old_state: Core::StateMachine::FAILED
      )

      unless success
        latest_state = repo.find_workflow(workflow_id)&.state || 'unknown'
        log.call(:error, "Failed to reset workflow state to RUNNING. Current state: #{latest_state}.")
        return false
      end

      # Delegate the logic for finding and re-enqueuing steps to the service
      retry_service = Core::WorkflowRetryService.new(
        workflow_id: workflow_id,
        repository: repo,
        notifier: notifier,
        worker_adapter: worker
      )

      count = retry_service.call
      log.call(:info, "Re-enqueued #{count} failed steps for workflow #{workflow_id}.")
      count # Return the count of re-enqueued jobs
    rescue => e
      log.call(:error, "Unexpected error during retry_failed_steps: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      false
    end

    private

    # Creates a logging lambda prefixed with the calling method context.
    # Falls back to puts if Yantra.logger is not configured.
    def self.log_method_for(context)
      prefix = "[Client.#{context}]"
      if Yantra.logger
        ->(level, msg) { Yantra.logger.send(level, "#{prefix} #{msg}") }
      else
        # Simple fallback logger
        ->(level, msg) { puts "#{level.upcase}: #{prefix} #{msg}" }
      end
    end
  end
end
