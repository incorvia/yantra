# lib/yantra/client.rb
# frozen_string_literal: true

require 'securerandom'
require_relative 'workflow'
require_relative 'step'
require_relative 'errors'
require_relative 'core/orchestrator'
require_relative 'core/state_machine'
require_relative 'core/workflow_retry_service'
require 'logger' # Ensure logger is available

module Yantra
  # Provides the public API for interacting with the Yantra workflow system.
  class Client
    # Creates and persists a new workflow instance, its steps, and dependencies.
    def self.create_workflow(workflow_klass, *args, **kwargs)
      unless workflow_klass.is_a?(Class) && workflow_klass < Yantra::Workflow
        raise ArgumentError, "#{workflow_klass} must be a subclass of Yantra::Workflow"
      end

      wf_instance = workflow_klass.new(*args, **kwargs)
      repo = Yantra.repository

      unless repo.create_workflow(wf_instance)
        raise Yantra::Errors::PersistenceError, "Failed to persist workflow record for ID: #{wf_instance.id}"
      end

      if wf_instance.steps.any?
        unless repo.create_steps_bulk(wf_instance.steps)
          raise Yantra::Errors::PersistenceError, "Failed to persist step records for workflow ID: #{wf_instance.id}"
        end
      end

      if wf_instance.dependencies.any?
        links_array = wf_instance.dependencies.flat_map do |step_id, dep_ids|
          dep_ids.map { |dep_id| { step_id: step_id, depends_on_step_id: dep_id } }
        end
        unless repo.add_step_dependencies_bulk(links_array)
          raise Yantra::Errors::PersistenceError, "Failed to persist dependency links for workflow ID: #{wf_instance.id}"
        end
      end

      wf_instance.id
    rescue Yantra::Errors::WorkflowDefinitionError, Yantra::Errors::DependencyNotFound => e
        log_method_for("create_workflow").call(:error, "Workflow definition error: #{e.message}")
        raise e
    rescue Yantra::Errors::PersistenceError => e
        log_method_for("create_workflow").call(:error, "Persistence error during workflow creation: #{e.message}")
        raise e
    rescue StandardError => e
        log_method_for("create_workflow").call(:error, "Unexpected error during workflow creation: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
        raise Yantra::Errors::WorkflowError, "Unexpected error creating workflow: #{e.message}"
    end

    # Initiates the execution of a previously created workflow.
    def self.start_workflow(workflow_id)
      Core::Orchestrator.new.start_workflow(workflow_id)
    end

    # Retrieves workflow details from the repository.
    def self.find_workflow(workflow_id)
      Yantra.repository.find_workflow(workflow_id)
    end

    # Retrieves step details from the repository.
    def self.find_step(step_id)
      Yantra.repository.find_step(step_id)
    end

    # Retrieves steps associated with a specific workflow, optionally filtered by status.
    def self.list_steps(workflow_id:, status: nil)
      Yantra.repository.list_steps(workflow_id: workflow_id, status: status)
    end

    # Attempts to cancel a running or pending workflow and its eligible steps.
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

      # Use StateMachine helper to check if already terminal (succeeded, cancelled)
      # Note: FAILED is not terminal for retry purposes, but should prevent cancellation here?
      # Let's prevent cancelling FAILED workflows via client.
      if [Core::StateMachine::SUCCEEDED, Core::StateMachine::CANCELLED, Core::StateMachine::FAILED].include?(current_state)
        log.call(:warn, "Workflow already in finished state (#{current_state}). Cannot cancel.")
        return false
      end

      # Try to atomically update state from PENDING or RUNNING to CANCELLED
      finished_at_time = Time.current
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
      end

      # Cancel associated steps that are in a truly cancellable state
      begin
        # Fetch all potentially relevant steps first
        all_steps = repo.list_steps(workflow_id: workflow_id) || []

        steps_to_cancel = all_steps.select do |step|
          enqueued_at = step.respond_to?(:enqueued_at) ? step.enqueued_at : nil
          Core::StateMachine.is_cancellable_state?(step.state.to_sym, enqueued_at)
        end

        step_ids = steps_to_cancel.map(&:id)
        log.call(:debug, "Steps eligible for cancellation: #{step_ids.inspect}")

        if step_ids.any?
          log.call(:info, "Attempting to cancel #{step_ids.size} steps in repository.")

          cancel_attrs = {
            state: Core::StateMachine::CANCELLED.to_s,
            finished_at: Time.current, # Use the same timestamp logic as before
            updated_at: Time.current
          }
          cancelled_count = repo.bulk_update_steps(step_ids, cancel_attrs)
          log.call(:info, "Repository confirmed cancellation of #{cancelled_count} steps.")

          # Publish step cancelled events
          step_ids.each do |step_id|
            begin
              notifier&.publish('yantra.step.cancelled', { step_id: step_id, workflow_id: workflow_id })
            rescue => e
              log.call(:error, "Failed to publish step.cancelled event for step #{step_id}: #{e.message}")
            end
          end
        else
          log.call(:info, "No steps found in cancellable states for workflow #{workflow_id}.")
        end
      rescue Yantra::Errors::PersistenceError => e
        log.call(:error, "Persistence error during step cancellation: #{e.message}")
      rescue => e
        log.call(:error, "Unexpected error during step cancellation processing: #{e.class} - #{e.message}")
      end

      true # Indicate cancellation process was successfully initiated for the workflow
    rescue => e
      log.call(:error, "Unexpected error during cancel_workflow: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      false
    end

    # Resets a FAILED workflow to RUNNING and re-enqueues its FAILED steps
    # (and potentially steps stuck in SCHEDULING).
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
      # Use direct repo call for now
      success = repo.update_workflow_attributes(
        workflow_id,
        {
          state: Core::StateMachine::RUNNING.to_s,
          has_failures: false,
          finished_at: nil
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
        worker_adapter: worker,
        logger: Yantra.logger
      )

      count = retry_service.call
      log.call(:info, "Re-enqueued #{count} failed/stuck steps for workflow #{workflow_id}.")
      count
    rescue => e
      log.call(:error, "Unexpected error during retry_failed_steps: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      false
    end

    private

    # Creates a logging lambda prefixed with the calling method context.
    def self.log_method_for(context)
      prefix = "[Client.#{context}]"
      logger = Yantra.logger || Logger.new(IO::NULL) # Ensure logger exists
      ->(level, msg) { logger.send(level, "#{prefix} #{msg}") }
    end
  end
end

