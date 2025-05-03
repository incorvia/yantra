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
      # Let ArgumentError bubble out
      unless workflow_klass.is_a?(Class) && workflow_klass < Yantra::Workflow
        raise ArgumentError, "#{workflow_klass} must be a subclass of Yantra::Workflow"
      end

      begin
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
        log_error("Workflow definition error: #{e.message}")
        raise e
      rescue Yantra::Errors::PersistenceError => e
        log_error("Persistence error during workflow creation: #{e.message}")
        raise e
      rescue StandardError => e
        backtrace_str = e.backtrace&.take(5)&.join("\n") || "No backtrace"
        log_error("Unexpected error during workflow creation: #{e.class} - #{e.message}\n#{backtrace_str}")
        raise Yantra::Errors::WorkflowError, "Unexpected error creating workflow: #{e.message}"
      end
    end



    # Initiates the execution of a previously created workflow.
    def self.start_workflow(workflow_id)
      Core::Orchestrator.new.start_workflow(workflow_id)
    rescue Yantra::Errors::EnqueueFailed => e
      # Let this propagate – the caller needs to know enqueueing failed
      raise e
    rescue StandardError => e
      log_error("Error starting workflow #{workflow_id}: #{e.message}")
      false # Or re-raise depending on desired behavior
    end

    # Retrieves workflow details from the repository.
    def self.find_workflow(workflow_id)
      Yantra.repository.find_workflow(workflow_id)
    rescue Yantra::Errors::PersistenceError => e
      log_error("Persistence error finding workflow #{workflow_id}: #{e.message}")
      nil
    end

    # Retrieves step details from the repository.
    def self.find_step(step_id)
      Yantra.repository.find_step(step_id)
    rescue Yantra::Errors::PersistenceError => e
      log_error("Persistence error finding step #{step_id}: #{e.message}")
      nil
    end

    # Retrieves steps associated with a specific workflow, optionally filtered by status.
    def self.list_steps(workflow_id:, status: nil)
      Yantra.repository.list_steps(workflow_id: workflow_id, status: status)
    rescue Yantra::Errors::PersistenceError => e
      log_error("Persistence error listing steps for workflow #{workflow_id}: #{e.message}")
      [] # Return empty array on error
    end

    # Attempts to cancel a running or pending workflow and its eligible steps.
    def self.cancel_workflow(workflow_id)
      log_info "Attempting to cancel workflow #{workflow_id}..."

      repo = Yantra.repository
      notifier = Yantra.notifier
      workflow = repo.find_workflow(workflow_id)

      unless workflow
        log_warn "Workflow #{workflow_id} not found."
        return false
      end

      current_state = workflow.state.to_sym

      if [Core::StateMachine::SUCCEEDED, Core::StateMachine::CANCELLED, Core::StateMachine::FAILED].include?(current_state)
        log_warn "Workflow already in finished state (#{current_state}). Cannot cancel."
        return false
      end

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
        log_warn "Failed to update workflow state to cancelled. Current state: #{latest_state}."
        return false
      end

      log_info "Workflow #{workflow_id} marked as cancelled in repository."

      # Publish workflow cancelled event
      begin
        payload = {
          workflow_id: workflow_id,
          klass: workflow.klass,
          state: Core::StateMachine::CANCELLED,
          finished_at: finished_at_time
        }
        notifier&.publish('yantra.workflow.cancelled', payload)
        log_info "Published yantra.workflow.cancelled event."
      rescue => e
        log_error "Failed to publish workflow cancelled event: #{e.message}"
      end

      # Cancel associated steps that are in a truly cancellable state
      begin
        all_steps = repo.list_steps(workflow_id: workflow_id) || []

        steps_to_cancel = all_steps.select do |step|
          enqueued_at = step.respond_to?(:enqueued_at) ? step.enqueued_at : nil
          Core::StateMachine.is_cancellable_state?(step.state.to_sym, enqueued_at)
        end

        step_ids = steps_to_cancel.map(&:id)
        log_debug "Steps eligible for cancellation: #{step_ids.inspect}"

        if step_ids.any?
          log_info "Attempting to cancel #{step_ids.size} steps in repository."
          cancel_attrs = {
            state: Core::StateMachine::CANCELLED.to_s,
            finished_at: finished_at_time,
            updated_at: finished_at_time
          }
          cancelled_count = repo.bulk_update_steps(step_ids, cancel_attrs)
          log_info "Repository confirmed cancellation of #{cancelled_count} steps."

          # Publish step cancelled events
          step_ids.each do |step_id|
            begin
              notifier&.publish('yantra.step.cancelled', { step_id: step_id, workflow_id: workflow_id })
            rescue => e
              log_error "Failed to publish step.cancelled event for step #{step_id}: #{e.message}"
            end
          end
        else
          log_info "No steps found in cancellable states for workflow #{workflow_id}."
        end
      rescue Yantra::Errors::PersistenceError => e
        log_error "Persistence error during step cancellation: #{e.message}"
      rescue => e
        log_error "Unexpected error during step cancellation processing: #{e.class} - #{e.message}"
      end

      true # Indicate cancellation process was successfully initiated for the workflow
    rescue => e # Catch unexpected errors in the main flow
      backtrace_str = e.backtrace&.take(5)&.join("\n") || "No backtrace"
      log_error("Unexpected error during cancel_workflow: #{e.class} - #{e.message}\n#{backtrace_str}")
      false # Return false on unexpected error
    end

    # Resets a FAILED workflow to RUNNING and attempts to re-enqueue FAILED steps
    # and steps stuck in SCHEDULING.
    def self.retry_failed_steps(workflow_id)
      repo = Yantra.repository
      worker = Yantra.worker_adapter
      notifier = Yantra.notifier
      workflow = repo.find_workflow(workflow_id)

      unless workflow
        log_warn "Workflow #{workflow_id} not found for retry."
        raise Yantra::Errors::WorkflowNotFound, "Workflow #{workflow_id} not found."
      end

      unless workflow.state.to_sym == Core::StateMachine::FAILED
        log_warn "Workflow #{workflow_id} is not in FAILED state (current: #{workflow.state}). Cannot retry."
        raise Yantra::Errors::InvalidWorkflowState, "Workflow #{workflow_id} is not in FAILED state (current: #{workflow.state})."
      end

      log_info "Resetting workflow #{workflow_id} state to RUNNING for retry."
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
        msg = "Failed to reset workflow state to RUNNING. Current state: #{latest_state}."
        log_error msg
        raise Yantra::Errors::PersistenceError, msg
      end

      retry_service = Core::WorkflowRetryService.new(
        workflow_id: workflow_id,
        repository: repo,
        notifier: notifier,
        worker_adapter: worker
        # Assuming service uses Yantra.logger internally
      )

      enqueued_count = retry_service.call

      log_info "Re-enqueued #{enqueued_count} failed/stuck steps for workflow #{workflow_id}."
      enqueued_count

    rescue Yantra::Errors::WorkflowNotFound, Yantra::Errors::InvalidWorkflowState, Yantra::Errors::PersistenceError, Yantra::Errors::EnqueueFailed => e
      log_error("Error during retry_failed_steps processing: #{e.class} - #{e.message}")
      raise e
    rescue => e
      backtrace_str = e.backtrace&.take(5)&.join("\n") || "No backtrace"
      log_error("Unexpected error during retry_failed_steps: #{e.class} - #{e.message}\n#{backtrace_str}")
      raise Yantra::Errors::WorkflowError, "Unexpected error during retry: #{e.message}"
    end

    # --- Logging Helpers (using def self.) ---
    def self.logger
      Yantra.logger || Logger.new(IO::NULL)
    end

    def self.log_info(msg)
      logger&.info("[Client] #{msg}")
    end
    def self.log_warn(msg)
      logger&.warn("[Client] #{msg}")
    end
    def self.log_error(msg)
      logger&.error("[Client] #{msg}")
    end
    def self.log_debug(msg)
      logger&.debug("[Client] #{msg}")
    end
    # --- END Logging Helpers ---

  end # class Client
end # module Yantra
