# lib/yantra/client.rb
# frozen_string_literal: true

require 'securerandom'
require_relative 'workflow'
require_relative 'step'
require_relative 'errors'
require_relative 'core/orchestrator'
require_relative 'core/state_machine'
require_relative 'core/workflow_retry_service'
require 'logger'

module Yantra
  class Client

    def self.create_workflow(workflow_klass, *args, **kwargs)
      unless workflow_klass.is_a?(Class) && workflow_klass < Yantra::Workflow
        raise ArgumentError, "#{workflow_klass} must be a subclass of Yantra::Workflow"
      end

      # Extract parent_workflow_id and idempotency_key from kwargs if present
      parent_workflow_id_key_present = kwargs.key?(:parent_workflow_id)
      parent_workflow_id = kwargs.delete(:parent_workflow_id)
      idempotency_key = kwargs.delete(:idempotency_key)

      # Validate parent_workflow_id if the key was provided
      if parent_workflow_id_key_present
        unless parent_workflow_id.is_a?(String) && !parent_workflow_id.empty?
          raise ArgumentError, "parent_workflow_id must be a non-empty string"
        end
      end

      begin
        wf_instance = workflow_klass.new(*args, **kwargs)
        repo = Yantra.repository

        # Choose the appropriate creation method based on whether this is a child workflow
        if parent_workflow_id
          unless repo.create_child_workflow(wf_instance, parent_workflow_id, idempotency_key: idempotency_key)
            raise Yantra::Errors::PersistenceError, "Failed to persist child workflow record for ID: #{wf_instance.id}"
          end
        else
          unless repo.create_workflow(wf_instance)
            raise Yantra::Errors::PersistenceError, "Failed to persist workflow record for ID: #{wf_instance.id}"
          end
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
      rescue ArgumentError => e
        # Re-raise ArgumentError directly without wrapping it
        raise e
      rescue StandardError => e
        backtrace_str = e.backtrace&.take(5)&.join("\n") || "No backtrace"
        log_error("Unexpected error during workflow creation: #{e.class} - #{e.message}\n#{backtrace_str}")
        raise Yantra::Errors::WorkflowError, "Unexpected error creating workflow: #{e.message}"
      end
    end

    def self.start_workflow(workflow_id)
      Core::Orchestrator.new.start_workflow(workflow_id)
    rescue Yantra::Errors::EnqueueFailed => e
      raise e
    rescue StandardError => e
      log_error("Error starting workflow #{workflow_id}: #{e.message}")
      false
    end

    def self.find_workflow(workflow_id)
      Yantra.repository.find_workflow(workflow_id)
    rescue Yantra::Errors::PersistenceError => e
      log_error("Persistence error finding workflow #{workflow_id}: #{e.message}")
      nil
    end

    def self.find_child_workflows(parent_workflow_id)
      Yantra.repository.find_child_workflows(parent_workflow_id)
    rescue Yantra::Errors::PersistenceError => e
      log_error("Persistence error finding child workflows for parent #{parent_workflow_id}: #{e.message}")
      []
    end

    def self.find_existing_idempotency_keys(parent_workflow_id, potential_keys)
      Yantra.repository.find_existing_idempotency_keys(parent_workflow_id, potential_keys)
    rescue Yantra::Errors::PersistenceError => e
      log_error("Persistence error finding existing idempotency keys for parent #{parent_workflow_id}: #{e.message}")
      []
    end

    def self.find_step(step_id)
      Yantra.repository.find_step(step_id)
    rescue Yantra::Errors::PersistenceError => e
      log_error("Persistence error finding step #{step_id}: #{e.message}")
      nil
    end

    def self.list_steps(workflow_id:, status: nil)
      Yantra.repository.list_steps(workflow_id: workflow_id, status: status)
    rescue Yantra::Errors::PersistenceError => e
      log_error("Persistence error listing steps for workflow #{workflow_id}: #{e.message}")
      []
    end

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

      begin
        all_steps = repo.list_steps(workflow_id: workflow_id) || []
        steps_to_cancel = all_steps.select do |step|
          Core::StateMachine.is_cancellable_state?(step.state.to_sym)
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

      true
    rescue => e
      backtrace_str = e.backtrace&.take(5)&.join("\n") || "No backtrace"
      log_error("Unexpected error during cancel_workflow: #{e.class} - #{e.message}\n#{backtrace_str}")
      false
    end

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
  end
end
