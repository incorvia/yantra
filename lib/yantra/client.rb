# lib/yantra/client.rb
# frozen_string_literal: true

require 'securerandom'
require_relative 'workflow'
require_relative 'step'
require_relative 'errors'
require_relative 'core/orchestrator'
require_relative 'core/state_machine'
require_relative 'core/workflow_retry_service'
require_relative 'core/workflow_creation_service'
require_relative 'core/workflow_cancellation_service'
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

        service = Core::WorkflowCreationService.new(repository: Yantra.repository)
        service.call(
          wf_instance,
          parent_workflow_id:,
          idempotency_key:
        )
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

    def self.find_existing_idempotency_keys(potential_keys)
      Yantra.repository.find_existing_idempotency_keys(potential_keys)
    rescue Yantra::Errors::PersistenceError => e
      log_error("Persistence error finding existing idempotency keys: #{e.message}")
      []
    end

    def self.find_workflow_by_idempotency_key(idempotency_key)
      Yantra.repository.find_workflow_by_idempotency_key(idempotency_key)
    rescue Yantra::Errors::PersistenceError => e
      log_error("Persistence error finding workflow by idempotency key #{idempotency_key}: #{e.message}")
      nil
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

      service = Core::WorkflowCancellationService.new(
        workflow_id: workflow_id,
        repository: Yantra.repository,
        notifier: Yantra.notifier
      )
      service.call
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
