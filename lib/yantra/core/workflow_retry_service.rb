# lib/yantra/core/workflow_retry_service.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require_relative 'step_enqueuer'
require 'logger'

module Yantra
  module Core
    class WorkflowRetryService
      attr_reader :workflow_id, :repository, :step_enqueuer, :logger

      def initialize(workflow_id:, repository:, worker_adapter:, notifier:)
        @workflow_id = workflow_id
        @repository  = repository
        @logger      = Yantra.logger || Logger.new(IO::NULL)

        @step_enqueuer = StepEnqueuer.new(
          repository: repository,
          worker_adapter: worker_adapter,
          notifier: notifier,
          logger: @logger
        )

        validate_repository_interface!
      end

      def call
        failed_steps = find_failed_steps
        return 0 if failed_steps.empty?

        retryable_ids = failed_steps.map(&:id)
        log_info "Retrying #{retryable_ids.size} failed steps: #{retryable_ids.inspect}"

        return 0 unless reset_steps_to_pending(retryable_ids)

        enqueue_retryable_steps(retryable_ids)
      end

      private

      def validate_repository_interface!
        unless repository.respond_to?(:list_steps) && repository.respond_to?(:bulk_update_steps)
          raise ArgumentError, "Repository must implement list_steps and bulk_update_steps"
        end
      end

      def find_failed_steps
        repository.list_steps(workflow_id: workflow_id, status: :failed) || []
      rescue => e
        log_error "Failed to fetch failed steps for workflow #{workflow_id}: #{e.message}"
        []
      end

      def reset_steps_to_pending(step_ids)
        attrs = {
          state: StateMachine::PENDING.to_s,
          error: nil,
          output: nil,
          started_at: nil,
          finished_at: nil,
          enqueued_at: nil,
          updated_at: Time.current
        }

        updated = repository.bulk_update_steps(step_ids, attrs)

        if updated != step_ids.size
          log_warn "Only updated #{updated}/#{step_ids.size} steps to PENDING. Aborting retry."
          return false
        end

        log_info "Reset #{updated} steps to PENDING: #{step_ids.inspect}"
        true
      rescue Yantra::Errors::PersistenceError => e
        log_error "Failed to reset steps to PENDING: #{e.message}"
        false
      end

      def enqueue_retryable_steps(step_ids)
        log_info "Enqueuing reset steps via StepEnqueuer..."

        enqueued_ids = step_enqueuer.call(
          workflow_id: workflow_id,
          step_ids_to_attempt: step_ids
        )

        count = enqueued_ids.is_a?(Array) ? enqueued_ids.size : 0
        log_info "Enqueued #{count} step(s)"
        count
      rescue Yantra::Errors::EnqueueFailed => e
        log_error "Enqueue failed: #{e.message}, Failed IDs: #{e.failed_ids.inspect}"
        0
      rescue => e
        log_error "Unexpected error during enqueue: #{e.class} - #{e.message}"
        0
      end

      def log_info(msg);  logger&.info  { "[WorkflowRetryService] #{msg}" } end
      def log_warn(msg);  logger&.warn  { "[WorkflowRetryService] #{msg}" } end
      def log_error(msg); logger&.error { "[WorkflowRetryService] #{msg}" } end
      def log_debug(msg); logger&.debug { "[WorkflowRetryService] #{msg}" } end
    end
  end
end

