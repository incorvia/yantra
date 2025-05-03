# lib/yantra/core/workflow_retry_service.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require_relative 'step_enqueuer'

module Yantra
  module Core
    class WorkflowRetryService
      attr_reader :workflow_id, :repository, :step_enqueuer

      def initialize(workflow_id:, repository:, worker_adapter:, notifier:)
        @workflow_id = workflow_id
        @repository  = repository
        @step_enqueuer = StepEnqueuer.new(
          repository: repository,
          worker_adapter: worker_adapter,
          notifier: notifier
        )

        unless repository&.respond_to?(:list_steps) && repository&.respond_to?(:bulk_update_steps)
          raise ArgumentError, "WorkflowRetryService requires a repository implementing #list_steps and #bulk_update_steps"
        end
      end

      def call
        failed_steps = find_failed_steps
        return 0 if failed_steps.empty?

        failed_step_ids = failed_steps.map(&:id)
        log_info "Attempting retry for #{failed_step_ids.size} failed steps: #{failed_step_ids.inspect}"

        reset_attrs = {
          state:       StateMachine::PENDING,
          error:       nil,
          output:      nil,
          started_at:  nil,
          finished_at: nil,
          enqueued_at: nil
        }

        begin
          updated = repository.bulk_update_steps(failed_step_ids, reset_attrs)
          unless updated
            log_warn "Bulk update to PENDING reported issues for steps: #{failed_step_ids.inspect}"
            return 0
          end
          log_info "Reset state to PENDING for steps: #{failed_step_ids.inspect}"
        rescue Yantra::Errors::PersistenceError => e
          log_error "Failed to reset steps to PENDING: #{e.message}"
          return 0
        end

        begin
          log_info "Enqueuing steps through StepEnqueuer..."
          count = step_enqueuer.call(workflow_id: workflow_id, step_ids_to_attempt: failed_step_ids)
          log_info "Successfully enqueued #{count} steps."
          count
        rescue StandardError => e
          log_error "StepEnqueuer call failed: #{e.class} - #{e.message}"
          0
        end
      end

      private

      def find_failed_steps
        repository.list_steps(workflow_id: workflow_id, status: :failed)
      rescue => e
        log_error "Error fetching failed steps for #{workflow_id}: #{e.message}"
        []
      end

      def log_info(msg);  Yantra.logger&.info  { "[WorkflowRetryService] #{msg}" }; end
      def log_warn(msg);  Yantra.logger&.warn  { "[WorkflowRetryService] #{msg}" }; end
      def log_error(msg); Yantra.logger&.error { "[WorkflowRetryService] #{msg}" }; end
    end
  end
end

