# lib/yantra/core/workflow_retry_service.rb
# frozen_string_literal: true

require 'yantra/core/state_machine'
require 'yantra/core/step_enqueuer'
require 'yantra/errors'
require 'logger' # Ensure logger is available

module Yantra
  module Core
    class WorkflowRetryService
      attr_reader :workflow_id, :repository, :step_enqueuer, :logger

      def initialize(workflow_id:, repository:, worker_adapter:, notifier:, logger: Yantra.logger)
        @workflow_id = workflow_id
        @repository  = repository
        @logger      = logger || Logger.new(IO::NULL) # Initialize logger
        @step_enqueuer = StepEnqueuer.new(
          repository: repository,
          worker_adapter: worker_adapter,
          notifier: notifier,
          logger: @logger # Pass logger to enqueuer
        )

        unless repository&.respond_to?(:list_steps) && repository&.respond_to?(:bulk_update_steps)
          raise ArgumentError, "WorkflowRetryService requires a repository implementing #list_steps and #bulk_update_steps"
        end
      end

      # Attempts to retry FAILED steps in the workflow.
      # Returns the COUNT of steps successfully re-enqueued.
      def call
        # --- MODIFIED: Only find FAILED steps ---
        failed_steps = find_failed_steps
        return 0 if failed_steps.empty?
        # --- END MODIFICATION ---

        retryable_step_ids = failed_steps.map(&:id)
        log_info "Attempting retry for #{retryable_step_ids.size} failed steps: #{retryable_step_ids.inspect}"

        # Reset state and clear errors/output for failed steps
        reset_attrs = {
          state:       StateMachine::PENDING.to_s, # Reset to PENDING
          error:       nil,
          output:      nil,
          started_at:  nil,
          finished_at: nil,
          enqueued_at: nil, # Clear enqueue timestamp
          updated_at:  Time.current # Ensure updated_at is set
        }

        begin
          updated_count = repository.bulk_update_steps(retryable_step_ids, reset_attrs)
          if updated_count != retryable_step_ids.size
            log_warn "Bulk update to PENDING reported updating #{updated_count} rows, expected #{retryable_step_ids.size} for steps: #{retryable_step_ids.inspect}"
          end
          log_info "Reset state to PENDING for steps: #{retryable_step_ids.inspect}"
        rescue Yantra::Errors::PersistenceError => e
          log_error "Failed to reset steps to PENDING: #{e.message}"
          return 0 # Return 0 on persistence error during reset
        end

        # Now, attempt to enqueue the steps (which are now PENDING)
        begin
          log_info "Enqueuing steps through StepEnqueuer..."
          # StepEnqueuer#call returns the count of successfully enqueued steps
          enqueued_count = step_enqueuer.call(workflow_id: workflow_id, step_ids_to_attempt: retryable_step_ids)
          log_info "Successfully enqueued #{enqueued_count} steps."
          enqueued_count # Return the count
        rescue Yantra::Errors::EnqueueFailed => e
          log_error "StepEnqueuer call failed during retry: #{e.class} - #{e.message}. Failed IDs: #{e.failed_ids.inspect}"
          0 # Return 0 if enqueue fails
        rescue StandardError => e # Catch other potential errors from StepEnqueuer
          log_error "Unexpected error during StepEnqueuer call in retry: #{e.class} - #{e.message}"
          0 # Return 0 on other errors
        end
      end

      private

      # Finds only steps in the FAILED state for the workflow.
      def find_failed_steps
        repository.list_steps(workflow_id: workflow_id, status: :failed) || []
      rescue => e
        log_error "Error fetching failed steps for #{workflow_id}: #{e.message}"
        []
      end

      # Logging helpers
      def log_info(msg);  @logger&.info  { "[WorkflowRetryService] #{msg}" }; end
      def log_warn(msg);  @logger&.warn  { "[WorkflowRetryService] #{msg}" }; end
      def log_error(msg); @logger&.error { "[WorkflowRetryService] #{msg}" }; end
      def log_debug(msg); @logger&.debug { "[WorkflowRetryService] #{msg}" }; end

    end # class WorkflowRetryService
  end # module Core
end # module Yantra

