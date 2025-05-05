# lib/yantra/core/workflow_retry_service.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require_relative 'step_enqueuer'
require 'logger' # Ensure logger is available

module Yantra
  module Core
    class WorkflowRetryService
      attr_reader :workflow_id, :repository, :step_enqueuer, :logger

      # Initialize with dependencies, using global logger by default
      def initialize(workflow_id:, repository:, worker_adapter:, notifier:)
        @workflow_id = workflow_id
        @repository  = repository
        @logger      = Yantra.logger || Logger.new(IO::NULL) # Use global or null logger
        @step_enqueuer = StepEnqueuer.new(
          repository: repository,
          worker_adapter: worker_adapter,
          notifier: notifier,
          logger: @logger # Pass the initialized logger
        )

        # Interface checks
        unless repository&.respond_to?(:list_steps) && repository&.respond_to?(:bulk_update_steps)
          raise ArgumentError, "WorkflowRetryService requires a repository implementing #list_steps and #bulk_update_steps"
        end
      end

      # Attempts to retry FAILED steps in the workflow.
      # Returns the COUNT of steps successfully re-enqueued.
      def call
        failed_steps = find_failed_steps
        return 0 if failed_steps.empty?

        retryable_step_ids = failed_steps.map(&:id)
        log_info "Attempting retry for #{retryable_step_ids.size} failed steps: #{retryable_step_ids.inspect}"

        reset_attrs = {
          state:       StateMachine::PENDING.to_s, # Reset to PENDING
          error:       nil,
          output:      nil,
          started_at:  nil,
          finished_at: nil,
          enqueued_at: nil, # Clear enqueue timestamp
          updated_at:  Time.current # Ensure updated_at is set
        }

        updated_count = 0 # Initialize count
        begin
          updated_count = repository.bulk_update_steps(retryable_step_ids, reset_attrs)
          # Check if update succeeded before proceeding
          # If the count doesn't match, log a warning and stop.
          if updated_count != retryable_step_ids.size
            log_warn "Bulk update to PENDING reported updating #{updated_count} rows, expected #{retryable_step_ids.size} for steps: #{retryable_step_ids.inspect}. Aborting retry enqueue."
            return 0 # Stop if reset didn't affect expected rows
          end
          log_info "Reset state to PENDING for steps: #{retryable_step_ids.inspect}"
        rescue Yantra::Errors::PersistenceError => e
          log_error "Failed to reset steps to PENDING: #{e.message}"
          return 0 # Return 0 on persistence error during reset
        end

        # Now, attempt to enqueue the steps (which are now PENDING)
        begin
          log_info "Enqueuing steps through StepEnqueuer..."
          # StepEnqueuer#call returns an ARRAY of successfully enqueued IDs
          enqueued_ids_array = step_enqueuer.call(workflow_id: workflow_id, step_ids_to_attempt: retryable_step_ids)
          # This service returns the COUNT
          enqueued_count = enqueued_ids_array.is_a?(Array) ? enqueued_ids_array.size : 0
          log_info "Successfully enqueued #{enqueued_count} steps."
          return enqueued_count # Return the count
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

