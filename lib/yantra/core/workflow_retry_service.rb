# lib/yantra/core/workflow_retry_service.rb

require_relative '../errors'
require_relative 'state_machine'

module Yantra
  module Core
    # Service class responsible for finding failed steps in a workflow
    # and re-enqueuing them via the configured worker adapter.
    class WorkflowRetryService
      attr_reader :workflow_id, :repository, :worker_adapter, :notifier # Added notifier

      # --- UPDATED: Inject notifier ---
      def initialize(workflow_id:, repository:, worker_adapter:, notifier:)
        @workflow_id = workflow_id
        @repository = repository
        @worker_adapter = worker_adapter
        @notifier = notifier # Store injected notifier

        # Validation (optional but recommended)
        unless @repository && @repository.respond_to?(:get_workflow_steps)
          raise ArgumentError, "WorkflowRetryService requires a valid repository."
        end
        unless @worker_adapter && @worker_adapter.respond_to?(:enqueue)
          raise ArgumentError, "WorkflowRetryService requires a valid worker adapter."
        end
        unless @notifier && @notifier.respond_to?(:publish)
           # Allow nil notifier for flexibility, but log if missing when needed?
           # Or raise ArgumentError, "WorkflowRetryService requires a valid notifier."
           Yantra.logger.warn { "[WorkflowRetryService] Notifier not provided or invalid." } if Yantra.logger && !@notifier
        end
      end

      # Finds failed steps and re-enqueues them.
      # Returns the number of steps successfully re-enqueued.
      def call
        failed_steps = find_failed_steps
        return 0 if failed_steps.empty?

        Yantra.logger.info { "[WorkflowRetryService] Found #{failed_steps.size} failed steps to retry: #{failed_steps.map(&:id)}." } if Yantra.logger

        reenqueued_count = 0
        failed_steps.each do |step|
          if reset_and_enqueue_step(step)
            reenqueued_count += 1
          else
            # Log if a step failed to re-enqueue
            Yantra.logger.error { "[WorkflowRetryService] Failed to reset and re-enqueue step #{step.id}." } if Yantra.logger
          end
        end
        reenqueued_count
      end

      private

      # Finds steps in the 'failed' state for the workflow.
      def find_failed_steps
        repository.get_workflow_steps(workflow_id, status: :failed)
      rescue => e
        Yantra.logger.error { "[WorkflowRetryService] Error finding failed steps for workflow #{workflow_id}: #{e.message}" } if Yantra.logger
        [] # Return empty array on error
      end

      # Resets a step's state to 'enqueued' and enqueues it via the worker adapter.
      # Publishes the 'yantra.step.enqueued' event.
      # Returns true on success, false on failure.
      def reset_and_enqueue_step(step)
        # Reset state back to enqueued, clear error/output/timestamps
        reset_attrs = {
          state: StateMachine::ENQUEUED.to_s,
          error: nil,
          output: nil,
          started_at: nil,
          finished_at: nil
          # Do not reset retries - let the StepJob/RetryHandler manage that on next run
        }
        update_success = repository.update_step_attributes(step.id, reset_attrs, expected_old_state: :failed)

        unless update_success
          Yantra.logger.warn { "[WorkflowRetryService] Failed to reset state for failed step #{step.id} (maybe state changed?)." } if Yantra.logger
          return false
        end

        # --- ADDED: Publish step.enqueued event ---
        begin
          if @notifier # Check if notifier was provided
            payload = { step_id: step.id, workflow_id: step.workflow_id, klass: step.klass }
            @notifier.publish('yantra.step.enqueued', payload)
            Yantra.logger.info { "[WorkflowRetryService] Published yantra.step.enqueued event for retried step #{step.id}." } if Yantra.logger
          end
        rescue => e
          Yantra.logger.error { "[WorkflowRetryService] Failed to publish yantra.step.enqueued event for step #{step.id}: #{e.message}" } if Yantra.logger
          # Continue with enqueuing even if event publishing fails
        end
        # --- END ADDED SECTION ---

        # Enqueue the job via the worker adapter
        worker_adapter.enqueue(step.id, step.workflow_id, step.klass, step.queue)
        Yantra.logger.info { "[WorkflowRetryService] step #{step.id} re-enqueued." } if Yantra.logger
        true # Indicate success

      rescue => e
        # Catch errors during the enqueue process for a specific step
        Yantra.logger.error { "[WorkflowRetryService] Error re-enqueuing step #{step.id}: #{e.class} - #{e.message}" } if Yantra.logger
        false # Indicate failure for this step
      end

    end
  end
end

