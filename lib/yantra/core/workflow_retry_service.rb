# lib/yantra/core/workflow_retry_service.rb
# Refactored to use StepEnqueuer

require_relative '../errors'
require_relative 'state_machine'
require_relative 'step_enqueuer' # <<< Add require for the new service

module Yantra
  module Core
    class WorkflowRetryService
      # No longer need direct access to worker_adapter or notifier here
      attr_reader :workflow_id, :repository, :step_enqueuer

      # Updated initializer takes the enqueuing service
      def initialize(workflow_id:, repository:, worker_adapter:, notifier:)
        @workflow_id = workflow_id
        @repository  = repository
        # Instantiate the enqueuing service, passing dependencies
        @step_enqueuer = StepEnqueuer.new(
          repository: repository,
          worker_adapter: worker_adapter,
          notifier: notifier
        )

        # Validation checks (repository still needs bulk_update_steps)
        unless repository&.respond_to?(:get_workflow_steps) && repository&.respond_to?(:bulk_update_steps)
          raise ArgumentError, "WorkflowRetryService requires a repository implementing #get_workflow_steps and #bulk_update_steps"
        end
        # No longer need to validate worker_adapter here directly
      end

      # Finds failed steps, resets them to pending, and delegates enqueuing
      # to StepEnqueuer.
      # Returns the number of steps successfully re-enqueued by the service.
      def call
        failed_steps = find_failed_steps # Still need step objects to get IDs
        return 0 if failed_steps.empty?

        failed_step_ids = failed_steps.map(&:id)
        Yantra.logger&.info { "[WorkflowRetryService] Attempting retry for #{failed_steps.size} failed steps: #{failed_step_ids}" }

        # 1. Bulk Reset State to PENDING and clear fields
        reset_to_pending_attrs = {
          state:       StateMachine::PENDING,
          error:       nil,
          output:      nil,
          started_at:  nil,
          finished_at: nil,
          enqueued_at: nil
        }

        begin
          update_success = repository.bulk_update_steps(failed_step_ids, reset_to_pending_attrs)
          unless update_success
            Yantra.logger&.warn { "[WorkflowRetryService] Bulk update to pending might have failed or reported issues for steps: #{failed_step_ids.inspect}" }
            return 0
          end
          Yantra.logger&.info { "[WorkflowRetryService] Bulk reset state to PENDING for steps: #{failed_step_ids.inspect}" }
        rescue Yantra::Errors::PersistenceError => e
          Yantra.logger&.error { "[WorkflowRetryService] Failed to bulk reset state to PENDING for steps #{failed_step_ids}: #{e.message}" }
          return 0
        end

        # --- 2. Delegate Enqueuing to the Service ---
        # The StepEnqueuer will handle:
        # - Fetching step data (find_steps)
        # - Looping and attempting enqueue via worker_adapter
        # - Collecting successful IDs
        # - Bulk updating successful IDs to ENQUEUED
        # - Publishing the bulk event
        begin
          Yantra.logger&.info { "[WorkflowRetryService] Delegating enqueue attempt to StepEnqueuer for steps: #{failed_step_ids.inspect}" }
          # Pass the IDs of the steps that are now pending and ready for retry
          reenqueued_count = @step_enqueuer.call(
            workflow_id: workflow_id,
            step_ids_to_attempt: failed_step_ids
          )
          Yantra.logger&.info { "[WorkflowRetryService] StepEnqueuer reported #{reenqueued_count} steps successfully enqueued."}
          return reenqueued_count # Return the count reported by the service
        rescue StandardError => e
           # Catch errors from the enqueuing service itself
           Yantra.logger&.error { "[WorkflowRetryService] Error during StepEnqueuer call: #{e.class} - #{e.message}" }
           return 0 # Indicate failure if the service call failed
        end
        # --- End Delegation ---

      end # end call

      private

      # Finds all failed steps for this workflow (Implementation remains the same)
      def find_failed_steps
        repository.get_workflow_steps(workflow_id, status: :failed)
      rescue => e
        Yantra.logger&.error { "[WorkflowRetryService] Error fetching failed steps for #{workflow_id}: #{e.message}" }
        []
      end
    end
  end
end
