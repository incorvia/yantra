# lib/yantra/core/workflow_retry_service.rb
# Refactored using the 3-step approach (Reset Pending -> Enqueue -> Update Enqueued)

require_relative '../errors'
require_relative 'state_machine'

module Yantra
  module Core
    class WorkflowRetryService
      attr_reader :workflow_id, :repository, :worker_adapter, :notifier

      def initialize(workflow_id:, repository:, worker_adapter:, notifier:)
        @workflow_id    = workflow_id
        @repository     = repository
        @worker_adapter = worker_adapter
        @notifier       = notifier
        # Validation checks remain the same...
        raise ArgumentError, "WorkflowRetryService requires a valid repository" unless repository&.respond_to?(:get_workflow_steps) && repository&.respond_to?(:bulk_update_steps) && repository&.respond_to?(:find_steps)
        raise ArgumentError, "WorkflowRetryService requires a valid worker adapter" unless worker_adapter&.respond_to?(:enqueue)
      end

      # Finds failed steps, resets them to pending, attempts to re-enqueue,
      # updates successfully enqueued steps, and publishes a bulk event.
      # Returns the number of steps successfully re-enqueued.
      def call
        failed_steps = find_failed_steps # Fetches step objects
        return 0 if failed_steps.empty?

        Yantra.logger&.info { "[WorkflowRetryService] Attempting retry for #{failed_steps.size} failed steps: #{failed_steps.map(&:id)}" }

        failed_step_ids = failed_steps.map(&:id)

        # 1. Bulk Reset State to PENDING and clear fields
        reset_to_pending_attrs = {
          state:       StateMachine::PENDING, # <<< Reset to PENDING
          error:       nil,
          output:      nil,
          started_at:  nil,
          finished_at: nil,
          enqueued_at: nil # Clear enqueued_at as well
          # Note: Retries count is NOT reset here by default
        }

        begin
          update_success = repository.bulk_update_steps(failed_step_ids, reset_to_pending_attrs)
          unless update_success
            Yantra.logger&.warn { "[WorkflowRetryService] Bulk update to pending might have failed or reported issues for steps: #{failed_step_ids.inspect}" }
            # If we can't even reset them, probably shouldn't proceed
            return 0
          end
          Yantra.logger&.info { "[WorkflowRetryService] Bulk reset state to PENDING for steps: #{failed_step_ids.inspect}" }
        rescue Yantra::Errors::PersistenceError => e
          Yantra.logger&.error { "[WorkflowRetryService] Failed to bulk reset state to PENDING for steps #{failed_step_ids}: #{e.message}" }
          return 0
        end

        # 2. Attempt to enqueue each step (now marked as pending)
        # We need the step objects for enqueuing. We could re-fetch them now that they are pending,
        # or potentially use the ones fetched earlier if klass/queue/args didn't change.
        # Let's assume we need to re-fetch for consistency, using the bulk find_steps.
        begin
          # Fetch data for the steps we intended to retry
          steps_to_enqueue = repository.find_steps(failed_step_ids)
          steps_to_enqueue_map = steps_to_enqueue.index_by(&:id)
        rescue Yantra::Errors::PersistenceError => e
           Yantra.logger&.error { "[WorkflowRetryService] Failed to fetch steps marked pending for retry: #{e.message}" }
           return 0 # Cannot proceed without step data
        end

        successfully_enqueued_ids = []
        failed_step_ids.each do |step_id| # Iterate through the original list of IDs we intended to retry
          step = steps_to_enqueue_map[step_id]
          unless step
            Yantra.logger&.warn { "[WorkflowRetryService] Step #{step_id} was not found in PENDING state after reset, skipping enqueue."}
            next
          end

          begin
            worker_adapter.enqueue(step.id, step.workflow_id, step.klass, step.queue)
            successfully_enqueued_ids << step.id
            Yantra.logger&.info { "[WorkflowRetryService] Step #{step.id} re-enqueued successfully." }
          rescue StandardError => e
            Yantra.logger&.error { "[WorkflowRetryService] Failed to re-enqueue step #{step.id} via worker adapter: #{e.class} - #{e.message}" }
            # Step remains PENDING in DB, will be picked up by future retries if applicable, or manual intervention.
          end
        end

        # 3. Bulk Update State to ENQUEUED only for successfully enqueued steps
        if successfully_enqueued_ids.any?
          Yantra.logger&.info { "[WorkflowRetryService] Bulk updating state to ENQUEUED for steps: #{successfully_enqueued_ids.inspect}" }
          update_to_enqueued_attrs = {
            state: StateMachine::ENQUEUED,
            enqueued_at: Time.current
          }
          begin
            update_success = repository.bulk_update_steps(successfully_enqueued_ids, update_to_enqueued_attrs)
            unless update_success
              Yantra.logger&.warn { "[WorkflowRetryService] Bulk update to enqueued might have failed or reported issues for steps: #{successfully_enqueued_ids.inspect}" }
            end

            # 4. Publish ONE bulk event for successfully enqueued steps
            # Assuming publish_bulk_enqueued_event helper exists or inline logic here
            publish_bulk_enqueued_event(workflow_id, successfully_enqueued_ids)

          rescue Yantra::Errors::PersistenceError => e
            Yantra.logger&.error { "[WorkflowRetryService] Failed to bulk update state to ENQUEUED for steps #{successfully_enqueued_ids}: #{e.message}" }
            # Note: Enqueue happened, but state update failed. Requires monitoring.
          end
        end

        # 5. Return the count of successfully enqueued steps
        successfully_enqueued_ids.count
      end

      private

      # Finds all failed steps for this workflow (Implementation remains the same)
      def find_failed_steps
        repository.get_workflow_steps(workflow_id, status: :failed)
      rescue => e
        Yantra.logger&.error { "[WorkflowRetryService] Error fetching failed steps for #{workflow_id}: #{e.message}" }
        []
      end

      # Add this helper (or ensure accessible from Orchestrator/shared location)
      def publish_bulk_enqueued_event(workflow_id, successfully_enqueued_ids)
        return if successfully_enqueued_ids.nil? || successfully_enqueued_ids.empty?
        payload = { workflow_id: workflow_id, enqueued_ids: successfully_enqueued_ids }
        begin
          notifier.publish('yantra.step.bulk_enqueued', payload)
          Yantra.logger&.info { "[WorkflowRetryService] Published 'yantra.steps.bulk_enqueued' for #{successfully_enqueued_ids.count} steps." }
        rescue StandardError => e
          Yantra.logger&.error { "[WorkflowRetryService] Failed to publish bulk enqueued event: #{e.message}" }
        end
      end
    end
  end
end
