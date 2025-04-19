# lib/yantra/core/workflow_retry_service.rb

require_relative '../errors'
require_relative 'state_machine'

module Yantra
  module Core
    # Service class responsible for finding failed steps in a workflow
    # and re-enqueuing them via the configured worker adapter.
    class WorkflowRetryService
      attr_reader :workflow_id, :repository, :worker_adapter, :notifier

      def initialize(workflow_id:, repository:, worker_adapter:, notifier:)
        @workflow_id     = workflow_id
        @repository      = repository
        @worker_adapter  = worker_adapter
        @notifier        = notifier

        raise ArgumentError, "WorkflowRetryService requires a valid repository"      unless repository&.respond_to?(:get_workflow_steps)
        raise ArgumentError, "WorkflowRetryService requires a valid worker adapter" unless worker_adapter&.respond_to?(:enqueue)

        unless notifier&.respond_to?(:publish)
          Yantra.logger&.warn { "[WorkflowRetryService] Notifier is missing or invalid. Events will not be published." }
        end
      end

      # Finds failed steps and re-enqueues them.
      # Returns the number of steps successfully re-enqueued.
      def call
        failed_steps = find_failed_steps
        return 0 if failed_steps.empty?

        Yantra.logger&.info { "[WorkflowRetryService] Retrying #{failed_steps.size} failed steps: #{failed_steps.map(&:id)}" }

        failed_steps.count do |step|
          if reset_and_enqueue_step(step)
            true
          else
            Yantra.logger&.error { "[WorkflowRetryService] Failed to re-enqueue step #{step.id}" }
            false
          end
        end
      end

      private

      # Finds all failed steps for this workflow.
      def find_failed_steps
        repository.get_workflow_steps(workflow_id, status: :failed)
      rescue => e
        Yantra.logger&.error { "[WorkflowRetryService] Error fetching failed steps for #{workflow_id}: #{e.message}" }
        []
      end

      # Resets a failed step and re-enqueues it.
      def reset_and_enqueue_step(step)
        reset_attrs = {
          state:       StateMachine::ENQUEUED.to_s,
          error:       nil,
          output:      nil,
          started_at:  nil,
          finished_at: nil
        }

        updated = repository.update_step_attributes(step.id, reset_attrs, expected_old_state: :failed)
        unless updated
          Yantra.logger&.warn { "[WorkflowRetryService] Could not reset step #{step.id} (possibly already updated)." }
          return false
        end

        publish_step_enqueued_event(step)

        worker_adapter.enqueue(step.id, step.workflow_id, step.klass, step.queue)
        Yantra.logger&.info { "[WorkflowRetryService] Step #{step.id} re-enqueued successfully." }
        true
      rescue => e
        Yantra.logger&.error { "[WorkflowRetryService] Error re-enqueuing step #{step.id}: #{e.class} - #{e.message}" }
        false
      end

      # Publishes the 'step.enqueued' event if a notifier is present.
      def publish_step_enqueued_event(step)
        return unless notifier

        payload = {
          step_id:     step.id,
          workflow_id: step.workflow_id,
          klass:       step.klass
        }

        notifier.publish('yantra.step.enqueued', payload)
        Yantra.logger&.info { "[WorkflowRetryService] Published 'yantra.step.enqueued' for step #{step.id}." }
      rescue => e
        Yantra.logger&.error { "[WorkflowRetryService] Failed to publish event for step #{step.id}: #{e.message}" }
      end
    end
  end
end

