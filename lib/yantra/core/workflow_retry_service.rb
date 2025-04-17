# lib/yantra/core/workflow_retry_service.rb

require_relative '../errors'
require_relative 'state_machine'

module Yantra
  module Core
    # Service class responsible for finding failed steps in a workflow
    # and re-enqueuing them.
    class WorkflowRetryService
      attr_reader :workflow_id, :repository, :worker_adapter

      # @param workflow_id [String] The ID of the workflow to process.
      # @param repository [#get_workflow_steps, #update_step_attributes, #find_step] The persistence adapter.
      # @param worker_adapter [#enqueue] The worker adapter.
      def initialize(workflow_id:, repository:, worker_adapter:)
        @workflow_id = workflow_id
        @repository = repository
        @worker_adapter = worker_adapter
      end

      # Finds failed steps and attempts to re-enqueue them.
      # @return [Integer] The number of steps successfully re-enqueued.
      def call
        failed_steps = find_failed_steps
        if failed_steps.empty?
          puts "INFO: [WorkflowRetryService] No steps found in 'failed' state for workflow #{workflow_id}."
          return 0
        end

        puts "INFO: [WorkflowRetryService] Found #{failed_steps.size} failed steps to retry: #{failed_steps.map(&:id)}."
        reenqueue_steps(failed_steps)
      end

      private

      # Fetches steps in the 'failed' state for the workflow.
      # @return [Array<Object>] Array of failed step objects from the repository.
      def find_failed_steps
        repository.get_workflow_steps(workflow_id, status: :failed)
      rescue StandardError => e
        puts "ERROR: [WorkflowRetryService] Failed to query failed steps for workflow #{workflow_id}: #{e.message}"
        [] # Return empty array on error to prevent further processing
      end

      # Iterates through failed steps and attempts to re-enqueue each one.
      # @param steps [Array<Object>] Array of failed step objects.
      # @return [Integer] Count of successfully re-enqueued steps.
      def reenqueue_steps(steps)
        reenqueued_count = 0
        steps.each do |step|
          if reenqueue_step(step)
            reenqueued_count += 1
          end
        end
        reenqueued_count
      end

      # Handles the state update and enqueuing for a single step.
      # @param step [Object] A failed step object from the repository.
      # @return [Boolean] true if successfully re-enqueued, false otherwise.
      def reenqueue_step(step)
        # Validate transition (failed -> enqueued)
        StateMachine.validate_transition!(:failed, :enqueued) # Let it raise if invalid

        # Update step state back to enqueued (atomically)
        step_update_success = repository.update_step_attributes(
          step.id,
          {
            state: Core::StateMachine::ENQUEUED.to_s,
            enqueued_at: Time.current,
            finished_at: nil # Clear finished_at timestamp
            # error: nil # Optionally clear error
          },
          expected_old_state: :failed
        )

        if step_update_success
          # Enqueue the step via the worker adapter
          queue_to_use = step.queue || 'default'
          worker_adapter.enqueue(step.id, step.workflow_id, step.klass, queue_to_use)
          puts "INFO: [WorkflowRetryService] step #{step.id} re-enqueued."
          # TODO: Emit step.retrying event?
          true
        else
          latest_step_state = repository.find_step(step.id)&.state || 'unknown'
          puts "WARN: [WorkflowRetryService] Failed to update step #{step.id} state to enqueued (maybe state changed concurrently? Current state: #{latest_step_state}). Skipping enqueue."
          false
        end

      rescue Yantra::Errors::InvalidStateTransition => e
        puts "ERROR: [WorkflowRetryService] Invalid state transition for step #{step.id}. #{e.message}"
        false
      rescue Yantra::Errors::WorkerError => e
        puts "ERROR: [WorkflowRetryService] Worker error enqueuing step #{step.id}. #{e.message}"
        false # Failed to enqueue this specific step
      rescue StandardError => e
        puts "ERROR: [WorkflowRetryService] Unexpected error retrying step #{step.id}: #{e.class} - #{e.message}"
        false # Failed to enqueue this specific step
      end

    end
  end
end

