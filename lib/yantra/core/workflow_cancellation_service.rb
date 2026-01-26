# lib/yantra/core/workflow_cancellation_service.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require 'logger'

module Yantra
  module Core
    # Service responsible for cancelling a workflow and its associated steps.
    # Publishes cancellation events via the notifier.
    class WorkflowCancellationService
      attr_reader :workflow_id, :repository, :notifier, :logger

      def initialize(workflow_id:, repository:, notifier:)
        @workflow_id = workflow_id
        @repository = repository
        @notifier = notifier
        @logger = Yantra.logger || Logger.new(IO::NULL)
      end

      # Cancels the workflow and its cancellable steps.
      #
      # @return [Boolean] true if cancellation succeeded, false otherwise
      def call
        workflow = repository.find_workflow(workflow_id)

        unless workflow
          log_warn "Workflow #{workflow_id} not found."
          return false
        end

        current_state = workflow.state.to_sym

        if finished_state?(current_state)
          log_warn "Workflow already in finished state (#{current_state}). Cannot cancel."
          return false
        end

        finished_at_time = Time.current

        unless update_workflow_to_cancelled(finished_at_time)
          return false
        end

        log_info "Workflow #{workflow_id} marked as cancelled in repository."

        publish_workflow_cancelled_event(workflow, finished_at_time)
        cancel_workflow_steps(finished_at_time)

        true
      end

      private

      def finished_state?(state)
        [StateMachine::SUCCEEDED, StateMachine::CANCELLED, StateMachine::FAILED].include?(state)
      end

      def update_workflow_to_cancelled(finished_at_time)
        update_success = [StateMachine::PENDING, StateMachine::RUNNING].any? do |prev_state|
          repository.update_workflow_attributes(
            workflow_id,
            { state: StateMachine::CANCELLED.to_s, finished_at: finished_at_time },
            expected_old_state: prev_state
          )
        end

        unless update_success
          latest_state = repository.find_workflow(workflow_id)&.state || 'unknown'
          log_warn "Failed to update workflow state to cancelled. Current state: #{latest_state}."
          return false
        end

        true
      end

      def publish_workflow_cancelled_event(workflow, finished_at_time)
        payload = {
          workflow_id: workflow_id,
          klass: workflow.klass,
          state: StateMachine::CANCELLED,
          finished_at: finished_at_time
        }
        notifier&.publish('yantra.workflow.cancelled', payload)
        log_info "Published yantra.workflow.cancelled event."
      rescue => e
        log_error "Failed to publish workflow cancelled event: #{e.message}"
      end

      def cancel_workflow_steps(finished_at_time)
        all_steps = repository.list_steps(workflow_id: workflow_id) || []
        steps_to_cancel = all_steps.select do |step|
          StateMachine.is_cancellable_state?(step.state.to_sym)
        end

        step_ids = steps_to_cancel.map(&:id)
        log_debug "Steps eligible for cancellation: #{step_ids.inspect}"

        if step_ids.any?
          cancel_steps(step_ids, finished_at_time)
        else
          log_info "No steps found in cancellable states for workflow #{workflow_id}."
        end
      rescue Yantra::Errors::PersistenceError => e
        log_error "Persistence error during step cancellation: #{e.message}"
      rescue => e
        log_error "Unexpected error during step cancellation processing: #{e.class} - #{e.message}"
      end

      def cancel_steps(step_ids, finished_at_time)
        log_info "Attempting to cancel #{step_ids.size} steps in repository."

        cancel_attrs = {
          state: StateMachine::CANCELLED.to_s,
          finished_at: finished_at_time,
          updated_at: finished_at_time
        }
        cancelled_count = repository.bulk_update_steps(step_ids, cancel_attrs)
        log_info "Repository confirmed cancellation of #{cancelled_count} steps."

        publish_step_cancelled_events(step_ids)
      end

      def publish_step_cancelled_events(step_ids)
        step_ids.each do |step_id|
          notifier&.publish('yantra.step.cancelled', { step_id: step_id, workflow_id: workflow_id })
        rescue => e
          log_error "Failed to publish step.cancelled event for step #{step_id}: #{e.message}"
        end
      end

      def log_info(msg)
        @logger&.info { "[WorkflowCancellationService] #{msg}" }
      end

      def log_warn(msg)
        @logger&.warn { "[WorkflowCancellationService] #{msg}" }
      end

      def log_error(msg)
        @logger&.error { "[WorkflowCancellationService] #{msg}" }
      end

      def log_debug(msg)
        @logger&.debug { "[WorkflowCancellationService] #{msg}" }
      end
    end
  end
end
