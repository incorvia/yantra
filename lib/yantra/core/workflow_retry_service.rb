# lib/yantra/core/workflow_retry_service.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require_relative 'step_enqueuer'
require_relative 'graph_utils' # <-- Require the new utility
require 'logger'
require 'set' # Needed by GraphUtils potentially

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

      # Attempts to retry failed steps and reset their cancelled dependents.
      # Returns the COUNT of originally failed steps successfully re-enqueued.
      def call
        failed_steps = find_failed_steps
        return 0 if failed_steps.empty?

        failed_step_ids = failed_steps.map(&:id)
        log_info "Found #{failed_step_ids.size} failed steps: #{failed_step_ids.inspect}"

        # Find descendants of the failed steps that are currently CANCELLED
        cancelled_descendant_ids = GraphUtils.find_descendants_matching_state(
          failed_step_ids,
          repository: @repository,
          include_starting_nodes: false, # We only care about descendants
          logger: @logger
        ) do |state_symbol|
          state_symbol == StateMachine::CANCELLED
        end
        log_info "Found #{cancelled_descendant_ids.size} cancelled descendants to reset: #{cancelled_descendant_ids.inspect}"

        # Combine failed steps and their cancelled descendants for reset
        ids_to_reset = (failed_step_ids + cancelled_descendant_ids).uniq
        log_debug "Total steps to reset to PENDING: #{ids_to_reset.size}"

        return 0 unless reset_steps_to_pending(ids_to_reset)

        # Only enqueue the steps that originally failed
        enqueue_retryable_steps(failed_step_ids)
      end

      private

      def validate_repository_interface!
         # Ensure repo has methods needed by this class and GraphUtils
        unless repository.respond_to?(:list_steps) &&
               repository.respond_to?(:bulk_update_steps) &&
               repository.respond_to?(:find_steps) && # Needed by GraphUtils
               repository.respond_to?(:get_dependent_ids_bulk) # Needed by GraphUtils
          # Note: get_dependent_ids is NOT directly needed by this service or GraphUtils helper
          raise ArgumentError, "Repository must implement list_steps, bulk_update_steps, find_steps, get_dependent_ids_bulk"
        end
      end

      def find_failed_steps
        repository.list_steps(workflow_id: workflow_id, status: :failed) || []
      rescue => e
        log_error "Failed to fetch failed steps for workflow #{workflow_id}: #{e.message}"
        []
      end

      # --- REMOVED: find_cancelled_descendants (now uses GraphUtils) ---
      def reset_steps_to_pending(step_ids)
        return true if step_ids.empty?

        attrs = {
          state:       StateMachine::PENDING.to_s,
          error:       nil,
          output:      nil,
          started_at:  nil,
          finished_at: nil,
          enqueued_at: nil,
          updated_at:  Time.current
        }

        updated_count = repository.bulk_update_steps(step_ids, attrs)
        if updated_count == 0
          log_warn "Reset to PENDING updated 0 rows, expected #{step_ids.size}."
          return false
        end

        if updated_count != step_ids.size
          log_warn "Reset to PENDING partially updated #{updated_count}/#{step_ids.size} rows."
        end

        log_info "Reset state to PENDING for #{updated_count} steps: #{step_ids.inspect}"
        true
      rescue Yantra::Errors::PersistenceError => e
        log_error "Failed to reset steps to PENDING: #{e.message}"
        false
      end

      def enqueue_retryable_steps(step_ids)
        return 0 if step_ids.empty? # Nothing to enqueue

        log_info "Enqueuing originally failed steps via StepEnqueuer..."
        begin
          enqueued_ids_array = step_enqueuer.call(
            workflow_id: workflow_id,
            step_ids_to_attempt: step_ids
          )
          count = enqueued_ids_array.is_a?(Array) ? enqueued_ids_array.size : 0
          log_info "Enqueued #{count} step(s)"
          count
        rescue Yantra::Errors::EnqueueFailed => e
          log_error "Enqueue failed during retry: #{e.message}, Failed IDs: #{e.failed_ids.inspect}"
          0
        rescue => e
          log_error "Unexpected error during enqueue during retry: #{e.class} - #{e.message}"
          0
        end
      end

      def log_info(msg);  @logger&.info  { "[WorkflowRetryService] #{msg}" } end
      def log_warn(msg);  @logger&.warn  { "[WorkflowRetryService] #{msg}" } end
      def log_error(msg); @logger&.error { "[WorkflowRetryService] #{msg}" } end
      def log_debug(msg); @logger&.debug { "[WorkflowRetryService] #{msg}" } end
    end
  end
end

