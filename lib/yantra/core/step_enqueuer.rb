# lib/yantra/core/step_enqueuer.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require 'active_support/core_ext/numeric/time'
require 'logger'

module Yantra
  module Core
    class StepEnqueuer
      attr_reader :repository, :worker_adapter, :notifier, :logger

      def initialize(repository:, worker_adapter:, notifier:, logger: Yantra.logger)
        @repository     = repository or raise ArgumentError, "StepEnqueuer requires a repository"
        @worker_adapter = worker_adapter or raise ArgumentError, "StepEnqueuer requires a worker_adapter"
        @notifier       = notifier or raise ArgumentError, "StepEnqueuer requires a notifier"
        @logger         = logger || Logger.new(IO::NULL)

        unless defined?(StateMachine::AWAITING_EXECUTION)
          raise Yantra::Errors::ConfigurationError, "StateMachine::AWAITING_EXECUTION is not defined"
        end
      end

      def call(workflow_id:, step_ids_to_attempt:)
        return [] if step_ids_to_attempt.blank?
        log_info "Enqueueing steps: #{step_ids_to_attempt.inspect} in workflow #{workflow_id}"

        now = Time.current

        transitioned_ids = begin
          transition_attrs = {
            state: StateMachine::AWAITING_EXECUTION.to_s,
            updated_at: now
          }
          repository.bulk_transition_steps(
            step_ids_to_attempt,
            transition_attrs,
            expected_old_state: StateMachine::PENDING
          )
        rescue Yantra::Errors::PersistenceError => e
          log_error "Failed transitioning steps to AWAITING_EXECUTION: #{e.message}"
          raise
        end

        successfully_enqueued, failed_enqueued = enqueue_steps(workflow_id, transitioned_ids)

        update_successful_enqueues(successfully_enqueued, now) if successfully_enqueued.any?
        raise_enqueue_error(failed_enqueued) if failed_enqueued.any?

        publish_success_event(workflow_id, successfully_enqueued, now) if failed_enqueued.empty?
        successfully_enqueued
      end

      private

      def enqueue_steps(workflow_id, step_ids)
        return [[], []] if step_ids.blank?

        steps = repository.find_steps(step_ids)

        success = []
        failed  = []

        steps.each do |step|
          begin
            result = enqueue_step(step, workflow_id)
            result ? success << step.id : failed << step.id
          rescue => e
            log_warn "Failed enqueueing step #{step.id}: #{e.message}"
            failed << step.id
          end
        end

        [success, failed]
      end

      def enqueue_step(step, workflow_id)
        delay = step.delay_seconds
        queue = step.queue

        if delay && delay > 0
          worker_adapter.enqueue_in(delay, step.id, workflow_id, step.klass, queue)
        else
          worker_adapter.enqueue(step.id, workflow_id, step.klass, queue)
        end
      end

      def update_successful_enqueues(step_ids, time)
        attrs = { enqueued_at: time, updated_at: time }

        repository.bulk_update_steps(step_ids, attrs)
        log_info "Phase 3: Set enqueued_at for #{step_ids.size} steps"
      rescue Yantra::Errors::PersistenceError => e
        log_error "Failed updating enqueued_at: #{e.message}"
        raise
      end

      def raise_enqueue_error(failed_step_ids)
        msg = "Failed to enqueue #{failed_step_ids.size} steps: #{failed_step_ids.inspect}"
        log_error msg
        raise Yantra::Errors::EnqueueFailed.new(msg, failed_ids: failed_step_ids)
      end

      def publish_success_event(workflow_id, step_ids, time)
        return unless notifier.respond_to?(:publish) && step_ids.any?

        payload = {
          workflow_id: workflow_id,
          enqueued_ids: step_ids,
          enqueued_at: time
        }

        notifier.publish('yantra.step.bulk_enqueued', payload)
      rescue => e
        log_error "Failed publishing event: #{e.message}"
      end

      def log_info(msg);  logger&.info  { "[StepEnqueuer] #{msg}" } end
      def log_warn(msg);  logger&.warn  { "[StepEnqueuer] #{msg}" } end
      def log_error(msg); logger&.error { "[StepEnqueuer] #{msg}" } end
    end
  end
end

