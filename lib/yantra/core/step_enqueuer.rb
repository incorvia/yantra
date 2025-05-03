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

        steps = fetch_candidate_steps(step_ids_to_attempt)
        return [] if steps.empty?

        now = Time.current
        successfully_enqueued, failed_enqueued = enqueue_steps(workflow_id, steps, now)

        update_successful_enqueues(successfully_enqueued, now) if successfully_enqueued.any?
        raise_enqueue_error(failed_enqueued) if failed_enqueued.any?

        publish_success_event(workflow_id, successfully_enqueued, now) if failed_enqueued.empty?
        successfully_enqueued.map(&:id)
      end

      private

      def fetch_candidate_steps(step_ids)
        repository.find_steps(step_ids).select do |step|
          StateMachine.is_enqueue_candidate_state?(step.state, step.enqueued_at)
        end
      rescue Yantra::Errors::PersistenceError => e
        log_error "Failed to fetch steps: #{e.message}"
        []
      end

      def enqueue_steps(workflow_id, steps, now)
        prepare_enqueue_metadata(steps, now)

        begin
          count = repository.bulk_upsert_steps(@initial_upserts)
          log_info "Phase 1: Upserted #{count} steps to AWAITING_EXECUTION"
        rescue Yantra::Errors::PersistenceError => e
          log_error "Failed upserting to AWAITING_EXECUTION: #{e.message}"
          raise
        end

        success = []
        failed  = []

        steps.each do |step|
          begin
            result = enqueue_step(step, workflow_id)
            result ? success << step : failed << step
          rescue => e
            log_warn "Failed enqueueing step #{step.id}: #{e.message}"
            failed << step
          end
        end

        [success, failed]
      end

      def prepare_enqueue_metadata(steps, now)
        @initial_upserts = steps.map do |step|
          delay = step.delay_seconds
          earliest = delay && delay > 0 ? now + delay.seconds : nil

          {
            id: step.id,
            state: StateMachine::AWAITING_EXECUTION.to_s,
            earliest_execution_time: earliest,
            updated_at: now,
            workflow_id: step.workflow_id,
            klass: step.klass.to_s,
            max_attempts: step.max_attempts,
            retries: step.retries,
            created_at: step.created_at
          }
        end
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

      def update_successful_enqueues(steps, time)
        attrs = { enqueued_at: time, updated_at: time }
        ids = steps.map(&:id)

        repository.bulk_update_steps(ids, attrs)
        log_info "Phase 3: Set enqueued_at for #{ids.size} steps"
      rescue Yantra::Errors::PersistenceError => e
        log_error "Failed updating enqueued_at: #{e.message}"
        raise
      end

      def raise_enqueue_error(failed_steps)
        ids = failed_steps.map(&:id)
        msg = "Failed to enqueue #{ids.size} steps: #{ids.inspect}"
        log_error msg
        raise Yantra::Errors::EnqueueFailed.new(msg, failed_ids: ids)
      end

      def publish_success_event(workflow_id, steps, time)
        return unless notifier.respond_to?(:publish) && steps.any?

        payload = {
          workflow_id: workflow_id,
          enqueued_ids: steps.map(&:id),
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

