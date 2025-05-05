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

        validate_repository_interface!
        validate_state_machine_constants!
      end

      def call(workflow_id:, step_ids_to_attempt:)
        return [] if step_ids_to_attempt.blank?
        log_info "Attempting to enqueue steps: #{step_ids_to_attempt.inspect} in workflow #{workflow_id}"

        now = Time.current

        initial_steps = repository.find_steps(step_ids_to_attempt) || []
        pending_ids = initial_steps.select { |s| s.state.to_sym == StateMachine::PENDING }.map(&:id)

        non_pending_ids = step_ids_to_attempt - pending_ids
        log_warn "Skipping non-PENDING steps: #{non_pending_ids.inspect}" if non_pending_ids.any?
        return [] if pending_ids.empty?

        transitioned_ids = transition_to_scheduling(pending_ids, now)
        return [] if transitioned_ids.empty?

        ids_to_process = transitioned_ids
        success_ids, failed_ids = attempt_enqueue_for_scheduled(workflow_id, ids_to_process)

        if failed_ids.any?
          rollback_failed_enqueue_steps(failed_ids, now)
        end

        update_enqueued_state(success_ids, now) if success_ids.any?
        publish_success_event(workflow_id, success_ids, now)
        raise_enqueue_error(failed_ids) unless failed_ids.empty?
        success_ids
      end

      private

      def transition_to_scheduling(step_ids, time)
        attrs = { state: StateMachine::SCHEDULING.to_s, updated_at: time }
        ids = repository.bulk_transition_steps(step_ids, attrs, expected_old_state: StateMachine::PENDING)
        log_info "Phase 1: Transitioned #{ids.size} steps from PENDING to SCHEDULING: #{ids.inspect}"
        ids
      rescue Yantra::Errors::PersistenceError => e
        log_error "Phase 1 Failed: #{e.message}"
        raise
      end

      def attempt_enqueue_for_scheduled(workflow_id, step_ids)
        # Re-fetch just before enqueue attempt to get the latest state
        steps = repository.find_steps(step_ids) || []
        log_warn "Mismatch between requested (#{step_ids.size}) and fetched (#{steps.size}) steps for enqueue." if steps.size != step_ids.size

        success, failed = [], []
        steps.each do |step|
          unless step.state.to_sym == StateMachine::SCHEDULING
            log_warn "Skipping enqueue for step #{step.id}: Expected SCHEDULING, found #{step.state} just before worker call."
            failed << step.id if step_ids.include?(step.id)
            next
          end

          begin
            if enqueue_step_with_worker(step, workflow_id)
              success << step.id
            else
              log_warn "Worker adapter returned false for enqueue step #{step.id}."
              failed << step.id
            end
          rescue => e
            log_error "Worker adapter error during enqueue for step #{step.id}: #{e.class} - #{e.message}"
            failed << step.id
          end
        end
        log_info "Enqueue attempt results: Success=#{success.size}, Failed=#{failed.size}"
        [success, failed]
      end

      def enqueue_step_with_worker(step, workflow_id)
        delay = step.delay_seconds
        queue = step.queue

        if delay&.positive?
          log_debug "Delayed enqueue: Step=#{step.id}, Delay=#{delay}s, Queue=#{queue}"
          worker_adapter.enqueue_in(delay, step.id, workflow_id, step.klass, queue)
        else
          log_debug "Immediate enqueue: Step=#{step.id}, Queue=#{queue}"
          worker_adapter.enqueue(step.id, workflow_id, step.klass, queue)
        end
      end

      def update_enqueued_state(step_ids, time)
        # Attributes for the final state
        attrs = {
          state: StateMachine::ENQUEUED.to_s,
          enqueued_at: time,
          updated_at: time # Ensure updated_at reflects this transition
        }

        transitioned_ids = repository.bulk_transition_steps(
          step_ids,
          attrs,
          expected_old_state: StateMachine::SCHEDULING
        )

        count = transitioned_ids.size
        if count != step_ids.size
          log_warn "Phase 3: Attempted to update #{step_ids.size} steps to ENQUEUED, but only #{count} were still in SCHEDULING state. Missed IDs: #{(step_ids - transitioned_ids).inspect}"
        else
          log_info "Phase 3: Updated #{count} steps to ENQUEUED."
        end

        # Rescue PersistenceError specifically from the transition attempt
      rescue Yantra::Errors::PersistenceError => e
        log_error "Phase 3 Failed (PersistenceError during transition): #{e.message}"
      rescue => e
        log_error "Phase 3 Failed (Unexpected Error): #{e.class} - #{e.message}"
      end

      def rollback_failed_enqueue_steps(step_ids, time)
        log_warn "Attempting to roll back #{step_ids.size} steps from SCHEDULING to PENDING due to enqueue failure: #{step_ids.inspect}"
        attrs = {
          state: StateMachine::PENDING.to_s,
          updated_at: time
        }
        begin
          # Use bulk_transition_steps for atomic rollback conditional on state
          rolled_back_ids = repository.bulk_transition_steps(
            step_ids,
            attrs,
            expected_old_state: StateMachine::SCHEDULING # Only rollback if still SCHEDULING
          )
          log_info "Successfully rolled back #{rolled_back_ids.size} steps to PENDING."
          if rolled_back_ids.size != step_ids.size
            log_warn "Could not roll back all failed steps. Missed: #{(step_ids - rolled_back_ids).inspect}"
          end
        rescue Yantra::Errors::PersistenceError => e
          log_error "Rollback Failed (PersistenceError during transition SCHEDULING -> PENDING): #{e.message}"
          # Log error but proceed, raising EnqueueFailed is the main signal
        rescue => e
          log_error "Rollback Failed (Unexpected Error): #{e.class} - #{e.message}"
          # Log error but proceed
        end
      end

      def raise_enqueue_error(failed_ids)
        msg = "Failed to enqueue #{failed_ids.size} steps: #{failed_ids.inspect}"
        log_error msg
        raise Yantra::Errors::EnqueueFailed.new(msg, failed_ids: failed_ids)
      end

      def publish_success_event(workflow_id, step_ids, time)
        return unless notifier.respond_to?(:publish) && step_ids.any?

        payload = {
          workflow_id: workflow_id,
          enqueued_ids: step_ids,
          enqueued_at: time
        }
        log_debug "Publishing yantra.step.bulk_enqueued event: #{payload.inspect}"
        notifier.publish('yantra.step.bulk_enqueued', payload)
      rescue => e
        log_error "Event publish failed: #{e.class} - #{e.message}"
      end

      def validate_repository_interface!
        unless repository.respond_to?(:bulk_transition_steps) && repository.respond_to?(:find_steps)
          raise Yantra::Errors::ConfigurationError, "Repository must implement #bulk_transition_steps and #find_steps"
        end
      end

      def validate_state_machine_constants!
        unless defined?(StateMachine::SCHEDULING) && defined?(StateMachine::ENQUEUED)
          raise Yantra::Errors::ConfigurationError, "StateMachine constants missing"
        end
      end

      def log_info(msg);  logger&.info  { "[StepEnqueuer] #{msg}" } end
      def log_warn(msg);  logger&.warn  { "[StepEnqueuer] #{msg}" } end
      def log_error(msg); logger&.error { "[StepEnqueuer] #{msg}" } end
      def log_debug(msg); logger&.debug { "[StepEnqueuer] #{msg}" } end
    end
  end
end

