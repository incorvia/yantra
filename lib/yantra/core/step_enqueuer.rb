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

        unless repository.respond_to?(:bulk_transition_steps) && repository.respond_to?(:find_steps)
           raise Yantra::Errors::ConfigurationError, "Configured repository must implement #bulk_transition_steps and #find_steps"
        end
        unless defined?(StateMachine::SCHEDULING) && defined?(StateMachine::ENQUEUED)
          raise Yantra::Errors::ConfigurationError, "StateMachine requires SCHEDULING and ENQUEUED states"
        end
      end

      def call(workflow_id:, step_ids_to_attempt:)
        return [] if step_ids_to_attempt.blank?
        log_info "Enqueueing steps: #{step_ids_to_attempt.inspect} in workflow #{workflow_id}"

        now = Time.current

        # --- Identify initial states ---
        initial_steps = repository.find_steps(step_ids_to_attempt) || []
        pending_ids = initial_steps.select { |s| s.state.to_sym == StateMachine::PENDING }.map(&:id)
        scheduling_ids = initial_steps.select { |s| s.state.to_sym == StateMachine::SCHEDULING }.map(&:id)
        other_state_ids = step_ids_to_attempt - pending_ids - scheduling_ids
        log_warn "Skipping steps not in PENDING or SCHEDULING state: #{other_state_ids.inspect}" if other_state_ids.any?

        # --- Phase 1: Transition PENDING -> SCHEDULING (only for pending steps) ---
        transitioned_pending_ids = []
        if pending_ids.any?
          transitioned_pending_ids = transition_to_scheduling(pending_ids, now)
          # If transition failed for some pending steps, they won't be processed further in this run.
        end

        # Combine successfully transitioned steps with those already scheduling
        ids_to_process = (transitioned_pending_ids + scheduling_ids).uniq
        return [] if ids_to_process.empty? # No steps are ready for enqueue attempt

        # --- Phase 2: Attempt Enqueue for SCHEDULING steps ---
        successfully_enqueued_ids, failed_enqueue_ids = attempt_enqueue_for_scheduled(workflow_id, ids_to_process)

        raise_enqueue_error(failed_enqueue_ids) unless failed_enqueue_ids.empty?

        # --- Phase 3: Update State SCHEDULING -> ENQUEUED ---
        update_enqueued_state(successfully_enqueued_ids, now) if successfully_enqueued_ids.any?

        publish_success_event(workflow_id, successfully_enqueued_ids, now)

        # Return the array of successfully enqueued IDs
        successfully_enqueued_ids
      end

      private

      # Phase 1: Atomically transitions steps from PENDING to SCHEDULING.
      def transition_to_scheduling(pending_step_ids, time)
        begin
          transition_attrs = {
            state: StateMachine::SCHEDULING.to_s,
            updated_at: time
          }
          ids = repository.bulk_transition_steps(
            pending_step_ids,
            transition_attrs,
            expected_old_state: StateMachine::PENDING
          )
          log_info "Phase 1: Transitioned #{ids.size} steps from PENDING to SCHEDULING: #{ids.inspect}"
          ids
        rescue Yantra::Errors::PersistenceError => e
          log_error "Phase 1 Failed: Error transitioning steps to SCHEDULING: #{e.message}"
          raise # Re-raise persistence errors
        end
      end

      # Phase 2: Fetches step details and attempts to enqueue them via worker adapter.
      def attempt_enqueue_for_scheduled(workflow_id, ids_to_process)
        steps_to_enqueue = repository.find_steps(ids_to_process) || []
        if steps_to_enqueue.size != ids_to_process.size
            log_warn "Mismatch between IDs to process (#{ids_to_process.size}) and fetched steps (#{steps_to_enqueue.size}) for enqueueing."
        end

        log_info "Phase 2: Attempting to enqueue #{steps_to_enqueue.size} steps..."
        successfully_enqueued_ids = []
        failed_enqueue_ids = []

        steps_to_enqueue.each do |step|
          # Ensure step is actually SCHEDULING before attempting enqueue
          unless step.state.to_sym == StateMachine::SCHEDULING
            log_warn "Skipping enqueue for step #{step.id}: Expected SCHEDULING, found #{step.state}."
            # Add to failed if it was in the list we intended to process but state changed
            failed_enqueue_ids << step.id if ids_to_process.include?(step.id)
            next
          end

          begin
            result = enqueue_step_with_worker(step, workflow_id)
            if result
              successfully_enqueued_ids << step.id
            else
              log_warn "Phase 2: Worker adapter failed to enqueue step #{step.id}."
              failed_enqueue_ids << step.id
            end
          rescue => e
            log_error "Phase 2: Error during worker adapter call for step #{step.id}: #{e.class} - #{e.message}"
            failed_enqueue_ids << step.id
          end
        end

        log_info "Phase 2 Results: Success=#{successfully_enqueued_ids.size}, Failed=#{failed_enqueue_ids.size}"
        [successfully_enqueued_ids, failed_enqueue_ids]
      end

      # Calls the appropriate worker adapter method based on delay.
      def enqueue_step_with_worker(step, workflow_id)
        delay = step.delay_seconds
        queue = step.queue
        adapter_call_result = false

        if delay && delay > 0
          log_debug "Enqueuing delayed job: Step=#{step.id}, Delay=#{delay}s, Queue=#{queue}"
          adapter_call_result = worker_adapter.enqueue_in(delay, step.id, workflow_id, step.klass, queue)
        else
          log_debug "Enqueuing immediate job: Step=#{step.id}, Queue=#{queue}"
          adapter_call_result = worker_adapter.enqueue(step.id, workflow_id, step.klass, queue)
        end

        return adapter_call_result
      end

      # Phase 3: Updates the state of successfully enqueued steps to ENQUEUED.
      def update_enqueued_state(step_ids, time)
        log_info "Phase 3: Updating #{step_ids.size} steps to ENQUEUED state..."
        attrs = {
          state: StateMachine::ENQUEUED.to_s,
          enqueued_at: time,
          updated_at: time
        }
        updated_count = repository.bulk_update_steps(step_ids, attrs)
        log_info "Phase 3: Repository reported #{updated_count} steps updated to ENQUEUED."
      rescue Yantra::Errors::PersistenceError => e
        log_error "Phase 3 Failed: Error updating steps to ENQUEUED: #{e.message}. Steps remain SCHEDULING."
      end

      # Raises a specific error if any enqueue attempts failed.
      def raise_enqueue_error(failed_step_ids)
        msg = "Failed to enqueue #{failed_step_ids.size} steps: #{failed_step_ids.inspect}"
        log_error msg
        raise Yantra::Errors::EnqueueFailed.new(msg, failed_ids: failed_step_ids)
      end

      # Publishes the success event.
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
        log_error "Failed publishing yantra.step.bulk_enqueued event: #{e.class} - #{e.message}"
      end

      # Logging helpers
      def log_info(msg);  logger&.info  { "[StepEnqueuer] #{msg}" } end
      def log_warn(msg);  logger&.warn  { "[StepEnqueuer] #{msg}" } end
      def log_error(msg); logger&.error { "[StepEnqueuer] #{msg}" } end
      def log_debug(msg); logger&.debug { "[StepEnqueuer] #{msg}" } end

    end # class StepEnqueuer
  end # module Core
end # module Yantra

