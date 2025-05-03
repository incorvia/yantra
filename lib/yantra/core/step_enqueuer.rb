# lib/yantra/core/step_enqueuer.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require_relative '../persistence/repository_interface'
require_relative '../worker/enqueuing_interface'
require_relative '../events/notifier_interface'
require 'logger'
require 'active_support/core_ext/numeric/time' # For .seconds

module Yantra
  module Core
    # Service responsible for validating and enqueuing steps
    # that are ready to be processed by background workers.
    # Uses a revised three-phase approach with a SCHEDULING state:
    # 1. Bulk upsert state to SCHEDULING and set delayed_until.
    # 2. Individually enqueue/schedule via adapter, tracking successes and failures.
    # 3. Bulk update state to set enqueued_at timestamp for successfully processed steps.
    # 4. If any enqueue failed in Phase 2, raise EnqueueFailed.
    class StepEnqueuer
      attr_reader :repository, :worker_adapter, :notifier, :logger

      def initialize(repository:, worker_adapter:, notifier:, logger: Yantra.logger)
        @repository     = repository
        @worker_adapter = worker_adapter
        @notifier       = notifier
        @logger         = logger || Logger.new(IO::NULL)

        unless @repository && @worker_adapter && @notifier
          raise ArgumentError, "StepEnqueuer requires repository, worker_adapter, and notifier."
        end
        unless defined?(StateMachine::SCHEDULING)
           raise Yantra::Errors::ConfigurationError, "StateMachine::SCHEDULING constant is not defined."
        end
      end

      # Attempts to enqueue/schedule a list of step IDs using the revised three-phase approach.
      # Raises Yantra::Errors::EnqueueFailed if any individual enqueue/schedule fails,
      # *after* attempting to update the state for the successful ones.
      #
      # @param workflow_id [String] The workflow context.
      # @param step_ids_to_attempt [Array<String>] IDs of steps deemed ready.
      # @return [Array<String>] IDs of steps successfully handed off to the worker adapter.
      # @raise [Yantra::Errors::EnqueueFailed] if any step fails handoff to worker adapter.
      def call(workflow_id:, step_ids_to_attempt:)
        return [] if step_ids_to_attempt.nil? || step_ids_to_attempt.empty?
        log_info "Attempting 3-phase enqueue/schedule for steps: #{step_ids_to_attempt.inspect} in workflow #{workflow_id}"

        steps_to_process = find_steps_to_process(step_ids_to_attempt)
        return [] if steps_to_process.empty?

        candidate_steps = steps_to_process.select do |step|
          StateMachine.is_enqueue_candidate_state?(step.state, step.enqueued_at)
        end

        candidate_ids = candidate_steps.map(&:id)
        return [] if candidate_ids.empty?

        now = Time.current
        successfully_enqueued_ids = []
        failed_enqueue_ids = [] # Track failures
        initial_upsert_data = []

        # --- Prepare Phase 1 Data ---
        candidate_steps.each do |step|
            delay = step.delay_seconds
            calculated_delayed_until = (delay && delay > 0) ? (now + delay.seconds) : nil
            initial_upsert_data << {
              id: step.id,
              state: StateMachine::SCHEDULING.to_s,
              delayed_until: calculated_delayed_until,
              updated_at: now,
              workflow_id: step.workflow_id, klass: step.klass.to_s,
              max_attempts: step.max_attempts, retries: step.retries,
              created_at: step.created_at
            }
        end

        # --- Phase 1: Bulk Upsert State to SCHEDULING & delayed_until ---
        begin
          log_debug "Phase 1: Bulk upserting state to SCHEDULING for candidate steps: #{candidate_ids.inspect}"
          updated_count_phase1 = repository.bulk_upsert_steps(initial_upsert_data)
          log_info "Phase 1: Initial bulk upsert processed #{updated_count_phase1} steps."
        rescue Yantra::Errors::PersistenceError => e
          log_error "Phase 1: Failed to bulk upsert steps: #{e.message}. Aborting enqueue attempt."
          raise e # Re-raise critical persistence error
        end

        # --- Phase 2: Individual Enqueue/Schedule via Adapter ---
        log_debug "Phase 2: Attempting individual enqueue/schedule for #{candidate_ids.size} steps."
        candidate_steps.each do |step|
          step_id = step.id
          delay = step.delay_seconds
          queue = step.queue
          success = false

          begin
            if delay && delay > 0
              success = worker_adapter.enqueue_in(delay, step_id, workflow_id, step.klass, queue)
            else
              success = worker_adapter.enqueue(step_id, workflow_id, step.klass, queue)
            end

            if success
              log_debug "Phase 2: Successfully handed off step #{step_id} to worker adapter."
              successfully_enqueued_ids << step_id
            else
              log_warn "Phase 2: Failed to enqueue/schedule step #{step_id} via worker adapter."
              failed_enqueue_ids << step_id # Track failure
            end
          rescue StandardError => e
            failed_enqueue_ids << step_id # Track failure
          end
        end

        # --- Phase 3: Bulk Update State to set enqueued_t & Set Timestamps for SUCCESSFUL steps ---
        # This runs even if there were failures in Phase 2, to update the successful ones.
        if successfully_enqueued_ids.any?
          log_debug "Phase 3: Bulk updating state to set enqueued_at and setting timestamps for steps: #{successfully_enqueued_ids.inspect}"
          begin
            final_update_attributes = {
              enqueued_at: now,
              updated_at: now
            }
            updated_count_phase3 = repository.bulk_update_steps(successfully_enqueued_ids, final_update_attributes)
            log_info "Phase 3: Bulk update to set enqueud_at processed #{updated_count_phase3} steps."
          rescue Yantra::Errors::PersistenceError => e
            log_error "Phase 3: Failed to bulk update state/timestamps after successful enqueue: #{e.message}"
            # If this fails, successfully enqueued steps remain SCHEDULING with NULL enqueued_at.
            # Re-raise to signal the inconsistency.
            raise e
          end
        end

        # --- MODIFIED: Check for failures AFTER Phase 3 ---
        # If any step failed the handoff in Phase 2, raise error now.
        if failed_enqueue_ids.any?
          error_message = "Failed to enqueue/schedule #{failed_enqueue_ids.count} step(s): #{failed_enqueue_ids.inspect}. Corresponding steps remain in SCHEDULING state with no enqueued_at timestamp."
          log_error error_message
          raise Yantra::Errors::EnqueueFailed.new(error_message, failed_ids: failed_enqueue_ids)
        end
        # --- END MODIFIED ---

        # --- Publish Event only if ALL steps succeeded ---
        if successfully_enqueued_ids.any? && failed_enqueue_ids.empty?
          publish_steps_enqueued_event(workflow_id, successfully_enqueued_ids, now)
        end

        successfully_enqueued_ids # Return IDs that were actually handed off
      end

      private

      # Fetches step records safely.
      def find_steps_to_process(step_ids)
        repository.find_steps(step_ids) || []
      rescue Yantra::Errors::PersistenceError => e
        log_error "Failed to bulk fetch steps #{step_ids.inspect} for enqueuing: #{e.message}"
        [] # Return empty array on error
      end

      # Publishes the bulk enqueued event.
      def publish_steps_enqueued_event(workflow_id, step_ids, enqueued_time)
        return unless notifier&.respond_to?(:publish) && step_ids.any?
        payload = {
          workflow_id: workflow_id,
          enqueued_ids: step_ids,
          enqueued_at: enqueued_time
        }
        notifier.publish('yantra.step.bulk_enqueued', payload)
      rescue => e
        log_error "Failed to publish step.bulk_enqueued event: #{e.message}"
      end

      # Logging Helpers
      def log_info(msg);  @logger&.info  { "[StepEnqueuer] #{msg}" } end
      def log_warn(msg);  @logger&.warn  { "[StepEnqueuer] #{msg}" } end
      def log_error(msg); @logger&.error { "[StepEnqueuer] #{msg}" } end
      def log_debug(msg); @logger&.debug { "[StepEnqueuer] #{msg}" } end

    end # class StepEnqueuer
  end # module Core
end # module Yantra
