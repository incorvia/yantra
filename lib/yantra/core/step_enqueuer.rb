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
    # Uses bulk_upsert_steps for efficient state updates.
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
      end

      # Attempts to enqueue/schedule a list of step IDs.
      # Fetches step details, calls the appropriate worker adapter method,
      # collects data for all successfully processed steps, and performs
      # a single bulk_upsert_steps call to update their state and timestamps.
      #
      # @param workflow_id [String] The workflow context.
      # @param step_ids_to_attempt [Array<String>] IDs of steps deemed ready.
      # @return [Array<String>] IDs of steps successfully handed off to the worker adapter.
      def call(workflow_id:, step_ids_to_attempt:)
        return [] if step_ids_to_attempt.nil? || step_ids_to_attempt.empty?
        log_info "Attempting to enqueue/schedule steps: #{step_ids_to_attempt.inspect} for workflow #{workflow_id}"

        steps_to_process = find_steps_to_process(step_ids_to_attempt)
        return [] if steps_to_process.empty?

        successfully_processed_data = [] # Collect hashes for bulk upsert
        processed_ids = [] # Track IDs successfully handed off to adapter
        now = Time.current # Consistent timestamp for this batch

        steps_to_process.each do |step|
          step_id = step.id

          # Final check: Ensure step is still PENDING before processing
          unless StateMachine::RELEASABLE_FROM_STATES.include?(step.state.to_sym)
            log_warn "Step #{step_id} state was not releasable (#{step.state}) when attempting enqueue, skipping."
            next
          end

          delay = step.delay_seconds
          success = false
          calculated_delayed_until = nil

          begin
            if delay && delay > 0
              # --- Handle Delayed Enqueue ---
              log_debug "Attempting to schedule step #{step_id} with delay: #{delay} seconds."
              success = worker_adapter.enqueue_in(delay, step_id, workflow_id, step.klass, step.queue)
              if success
                calculated_delayed_until = now + delay.seconds
                log_info "Step #{step_id} successfully scheduled via adapter to run around #{calculated_delayed_until}."
              else
                log_warn "Failed to schedule delayed step #{step_id} via worker adapter."
              end
            else
              # --- Handle Immediate Enqueue ---
              log_debug "Attempting to enqueue step #{step_id} immediately."
              success = worker_adapter.enqueue(step_id, workflow_id, step.klass, step.queue)
              if success
                log_info "Step #{step_id} successfully enqueued immediately via adapter."
              else
                log_warn "Failed to enqueue step #{step_id} via worker adapter."
              end
            end

            # If successfully handed off to adapter (immediate or delayed)
            if success
              processed_ids << step_id
              # Prepare data for bulk upsert
              successfully_processed_data << {
                id: step_id,
                state: StateMachine::ENQUEUED.to_s,
                enqueued_at: now,
                delayed_until: calculated_delayed_until, # Will be nil for immediate
                updated_at: now,
                # Include other non-nullable fields required by upsert_all
                workflow_id: step.workflow_id,
                klass: step.klass.to_s, # Ensure string
                max_attempts: step.max_attempts,
                retries: step.retries,
                created_at: step.created_at # Keep original created_at
                # Note: arguments, output, error are not typically modified here
              }
            end

          rescue StandardError => e
            log_error "Error during adapter enqueue/enqueue_in for step #{step_id}: #{e.class} - #{e.message}"
            # Continue to next step
          end
        end

        # --- Perform Bulk Update for all successfully processed steps ---
        if successfully_processed_data.any?
          log_debug "Bulk updating state to ENQUEUED for steps: #{processed_ids.inspect}"
          begin
            # Use the new bulk_upsert_steps method
            updated_count = repository.bulk_upsert_steps(successfully_processed_data)
            log_info "Bulk upsert processed #{updated_count} steps to ENQUEUED state (includes delayed)."

            # Publish event for ALL steps processed in this batch
            publish_steps_enqueued_event(workflow_id, processed_ids, now)

          rescue Yantra::Errors::PersistenceError => e
            log_error "Failed to bulk update steps after enqueueing: #{e.message}"
            # Note: Jobs were already sent to adapter, state might be inconsistent.
            # Consider adding monitoring or recovery logic here if needed.
          end
        end

        processed_ids # Return IDs that were successfully handed off to the adapter
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
