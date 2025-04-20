# Proposed file: lib/yantra/core/step_enqueuing_service.rb

require_relative '../errors'
require_relative 'state_machine'

module Yantra
  module Core
    # Service class responsible for taking a list of step IDs ready to be run,
    # attempting to enqueue them via the worker adapter, updating their state,
    # and publishing appropriate events. Uses bulk operations for efficiency.
    class StepEnqueuingService
      attr_reader :repository, :worker_adapter, :notifier

      # @param repository [#find_steps, #bulk_update_steps] The persistence adapter.
      # @param worker_adapter [#enqueue] The worker adapter.
      # @param notifier [#publish, nil] The notification adapter (optional).
      def initialize(repository:, worker_adapter:, notifier:)
        @repository     = repository
        @worker_adapter = worker_adapter
        @notifier       = notifier

        # Basic validation of dependencies
        unless repository&.respond_to?(:find_steps) && repository&.respond_to?(:bulk_update_steps)
           raise ArgumentError, "StepEnqueuingService requires a repository implementing #find_steps and #bulk_update_steps"
        end
        unless worker_adapter&.respond_to?(:enqueue)
           raise ArgumentError, "StepEnqueuingService requires a worker adapter implementing #enqueue"
        end
        # Notifier is optional, checked during publish
      end

      # Attempts to enqueue the given steps. Assumes the caller has already
      # determined these steps *should* be ready and pending.
      #
      # @param workflow_id [String] The ID of the workflow these steps belong to.
      # @param step_ids_to_attempt [Array<String>] An array of step IDs to try enqueuing.
      # @return [Integer] The number of steps successfully enqueued.
      def call(workflow_id:, step_ids_to_attempt:)
        # 1. Handle empty input
        return 0 if step_ids_to_attempt.nil? || step_ids_to_attempt.empty?
        log_info "Attempting to enqueue steps: #{step_ids_to_attempt.inspect} for workflow #{workflow_id}"

        # 2. Bulk fetch step data
        begin
          # Fetch steps we intend to enqueue. No state filter needed here as caller
          # should have provided IDs that are believed to be pending.
          steps_to_process = []
          measurement = Benchmark.measure do 
            steps_to_process += repository.find_steps(step_ids_to_attempt)
          end
          puts format_benchmark("Fan-Out (find_steps -> N)", measurement)
          steps_map = {}
          Benchmark.measure do
            steps_map = steps_to_process.index_by(&:id)
          end
          puts format_benchmark("Fan-Out (build steps map -> N)", measurement)
        rescue Yantra::Errors::PersistenceError => e
          log_error "Failed to bulk fetch steps #{step_ids_to_attempt.inspect} for enqueuing: #{e.message}"
          return 0 # Cannot proceed without step data
        end

        # 3. Attempt to enqueue each step via the worker adapter
        successfully_enqueued_ids = []
        measurement = Benchmark.measure do
          step_ids_to_attempt.each do |step_id|
            step = steps_map[step_id]
            unless step
              log_warn "Could not find pre-fetched step data for #{step_id}, skipping enqueue."
              next
            end

            # Safety check: Ensure step is still pending before enqueuing
            unless step.state.to_sym == StateMachine::PENDING
                log_warn "Step #{step_id} state was not PENDING (#{step.state}) when attempting enqueue, skipping."
                next
            end

            begin
              # Delegate actual enqueueing to the worker adapter
                worker_adapter.enqueue(step.id, step.workflow_id, step.klass, step.queue)
              successfully_enqueued_ids << step_id # Collect IDs of successfully enqueued steps
              log_debug { "Successfully called enqueue via adapter for step #{step.id}" } # Use block for debug
            rescue StandardError => e
              log_error "Failed to enqueue step #{step_id} via worker adapter: #{e.class} - #{e.message}"
              # Step remains pending in DB; will not be included in bulk update below.
            end
          end
        end
        puts format_benchmark("Fan-Out (step_ids_to_attempt.each (enqueue) -> N)", measurement)

        # 4. Bulk update state ONLY for successfully enqueued steps
        if successfully_enqueued_ids.any?
          log_info "Bulk updating state to ENQUEUED for steps: #{successfully_enqueued_ids.inspect}"
          update_attributes = { enqueued_at: Time.current }
          begin
            update_success = repository.bulk_update_steps(
              successfully_enqueued_ids,
              { state: StateMachine::ENQUEUED }.merge(update_attributes)
            )
            unless update_success
              log_warn "Bulk update to enqueued might have failed or reported issues for steps: #{successfully_enqueued_ids.inspect}"
            end

            # 5. Publish ONE bulk event for successfully enqueued steps
            publish_bulk_enqueued_event(workflow_id, successfully_enqueued_ids)

          rescue Yantra::Errors::PersistenceError => e
            log_error "Failed to bulk update state to ENQUEUED for steps #{successfully_enqueued_ids}: #{e.message}"
            # Note: Enqueue likely happened, but state update failed. Requires monitoring.
          end
        else
            log_info("No steps were successfully enqueued in this attempt.") if step_ids_to_attempt.any?
        end

        # 6. Return the count
        successfully_enqueued_ids.count
      end

      private

      # Publishes a single event indicating that a batch of steps has been successfully
      # submitted for enqueuing. Uses a minimal payload containing only IDs.
      # (This helper method now belongs to this service class)
      #
      # @param workflow_id [String] The ID of the workflow these steps belong to.
      # @param successfully_enqueued_ids [Array<String>] An array of the step IDs.
      # @return [void]
      def publish_bulk_enqueued_event(workflow_id, successfully_enqueued_ids)
        notifier&.publish('yantra.step.bulk_enqueued', {
          workflow_id:  workflow_id,
          enqueued_ids: successfully_enqueued_ids
        })
      end

      # Simple logging helpers (or use Yantra.logger directly)
      def log_info(msg); Yantra.logger&.info { "[StepEnqueuingService] #{msg}" }; end
      def log_debug(msg); Yantra.logger&.debug { "[StepEnqueuingService] #{msg}" }; end
      def log_warn(msg); Yantra.logger&.warn { "[StepEnqueuingService] #{msg}" }; end
      def log_error(msg); Yantra.logger&.error { "[StepEnqueuingService] #{msg}" }; end
      def format_benchmark(label, measurement)
        "#{label}: #{measurement.real.round(4)}s real, #{measurement.total.round(4)}s cpu"
      end
    end
  end
end
