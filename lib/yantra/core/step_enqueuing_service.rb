# lib/yantra/core/step_enqueuing_service.rb

require_relative '../errors'
require_relative 'state_machine'
require_relative '../persistence/repository_interface'
require_relative '../worker/enqueuing_interface'
require_relative '../events/notifier_interface'

module Yantra
  module Core
    # Service class responsible for taking a list of step IDs ready to be run,
    # attempting to enqueue them via the worker adapter, updating their state,
    # and publishing appropriate events. Uses bulk operations for efficiency.
    class StepEnqueuingService
      attr_reader :repository, :worker_adapter, :notifier

      # @param repository [#find_steps, #bulk_update_steps]
      # @param worker_adapter [#enqueue, #enqueue_bulk (optional)]
      # @param notifier [#publish, nil]
      def initialize(repository:, worker_adapter:, notifier:)
        @repository     = repository
        @worker_adapter = worker_adapter
        @notifier       = notifier

        # Validation
        unless repository&.respond_to?(:find_steps) && repository&.respond_to?(:bulk_update_steps)
           raise ArgumentError, "StepEnqueuingService requires a repository implementing #find_steps and #bulk_update_steps"
        end
        unless worker_adapter&.respond_to?(:enqueue) # Base enqueue is required
           raise ArgumentError, "StepEnqueuingService requires a worker adapter implementing #enqueue"
        end
      end

      # Attempts to enqueue the given steps, preferring bulk enqueue if available.
      #
      # @param workflow_id [String] The ID of the workflow.
      # @param step_ids_to_attempt [Array<String>] An array of step IDs to try enqueuing.
      # @return [Integer] The number of steps successfully submitted for enqueuing.
      def call(workflow_id:, step_ids_to_attempt:)
        return 0 if step_ids_to_attempt.nil? || step_ids_to_attempt.empty?
        log_info "Attempting to enqueue steps: #{step_ids_to_attempt.inspect} for workflow #{workflow_id}"

        # Fetch step data for all potential candidates
        begin
          steps_to_process = repository.find_steps(step_ids_to_attempt)
          steps_map = steps_to_process.index_by(&:id)
        rescue Yantra::Errors::PersistenceError => e
          log_error "Failed to bulk fetch steps #{step_ids_to_attempt.inspect} for enqueuing: #{e.message}"
          return 0
        end

        # Filter out steps that weren't found or are not actually pending anymore
        valid_steps_to_enqueue = []
        step_ids_to_attempt.each do |step_id|
            step = steps_map[step_id]
            unless step
              log_warn "Could not find pre-fetched step data for #{step_id}, skipping enqueue."
              next
            end
            unless step.state.to_sym == Yantra::Core::StateMachine::PENDING
              log_warn "Step #{step_id} state was not PENDING (#{step.state}) when attempting enqueue, skipping."
              next
            end
            valid_steps_to_enqueue << step
        end

        return 0 if valid_steps_to_enqueue.empty?

        successfully_enqueued_ids = []
        enqueue_successful = false

        # --- <<< CHANGED: Use Bulk or Fallback >>> ---
        if worker_adapter.respond_to?(:enqueue_bulk)
          # --- Bulk Path ---
          log_debug "Worker adapter supports enqueue_bulk. Preparing data."
          jobs_data_array = valid_steps_to_enqueue.map do |step|
            {
              step_id: step.id,
              workflow_id: step.workflow_id,
              klass: step.klass,
              queue: step.queue
              # Add :args if your bulk interface/adapters need it
            }
          end

          begin
            # Assume enqueue_bulk returns true on success, false/raises on failure
            enqueue_successful = worker_adapter.enqueue_bulk(jobs_data_array)
            if enqueue_successful
              # If bulk call succeeded, assume all valid steps passed to it were enqueued
              successfully_enqueued_ids = valid_steps_to_enqueue.map(&:id)
              log_info "Successfully submitted #{successfully_enqueued_ids.count} jobs via enqueue_bulk."
            else
              log_warn "Worker adapter enqueue_bulk returned false for steps: #{valid_steps_to_enqueue.map(&:id).inspect}"
            end
          rescue StandardError => e
            log_error "Error calling worker_adapter.enqueue_bulk: #{e.class} - #{e.message}"
            enqueue_successful = false # Ensure flag is false on error
          end
          # --- End Bulk Path ---
        else
          # --- Fallback Path (Single Enqueue Loop) ---
          log_debug "Worker adapter does not support enqueue_bulk. Using single enqueue loop."
          valid_steps_to_enqueue.each do |step|
            begin
              # Delegate actual enqueueing to the worker adapter
              # Assume enqueue returns true/JID on success, false/nil/raises on failure
              if worker_adapter.enqueue(step.id, step.workflow_id, step.klass, step.queue)
                successfully_enqueued_ids << step.id # Collect IDs of successfully enqueued steps
                log_debug "Successfully called enqueue via adapter for step #{step.id}"
              else
                 log_warn "Failed to enqueue step #{step.id} via worker adapter (adapter returned false/nil)."
              end
            rescue StandardError => e
              log_error "Failed to enqueue step #{step.id} via worker adapter: #{e.class} - #{e.message}"
              # Step remains pending in DB; not added to successfully_enqueued_ids
            end
          end
          # Set flag based on whether any steps succeeded in the loop
          enqueue_successful = successfully_enqueued_ids.any?
          # --- End Fallback Path ---
        end
        # --- <<< END CHANGED >>> ---


        # 4. Bulk update state ONLY for steps successfully submitted for enqueuing
        if successfully_enqueued_ids.any?
          log_info "Bulk updating state to ENQUEUED for steps: #{successfully_enqueued_ids.inspect}"
          update_attributes = { enqueued_at: Time.current }
          begin
            update_success = repository.bulk_update_steps(
              successfully_enqueued_ids,
              { state: Yantra::Core::StateMachine::ENQUEUED.to_s }.merge(update_attributes)
            )
            unless update_success
              log_warn "Bulk update to enqueued might have failed or reported issues for steps: #{successfully_enqueued_ids.inspect}"
            end

            # 5. Publish ONE bulk event for successfully enqueued steps
            publish_bulk_enqueued_event(workflow_id, successfully_enqueued_ids)

          rescue Yantra::Errors::PersistenceError => e
            log_error "Failed to bulk update state to ENQUEUED for steps #{successfully_enqueued_ids}: #{e.message}"
          end
        else
            log_info("No steps were successfully submitted for enqueuing in this attempt.") if valid_steps_to_enqueue.any?
        end

        # 6. Return the count of successfully submitted steps
        successfully_enqueued_ids.count
      end

      private

      # Publishes a single event indicating that a batch of steps has been successfully
      # submitted for enqueuing.
      def publish_bulk_enqueued_event(workflow_id, successfully_enqueued_ids)
         payload = {
           workflow_id:  workflow_id,
           enqueued_ids: successfully_enqueued_ids # Changed key for clarity
         }
         log_debug "Publishing yantra.step.bulk_enqueued event for workflow #{workflow_id}"
         notifier.publish('yantra.step.bulk_enqueued', payload)
      rescue => e
         log_error "Failed to publish bulk_enqueued event: #{e.message}"
      end

      # Logging helpers (expecting strings)
      def log_info(msg); Yantra.logger&.info("[StepEnqueuingService] #{msg}") end
      def log_debug(msg); Yantra.logger&.debug("[StepEnqueuingService] #{msg}") end
      def log_warn(msg); Yantra.logger&.warn("[StepEnqueuingService] #{msg}") end
      def log_error(msg); Yantra.logger&.error("[StepEnqueuingService] #{msg}") end

    end # class StepEnqueuingService
  end # module Core
end # module Yantra

