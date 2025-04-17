# --- lib/yantra/worker/retry_handler.rb ---

require_relative '../core/state_machine'
require_relative '../errors'
# Ensure NotifierInterface is available if needed for type checking (optional)
# require_relative '../events/notifier_interface'

module Yantra
  module Worker
    class RetryHandler
      attr_reader :repository, :step_record, :error, :executions, :user_step_klass, :notifier # Added notifier reader

      # --- UPDATED: Added notifier keyword argument ---
      def initialize(repository:, step_record:, error:, executions:, user_step_klass:, notifier:)
        @repository = repository
        @step_record = step_record
        @error = error # Should be the original exception object
        @executions = executions
        @user_step_klass = user_step_klass
        @notifier = notifier # Store injected notifier

        # Optional: Validate notifier type
        # unless @notifier && @notifier.respond_to?(:publish) # Or use is_a?(Yantra::Events::NotifierInterface)
        #   raise ArgumentError, "RetryHandler requires a valid notifier instance."
        # end
      end

      # Determines whether to retry or fail permanently.
      # Re-raises the original error if retry is allowed (for background system).
      # Returns :failed if the step fails permanently.
      # @return [:failed, void] Returns :failed or raises error.
      def handle_error!
        max_attempts = get_max_attempts

        if executions >= max_attempts
          fail_permanently! # This method now publishes the event via @notifier
          return :failed # Signal permanent failure
        else
          prepare_for_retry!
          raise error # Re-raise original error for background job system retry
        end
      end

      private

      # Calculates the maximum attempts allowed for the step.
      def get_max_attempts
        # ... (logic remains the same) ...
        step_defined_attempts = user_step_klass.try(:yantra_max_attempts)
        return step_defined_attempts if step_defined_attempts.is_a?(Integer) && step_defined_attempts >= 0
        global_retries = Yantra.configuration&.default_step_options&.dig(:retries)
        global_attempts = global_retries.is_a?(Integer) && global_retries >= 0 ? global_retries + 1 : 1
        default_max_attempts = Yantra.configuration.try(:default_max_step_attempts) || global_attempts
        [default_max_attempts, 1].max
      end

      # Updates step state to failed, records error, sets workflow flag, and publishes event.
      def fail_permanently!
        puts "INFO: [RetryHandler] Job #{step_record.id} reached max attempts (#{get_max_attempts}). Marking as failed."
        finished_at_time = Time.current # Capture time
        final_attrs = {
          state: Yantra::Core::StateMachine::FAILED.to_s,
          finished_at: finished_at_time
        }
        update_success = repository.update_step_attributes(step_record.id, final_attrs, expected_old_state: :running)

        if update_success
          error_details = repository.record_step_error(step_record.id, @error)
          repository.set_workflow_has_failures_flag(step_record.workflow_id)

          # --- Publish yantra.step.failed event using injected notifier ---
          begin
            # --- UPDATED: Use instance variable @notifier ---
            notifier = @notifier # Use the injected notifier
            unless notifier # Safety check if somehow nil was injected
               puts "ERROR: [RetryHandler] Notifier not available, cannot publish yantra.step.failed event."
               return # Exit early if no notifier
            end

            payload = {
              step_id: step_record.id,
              workflow_id: step_record.workflow_id,
              klass: step_record.klass,
              state: Core::StateMachine::FAILED,
              finished_at: finished_at_time,
              error: error_details || { class: @error.class.name, message: @error.message, backtrace: @error.backtrace&.first(10) },
              retries: step_record.retries
            }
            notifier.publish('yantra.step.failed', payload)
            puts "INFO: [RetryHandler] Published yantra.step.failed event for #{step_record.id}."
          rescue => e
            log_msg = "[RetryHandler] Failed to publish yantra.step.failed event for #{step_record.id}: #{e.message}"
            # Log error using Yantra logger if available
            if defined?(Yantra.logger) && Yantra.logger
               Yantra.logger.error { log_msg }
            else
               puts "ERROR: #{log_msg}"
            end
          end
          # --- END Publish Event Section ---

        else
          latest_state = repository.find_step(step_record.id)&.state || 'unknown'
          puts "WARN: [RetryHandler] Failed to update job #{step_record.id} state to failed (maybe already changed? Current state: #{latest_state}). Event not published."
        end
      end

      # Increments retry count and records the error for a retry attempt.
      def prepare_for_retry!
        # ... (logic remains the same) ...
        puts "INFO: [RetryHandler] Job #{step_record.id} failed, allowing retry (attempt #{executions}/#{get_max_attempts}). Re-raising error for backend."
        repository.increment_step_retries(step_record.id)
        repository.record_step_error(step_record.id, @error)
      end

    end
  end
end

