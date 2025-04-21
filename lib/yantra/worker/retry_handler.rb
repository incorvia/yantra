# lib/yantra/worker/retry_handler.rb

require_relative '../core/state_machine'
require_relative '../errors'
# Require Orchestrator if type hinting/checking needed (optional)
# require_relative '../core/orchestrator'

module Yantra
  module Worker
    class RetryHandler
      # --- UPDATED: Added orchestrator reader ---
      attr_reader :repository, :step_record, :error, :executions, :user_step_klass, :notifier, :orchestrator

      # --- UPDATED: Added orchestrator keyword argument ---
      def initialize(repository:, step_record:, error:, executions:, user_step_klass:, notifier:, orchestrator:)
        @repository = repository
        @step_record = step_record
        @error = error # Should be the original exception object
        @executions = executions
        @user_step_klass = user_step_klass
        @notifier = notifier # Store injected notifier
        @orchestrator = orchestrator # Store injected orchestrator
        @repository = repository

        # Optional: Validate orchestrator type
        # unless @orchestrator&.is_a?(Yantra::Core::Orchestrator)
        #   raise ArgumentError, "RetryHandler requires a valid orchestrator instance."
        # end
      end

      # Determines whether to retry or fail permanently.
      # Re-raises the original error if retry is allowed (for background system).
      # Returns :failed if the step fails permanently.
      # @return [:failed, void] Returns :failed or raises error.
      def handle_error!
        max_attempts = get_max_attempts

        if executions >= max_attempts
          fail_permanently! # Call the updated method below
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

      # --- UPDATED: Calls orchestrator to handle permanent failure ---
      # Calls the orchestrator to mark the step as failed and trigger subsequent logic.
      def fail_permanently!
        # Prepare error details hash for the orchestrator
        error_details = {
          class: @error.class.name,
          message: @error.message,
          # Include limited backtrace if desired and available
          backtrace: @error.backtrace&.first(10)
        }

        # Call the orchestrator's public method to handle the failure
        # This centralizes state updates, flag setting, event publishing,
        # and triggering of step_finished.
        orchestrator.step_failed(step_record.id, error_details)

      # Rescue potential errors during the call to the orchestrator itself
      rescue => e
         log_msg = "[RetryHandler] ERROR calling orchestrator.step_failed for #{step_record.id}: #{e.class} - #{e.message}\n#{e.backtrace.first(5).join("\n")}"
         # Log error using Yantra logger if available
         if defined?(Yantra.logger) && Yantra.logger
           Yantra.logger.error { log_msg }
         else
           warn log_msg # Fallback to standard warning output
         end
         # Note: Depending on desired behavior, you might want to re-raise 'e' here
         # or attempt direct repository updates as a fallback (though less ideal).
         # Currently, it logs the error and the step might remain in 'running'.
         raise e
      end
      # --- END UPDATED METHOD ---


      # Increments retry count and records the error for a retry attempt.
      def prepare_for_retry!
        # ... (logic remains the same) ...
        # Note: This still interacts directly with repository for retry prep.
        repository.increment_step_retries(step_record.id)
        repository.record_step_error(step_record.id, @error) # Record error for each retry attempt
      end

    end
  end
end
