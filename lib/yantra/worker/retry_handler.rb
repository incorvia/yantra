# --- lib/yantra/worker/retry_handler.rb ---
# (Modify prepare_for_retry! to pass the raw error object)

require_relative '../core/state_machine'
require_relative '../errors'

module Yantra
  module Worker
    class RetryHandler
      attr_reader :repository, :step_record, :error, :executions, :user_step_klass

      def initialize(repository:, step_record:, error:, executions:, user_step_klass:)
        @repository = repository
        @step_record = step_record
        @error = error # Should be the original exception object
        @executions = executions
        @user_step_klass = user_step_klass
      end

      def handle_error!
        max_attempts = get_max_attempts

        if executions >= max_attempts
          fail_permanently!
          return :failed
        else
          prepare_for_retry!
          raise error # Re-raise original error
        end
      end

      private

      def get_max_attempts
        step_defined_attempts = user_step_klass.try(:yantra_max_attempts)
        return step_defined_attempts if step_defined_attempts.is_a?(Integer) && step_defined_attempts >= 0
        global_attempts = Yantra.configuration.try(:default_max_step_attempts)
        return global_attempts if global_attempts.is_a?(Integer) && global_attempts >= 0
        3
      end

      def fail_permanently!
        puts "INFO: [RetryHandler] Job #{step_record.id} reached max attempts (#{get_max_attempts}). Marking as failed."
        final_attrs = {
          state: Yantra::Core::StateMachine::FAILED.to_s,
          finished_at: Time.current # Or Time.now.utc
        }
        # Assume state before permanent failure is :running
        update_success = repository.update_step_attributes(step_record.id, final_attrs, expected_old_state: :running)

        if update_success
          # Pass the ORIGINAL error object to the adapter
          repository.record_step_error(step_record.id, @error)
          repository.set_workflow_has_failures_flag(step_record.workflow_id)
        else
          puts "WARN: [RetryHandler] Failed to update job #{step_record.id} state to failed (maybe already changed?)."
        end
      end

      def prepare_for_retry!
        puts "INFO: [RetryHandler] Job #{step_record.id} failed, allowing retry (attempt #{executions}/#{get_max_attempts}). Re-raising error for backend."
        repository.increment_step_retries(step_record.id)
        # --- FIXED: Pass the ORIGINAL error object to the adapter ---
        # The adapter's record_step_error method will handle formatting.
        repository.record_step_error(step_record.id, @error)
        # --- END FIX ---
      end

      # format_error helper is no longer needed here if adapter handles it
      # def format_error(error) ... end

    end
  end
end
