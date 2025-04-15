# lib/yantra/worker/retry_handler.rb

require_relative '../core/state_machine'
require_relative '../errors'
# No longer requires orchestrator

module Yantra
  module Worker
    # Handles the logic for determining whether a failed job execution should
    # be retried or marked as permanently failed.
    class RetryHandler
      # Removed orchestrator from reader
      attr_reader :repository, :job_record, :error, :executions, :user_job_klass

      # @param repository [#update_job_attributes, #record_job_error, #set_workflow_has_failures_flag, #increment_job_retries, #find_job]
      # @param job_record [Object] The persisted job record
      # @param error [StandardError] The exception raised during perform.
      # @param executions [Integer] The current execution attempt number.
      # @param user_job_klass [Class] The user's Yantra::Job subclass.
      def initialize(repository:, job_record:, error:, executions:, user_job_klass:) # Removed orchestrator:
        @repository = repository
        @job_record = job_record
        @error = error
        @executions = executions
        @user_job_klass = user_job_klass
      end

      # Processes the error and decides the outcome.
      # Returns :failed if the job failed permanently.
      # Re-raises the error if a retry is permitted by the backend.
      # @return [:failed] if the job failed permanently.
      # @raise [StandardError] Re-raises the original error if retry is allowed.
      def handle_error!
        max_attempts = get_max_attempts

        if executions >= max_attempts
          fail_permanently! # Update DB state
          return :failed    # Return status, DO NOT re-raise
        else
          prepare_for_retry! # Update DB state (retry count)
          raise error        # Re-raise to let backend handle retry scheduling
        end
      end

      private

      def get_max_attempts
        # (logic remains same)
        job_defined_attempts = user_job_klass.try(:yantra_max_attempts)
        return job_defined_attempts if job_defined_attempts.is_a?(Integer) && job_defined_attempts >= 0
        global_attempts = Yantra.configuration.try(:default_max_job_attempts)
        return global_attempts if global_attempts.is_a?(Integer) && global_attempts >= 0
        3
      end

      # Marks the job as permanently failed in the repository.
      def fail_permanently!
        puts "INFO: [RetryHandler] Job #{job_record.id} reached max attempts (#{get_max_attempts}). Marking as failed."
        final_attrs = {
          state: Yantra::Core::StateMachine::FAILED.to_s,
          finished_at: Time.current
        }
        update_success = repository.update_job_attributes(job_record.id, final_attrs, expected_old_state: :running)

        if update_success
           repository.record_job_error(job_record.id, error)
           repository.set_workflow_has_failures_flag(job_record.workflow_id)
           # TODO: Emit yantra.job.failed event (permanent)
           # REMOVED orchestrator.job_finished call
        else
           puts "WARN: [RetryHandler] Failed to update job #{job_record.id} state to failed (maybe already changed?)."
        end
      end

      # Performs actions needed before re-raising for a retry (e.g., incrementing counter).
      def prepare_for_retry!
        puts "INFO: [RetryHandler] Job #{job_record.id} failed, allowing retry (attempt #{executions}/#{get_max_attempts}). Re-raising error for backend."
        repository.increment_job_retries(job_record.id) # Increment Yantra's counter
        # TODO: Emit yantra.job.retrying event?
      end

    end
  end
end

