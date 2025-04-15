# lib/yantra/worker/retry_handler.rb

require_relative '../core/state_machine'
require_relative '../errors'
# require_relative '../yantra' # For Yantra.repository/configuration

module Yantra
  module Worker
    # Handles the logic for determining whether a failed job execution should
    # be retried or marked as permanently failed.
    class RetryHandler
      attr_reader :repository, :job_record, :error, :executions, :user_job_klass

      # @param repository [#update_job_attributes, #record_job_error, #set_workflow_has_failures_flag, #increment_job_retries]
      # @param job_record [Object] The persisted job record (responds to #id, #workflow_id, #arguments, etc.)
      # @param error [StandardError] The exception raised during perform.
      # @param executions [Integer] The current execution attempt number (from ActiveJob's self.executions).
      # @param user_job_klass [Class] The user's Yantra::Job subclass.
      def initialize(repository:, job_record:, error:, executions:, user_job_klass:)
        @repository = repository
        @job_record = job_record
        @error = error
        @executions = executions
        @user_job_klass = user_job_klass
      end

      # Processes the error and decides the outcome.
      # Re-raises the error if a retry is permitted by the backend.
      # Updates state to :failed and notifies orchestrator if retries are exhausted.
      def handle_error!
        # TODO: Add logic here to check if the specific `error` class is
        # configured as non-retryable for this job type or globally.
        # For now, we assume all StandardErrors might be retryable up to max attempts.

        # Determine max attempts allowed for this job
        max_attempts = get_max_attempts

        if executions >= max_attempts
          # Max attempts reached - fail permanently
          fail_permanently!
          # Do not re-raise, we have handled the final state.
        else
          # Retry allowed - prepare for next attempt
          prepare_for_retry!
          # Re-raise the original error to let ActiveJob backend handle scheduling
          raise error
        end
      end

      private

      # Determines the maximum retry attempts for the current job.
      # Looks for a class method on the user's job class, falls back to config.
      # @return [Integer]
      def get_max_attempts
        # Check user's job class first (e.g., `def self.yantra_max_attempts; 5; end`)
        job_defined_attempts = user_job_klass.try(:yantra_max_attempts)
        return job_defined_attempts if job_defined_attempts.is_a?(Integer) && job_defined_attempts >= 0

        # Check global config (e.g., `config.default_max_job_attempts`)
        global_attempts = Yantra.configuration.try(:default_max_job_attempts)
        return global_attempts if global_attempts.is_a?(Integer) && global_attempts >= 0

        # Fallback default
        3
      end

      # Marks the job as permanently failed in the repository and notifies orchestrator.
      def fail_permanently!
        puts "INFO: [RetryHandler] Job #{job_record.id} reached max attempts (#{get_max_attempts}). Marking as failed."
        final_attrs = {
          state: Yantra::Core::StateMachine::FAILED.to_s,
          finished_at: Time.current
        }
        # Use expected_old_state :running for optimistic lock
        update_success = repository.update_job_attributes(job_record.id, final_attrs, expected_old_state: :running)

        if update_success
           repository.record_job_error(job_record.id, error)
           repository.set_workflow_has_failures_flag(job_record.workflow_id)
           # TODO: Emit yantra.job.failed event (permanent)

           # Notify orchestrator that job has terminally failed
           # Need an orchestrator instance - how should handler get it? Pass it in?
           # For now, create one (assuming global repo/worker access is okay for Orchestrator init)
           orchestrator = Yantra::Core::Orchestrator.new
           puts "INFO: [RetryHandler] Notifying orchestrator job finished (failed) for #{job_record.id}"
           orchestrator.job_finished(job_record.id)
        else
           puts "WARN: [RetryHandler] Failed to update job #{job_record.id} state to failed (maybe already changed?). Not notifying orchestrator."
        end
      end

      # Performs actions needed before re-raising for a retry (e.g., incrementing counter).
      def prepare_for_retry!
        puts "INFO: [RetryHandler] Job #{job_record.id} failed, allowing retry (attempt #{executions}/#{get_max_attempts}). Re-raising error for backend."
        # Optional: Increment Yantra's internal retry counter for visibility
        repository.increment_job_retries(job_record.id)
        # TODO: Emit yantra.job.retrying event?
      end

    end
  end
end

