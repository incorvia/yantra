# lib/yantra/core/step_executor.rb

require_relative '../errors'
require_relative '../step'
require_relative 'orchestrator'
require_relative '../worker/retry_handler'

module Yantra
  module Core
    # Service responsible for executing a single Yantra Step.
    class StepExecutor
      attr_reader :repository, :orchestrator, :notifier, :retry_handler_class

      def initialize(repository:, orchestrator:, notifier:, retry_handler_class:)
        @repository          = repository
        @orchestrator        = orchestrator
        @notifier            = notifier
        @retry_handler_class = retry_handler_class

        # ... (initializer validations) ...
        unless repository&.respond_to?(:find_step) && repository&.respond_to?(:record_step_error)
          raise ArgumentError, "StepExecutor requires a repository"
        end
        unless orchestrator&.respond_to?(:step_starting) && orchestrator&.respond_to?(:step_succeeded) && orchestrator&.respond_to?(:step_failed)
          raise ArgumentError, "StepExecutor requires an orchestrator"
        end
        unless retry_handler_class&.respond_to?(:new)
           raise ArgumentError, "StepExecutor requires a valid retry_handler_class"
        end
      end

      def execute(step_id:, workflow_id:, step_klass_name:, job_executions:)
        log_info "Executing step via StepExecutor: #{step_id}" # Use string

        unless orchestrator.step_starting(step_id)
          log_warn "Orchestrator prevented step start for #{step_id}" # Use string
          return
        end

        step_record = repository.find_step(step_id)
        unless step_record
          raise Yantra::Errors::StepNotFound, "Step record #{step_id} not found after starting."
        end

        user_step_klass = load_user_step_class(step_klass_name)
        user_step_instance = instantiate_user_step(user_step_klass, step_record)
        symbolized_args = prepare_arguments(step_record.arguments, step_id, workflow_id)

        log_info "Calling user perform method for: #{step_klass_name} (Step ID: #{step_id})" # Use string
        output = user_step_instance.perform(**symbolized_args)

        orchestrator.step_succeeded(step_id, output)
        log_info "Step succeeded: #{step_id}" # Use string

      # --- Specific Rescues ---
      rescue Yantra::Errors::StepDefinitionError => e
         # Definition errors are always permanent failures
         handle_failure(step_id, e, is_definition_error: true)
         raise e # Re-raise critical definition error

      rescue Yantra::Errors::StepNotFound => e
          log_error "Caught StepNotFound within StepExecutor: #{e.message}" # Use string
          raise e # Re-raise critical error

      # --- General Rescue for User Code Runtime Errors & Retries ---
      rescue => e # Catch other StandardErrors from user_step_instance.perform
        log_error "Error during user step perform for #{step_id}: #{e.class} - #{e.message}" # Use string

        # Handle failure attempts retry or marks step failed via orchestrator.
        # It returns :failed if handled permanently, otherwise re-raises 'e' for retry.
        failure_status = handle_failure(step_id, e, job_executions: job_executions)

        # If handle_failure completed (meaning RetryHandler returned :failed or
        # it was a definition error), we don't re-raise.
        # If handle_failure raised (because RetryHandler raised for retry),
        # that exception will propagate naturally. We only need to explicitly
        # re-raise if handle_failure somehow caught the retry error itself (which it shouldn't).
        # Let's simplify: If handle_failure didn't raise, assume it was handled.
        # The re-raise should only happen if handle_failure *itself* fails unexpectedly,
        # which is unlikely here. The error for retry is raised *inside* handle_failure.

        # Let's refine the logic based on RetryHandler's return value:
        # If handle_failure returns :failed, it was handled.
        # If handle_failure raises, the exception propagates.
        # So, we only need to re-raise 'e' if handle_failure somehow completes
        # *without* returning :failed and *without* raising. This shouldn't happen.

        # Therefore, we don't need the final 'raise e' here anymore,
        # as the RetryHandler inside handle_failure is responsible for raising on retry.
        # Let's remove it for now. If handle_failure has an issue, its own
        # (now removed) rescue block would have caught it, or it propagates.

        # raise e #
      end

      private

      def load_user_step_class(class_name)
        # ... (implementation as before) ...
        class_name.constantize
      rescue NameError => e
        raise Yantra::Errors::StepDefinitionError.new("Class #{class_name} could not be loaded: #{e.message}", original_exception: e)
      rescue LoadError => e
        raise Yantra::Errors::StepDefinitionError.new("Class file for #{class_name} could not be loaded: #{e.message}", original_exception: e)
      end

      def instantiate_user_step(user_step_klass, step_record)
        # ... (implementation as before) ...
         user_step_klass.new(
          step_id: step_record.id,
          workflow_id: step_record.workflow_id,
          klass: user_step_klass,
          state: step_record.state&.to_sym,
          arguments: step_record.arguments,
          queue: step_record.queue,
          retries: step_record.retries,
          max_attempts: step_record.max_attempts,
          repository: repository
        )
      rescue ArgumentError => e
        raise Yantra::Errors::StepDefinitionError.new("Failed to initialize #{user_step_klass.name}: #{e.message}. Check Step initializer arguments.", original_exception: e)
      end

      def prepare_arguments(args_data, step_id, workflow_id)
        # ... (implementation as before) ...
         (args_data || {}).deep_symbolize_keys
      rescue StandardError => e
         log_warn "Failed to symbolize step arguments for #{step_id} (workflow: #{workflow_id}): #{e.message}. Using empty hash."
         {}
      end

      # Handles failures, deciding between retry or marking as permanently failed.
      # Returns status from RetryHandler (:failed) or lets exceptions propagate.
      def handle_failure(step_id, error, job_executions: nil, is_definition_error: false)
        current_step_record = repository.find_step(step_id)
        unless current_step_record
           log_error "Step record #{step_id} not found when handling failure!"
           raise error # Re-raise original if step not found
        end

        if is_definition_error
            log_error "Step definition error for #{step_id}. Marking failed."
            error_info = format_error(error)
            orchestrator.step_failed(step_id, error_info)
            return :failed # Indicate permanent failure handled
        end

        # For runtime errors, use the RetryHandler
        user_klass = load_user_step_class(current_step_record.klass) # Let error propagate if load fails

        handler = retry_handler_class.new(
          repository: repository,
          step_record: current_step_record,
          error: error,
          executions: job_executions || (current_step_record.retries.to_i + 1),
          user_step_klass: user_klass,
          notifier: notifier,
          orchestrator: orchestrator
        )
        # handle_error! will either:
        # 1. Mark step failed via orchestrator and return :failed.
        # 2. Prepare for retry and re-raise the original error.
        # Let any exception raised by handle_error! propagate up.
        handler.handle_error! # Returns :failed or raises error
      end


      # Formats an exception or hash into the standard error hash.
      def format_error(error)
        # ... (implementation as before) ...
         if error.is_a?(Exception)
            { class: error.class.name, message: error.message, backtrace: error.backtrace&.first(10) }
         elsif error.is_a?(Hash)
            {
              class: error[:class] || error['class'] || 'UnknownError',
              message: error[:message] || error['message'] || 'Unknown error details',
              backtrace: error[:backtrace] || error['backtrace']
            }.compact
         else
            { class: error.class.name, message: error.to_s }
         end
      end

      # Logging helpers (expecting strings)
      def log_info(msg);  Yantra.logger&.info("[StepExecutor] #{msg}") end
      def log_warn(msg);  Yantra.logger&.warn("[StepExecutor] #{msg}") end
      def log_error(msg); Yantra.logger&.error("[StepExecutor] #{msg}") end
      def log_debug(msg); Yantra.logger&.debug("[StepExecutor] #{msg}") end

    end # class StepExecutor
  end # module Core
end # module Yantra

