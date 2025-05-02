# lib/yantra/core/step_executor.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative '../step'
require_relative 'orchestrator' # Orchestrator interface expected
require_relative '../worker/retry_handler' # RetryHandler interface expected
require_relative 'state_machine' # StateMachine constants

module Yantra
  module Core
    # Service responsible for executing a single Yantra Step.
    # Includes logic to handle step execution idempotency using `performed_at`,
    # transitions step to POST_PROCESSING after successful perform,
    # and coordinates with Orchestrator and RetryHandler.
    class StepExecutor
      attr_reader :repository, :orchestrator, :notifier, :retry_handler_class, :logger

      def initialize(repository:, orchestrator:, notifier:, retry_handler_class:, logger: Yantra.logger)
        @repository          = repository
        @orchestrator        = orchestrator
        @notifier            = notifier # Keep for potential future use or pass-through
        @retry_handler_class = retry_handler_class
        @logger              = logger || Logger.new(IO::NULL)

        # Basic validation
        unless repository&.respond_to?(:find_step) && repository&.respond_to?(:update_step_error) && repository&.respond_to?(:update_step_attributes)
          raise ArgumentError, "StepExecutor requires a repository with find_step, update_step_error, update_step_attributes"
        end
        # Check for the orchestrator methods needed
        unless orchestrator&.respond_to?(:step_starting) && orchestrator&.respond_to?(:handle_post_processing) && orchestrator&.respond_to?(:step_failed)
          raise ArgumentError, "StepExecutor requires an orchestrator with step_starting, handle_post_processing, step_failed"
        end
        unless retry_handler_class&.respond_to?(:new)
          raise ArgumentError, "StepExecutor requires a valid retry_handler_class"
        end
      end

      # Executes the step logic, handles retries, and coordinates with the orchestrator.
      def execute(step_id:, workflow_id:, step_klass_name:, job_executions:)
        log_info "Executing step via StepExecutor: #{step_id}, Attempt: #{job_executions}"

        # --- Phase 1: Pre-execution Checks & Start ---
        step_record = repository.find_step(step_id)
        unless step_record
          raise Yantra::Errors::StepNotFound, "Step record #{step_id} not found during execution attempt."
        end

        # --- MODIFIED: Check performed_at ---
        # Check if core logic already performed successfully on a previous attempt
        if step_record.performed_at.present?
          log_info "Step #{step_id} core logic already performed at #{step_record.performed_at}. Skipping perform, proceeding directly to post-processing check."
          # Trigger post-processing again to re-attempt dependent enqueueing if needed.
          # This assumes handle_post_processing is idempotent or handles steps not in POST_PROCESSING state.
          orchestrator.handle_post_processing(step_id)
          return # Exit execute method
        end
        # --- END MODIFIED ---

        # Signal start to orchestrator (updates state to RUNNING if valid)
        unless orchestrator.step_starting(step_id)
          log_warn "Orchestrator prevented step start for #{step_id}. Current state: #{step_record.state}"
          return # Exit if start is prevented
        end

        # Reload step record *after* step_starting to ensure we have the RUNNING state
        step_record = repository.find_step(step_id)
        unless step_record&.state.to_s == StateMachine::RUNNING.to_s
           log_error "Step #{step_id} failed to transition to RUNNING state before perform. Current state: #{step_record&.state || 'Not Found'}. Aborting."
           raise Yantra::Errors::OrchestrationError, "Step #{step_id} did not enter RUNNING state."
        end

        # --- Phase 2: Execute User Code ---
        output = nil
        begin
          user_step_klass = load_user_step_class(step_klass_name)
          user_step_instance = instantiate_user_step(user_step_klass, step_record)
          symbolized_args = prepare_arguments(step_record.arguments, step_id, workflow_id)

          log_info "Calling user perform method for: #{step_klass_name} (Step ID: #{step_id})"
          output = user_step_instance.perform(**symbolized_args)
          log_info "User perform method completed successfully for: #{step_id}"

          # --- Phase 3: Handle Successful Execution (Transition to Post-Processing) ---
          # Atomically record output, performed_at, and update state to POST_PROCESSING
          now = Time.current
          success = repository.update_step_attributes(
            step_id,
            {
              performed_at: now,
              output: output, # Record output here
              state: StateMachine::POST_PROCESSING.to_s,
              updated_at: now # Explicitly set updated_at
            },
            expected_old_state: StateMachine::RUNNING # Ensure we were still running
          )

          if success
            log_info "Step #{step_id} marked as POST_PROCESSING, triggering dependent handling."
            # Trigger post-processing (dependent enqueueing, final state update)
            orchestrator.handle_post_processing(step_id)
          else
            # This indicates a potential race condition or unexpected state change
            current_state = repository.find_step(step_id)&.state || 'unknown'
            log_error "Failed to transition step #{step_id} to POST_PROCESSING. Expected RUNNING, found '#{current_state}'. Dependent processing skipped."
            # Raise an error? Or just log? Raising might be safer.
            raise Yantra::Errors::OrchestrationError, "Failed to transition step #{step_id} to POST_PROCESSING"
          end

        rescue Yantra::Errors::StepDefinitionError, Yantra::Errors::StepNotFound => e
          # Critical Yantra errors - fail permanently and re-raise
          log_error("Critical Yantra error during step execution for #{step_id}: #{e.class} - #{e.message}")
          handle_failure(step_id, e, is_definition_error: true) # Mark failed
          raise e # Re-raise for job backend

        rescue => e # Catch runtime errors from user_step_instance.perform
          # --- Phase 3: Handle Runtime Failure ---
          log_error "Error during user step perform for #{step_id}: #{e.class} - #{e.message}"
          # Delegate to RetryHandler - it will either re-raise for backend retry
          # or call orchestrator.step_failed for permanent failure.
          handle_failure(step_id, e, job_executions: job_executions)
          # Exception propagation handled by handle_failure/RetryHandler
        end
      end

      private

      # Loads the user-defined step class constant.
      def load_user_step_class(class_name)
        class_name.constantize
      rescue NameError => e
        raise Yantra::Errors::StepDefinitionError.new("Class #{class_name} could not be loaded: #{e.message}", original_exception: e)
      rescue LoadError => e
        raise Yantra::Errors::StepDefinitionError.new("Class file for #{class_name} could not be loaded: #{e.message}", original_exception: e)
      end

      # Instantiates the user step class, injecting dependencies.
      def instantiate_user_step(user_step_klass, step_record)
         user_step_klass.new(
           step_id: step_record.id,
           workflow_id: step_record.workflow_id,
           klass: user_step_klass,
           state: step_record.state&.to_sym,
           arguments: step_record.arguments,
           retries: step_record.retries,
           max_attempts: step_record.max_attempts,
           delay_seconds: step_record.delay_seconds,
           repository: repository
         )
      rescue ArgumentError => e
        raise Yantra::Errors::StepDefinitionError.new("Failed to initialize #{user_step_klass.name}: #{e.message}. Check Step initializer arguments.", original_exception: e)
      end

      # Prepares arguments for the step's perform method.
      def prepare_arguments(args_data, step_id, workflow_id)
         (args_data || {}).deep_symbolize_keys
      rescue StandardError => e
         log_warn "Failed to symbolize step arguments for #{step_id} (workflow: #{workflow_id}): #{e.message}. Using empty hash."
         {}
      end

      # Handles failures by delegating to RetryHandler or marking as failed directly.
      # Lets exceptions propagate if a retry is needed.
      def handle_failure(step_id, error, job_executions: nil, is_definition_error: false)
        current_step_record = repository.find_step(step_id)
        unless current_step_record
           log_error "Step record #{step_id} not found when handling failure!"
           raise error # Re-raise original if step not found
        end

        # Determine the state we expect the step to be in when failure occurs
        # It should be RUNNING unless it failed validation before starting
        expected_state_on_failure = StateMachine::RUNNING

        if is_definition_error
           log_error "Step definition error for #{step_id}. Marking failed."
           error_info = format_error(error)
           # Use orchestrator to mark failed (handles state, events, dependents)
           orchestrator.step_failed(step_id, error_info, expected_old_state: expected_state_on_failure)
        else
          # For runtime errors, use the RetryHandler
          begin
            user_klass = load_user_step_class(current_step_record.klass)
          rescue Yantra::Errors::StepDefinitionError => load_error
            log_error "Failed to load step class #{current_step_record.klass} during error handling for #{step_id}. Marking failed."
            error_info = format_error(load_error)
            orchestrator.step_failed(step_id, error_info, expected_old_state: expected_state_on_failure)
            return # Exit handle_failure
          end

          handler = retry_handler_class.new(
            repository: repository,
            step_record: current_step_record,
            error: error,
            executions: job_executions || (current_step_record.retries.to_i + 1),
            user_step_klass: user_klass,
            orchestrator: orchestrator
          )
          # handle_error! will either call orchestrator.step_failed OR re-raise original error
          handler.handle_error!
        end
      end

      # Formats an exception or hash into the standard error hash.
      def format_error(error)
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
      def log_info(msg);  @logger&.info("[StepExecutor] #{msg}") end
      def log_warn(msg);  @logger&.warn("[StepExecutor] #{msg}") end
      def log_error(msg); @logger&.error("[StepExecutor] #{msg}") end
      def log_debug(msg); @logger&.debug("[StepExecutor] #{msg}") end

    end # class StepExecutor
  end # module Core
end # module Yantra
