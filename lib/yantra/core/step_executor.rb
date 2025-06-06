# lib/yantra/core/step_executor.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative '../step'
require_relative 'orchestrator'
require_relative '../worker/retry_handler'
require_relative 'state_machine'

module Yantra
  module Core
    class StepExecutor
      attr_reader :repository, :orchestrator, :notifier, :retry_handler_class, :logger

      def initialize(repository:, orchestrator:, notifier:, retry_handler_class:, logger: Yantra.logger)
        @repository = repository
        @orchestrator = orchestrator
        @notifier = notifier
        @retry_handler_class = retry_handler_class
        @logger = logger || Logger.new(IO::NULL)

        validate_interfaces!
      end

      def execute(step_id:, workflow_id:, step_klass_name:)
        step = load_step_record!(step_id)
        return handle_already_performed_step(step) if step.performed_at.present?
        return unless orchestrator_allows_start?(step_id, step)

        run_user_step!(step, step_klass_name, step_id, workflow_id)
      rescue Yantra::Errors::StepDefinitionError, Yantra::Errors::StepNotFound => e
        log_error "Critical Yantra error on step #{step_id}: #{e.class} - #{e.message}"
        handle_failure(step_id, e, is_definition_error: true)
        raise
      rescue => e
        log_error "Step execution error #{step_id}: #{e.class} - #{e.message}"
        handle_failure(step_id, e)
      end

      private

      def validate_interfaces!
        unless repository.respond_to?(:find_step) && repository.respond_to?(:update_step_error) && repository.respond_to?(:update_step_attributes)
          raise ArgumentError, "Repository must implement find_step, update_step_error, update_step_attributes"
        end
        unless orchestrator.respond_to?(:step_starting) && orchestrator.respond_to?(:handle_post_processing) && orchestrator.respond_to?(:step_failed)
          raise ArgumentError, "Orchestrator must implement step_starting, handle_post_processing, step_failed"
        end
        raise ArgumentError, "RetryHandler class must be instantiable" unless retry_handler_class.respond_to?(:new)
      end

      def load_step_record!(step_id)
        repository.find_step(step_id) || raise(Yantra::Errors::StepNotFound, "Step #{step_id} not found")
      end

      def handle_already_performed_step(step)
        log_info "Step #{step.id} already performed at #{step.performed_at}, triggering post-processing"
        orchestrator.handle_post_processing(step.id)
      end

      def orchestrator_allows_start?(step_id, step)
        unless orchestrator.step_starting(step_id)
          log_warn "Orchestrator blocked step start for #{step_id}, current state: #{step.state}"
          return false
        end

        reloaded = repository.find_step(step_id)
        unless reloaded&.state.to_s == StateMachine::RUNNING.to_s
          log_error "Step #{step_id} did not enter RUNNING state (found: #{reloaded&.state || 'nil'})"
          raise Yantra::Errors::OrchestrationError, "Step #{step_id} not RUNNING"
        end

        true
      end

      def run_user_step!(step, class_name, step_id, workflow_id)
        user_class = load_user_step_class(class_name)
        user_step = instantiate_user_step(user_class, step)
        args = prepare_arguments(step.arguments, step_id, workflow_id)

        log_info "Calling user perform: #{class_name} (Step ID: #{step_id})"
        output = user_step.perform(**args)
        log_info "User perform succeeded: #{step_id}"

        now = Time.current
        updated = repository.update_step_attributes(
          step_id,
          {
            performed_at: now,
            output: output,
            state: StateMachine::POST_PROCESSING.to_s,
            updated_at: now
          },
          expected_old_state: StateMachine::RUNNING
        )

        if updated
          log_info "Step #{step_id} marked POST_PROCESSING."
          orchestrator.handle_post_processing(step_id)
        else
          actual = repository.find_step(step_id)&.state || 'unknown'
          msg = "Step #{step_id} failed to update to POST_PROCESSING, found #{actual}"
          log_error msg
          raise Yantra::Errors::OrchestrationError, msg
        end
      end

      def load_user_step_class(name)
        name.constantize
      rescue NameError, LoadError => e
        raise Yantra::Errors::StepDefinitionError.new("Failed to load class #{name}: #{e.message}", original_exception: e)
      end

      def instantiate_user_step(klass, step)
        klass.new(
          step_id: step.id,
          workflow_id: step.workflow_id,
          klass: klass,
          state: step.state&.to_sym,
          arguments: step.arguments,
          retries: step.retries,
          max_attempts: step.max_attempts,
          delay_seconds: step.delay_seconds,
          repository: repository
        )
      rescue ArgumentError => e
        raise Yantra::Errors::StepDefinitionError.new("Invalid initializer for #{klass.name}: #{e.message}", original_exception: e)
      end

      def prepare_arguments(data, step_id, workflow_id)
        (data || {}).deep_symbolize_keys
      rescue => e
        log_warn "Step #{step_id} arg error (workflow #{workflow_id}): #{e.message}, defaulting to {}"
        {}
      end

      def handle_failure(step_id, error, is_definition_error: false)
        step = repository.find_step(step_id)
        raise error unless step

        expected_state = StateMachine::RUNNING

        if is_definition_error
          log_error "Definition error on #{step_id}, marking failed"
          orchestrator.step_failed(step_id, format_error(error), expected_old_state: expected_state)
          return
        end

        begin
          user_class = load_user_step_class(step.klass)
        rescue Yantra::Errors::StepDefinitionError => e
          log_error "Step class load failed on error path for #{step_id}, marking failed"
          orchestrator.step_failed(step_id, format_error(e), expected_old_state: expected_state)
          return
        end

        retry_handler_class.new(
          repository: repository,
          step_record: step,
          error: error,
          user_step_klass: user_class,
          orchestrator: orchestrator
        ).handle_error!
      end

      def format_error(error)
        return {
          class: error.class.name,
          message: error.message,
          backtrace: error.backtrace&.first(10)
        } if error.is_a?(Exception)

        if error.is_a?(Hash)
          {
            class: error[:class] || error['class'] || 'UnknownError',
            message: error[:message] || error['message'] || 'Unknown message',
            backtrace: error[:backtrace] || error['backtrace']
          }.compact
        else
          { class: error.class.name, message: error.to_s }
        end
      end

      def log_info(msg);  logger&.info  { "[StepExecutor] #{msg}" } end
      def log_warn(msg);  logger&.warn  { "[StepExecutor] #{msg}" } end
      def log_error(msg); logger&.error { "[StepExecutor] #{msg}" } end
      def log_debug(msg); logger&.debug { "[StepExecutor] #{msg}" } end
    end
  end
end

