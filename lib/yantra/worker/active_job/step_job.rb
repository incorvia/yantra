# lib/yantra/worker/active_job/step_job.rb

require 'active_job'
require 'yantra/errors'
require_relative '../../core/step_executor' # <<< Require the executor
require_relative '../../core/orchestrator'
require_relative '../retry_handler'
require_relative '../../persistence/repository_interface'
require_relative '../../events/notifier_interface'

module Yantra
  module Worker
    module ActiveJob
      # ActiveJob job class responsible for executing a Yantra step
      # by delegating to the Yantra::Core::StepExecutor service.
      class StepJob < ::ActiveJob::Base
        # Configure ActiveJob options if needed (e.g., retries managed by Yantra)
        # self.queue_adapter = :sidekiq # Example
        # sidekiq_options retry: false # Disable Sidekiq retries, use Yantra's logic via RetryHandler

        rescue_from(StandardError) do |exception|
          step_id = arguments[0] rescue nil
          workflow_id = arguments[1] rescue nil
          log_job_error("Unhandled exception in StepJob wrapper", step_id, workflow_id, exception)

          if step_id && !exception.is_a?(Yantra::Errors::StepNotFound)
             begin
                error_info = { class: exception.class.name, message: "Unhandled StepJob wrapper error: #{exception.message}", backtrace: exception.backtrace }
                # Get a fresh orchestrator instance to report failure
                # Ensure Orchestrator.new uses configured adapters if called here
                # Yantra::Core::Orchestrator.new.step_failed(step_id, error_info, expected_old_state: nil)
             rescue => inner_e
                log_job_error("Failed to mark step failed after unhandled StepJob wrapper error", step_id, workflow_id, inner_e)
             end
          end
          # Re-raise non-Yantra errors to let ActiveJob handle them
          raise exception unless exception.is_a?(Yantra::Error)
        end

        # Main execution method called by ActiveJob.
        # Delegates the core step execution logic to StepExecutor.
        def perform(step_id, workflow_id, step_klass_name)
          log_job_info "Received job, delegating to StepExecutor. AJ executions=#{executions.inspect}", step_id, workflow_id

          begin
            # Instantiate the executor and execute the step
            step_executor.execute(
              step_id: step_id,
              workflow_id: workflow_id,
              step_klass_name: step_klass_name,
              # <<< FIX: Pass the raw ActiveJob executions count >>>
              # This count appears to be 1-based *within* the perform method context
              # based on pry testing. RetryHandler expects 1-based.
              job_executions: executions
              # <<< END FIX >>>
            )
            log_job_info "StepExecutor finished successfully", step_id, workflow_id
          rescue Yantra::Errors::StepDefinitionError, Yantra::Errors::StepNotFound => e
             log_job_error("StepExecutor raised critical error: #{e.class}", step_id, workflow_id, e)
             raise e # Re-raise critical Yantra errors
          rescue StandardError => e
             # This catches unexpected errors during the execute call itself
             log_job_error("Unexpected error during StepExecutor#execute call", step_id, workflow_id, e)
             raise e # Re-raise for generic rescue_from handler
          end
        end

        private

        # Lazily initializes the StepExecutor service instance.
        def step_executor
          @step_executor ||= Yantra::Core::StepExecutor.new(
            repository: Yantra.repository,
            orchestrator: Yantra::Core::Orchestrator.new( # Create orchestrator with global config
                            repository: Yantra.repository,
                            worker_adapter: Yantra.worker_adapter,
                            notifier: Yantra.notifier
                          ),
            notifier: Yantra.notifier,
            retry_handler_class: Yantra::Worker::RetryHandler
          )
        end

        # Logging helpers with context
        def log_job_info(message, step_id, workflow_id)
          Yantra.logger&.info { "[StepJob][#{job_id}] W:#{workflow_id} S:#{step_id} - #{message}" }
        end
        def log_job_warn(message, step_id, workflow_id)
          Yantra.logger&.warn { "[StepJob][#{job_id}] W:#{workflow_id} S:#{step_id} - #{message}" }
        end
        def log_job_error(message, step_id, workflow_id, error = nil)
          full_message = "[StepJob][#{job_id}] W:#{workflow_id} S:#{step_id} - ERROR: #{message}"
          if error
             backtrace_str = error.backtrace.is_a?(Array) ? error.backtrace.first(10).join("\n") : "No backtrace available"
             full_message += "\nException: #{error.class}: #{error.message}\nBacktrace:\n#{backtrace_str}"
          end
          Yantra.logger&.error { full_message }
        end

      end # class StepJob
    end # module ActiveJob
  end # module Worker
end # module Yantra

