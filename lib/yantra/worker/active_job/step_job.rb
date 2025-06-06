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

         # --- Determine Correct Wait Symbol based on Rails/ActiveJob Version ---
        # Define the version where the symbol potentially changed
        # (Assuming 7.0 based on discussion, verify if precise compatibility needed)
        RAILS_7_VERSION_THRESHOLD = Gem::Version.new('7.0.0')

        # Determine the correct symbol for exponential/polynomial backoff
        # Default to the older symbol for safety/backward compatibility
        backoff_symbol = :exponentially_longer
        if defined?(::ActiveJob) && ::ActiveJob.gem_version >= RAILS_7_VERSION_THRESHOLD
          # Use the symbol found in Rails 7.x+ docs
          backoff_symbol = :polynomially_longer
        end

        # --- Determine Wait Strategy based on Environment ---
        # Use a simple integer wait in test env to avoid :test adapter issues
        # Use the version-appropriate backoff symbol otherwise
        wait_strategy = if defined?(Rails) && Rails.env.test?
                          5 # Simple 5-second wait for tests
                        else
                          backoff_symbol # Use :polynomially_longer or :exponentially_longer
                        end

        # --- Apply Final Configuration ---
        # Use the determined wait strategy in the retry_on call.
        # Set attempts high enough for Yantra's logic to control stopping.
        retry_on StandardError, attempts: 25, wait: wait_strategy, jitter: 0.15 # Example jitter


        # Main execution method called by ActiveJob.
        # Delegates the core step execution logic to StepExecutor.
        def perform(step_id, workflow_id, step_klass_name)
          # Log entry point
          log_job_info "Received job, delegating to StepExecutor. AJ executions=#{executions.inspect}", step_id, workflow_id

          # Directly execute; let StepExecutor handle internal errors/logging.
          # Exceptions needing retry or final handling will propagate up to rescue_from.
          step_executor.execute(
            step_id: step_id,
            workflow_id: workflow_id,
            step_klass_name: step_klass_name
          )

          # This log only happens if execute completes without raising an error
          log_job_info "StepExecutor finished successfully", step_id, workflow_id
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

