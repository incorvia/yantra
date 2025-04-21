# lib/yantra/worker/sidekiq/step_worker.rb

# Ensure sidekiq is available if creating this file
begin
  require 'sidekiq'
rescue LoadError
  # Handle case where sidekiq gem isn't present
  puts "WARN: 'sidekiq' gem not found. Yantra::Worker::Sidekiq::StepJob will not be fully functional."
end

require_relative '../../errors'
require_relative '../../core/step_executor' # <<< Require the executor
require_relative '../../core/orchestrator'
require_relative '../retry_handler' # Assuming same retry handler can be used
require_relative '../../persistence/repository_interface'
require_relative '../../events/notifier_interface'

module Yantra
  module Worker
    module Sidekiq
      # Sidekiq worker class responsible for executing a Yantra step
      # by delegating to the Yantra::Core::StepExecutor service.
      class StepJob
        # Check if Sidekiq::Job is defined (Sidekiq 7+) otherwise use Sidekiq::Worker
        if defined?(::Sidekiq::Job)
           include ::Sidekiq::Job
           sidekiq_options retry: 25 # Disable Sidekiq retries, use Yantra's logic via RetryHandler
        elsif defined?(::Sidekiq::Worker)
           include ::Sidekiq::Worker
           sidekiq_options retry: 25 # Disable Sidekiq retries for older versions
        end

        # Main execution method called by Sidekiq.
        # Arguments are passed directly from the Sidekiq queue.
        # Assumes the Sidekiq adapter enqueues: step_id, workflow_id, step_klass_name
        def perform(step_id, workflow_id, step_klass_name)
          log_job_info "Received job, delegating to StepExecutor", step_id, workflow_id

          # Calculate execution count based on Sidekiq's retry_count (which is 0-based)
          # Handle potential absence of retry_count on first attempt or if middleware modifies things
          current_sidekiq_retry_count = jid && respond_to?(:retry_count) ? retry_count.to_i : 0
          # Yantra's execution count is typically 1-based
          job_executions = current_sidekiq_retry_count + 1

          # Directly execute; let StepExecutor handle internal error logging.
          # Exceptions representing needed retries (original error from RetryHandler)
          # or critical/unexpected errors will propagate upwards.
          step_executor.execute(
            step_id: step_id,
            workflow_id: workflow_id,
            step_klass_name: step_klass_name,
            job_executions: job_executions
          )

          # This log only executes if StepExecutor completes without raising an error
          # (meaning step succeeded OR RetryHandler handled terminal failure internally)
          log_job_info "StepExecutor finished successfully", step_id, workflow_id

          # No redundant rescue block needed here
        end

        private

        # Lazily initializes the StepExecutor service instance.
        # Uses globally configured Yantra components.
        def step_executor
          # Ensure Yantra components are loaded/configured in Sidekiq worker environment
          @step_executor ||= Yantra::Core::StepExecutor.new(
            repository: Yantra.repository,
            orchestrator: Yantra::Core::Orchestrator.new(
                            repository: Yantra.repository,
                            worker_adapter: Yantra.worker_adapter,
                            notifier: Yantra.notifier
                          ),
            notifier: Yantra.notifier,
            retry_handler_class: Yantra::Worker::RetryHandler # Use same handler? Or Sidekiq specific one?
          )
        end

        # Accessor for Sidekiq Job ID (jid)
        def job_id
           self.jid rescue "N/A" # Rescue if jid isn't available (older versions?)
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
    end # module Sidekiq
  end # module Worker
end # module Yantra

