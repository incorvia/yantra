# lib/yantra/worker/active_job/step_job.rb

require 'active_job'
require 'yantra/errors' # Require the errors file defining Yantra::Error
require_relative '../../step'
require_relative '../../core/orchestrator' # Needed for orchestrator instance
require_relative '../retry_handler'     # Needed for RetryHandler
require_relative '../../persistence/repository_interface' # Needed for repository type check
require_relative '../../events/notifier_interface'       # Needed for notifier type check


module Yantra
  module Worker
    module ActiveJob
      # The ActiveJob job class responsible for executing a Yantra step.
      class StepJob < ::ActiveJob::Base
        # Configure ActiveJob options if needed
        # sidekiq_options retry: 5 # Example

        # Error handling for ActiveJob specific issues or unhandled exceptions
        rescue_from(StandardError) do |exception|
          # This top-level rescue is for unexpected errors *outside* the main perform logic,
          # or if the perform rescue itself fails.
          # Attempt to extract arguments safely
          step_id = arguments[0] rescue nil
          workflow_id = arguments[1] rescue nil
          log_job_error("Unhandled exception in StepJob", step_id, workflow_id, exception)

          # Optionally, try to mark the step as failed in Yantra as a last resort
          if step_id # Only attempt if we have a step_id
            begin
              error_info = { class: exception.class.name, message: "Unhandled StepJob error: #{exception.message}", backtrace: exception.backtrace }
              # Use a safe way to access orchestrator if it exists
              # safe_orchestrator = self.respond_to?(:orchestrator, true) ? self.send(:orchestrator) : nil
              # safe_orchestrator&.step_failed(step_id, error_info) if safe_orchestrator
            rescue => inner_e
              log_job_error("Failed to mark step failed after unhandled StepJob error", step_id, workflow_id, inner_e)
            end
          end

          # Re-raise the original error for ActiveJob's default handling
          # unless it's a Yantra-specific error (which should have been handled below).
          # Only re-raise if it's not already a Yantra::Error, as those should be handled explicitly
          raise exception unless defined?(Yantra::Error) && exception.is_a?(Yantra::Error)
        end

        # Main execution method called by ActiveJob.
        def perform(step_id, workflow_id, step_klass_name)
          log_job_info "Received job", step_id, workflow_id

          # Notify orchestrator that step processing is starting
          unless orchestrator.step_starting(step_id)
            log_job_warn "Orchestrator prevented step start", step_id, workflow_id
            return # Stop processing
          end

          # Fetch step details (including arguments)
          step_record = repository.find_step(step_id) # First find_step call
          unless step_record
            err = Yantra::Errors::StepNotFound.new("Step record #{step_id} not found after starting.") # Error raised here
            log_job_error err.message, step_id, workflow_id, err
            raise err # This will be caught by the specific StepNotFound rescue below
          end

          # Load the user's step implementation class
          user_step_klass = load_user_step_class(step_klass_name) # Can raise StepDefinitionError

          # Instantiate the user step class, passing required keywords
          begin
            user_step_instance = user_step_klass.new(
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
             # Wrap initialization ArgumentErrors as StepDefinitionError
             raise Yantra::Errors::StepDefinitionError.new("Failed to initialize #{step_klass_name}: #{e.message}. Check Step initializer arguments.", original_exception: e)
          end

          # Prepare arguments (symbolize keys)
          symbolized_args = begin
                              (step_record.arguments || {}).deep_symbolize_keys
                            rescue StandardError => e
                              log_job_warn("Failed to symbolize step arguments: #{e.message}. Using empty hash.", step_id, workflow_id)
                              {}
                            end


          # --- Execute User Code ---
          log_job_info "Executing user step code: #{step_klass_name}", step_id, workflow_id
          puts "[StepJob] Value of user_step_instance.id: #{user_step_instance.id} *****"
          output = user_step_instance.perform(**symbolized_args) # Can raise runtime errors
          # --- End User Code ---

          # Notify orchestrator of success
          orchestrator.step_succeeded(step_id, output)
          log_job_info "Step succeeded", step_id, workflow_id

        # --- Specific Rescues Order: More specific / setup errors first ---

        rescue LoadError, NameError => e
          # Catches direct LoadError/NameError if load_user_step_class itself fails unexpectedly
          # OR if constantize raises LoadError instead of NameError.
          err = Yantra::Errors::StepDefinitionError.new("Class #{step_klass_name} could not be loaded directly: #{e.message}", original_exception: e)
          log_job_error err.message, step_id, workflow_id, e
          error_info = { class: err.class.name, message: err.message, backtrace: e.backtrace }
          orchestrator.step_failed(step_id, error_info)
          raise err # Re-raise specific error

        rescue ArgumentError => e
          # Catches ArgumentError from user_step_instance.perform if keyword args don't match
          # (Initialization ArgumentErrors are wrapped in StepDefinitionError above)
          # Assume runtime ArgumentErrors are definition issues unless proven otherwise
          err = Yantra::Errors::StepDefinitionError.new("Argument mismatch calling #{step_klass_name}#perform. Check keyword arguments. Original error: #{e.message}", original_exception: e)
          log_job_error err.message, step_id, workflow_id, e
          error_info = { class: err.class.name, message: err.message, backtrace: e.backtrace }
          orchestrator.step_failed(step_id, error_info)
          raise err # Re-raise specific error

        # --- NEW: Specific rescue for StepDefinitionError ---
        rescue Yantra::Errors::StepDefinitionError => e
            # This specifically catches errors from:
            # 1. load_user_step_class failing to constantize (wrapped NameError)
            # 2. Step initialization failing (wrapped ArgumentError)
            # 3. ArgumentError during perform (wrapped above)
            # 4. Direct LoadError/NameError (wrapped above)
            log_job_error("Caught StepDefinitionError: #{e.message}. Step will be marked as failed.", step_id, workflow_id, e)

            # Explicitly notify orchestrator about the failure
            error_info = {
              class: e.class.name,
              message: e.message,
              # Include original backtrace if available, otherwise the wrapper's
              backtrace: e.original_exception&.backtrace || e.backtrace
            }
            orchestrator.step_failed(step_id, error_info) # Call step_failed directly

            # Re-raise the error for ActiveJob handling (e.g., mark job failed)
            raise e
        # --- END NEW BLOCK ---

        # --- Specific rescue for StepNotFound ---
        rescue Yantra::Errors::StepNotFound => e
          # This catches the StepNotFound raised specifically when the record isn't found after starting.
          log_job_error("Caught StepNotFound: #{e.message}. Step will be marked as failed.", step_id, workflow_id, e)
          # No need to call find_step again or invoke RetryHandler.
          # Optionally notify orchestrator, though raising might suffice depending on AJ failure handling.
          # error_info = { class: e.class.name, message: e.message }
          # orchestrator.step_failed(step_id, error_info) # Consider if needed
          raise e # Re-raise the error so ActiveJob handles it
        # --- END StepNotFound BLOCK ---

        # --- General Rescue for User Code Runtime Errors & Retries ---
        rescue => e
          # This block now catches errors from user_step_instance.perform that are not
          # ArgumentError, OR other unexpected StandardErrors.
          # It will *not* catch StepDefinitionError or StepNotFound.
          log_job_error "Error performing user step code: #{e.class} - #{e.message}", step_id, workflow_id, e

          # Re-fetch the record to get the latest state for RetryHandler
          current_step_record = repository.find_step(step_id) # Fetch #2 - Justified here
          unless current_step_record
             # Handle the unlikely case where the step disappears *during* error handling
             log_job_error "Step record #{step_id} not found after execution error!", step_id, workflow_id, e
             # Cannot proceed with retry logic if record is gone, raise original error
             raise e
          end

          # Load class again safely in case needed by handler (though unlikely for runtime errors)
          user_klass = load_user_step_class(step_klass_name) rescue nil

          # Instantiate RetryHandler
          handler = Yantra::Worker::RetryHandler.new(
            repository: repository,
            step_record: current_step_record, # Use re-fetched record
            error: e,
            executions: executions, # ActiveJob's execution count (0-based) + 1
            user_step_klass: user_klass,
            notifier: notifier,
            orchestrator: orchestrator
          )

          # Handle the error (retry or mark failed)
          handler.handle_error! # This might call orchestrator.step_failed internally
        end

        private

        # Helper to load the user's step class safely
        def load_user_step_class(class_name)
          class_name.constantize
        rescue NameError => e
          # Wrap NameError as our specific type
          raise Yantra::Errors::StepDefinitionError.new("Class #{class_name} could not be loaded: #{e.message}", original_exception: e)
        # Add rescue for LoadError just in case constantize raises it
        rescue LoadError => e
           raise Yantra::Errors::StepDefinitionError.new("Class file for #{class_name} could not be loaded: #{e.message}", original_exception: e)
        end

        # Accessor for Orchestrator instance (lazily initialized)
        def orchestrator
          @orchestrator ||= Yantra::Core::Orchestrator.new(
              repository: repository(),
              worker_adapter: worker_adapter(),
              notifier: notifier()
          )
        end

        # Accessor for Repository instance (lazily initialized)
        def repository
          @repository ||= Yantra.repository
        end

        # Accessor for Notifier instance (lazily initialized)
        def notifier
           @notifier ||= Yantra.notifier
        end

        # Accessor for Worker Adapter instance (lazily initialized)
        def worker_adapter
           @worker_adapter ||= Yantra.worker_adapter
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
          # Safely access backtrace, ensuring it's an array before joining
          backtrace_str = error&.backtrace.is_a?(Array) ? error.backtrace.join("\n") : "No backtrace available"
          full_message += "\nException: #{error.class}: #{error.message}\nBacktrace:\n#{backtrace_str}" if error
          Yantra.logger&.error { full_message }
        end

      end
    end
  end
end
