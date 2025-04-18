# --- lib/yantra/worker/active_job/step_job.rb ---

# Conditionally require 'active_job' and define a basic fallback if not found.
begin
  require 'active_job'
rescue LoadError
  # Define a minimal ActiveJob::Base if it doesn't exist,
  # allowing the StepJob class definition to proceed without error,
  # although ActiveJob functionality would be missing.
  unless defined?(ActiveJob::Base)
    module ActiveJob
      class Base
        # Mock methods used by some frameworks or initialization logic
        def self.set(*); self; end
        def self.perform_later(*); end
        # Add executions attribute if needed by retry logic fallbacks
        attr_accessor :executions
      end
    end
    puts "WARN: [Yantra::AJ::StepJob] 'active_job' gem not found. Using basic fallback ActiveJob::Base." if Yantra.logger
  end
end

# Conditionally require 'active_support/core_ext/hash/keys' for deep_symbolize_keys
# and define a fallback implementation if not available.
AS_DEEP_SYMBOLIZE_LOADED = begin
  require 'active_support/core_ext/hash/keys'
  true
rescue LoadError
  # Define deep_symbolize_keys directly on Hash if it's missing.
  unless Hash.method_defined?(:deep_symbolize_keys)
    puts "WARN: [Yantra::AJ::StepJob] 'active_support/core_ext/hash/keys' not found. Defining fallback Hash#deep_symbolize_keys." if Yantra.logger
    class ::Hash
      def deep_symbolize_keys
        each_with_object({}) do |(key, value), result|
          new_key = begin
                      key.to_sym
                    rescue
                      key # Keep original key if conversion fails
                    end
          new_value = value.is_a?(Hash) ? value.deep_symbolize_keys : value
          result[new_key] = new_value
        end
      end
    end
  end
  false
end

require_relative '../../step'
require_relative '../../core/orchestrator'
require_relative '../../core/state_machine'
require_relative '../../errors'
require_relative '../retry_handler' # Ensure RetryHandler is loaded

module Yantra
  module Worker
    module ActiveJob
      # ActiveJob wrapper for executing Yantra::Step instances.
      class StepJob < ::ActiveJob::Base
        # Ensure executions attribute exists, potentially needed by ActiveJob retry mechanisms
        # or the fallback RetryHandler.
        attr_accessor :executions unless defined?(executions)

        # Main perform method called by ActiveJob.
        # Arguments are provided by the Yantra::Worker::ActiveJob::Adapter.
        def perform(yantra_step_id, yantra_workflow_id, yantra_step_klass_name)
          # Initialize execution count (used by retry logic). ActiveJob might manage this itself.
          self.executions = self.executions || 1 # Default to 1 if not set by AJ

          # Use Yantra logger if available, otherwise fallback to puts
          log_method = Yantra.logger ? ->(level, msg) { Yantra.logger.send(level, "[AJ::StepJob] #{msg}") } : ->(level, msg) { puts "#{level.upcase}: [AJ::StepJob] #{msg}" }

          log_method.call(:info, "Attempt ##{self.executions} for Yantra step: #{yantra_step_id} WF: #{yantra_workflow_id} Klass: #{yantra_step_klass_name}")

          # --- Setup Dependencies ---
          repo = Yantra.repository
          notifier = Yantra.notifier # Get notifier for Orchestrator and potentially RetryHandler
          unless repo
            raise Yantra::Errors::ConfigurationError, "Yantra repository not configured."
          end
          # Pass along notifier if available
          orchestrator = Yantra::Core::Orchestrator.new(repository: repo, notifier: notifier)

          step_record = nil
          user_step_klass = nil

          # --- Pre-Execution Check ---
          # Ask the orchestrator if it's okay to start this step.
          # This handles state transitions and prevents starting already completed/failed steps.
          unless orchestrator.step_starting(yantra_step_id)
            log_method.call(:warn, "Orchestrator#step_starting indicated job #{yantra_step_id} should not proceed. Aborting.")
            return # Do not proceed if orchestrator says no
          end

          # --- Main Execution Block ---
          begin
            # Load the step record from the repository.
            step_record = repo.find_step(yantra_step_id)
            unless step_record
              # This should ideally not happen if step_starting succeeded.
              raise Yantra::Errors::StepNotFound, "Job record #{yantra_step_id} not found after starting."
            end
            # Use class name from the record for consistency.
            yantra_step_klass_name = step_record.klass

            # Load the user's Step class.
            begin
              user_step_klass = Object.const_get(yantra_step_klass_name)
            rescue NameError => e
              raise Yantra::Errors::StepDefinitionError, "Class #{yantra_step_klass_name} could not be loaded: #{e.message}"
            end
            unless user_step_klass && user_step_klass < Yantra::Step
              raise Yantra::Errors::StepDefinitionError, "Class #{yantra_step_klass_name} is not a Yantra::Step subclass."
            end

            # --- Argument Processing ---
            # Deserialize and symbolize arguments from the step record.
            arguments_hash = process_arguments(step_record.arguments, log_method)
            final_arguments_hash = symbolize_arguments(arguments_hash, log_method)

            # --- Step Instantiation ---
            parent_ids = repo.get_step_dependencies(yantra_step_id)
            user_step_instance = user_step_klass.new(
              step_id: step_record.id,
              workflow_id: step_record.workflow_id,
              klass: user_step_klass,
              arguments: final_arguments_hash, # Pass symbolized hash
              parent_ids: parent_ids,
              queue_name: step_record.queue,
              repository: repo # Inject repository for potential use within the step
            )

            # Ensure keys are symbols for splat operator (**).
            # This might be redundant if symbolize_arguments worked correctly.
            final_arguments_hash_for_perform = final_arguments_hash.transform_keys(&:to_sym) rescue {}
            log_method.call(:debug, "Arguments being passed to perform: #{final_arguments_hash_for_perform.inspect}")

            # --- Execute the User's Step Logic ---
            result = user_step_instance.perform(**final_arguments_hash_for_perform)

            # --- Success Handling ---
            # Notify the orchestrator of successful completion.
            orchestrator.step_succeeded(yantra_step_id, result)
            log_method.call(:info, "Step #{yantra_step_id} succeeded.")

          # --- Error Handling Block ---
          rescue StandardError => e
            log_method.call(:error, "Rescued error in perform for step #{yantra_step_id}: #{e.class} - #{e.message}\n#{e.backtrace.first(5).join("\n")}")

            # Cannot proceed with error handling if the step record wasn't loaded.
            unless step_record
              log_method.call(:fatal, "Cannot handle error - step_record not loaded. Re-raising.")
              raise e
            end

            # Determine which user class to use for retry configuration (loaded class or base Step).
            user_klass_for_handler = user_step_klass || Yantra::Step
            # Get configured or default retry handler class.
            retry_handler_class = (Yantra.configuration.respond_to?(:retry_handler_class) && Yantra.configuration.retry_handler_class) || Yantra::Worker::RetryHandler

            log_method.call(:debug, "Using RetryHandler: #{retry_handler_class}")
            log_method.call(:debug, "Current executions: #{self.executions}")

            # Instantiate the retry handler.
            handler = retry_handler_class.new(
              repository: repo,
              step_record: step_record,
              error: e,
              executions: self.executions,
              user_step_klass: user_klass_for_handler,
              notifier: notifier # Pass notifier for potential event publishing in handler
            )

            # --- Retry/Failure Decision ---
            begin
              # Let the handler decide the outcome (e.g., :retry, :failed, or raise for retry).
              outcome = handler.handle_error!
              log_method.call(:debug, "RetryHandler outcome: #{outcome.inspect}")

              if outcome == :failed
                # The handler decided the step has permanently failed and updated the record.
                log_method.call(:debug, "Outcome is :failed. Notifying orchestrator step finished.")
                # Double-check the state before notifying orchestrator.
                final_record = repo.find_step(yantra_step_id)
                if final_record&.state&.to_sym == Yantra::Core::StateMachine::FAILED
                  # Tell orchestrator to process dependents/check workflow completion.
                  orchestrator.step_finished(yantra_step_id)
                else
                  log_method.call(:warn, "RetryHandler indicated permanent failure, but step state is not 'failed'. State: #{final_record&.state}. Orchestrator#step_finished NOT called.")
                end
                # Do NOT re-raise the error; the job is considered handled (failed).
              else
                # Handler should have raised an error if retry is needed by ActiveJob.
                # If it returns anything else, we assume retry is needed and re-raise original error.
                log_method.call(:debug, "Outcome is NOT :failed. Re-raising error for background job system retry.")
                raise e # Re-raise the original error 'e' to trigger ActiveJob retry.
              end

            # Rescue errors *from the handler itself*.
            rescue => retry_handler_error
              log_method.call(:error, "Error occurred within RetryHandler: #{retry_handler_error.class} - #{retry_handler_error.message}")
              # If the handler raised something different than the original error, log it,
              # but still raise the *original* error 'e' for ActiveJob's retry mechanism.
              unless retry_handler_error.equal?(e)
                log_method.call(:warn, "RetryHandler raised a different error than expected. Propagating original error '#{e.class}' for retry.")
              end
              # Raise the original error to let ActiveJob handle the retry based on it.
              raise e
            end # Inner begin/rescue for handler execution
          end # Outer begin/rescue for perform execution
        end # def perform

        private

        # Helper to process raw arguments from the database.
        def process_arguments(raw_arguments, log_method)
          arguments_hash = {}
          if raw_arguments.is_a?(Hash)
            arguments_hash = raw_arguments
          elsif raw_arguments.is_a?(String) && !raw_arguments.empty?
            begin
              arguments_hash = JSON.parse(raw_arguments)
              # Ensure it's a hash after parsing
              arguments_hash = {} unless arguments_hash.is_a?(Hash)
            rescue JSON::ParserError => json_error
              log_method.call(:warn, "Failed to parse arguments JSON: #{json_error.message}. Using empty hash.")
              arguments_hash = {}
            end
          elsif raw_arguments.nil?
            arguments_hash = {} # Explicitly handle nil
          else
            log_method.call(:warn, "Unexpected argument type: #{raw_arguments.class}. Using empty hash.")
            arguments_hash = {}
          end
          arguments_hash
        end

        # Helper to symbolize argument keys.
        def symbolize_arguments(arguments_hash, log_method)
          # Use deep_symbolize_keys if available (handles nested hashes)
          if arguments_hash.respond_to?(:deep_symbolize_keys)
            begin
              arguments_hash.deep_symbolize_keys
            rescue => e
              log_method.call(:warn, "deep_symbolize_keys failed: #{e.message}. Falling back to shallow symbolization.")
              # Fallback to shallow symbolization on error
              arguments_hash.transform_keys(&:to_sym) rescue {}
            end
          else
            # Fallback to shallow symbolization if deep_symbolize_keys is not defined
            arguments_hash.transform_keys(&:to_sym) rescue {}
          end
        end

      end # class StepJob
    end # module ActiveJob
  end # module Worker
end # module Yantra
