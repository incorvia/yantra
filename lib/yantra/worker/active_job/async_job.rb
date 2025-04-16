# --- lib/yantra/worker/active_job/async_job.rb ---
# (Modify perform to catch NameError specifically)

begin
  require 'active_job'
rescue LoadError
  puts "WARN: 'active_job' gem not found. Yantra ActiveJob adapter requires it."
  Object.const_set("ActiveJob", Module.new { const_set("Base", Class.new) })
end

# Require for deep_symbolize_keys. Add 'activesupport' as a dependency if not already present.
AS_DEEP_SYMBOLIZE_LOADED = begin
  require 'active_support/core_ext/hash/keys'
  true
rescue LoadError
  # Define a simple fallback if ActiveSupport is not available
  unless Hash.method_defined?(:deep_symbolize_keys)
    puts "WARN: ActiveSupport not found. Defining basic deep_symbolize_keys fallback for Hash."
    class ::Hash
      def deep_symbolize_keys
        each_with_object({}) do |(key, value), result| # Start block
          new_key = key.to_sym rescue key
          new_value = value.is_a?(Hash) ? value.deep_symbolize_keys : value
          result[new_key] = new_value
        end # End block
      end
    end # End class ::Hash
  end # End unless
  false # Indicate AS version was not loaded
end


require_relative '../../step'
require_relative '../../core/orchestrator'
require_relative '../../core/state_machine'
require_relative '../../errors'
require_relative '../retry_handler'

module Yantra
  module Worker
    module ActiveJob
      # This ActiveJob class is enqueued by ActiveJob::Adapter.
      # Its perform method executes the actual Yantra::Step logic asynchronously.
      # It delegates error/retry handling to a RetryHandler class.
      class AsyncJob < ::ActiveJob::Base # Inherit from the loaded ActiveJob::Base

        # Main execution logic called by ActiveJob backend.
        def perform(yantra_step_id, yantra_workflow_id, yantra_step_klass_name)
          puts "INFO: [AJ::AsyncJob] Attempt ##{self.executions} for Yantra step: #{yantra_step_id} WF: #{yantra_workflow_id} Klass: #{yantra_step_klass_name}"
          repo = Yantra.repository
          orchestrator = Yantra::Core::Orchestrator.new

          # --- 1. Notify Orchestrator: Starting ---
          unless orchestrator.step_starting(yantra_step_id)
             puts "WARN: [AJ::AsyncJob] Orchestrator#step_starting indicated job #{yantra_step_id} should not proceed. Aborting."
             return
          end

          # --- 2. Execute User Code ---
          step_record = nil
          user_step_klass = nil
          begin
            # Fetch job record
            step_record = repo.find_step(yantra_step_id)
            unless step_record
               # Use specific error for clarity
               raise Yantra::Errors::StepNotFound, "Job record #{yantra_step_id} not found after starting."
            end

            # --- Specific Rescue for Class Loading ---
            begin
              # Attempt to load the user's job class by name
              user_step_klass = Object.const_get(yantra_step_klass_name)
            rescue NameError => e
              # If class name is invalid/not found, raise a specific Yantra error
              # This prevents it falling into the general StandardError rescue below
              raise Yantra::Errors::StepDefinitionError, "Class #{yantra_step_klass_name} could not be loaded: #{e.message}"
            end
            # --- End Specific Rescue ---

            # Validate that the loaded constant is actually a Yantra::Step subclass
            unless user_step_klass && user_step_klass < Yantra::Step
               raise Yantra::Errors::StepDefinitionError, "Class #{yantra_step_klass_name} is not a Yantra::Step subclass."
            end

            # Prepare arguments for the user's perform method
            arguments_hash = step_record.arguments || {}
            if arguments_hash.respond_to?(:deep_symbolize_keys)
              arguments_hash = arguments_hash.deep_symbolize_keys
            else
              arguments_hash = arguments_hash.transform_keys(&:to_sym) rescue arguments_hash
            end

            # Instantiate the user's job class
            user_step_instance = user_step_klass.new(
              id: step_record.id, workflow_id: step_record.workflow_id,
              klass: user_step_klass, arguments: arguments_hash
            )

            # Execute the user's perform method
            result = user_step_instance.perform(**arguments_hash)

            # --- 3a. Notify Orchestrator: Success ---
            orchestrator.step_succeeded(yantra_step_id, result)

          # Rescue StepDefinitionError specifically if you want distinct handling,
          # otherwise it will be caught by StandardError.
          # rescue Yantra::Errors::StepDefinitionError => e
          #   puts "ERROR: [AJ::AsyncJob] Job definition error for #{yantra_step_id}: #{e.message}"
          #   # Decide how to handle this - likely fail permanently without retry?
          #   # Example: Mark as failed directly?
          #   # repo.update_step_attributes(yantra_step_id, { state: 'failed', finished_at: Time.current })
          #   # repo.record_step_error(yantra_step_id, e) # Record the definition error
          #   # repo.set_workflow_has_failures_flag(yantra_workflow_id)
          #   # orchestrator.step_finished(yantra_step_id) # Trigger downstream check

          rescue StandardError => e
            # --- 3b. Handle Failure via RetryHandler (for runtime errors) ---
            puts "ERROR: [AJ::AsyncJob] Job #{yantra_step_id} failed on attempt #{self.executions}. Delegating to RetryHandler."
            # Ensure step_record was loaded before the error
            unless step_record
              puts "FATAL: [AJ::AsyncJob] Cannot handle error for job #{yantra_step_id} - step_record not loaded."
              raise e # Re-raise original if step_record is missing here
            end
            # Ensure user_step_klass is available, default to base if const_get failed earlier
            # (though the specific rescue should prevent reaching here in that case)
            user_klass_for_handler = user_step_klass || Yantra::Step

            retry_handler_class = Yantra.configuration.try(:retry_handler_class) || Yantra::Worker::RetryHandler
            handler = retry_handler_class.new(
              repository: repo,
              step_record: step_record,
              error: e, # Pass the original runtime error
              executions: self.executions,
              user_step_klass: user_klass_for_handler
            )

            begin
              # handle_error! will:
              # 1. Call repo methods (increment retries, record error) and RAISE e if retrying
              # 2. Call repo methods (set state failed, record error, set flag) and RETURN :failed if permanent
              outcome = handler.handle_error!
              if outcome == :failed
                # If handler returned :failed, it means it handled the permanent failure state update.
                # We just need to notify the orchestrator to check downstream jobs/workflow state.
                puts "INFO: [AJ::AsyncJob] Notifying orchestrator job finished (failed permanently) for #{yantra_step_id}"
                orchestrator.step_finished(yantra_step_id)
              end
            rescue => retry_error # Catch the error re-raised by handler on retry path
              # Ensure we're re-raising the *original* error passed to the handler
              unless retry_error.equal?(e)
                 puts "WARN: [AJ::AsyncJob] RetryHandler raised a different error than expected. Re-raising original error."
                 retry_error = e
              end
              puts "DEBUG: [AJ::AsyncJob] Re-raising error for ActiveJob retry: #{retry_error.class}"
              raise retry_error # Let ActiveJob handle the retry scheduling
            end
          end
        end # end perform
      end # class AsyncJob
    end
  end
end

