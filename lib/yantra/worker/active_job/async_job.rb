# lib/yantra/worker/active_job/async_job.rb

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
    class ::Hash
      def deep_symbolize_keys
        each_with_object({}) do |(key, value), result|
          new_key = key.to_sym rescue key
          new_value = value.is_a?(Hash) ? value.deep_symbolize_keys : value
          result[new_key] = new_value
        end
      end
    end
    puts "WARN: ActiveSupport not found. Using basic deep_symbolize_keys fallback."
  end
  false # Indicate AS version was not loaded
end


require_relative '../../job'
require_relative '../../core/orchestrator'
require_relative '../../core/state_machine'
require_relative '../../errors'
require_relative '../retry_handler'

module Yantra
  module Worker
    module ActiveJob
      # This ActiveJob class is enqueued by ActiveJob::Adapter.
      # Its perform method executes the actual Yantra::Job logic asynchronously.
      # It delegates error/retry handling to a RetryHandler class.
      class AsyncJob < ::ActiveJob::Base

        # Main execution logic called by ActiveJob backend.
        def perform(yantra_job_id, yantra_workflow_id, yantra_job_klass_name)
          puts "INFO: [AJ::AsyncJob] Attempt ##{self.executions} for Yantra job: #{yantra_job_id} WF: #{yantra_workflow_id} Klass: #{yantra_job_klass_name}"
          repo = Yantra.repository
          orchestrator = Yantra::Core::Orchestrator.new

          # --- 1. Fetch Job & Validate State ---
          job_record = repo.find_job(yantra_job_id)
          # ... (validation remains same) ...
          unless job_record
            puts "ERROR: [AJ::AsyncJob] Job record #{yantra_job_id} not found. Cannot execute."
            return
          end
          current_state = job_record.state.to_sym
          unless [Yantra::Core::StateMachine::ENQUEUED, Yantra::Core::StateMachine::RUNNING].include?(current_state)
            puts "WARN: [AJ::AsyncJob] Job #{yantra_job_id} expected state :enqueued or :running but found :#{current_state}. Aborting execution."
            return
          end

          # --- 2. Update State to Running (if not already) ---
          # ... (logic remains same) ...
          if current_state == Yantra::Core::StateMachine::ENQUEUED
            puts "DEBUG: [AJ::AsyncJob] Setting job #{yantra_job_id} to running."
            update_attrs = { state: Yantra::Core::StateMachine::RUNNING.to_s, started_at: Time.current }
            unless repo.update_job_attributes(yantra_job_id, update_attrs, expected_old_state: :enqueued)
               puts "WARN: [AJ::AsyncJob] Failed to update job #{yantra_job_id} state to running (maybe already changed?). Aborting."
               return
            end
          end


          user_job_klass = nil # Define outside begin block
          begin
            # --- 3. Instantiate User's Job Class ---
            user_job_klass = Object.const_get(yantra_job_klass_name) rescue nil
            unless user_job_klass && user_job_klass < Yantra::Job
               raise Yantra::Errors::JobDefinitionError, "Class #{yantra_job_klass_name} not found or not a Yantra::Job subclass."
            end

            # Fetch arguments and DEEP symbolize keys
            arguments_hash_original = job_record.arguments || {}
            puts "DEBUG: [AJ::AsyncJob] Arguments BEFORE symbolize: #{arguments_hash_original.inspect} (Class: #{arguments_hash_original.class})"
            puts "DEBUG: [AJ::AsyncJob] ActiveSupport deep_symbolize_keys loaded? #{AS_DEEP_SYMBOLIZE_LOADED}"
            puts "DEBUG: [AJ::AsyncJob] Hash responds to deep_symbolize_keys? #{arguments_hash_original.respond_to?(:deep_symbolize_keys)}"

            arguments_hash_symbolized = if arguments_hash_original.respond_to?(:deep_symbolize_keys)
                                          arguments_hash_original.deep_symbolize_keys
                                        else
                                          puts "WARN: [AJ::AsyncJob] deep_symbolize_keys not available! Falling back to shallow transform."
                                          arguments_hash_original.transform_keys(&:to_sym) rescue arguments_hash_original
                                        end

            puts "DEBUG: [AJ::AsyncJob] Arguments AFTER symbolize: #{arguments_hash_symbolized.inspect} (Class: #{arguments_hash_symbolized.class})"
            # Explicitly check nested key type after symbolization
            if arguments_hash_symbolized.key?(:input_data) && arguments_hash_symbolized[:input_data].is_a?(Hash)
              nested_hash = arguments_hash_symbolized[:input_data]
              puts "DEBUG: [AJ::AsyncJob] Nested input_data class: #{nested_hash.class}"
              puts "DEBUG: [AJ::AsyncJob] Nested input_data keys: #{nested_hash.keys.inspect}"
              puts "DEBUG: [AJ::AsyncJob] Nested input_data[:a_out] value: #{nested_hash[:a_out].inspect}"
              puts "DEBUG: [AJ::AsyncJob] Nested input_data['a_out'] value: #{nested_hash['a_out'].inspect}"
            else
              puts "DEBUG: [AJ::AsyncJob] Nested :input_data key not found or not a Hash after symbolize."
            end

            user_job_instance = user_job_klass.new(
              id: job_record.id, workflow_id: job_record.workflow_id,
              klass: user_job_klass, arguments: arguments_hash_symbolized # Pass potentially symbolized hash
            )

            # --- 4. Execute User's Code ---
            puts "INFO: [AJ::AsyncJob] Executing user perform for job #{yantra_job_id} (Attempt: #{self.executions})"
            # Pass arguments hash using double splat `**`
            result = user_job_instance.perform(**arguments_hash_symbolized)
            puts "INFO: [AJ::AsyncJob] Job #{yantra_job_id} user perform finished successfully."

            # --- 5. Update State to Succeeded ---
            # ... (logic remains same) ...
            puts "DEBUG: [AJ::AsyncJob] Setting job #{yantra_job_id} to succeeded."
            final_attrs = { state: Yantra::Core::StateMachine::SUCCEEDED.to_s, finished_at: Time.current, output: result }
            update_success = repo.update_job_attributes(yantra_job_id, final_attrs, expected_old_state: :running)

            # --- 6. Notify Orchestrator (only on success update) ---
            # ... (logic remains same) ...
            if update_success
               puts "INFO: [AJ::AsyncJob] Notifying orchestrator job finished (succeeded) for #{yantra_job_id}"
               orchestrator.job_finished(yantra_job_id)
            else
               puts "WARN: [AJ::AsyncJob] Failed to update job #{yantra_job_id} state to succeeded (maybe already changed?). Not notifying orchestrator."
            end

          rescue StandardError => e
            # --- 7. Delegate Error/Retry Handling ---
            # ... (logic remains same) ...
            puts "ERROR: [AJ::AsyncJob] Job #{yantra_job_id} user perform failed on attempt #{self.executions}. Delegating to RetryHandler."
            retry_handler_class = Yantra.configuration.try(:retry_handler_class) || Yantra::Worker::RetryHandler
            handler = retry_handler_class.new(
              repository: repo, job_record: job_record, error: e,
              executions: self.executions, user_job_klass: user_job_klass || Yantra::Job
            )
            handler.handle_error!
          end
        end # end perform

      end # class AsyncJob
    end
  end
end

