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
    puts "WARN: ActiveSupport not found. Defining basic deep_symbolize_keys fallback for Hash."
    class ::Hash
      def deep_symbolize_keys
        each_with_object({}) do |(key, value), result| # Start block
          new_key = key.to_sym rescue key
          new_value = value.is_a?(Hash) ? value.deep_symbolize_keys : value
          result[new_key] = new_value
        end # End block <<< THIS WAS MISSING/IMPLIED BY COMMENT
      end
    end # End class ::Hash
  end # End unless
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
      class AsyncJob < ::ActiveJob::Base # Inherit from the loaded ActiveJob::Base

        # Main execution logic called by ActiveJob backend.
        def perform(yantra_job_id, yantra_workflow_id, yantra_job_klass_name)
          puts "INFO: [AJ::AsyncJob] Attempt ##{self.executions} for Yantra job: #{yantra_job_id} WF: #{yantra_workflow_id} Klass: #{yantra_job_klass_name}"
          repo = Yantra.repository
          orchestrator = Yantra::Core::Orchestrator.new

          # --- 1. Notify Orchestrator: Starting ---
          unless orchestrator.job_starting(yantra_job_id)
             puts "WARN: [AJ::AsyncJob] Orchestrator#job_starting indicated job #{yantra_job_id} should not proceed. Aborting."
             return
          end

          # --- 2. Execute User Code ---
          job_record = nil
          user_job_klass = nil
          begin
            # Fetch job record
            job_record = repo.find_job(yantra_job_id)
            unless job_record
               raise Yantra::Errors::PersistenceError, "Job record #{yantra_job_id} not found after starting."
            end

            # Instantiate User's Job Class
            user_job_klass = Object.const_get(yantra_job_klass_name) rescue nil
            unless user_job_klass && user_job_klass < Yantra::Job
               raise Yantra::Errors::JobDefinitionError, "Class #{yantra_job_klass_name} not found or not a Yantra::Job subclass."
            end

            arguments_hash = job_record.arguments || {}
            # Perform deep symbolization
            if arguments_hash.respond_to?(:deep_symbolize_keys)
              arguments_hash = arguments_hash.deep_symbolize_keys
            else
              # Fallback if deep_symbolize_keys wasn't defined (e.g., no AS and fallback failed)
              arguments_hash = arguments_hash.transform_keys(&:to_sym) rescue arguments_hash
            end

            user_job_instance = user_job_klass.new(
              id: job_record.id, workflow_id: job_record.workflow_id,
              klass: user_job_klass, arguments: arguments_hash
            )

            # Execute User's Code
            result = user_job_instance.perform(**arguments_hash)

            # --- 3a. Notify Orchestrator: Success ---
            orchestrator.job_succeeded(yantra_job_id, result)

          rescue StandardError => e
            # --- 3b. Handle Failure via RetryHandler ---
            puts "ERROR: [AJ::AsyncJob] Job #{yantra_job_id} failed on attempt #{self.executions}. Delegating to RetryHandler."
            unless job_record # Should have been loaded in begin block
              puts "FATAL: [AJ::AsyncJob] Cannot handle error for job #{yantra_job_id} - job_record not loaded."
              raise e
            end

            retry_handler_class = Yantra.configuration.try(:retry_handler_class) || Yantra::Worker::RetryHandler
            handler = retry_handler_class.new(
              repository: repo,
              job_record: job_record,
              error: e,
              executions: self.executions,
              user_job_klass: user_job_klass || Yantra::Job
            )

            begin
              outcome = handler.handle_error!
              if outcome == :failed
                puts "INFO: [AJ::AsyncJob] Notifying orchestrator job finished (failed permanently) for #{yantra_job_id}"
                orchestrator.job_finished(yantra_job_id)
              end
            rescue => retry_error
              puts "DEBUG: [AJ::AsyncJob] Re-raising error for ActiveJob retry: #{retry_error.class}"
              raise retry_error
            end
          end
        end # end perform

      end # class AsyncJob
    end
  end
end

