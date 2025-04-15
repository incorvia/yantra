# lib/yantra/worker/active_job/async_job.rb

begin
  require 'active_job'
rescue LoadError
  puts "WARN: 'active_job' gem not found. Yantra ActiveJob adapter requires it."
  Object.const_set("ActiveJob", Module.new { const_set("Base", Class.new) })
end

# Require for deep_symbolize_keys. Add 'activesupport' as a dependency if not already present.
begin
  require 'active_support/core_ext/hash/keys'
rescue LoadError
  # Define a simple fallback if ActiveSupport is not available
  unless Hash.method_defined?(:deep_symbolize_keys)
    class ::Hash; def deep_symbolize_keys; each_with_object({}) { |(k, v), r| nk=k.to_sym rescue k; nv=v.is_a?(Hash) ? v.deep_symbolize_keys : v; r[nk]=nv }; end; end
    puts "WARN: ActiveSupport not found. Using basic deep_symbolize_keys fallback."
  end
end


require_relative '../../job'
require_relative '../../core/orchestrator' # Needs Orchestrator to report status
require_relative '../../errors'
# No longer needs RetryHandler directly

module Yantra
  module Worker
    module ActiveJob
      # This ActiveJob class is enqueued by ActiveJob::Adapter.
      # Its perform method executes the actual Yantra::Job logic asynchronously.
      # It coordinates with the Orchestrator for state updates and error handling.
      class AsyncJob < ::ActiveJob::Base
        # Note: Configure ActiveJob retry *scheduling* via standard AJ/backend mechanisms.
        # Yantra's Orchestrator (via RetryHandler) controls *final* failure state.

        # Main execution logic called by ActiveJob backend.
        def perform(yantra_job_id, yantra_workflow_id, yantra_job_klass_name)
          puts "INFO: [AJ::AsyncJob] Executing job: #{yantra_job_id} WF: #{yantra_workflow_id} Klass: #{yantra_job_klass_name} Attempt: #{self.executions}"

          # Instantiate orchestrator to report status changes
          # TODO: Consider dependency injection for Orchestrator instance
          orchestrator = Yantra::Core::Orchestrator.new
          job_record = nil # Define scope outside begin block

          # --- 1. Notify Orchestrator: Starting ---
          # Orchestrator finds job, validates state, updates state to :running
          # Returns true if okay to proceed, false otherwise.
          unless orchestrator.job_starting(yantra_job_id)
             puts "WARN: [AJ::AsyncJob] Orchestrator#job_starting indicated job #{yantra_job_id} should not proceed. Aborting."
             return # Do not proceed if orchestrator says no (e.g., state mismatch)
          end

          # --- 2. Execute User Code ---
          begin
            # Fetch job record again to get arguments (Orchestrator#job_starting already found it once)
            # TODO: Optimize - Orchestrator#job_starting could return needed data?
            repo = Yantra.repository
            job_record = repo.find_job(yantra_job_id)
            unless job_record
               # Should not happen if job_starting succeeded, but check defensively
               raise Yantra::Errors::PersistenceError, "Job record #{yantra_job_id} not found after starting."
            end

            # Instantiate User's Job Class
            user_job_klass = Object.const_get(yantra_job_klass_name) rescue nil
            unless user_job_klass && user_job_klass < Yantra::Job
               raise Yantra::Errors::JobDefinitionError, "Class #{yantra_job_klass_name} not found or not a Yantra::Job subclass."
            end

            arguments_hash = job_record.arguments || {}
            # Perform deep symbolization before passing to user code and perform
            if arguments_hash.respond_to?(:deep_symbolize_keys)
              arguments_hash = arguments_hash.deep_symbolize_keys
            else
              puts "WARN: [AJ::AsyncJob] deep_symbolize_keys not available!"
              arguments_hash = arguments_hash.transform_keys(&:to_sym) rescue arguments_hash
            end

            user_job_instance = user_job_klass.new(
              id: job_record.id, workflow_id: job_record.workflow_id,
              klass: user_job_klass, arguments: arguments_hash
            )

            # Execute User's Code
            puts "INFO: [AJ::AsyncJob] Executing user perform for job #{yantra_job_id}"
            result = user_job_instance.perform(**arguments_hash)
            puts "INFO: [AJ::AsyncJob] Job #{yantra_job_id} user perform finished successfully."

            # --- 3a. Notify Orchestrator: Success ---
            orchestrator.job_succeeded(yantra_job_id, result)

          rescue StandardError => e
            # --- 3b. Notify Orchestrator: Failure ---
            puts "ERROR: [AJ::AsyncJob] Job #{yantra_job_id} user perform failed on attempt #{self.executions}. Notifying orchestrator."
            # Pass necessary info to the orchestrator to handle retries/failure
            orchestrator.job_failed(
              yantra_job_id,
              e,
              self.executions # Pass current attempt number
              # user_job_klass is already loaded above
            )
            # Orchestrator#job_failed will call RetryHandler which re-raises if needed
          end
        end # end perform

      end # class AsyncJob
    end
  end
end

