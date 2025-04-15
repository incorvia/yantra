# lib/yantra/worker/active_job/async_job.rb

begin
  require 'active_job'
rescue LoadError
  puts "WARN: 'active_job' gem not found. Yantra ActiveJob adapter requires it."
  Object.const_set("ActiveJob", Module.new { const_set("Base", Class.new) })
end

require_relative '../../job'
require_relative '../../core/orchestrator'
require_relative '../../core/state_machine'
require_relative '../../errors'
require_relative '../retry_handler' # <<< REQUIRE the new handler

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
          orchestrator = Yantra::Core::Orchestrator.new # Used only on final success/failure notification

          # --- 1. Fetch Job & Validate State ---
          job_record = repo.find_job(yantra_job_id)
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
          if current_state == Yantra::Core::StateMachine::ENQUEUED
            puts "DEBUG: [AJ::AsyncJob] Setting job #{yantra_job_id} to running."
            update_attrs = { state: Yantra::Core::StateMachine::RUNNING.to_s, started_at: Time.current }
            unless repo.update_job_attributes(yantra_job_id, update_attrs, expected_old_state: :enqueued)
               puts "WARN: [AJ::AsyncJob] Failed to update job #{yantra_job_id} state to running (maybe already changed?). Aborting."
               return
            end
            # TODO: Emit yantra.job.started event
          end

          user_job_klass = nil # Define outside begin block
          begin
            # --- 3. Instantiate User's Job Class ---
            user_job_klass = Object.const_get(yantra_job_klass_name) rescue nil
            unless user_job_klass && user_job_klass < Yantra::Job
               raise Yantra::Errors::JobDefinitionError, "Class #{yantra_job_klass_name} not found or not a Yantra::Job subclass."
            end
            arguments_hash = job_record.arguments || {}
            arguments_hash = arguments_hash.transform_keys(&:to_sym) rescue arguments_hash
            user_job_instance = user_job_klass.new(
              id: job_record.id, workflow_id: job_record.workflow_id,
              klass: user_job_klass, arguments: arguments_hash
            )

            # --- 4. Execute User's Code ---
            puts "INFO: [AJ::AsyncJob] Executing user perform for job #{yantra_job_id} (Attempt: #{self.executions})"
            result = user_job_instance.perform(**arguments_hash)
            puts "INFO: [AJ::AsyncJob] Job #{yantra_job_id} user perform finished successfully."

            # --- 5. Update State to Succeeded ---
            puts "DEBUG: [AJ::AsyncJob] Setting job #{yantra_job_id} to succeeded."
            final_attrs = {
              state: Yantra::Core::StateMachine::SUCCEEDED.to_s,
              finished_at: Time.current,
              output: result
            }
            update_success = repo.update_job_attributes(yantra_job_id, final_attrs, expected_old_state: :running)
            # TODO: Emit yantra.job.succeeded event

            # --- 6. Notify Orchestrator (only on success update) ---
            if update_success
               puts "INFO: [AJ::AsyncJob] Notifying orchestrator job finished (succeeded) for #{yantra_job_id}"
               orchestrator.job_finished(yantra_job_id)
            else
               puts "WARN: [AJ::AsyncJob] Failed to update job #{yantra_job_id} state to succeeded (maybe already changed?). Not notifying orchestrator."
            end

          rescue StandardError => e
            # --- 7. Delegate Error/Retry Handling ---
            puts "ERROR: [AJ::AsyncJob] Job #{yantra_job_id} user perform failed on attempt #{self.executions}. Delegating to RetryHandler."

            # Determine which handler class to use (from config or default)
            # TODO: Add config.retry_handler_class to Yantra::Configuration
            retry_handler_class = Yantra.configuration.try(:retry_handler_class) || Yantra::Worker::RetryHandler

            # Instantiate handler and call it
            handler = retry_handler_class.new(
              repository: repo,
              job_record: job_record, # Pass the fetched record
              error: e,
              executions: self.executions,
              user_job_klass: user_job_klass || Yantra::Job # Pass klass if loaded, else base
            )
            handler.handle_error! # This will re-raise if retry is needed

          # Note: No 'ensure' block needed here to call orchestrator.job_finished,
          # because it's called explicitly on *final* success or *final* failure (inside the handler).
          end
        end # end perform

      end # class AsyncJob
    end
  end
end

