# lib/yantra/worker/active_job/async_job.rb # <-- Filename might change too

# Ensure ActiveJob is loaded (usually via Rails or requiring 'active_job')
begin
  require 'active_job'
rescue LoadError
  puts "WARN: 'active_job' gem not found. Yantra ActiveJob adapter requires it."
  # Define dummy class to avoid downstream loading errors if ActiveJob is optional
  Object.const_set("ActiveJob", Module.new { const_set("Base", Class.new) })
end

require_relative '../../job' # Base Yantra::Job
require_relative '../../core/orchestrator'
require_relative '../../core/state_machine'
require_relative '../../errors'
# Assuming Yantra module provides access to configured repository
# require_relative '../../yantra'

module Yantra
  module Worker
    module ActiveJob
      # This ActiveJob class is enqueued by ActiveJob::Adapter.
      # Its perform method executes the actual Yantra::Job logic asynchronously.
      class AsyncJob < ::ActiveJob::Base # <<< RENAMED CLASS HERE
        # queue_as :default # Can set a default queue here if desired

        # ArgumentError if job not found or in wrong state? Retry?
        # rescue_from ActiveRecord::RecordNotFound --> perhaps retry?

        # Main execution logic called by ActiveJob backend.
        # @param yantra_job_id [String] The UUID of the Yantra::Job record.
        # @param yantra_workflow_id [String] The UUID of the parent workflow.
        # @param yantra_job_klass_name [String] The class name of the user's Yantra::Job.
        def perform(yantra_job_id, yantra_workflow_id, yantra_job_klass_name)
          # Renamed class name in log messages for clarity
          puts "INFO: [AJ::AsyncJob] Starting Yantra job: #{yantra_job_id} WF: #{yantra_workflow_id} Klass: #{yantra_job_klass_name}"
          repo = Yantra.repository
          # TODO: Consider if Orchestrator should be a singleton or passed differently
          orchestrator = Yantra::Core::Orchestrator.new

          # --- 1. Fetch Job & Validate State ---
          job_record = repo.find_job(yantra_job_id)

          unless job_record
            puts "ERROR: [AJ::AsyncJob] Job record #{yantra_job_id} not found. Cannot execute."
            return
          end

          current_state = job_record.state.to_sym
          unless current_state == Yantra::Core::StateMachine::ENQUEUED
            puts "WARN: [AJ::AsyncJob] Job #{yantra_job_id} expected state :enqueued but found :#{current_state}. Aborting execution."
            return
          end

          # --- 2. Update State to Running ---
          puts "DEBUG: [AJ::AsyncJob] Setting job #{yantra_job_id} to running."
          update_attrs = { state: Yantra::Core::StateMachine::RUNNING.to_s, started_at: Time.current }
          unless repo.update_job_attributes(yantra_job_id, update_attrs, expected_old_state: :enqueued)
             puts "WARN: [AJ::AsyncJob] Failed to update job #{yantra_job_id} state to running (maybe already changed?). Aborting."
             return
          end
          # TODO: Emit yantra.job.started event

          begin
            # --- 3. Instantiate User's Job Class ---
            user_job_klass = Object.const_get(yantra_job_klass_name) # TODO: Add rescue NameError
            arguments_hash = job_record.arguments || {}
            arguments_hash = arguments_hash.transform_keys do |k|
               k.to_sym rescue k
            end

            user_job_instance = user_job_klass.new(
              id: job_record.id,
              workflow_id: job_record.workflow_id,
              klass: user_job_klass,
              arguments: arguments_hash
              # Add other state from job_record if needed by Job#initialize via internal_state
            )

            # --- 4. Execute User's Code ---
            puts "INFO: [AJ::AsyncJob] Executing user perform for job #{yantra_job_id}"
            result = user_job_instance.perform(**arguments_hash)
            puts "INFO: [AJ::AsyncJob] Job #{yantra_job_id} user perform finished."

            # --- 5. Update State to Succeeded ---
            puts "DEBUG: [AJ::AsyncJob] Setting job #{yantra_job_id} to succeeded."
            final_attrs = {
              state: Yantra::Core::StateMachine::SUCCEEDED.to_s,
              finished_at: Time.current,
              output: result
            }
            repo.update_job_attributes(yantra_job_id, final_attrs, expected_old_state: :running)
            # TODO: Emit yantra.job.succeeded event

          rescue StandardError => e
            # --- 6. Update State to Failed on Error ---
            puts "ERROR: [AJ::AsyncJob] Job #{yantra_job_id} user perform failed: #{e.class} - #{e.message}"
            puts e.backtrace.take(15).join("\n")

            # TODO: Implement Retry Logic Check Here

            final_attrs = {
              state: Yantra::Core::StateMachine::FAILED.to_s,
              finished_at: Time.current
            }
            repo.update_job_attributes(yantra_job_id, final_attrs, expected_old_state: :running)
            repo.record_job_error(yantra_job_id, e)
            repo.set_workflow_has_failures_flag(yantra_workflow_id)
            # TODO: Emit yantra.job.failed event

          ensure
            # --- 7. Notify Orchestrator ---
            if job_record
              puts "INFO: [AJ::AsyncJob] Notifying orchestrator job finished for #{yantra_job_id}"
              orchestrator.job_finished(yantra_job_id)
            end
          end
        end # end perform

      end # <<< END RENAMED CLASS
    end
  end
end

