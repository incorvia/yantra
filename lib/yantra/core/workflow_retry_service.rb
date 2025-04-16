# lib/yantra/core/workflow_retry_service.rb

require_relative '../errors'
require_relative 'state_machine'

module Yantra
  module Core
    # Service class responsible for finding failed jobs in a workflow
    # and re-enqueuing them.
    class WorkflowRetryService
      attr_reader :workflow_id, :repository, :worker_adapter

      # @param workflow_id [String] The ID of the workflow to process.
      # @param repository [#get_workflow_steps, #update_step_attributes, #find_step] The persistence adapter.
      # @param worker_adapter [#enqueue] The worker adapter.
      def initialize(workflow_id:, repository:, worker_adapter:)
        @workflow_id = workflow_id
        @repository = repository
        @worker_adapter = worker_adapter
      end

      # Finds failed jobs and attempts to re-enqueue them.
      # @return [Integer] The number of jobs successfully re-enqueued.
      def call
        failed_steps = find_failed_steps
        if failed_steps.empty?
          puts "INFO: [WorkflowRetryService] No jobs found in 'failed' state for workflow #{workflow_id}."
          return 0
        end

        puts "INFO: [WorkflowRetryService] Found #{failed_steps.size} failed jobs to retry: #{failed_steps.map(&:id)}."
        reenqueue_jobs(failed_steps)
      end

      private

      # Fetches jobs in the 'failed' state for the workflow.
      # @return [Array<Object>] Array of failed job objects from the repository.
      def find_failed_steps
        repository.get_workflow_steps(workflow_id, status: :failed)
      rescue StandardError => e
        puts "ERROR: [WorkflowRetryService] Failed to query failed jobs for workflow #{workflow_id}: #{e.message}"
        [] # Return empty array on error to prevent further processing
      end

      # Iterates through failed jobs and attempts to re-enqueue each one.
      # @param jobs [Array<Object>] Array of failed job objects.
      # @return [Integer] Count of successfully re-enqueued jobs.
      def reenqueue_jobs(jobs)
        reenqueued_count = 0
        jobs.each do |job|
          if reenqueue_job(job)
            reenqueued_count += 1
          end
        end
        reenqueued_count
      end

      # Handles the state update and enqueuing for a single job.
      # @param job [Object] A failed job object from the repository.
      # @return [Boolean] true if successfully re-enqueued, false otherwise.
      def reenqueue_job(job)
        # Validate transition (failed -> enqueued)
        StateMachine.validate_transition!(:failed, :enqueued) # Let it raise if invalid

        # Update job state back to enqueued (atomically)
        step_update_success = repository.update_step_attributes(
          job.id,
          {
            state: Core::StateMachine::ENQUEUED.to_s,
            enqueued_at: Time.current,
            finished_at: nil # Clear finished_at timestamp
            # error: nil # Optionally clear error
          },
          expected_old_state: :failed
        )

        if step_update_success
          # Enqueue the job via the worker adapter
          queue_to_use = job.queue || 'default'
          worker_adapter.enqueue(job.id, job.workflow_id, job.klass, queue_to_use)
          puts "INFO: [WorkflowRetryService] Job #{job.id} re-enqueued."
          # TODO: Emit job.retrying event?
          true
        else
          latest_step_state = repository.find_step(job.id)&.state || 'unknown'
          puts "WARN: [WorkflowRetryService] Failed to update job #{job.id} state to enqueued (maybe state changed concurrently? Current state: #{latest_step_state}). Skipping enqueue."
          false
        end

      rescue Yantra::Errors::InvalidStateTransition => e
        puts "ERROR: [WorkflowRetryService] Invalid state transition for job #{job.id}. #{e.message}"
        false
      rescue Yantra::Errors::WorkerError => e
        puts "ERROR: [WorkflowRetryService] Worker error enqueuing job #{job.id}. #{e.message}"
        false # Failed to enqueue this specific job
      rescue StandardError => e
        puts "ERROR: [WorkflowRetryService] Unexpected error retrying job #{job.id}: #{e.class} - #{e.message}"
        false # Failed to enqueue this specific job
      end

    end
  end
end

