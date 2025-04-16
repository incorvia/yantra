# lib/yantra/core/orchestrator.rb

require_relative '../errors'
require_relative '../step'
require_relative 'state_machine'
require_relative '../persistence/repository_interface'
require_relative '../worker/enqueuing_interface'
require_relative '../events/notifier_interface'

module Yantra
  module Core
    class Orchestrator
      attr_reader :repository, :worker_adapter, :notifier

      def initialize(repository: nil, worker_adapter: nil, notifier: nil)
        @repository = repository || Yantra.repository
        @worker_adapter = worker_adapter || Yantra.worker_adapter
        @notifier = notifier || Yantra.notifier

        # Validate dependencies using fully qualified error class name and is_a? check
        unless @repository && @repository.is_a?(Persistence::RepositoryInterface)
          raise Yantra::Errors::ConfigurationError, "Yantra Orchestrator requires a valid persistence repository adapter (must include Yantra::Persistence::RepositoryInterface)."
        end
        unless @worker_adapter && @worker_adapter.is_a?(Worker::EnqueuingInterface)
          raise Yantra::Errors::ConfigurationError, "Yantra Orchestrator requires a valid worker enqueuing adapter (must include Yantra::Worker::EnqueuingInterface)."
        end
        # *** FIX HERE: Change validation to use is_a? ***
        unless @notifier && @notifier.is_a?(Events::NotifierInterface)
           raise Yantra::Errors::ConfigurationError, "Yantra Orchestrator requires a valid event notifier adapter (must include Yantra::Events::NotifierInterface)."
        end
      end

      # ... rest of Orchestrator methods remain the same ...

      # Starts a workflow if it's in a pending state.
      def start_workflow(workflow_id)
        puts "INFO: [Orchestrator] Attempting to start workflow #{workflow_id}"
        update_success = repository.update_workflow_attributes(
          workflow_id,
          { state: StateMachine::RUNNING.to_s, started_at: Time.current },
          expected_old_state: StateMachine::PENDING
        )

        if update_success
          puts "INFO: [Orchestrator] Workflow #{workflow_id} state set to RUNNING."
          # TODO: Publish yantra.workflow.started event
          # wf_record = repository.find_workflow(workflow_id)
          # payload = { workflow_id: workflow_id, klass: wf_record&.klass, started_at: wf_record&.started_at }
          # @notifier.publish('yantra.workflow.started', payload)

          find_and_enqueue_ready_jobs(workflow_id)
          true
        else
          current_workflow = repository.find_workflow(workflow_id)
          if current_workflow.nil?
             puts "WARN: [Orchestrator] Workflow #{workflow_id} not found during start attempt."
          elsif current_workflow.state != StateMachine::PENDING.to_s
             puts "WARN: [Orchestrator] Workflow #{workflow_id} cannot start; already in state :#{current_workflow.state}."
          else
             puts "WARN: [Orchestrator] Failed to update workflow #{workflow_id} state to RUNNING (maybe state changed concurrently?)."
          end
          false
        end
      end

      # Called by the worker job just before executing the step's perform method.
      def step_starting(step_id)
        puts "INFO: [Orchestrator] Starting job #{step_id}"
        step = repository.find_step(step_id)
        return false unless step

        allowed_start_states = [StateMachine::ENQUEUED.to_s, StateMachine::RUNNING.to_s]
        unless allowed_start_states.include?(step.state)
           puts "WARN: [Orchestrator] Step #{step_id} cannot start; not in enqueued or running state (current: #{step.state})."
           return false
        end

        if step.state == StateMachine::ENQUEUED.to_s
          update_success = repository.update_step_attributes(
            step_id,
            { state: StateMachine::RUNNING.to_s, started_at: Time.current },
            expected_old_state: StateMachine::ENQUEUED
          )
          unless update_success
             puts "WARN: [Orchestrator] Failed to update step #{step_id} state to running before execution (maybe state changed concurrently?)."
             return false
          end
        end

        # TODO: Publish yantra.step.started event
        # step_record = repository.find_step(step_id)
        # payload = { step_id: step_id, workflow_id: step_record&.workflow_id, klass: step_record&.klass, started_at: step_record&.started_at }
        # @notifier.publish('yantra.step.started', payload)
        true
      end

      # Called by the worker job after the step's perform method completes successfully.
      def step_succeeded(step_id, result)
        puts "INFO: [Orchestrator] Job #{step_id} succeeded."
        final_attrs = {
          state: StateMachine::SUCCEEDED.to_s,
          finished_at: Time.current,
          output: result
        }
        update_success = repository.update_step_attributes(
          step_id,
          final_attrs,
          expected_old_state: StateMachine::RUNNING
        )

        if update_success
          # Publish Event on Success
          begin
            step_record = repository.find_step(step_id)
            if step_record
              payload = { step_id: step_id, workflow_id: step_record.workflow_id, klass: step_record.klass, finished_at: step_record.finished_at, output: step_record.output }
              @notifier.publish('yantra.step.succeeded', payload)
            else
              puts "WARN: [Orchestrator] Could not find step #{step_id} after success update to publish event."
            end
          rescue => e
             puts "ERROR: [Orchestrator] Failed to publish yantra.step.succeeded event for #{step_id}: #{e.message}"
          end
          step_finished(step_id)
        else
          puts "WARN: [Orchestrator] Failed to update job #{step_id} state to succeeded (maybe already changed?)."
          step_finished(step_id)
        end
      end

      # Called by the worker job when a step fails permanently.
      def step_failed(step_id)
        puts "INFO: [Orchestrator] Job #{step_id} failed permanently."
        # Event 'yantra.step.failed' should be published by RetryHandler
        step_finished(step_id)
      end

      # Common logic executed after a step finishes.
      def step_finished(finished_step_id)
        puts "INFO: [Orchestrator] Handling post-finish logic for job #{finished_step_id}."
        finished_step = repository.find_step(finished_step_id)
        unless finished_step
          puts "WARN: [Orchestrator] Cannot find finished step #{finished_step_id} during step_finished."
          return
        end
        workflow_id = finished_step.workflow_id
        finished_state = finished_step.state.to_sym rescue :unknown

        process_dependents(finished_step_id, finished_state, workflow_id)
        check_workflow_completion(workflow_id)
      end

      private

      # Finds jobs in the workflow that have no unsatisfied dependencies and enqueues them.
      def find_and_enqueue_ready_jobs(workflow_id)
        ready_step_ids = repository.find_ready_jobs(workflow_id)
        puts "INFO: [Orchestrator] Found ready jobs for workflow #{workflow_id}: #{ready_step_ids}"
        ready_step_ids.each do |step_id|
          enqueue_job(step_id, workflow_id)
        end
      end

      # Enqueues a specific job using the configured worker adapter.
      def enqueue_job(step_id, workflow_id)
        step = repository.find_step(step_id)
        return unless step

        update_success = repository.update_step_attributes(
          step_id,
          { state: StateMachine::ENQUEUED.to_s, enqueued_at: Time.current },
          expected_old_state: StateMachine::PENDING
        )

        if update_success
          begin
            @worker_adapter.enqueue(step_id, workflow_id, step.klass, step.queue)
             # TODO: Publish yantra.step.enqueued event
             # step_record = repository.find_step(step_id)
             # payload = { step_id: step_id, workflow_id: workflow_id, klass: step.klass, queue: step.queue, enqueued_at: step_record&.enqueued_at }
             # @notifier.publish('yantra.step.enqueued', payload)
          rescue => e
            puts "ERROR: [Orchestrator] Failed to enqueue job #{step_id} via worker adapter: #{e.message}. Reverting state to pending."
            repository.update_step_attributes(step_id, { state: StateMachine::PENDING.to_s, enqueued_at: nil })
            repository.set_workflow_has_failures_flag(workflow_id)
          end
        else
          puts "WARN: [Orchestrator] Failed to update job #{step_id} state to enqueued before enqueuing."
        end
      end

      # Checks if all dependencies for a given step are met.
      def dependencies_met?(step_id)
        parent_ids = repository.get_step_dependencies(step_id)
        return true if parent_ids.empty?
        parent_steps = parent_ids.map { |pid| repository.find_step(pid) }.compact
        parent_steps.length == parent_ids.length && parent_steps.all? { |p| p.state == StateMachine::SUCCEEDED.to_s }
      end

      # Checks if a workflow has completed.
      def check_workflow_completion(workflow_id)
        running_count = repository.running_step_count(workflow_id)
        enqueued_count = repository.enqueued_step_count(workflow_id)
        puts "DEBUG: [Orchestrator] Checking workflow completion for #{workflow_id}. Running: #{running_count}, Enqueued: #{enqueued_count}"

        if running_count == 0 && enqueued_count == 0
          has_failures = repository.workflow_has_failures?(workflow_id)
          final_state = has_failures ? StateMachine::FAILED : StateMachine::SUCCEEDED
          puts "INFO: [Orchestrator] Workflow #{workflow_id} appears complete. Setting state to #{final_state}."

          update_success = repository.update_workflow_attributes(
            workflow_id,
            { state: final_state.to_s, finished_at: Time.current },
            expected_old_state: StateMachine::RUNNING
          )

          if update_success
            puts "INFO: [Orchestrator] Workflow #{workflow_id} successfully set to #{final_state}."
            # Publish Event on Workflow Completion
            begin
              wf_record = repository.find_workflow(workflow_id)
              if wf_record
                event_name = has_failures ? 'yantra.workflow.failed' : 'yantra.workflow.succeeded'
                payload = { workflow_id: workflow_id, klass: wf_record.klass, finished_at: wf_record.finished_at, state: wf_record.state }
                @notifier.publish(event_name, payload)
              else
                 puts "WARN: [Orchestrator] Could not find workflow #{workflow_id} after final state update to publish event."
              end
            rescue => e
              puts "ERROR: [Orchestrator] Failed to publish workflow completion event for #{workflow_id}: #{e.message}"
            end
          else
             puts "WARN: [Orchestrator] Failed to update workflow #{workflow_id} final state to #{final_state} (maybe already finished?)."
          end
        end
      end

      # Processes steps that depend on a finished step.
      def process_dependents(finished_step_id, finished_state, workflow_id)
        dependent_ids = repository.get_step_dependents(finished_step_id)
        return if dependent_ids.empty?

        if finished_state == StateMachine::SUCCEEDED
          dependent_ids.each do |dep_id|
            dependent_step = repository.find_step(dep_id)
            if dependent_step&.state == StateMachine::PENDING.to_s && dependencies_met?(dep_id)
              enqueue_job(dep_id, workflow_id)
            end
          end
        elsif [StateMachine::FAILED, StateMachine::CANCELLED].include?(finished_state)
          cancel_downstream_pending(dependent_ids, workflow_id)
        end
      end

      # Recursively finds and cancels pending dependents.
      def cancel_downstream_pending(step_ids, workflow_id)
         pending_dependents_to_cancel = []
         step_ids.each do |sid|
            step = repository.find_step(sid)
            pending_dependents_to_cancel << sid if step&.state == StateMachine::PENDING.to_s
         end
         return if pending_dependents_to_cancel.empty?
         puts "INFO: [Orchestrator] Attempting to bulk cancel #{pending_dependents_to_cancel.size} downstream jobs: #{pending_dependents_to_cancel}."
         cancelled_count = repository.cancel_jobs_bulk(pending_dependents_to_cancel)
         puts "INFO: [Orchestrator] Bulk cancellation request processed for #{cancelled_count} jobs."
         next_level_dependents = []
         pending_dependents_to_cancel.each do |cancelled_id|
            next_level_dependents.concat(repository.get_step_dependents(cancelled_id))
         end
         cancel_downstream_pending(next_level_dependents.uniq, workflow_id) unless next_level_dependents.empty?
      end

    end
  end
end

