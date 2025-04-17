# lib/yantra/core/orchestrator.rb

require_relative '../errors' # Ensure Errors module is required
require_relative '../step'
require_relative 'state_machine'
require_relative '../persistence/repository_interface'
require_relative '../worker/enqueuing_interface'
require_relative '../events/notifier_interface'

module Yantra
  module Core
    # Orchestrates the execution flow of a workflow based on step dependencies and states.
    # Interacts with the Repository for persistence and the WorkerAdapter for enqueuing jobs.
    class Orchestrator
      attr_reader :repository, :worker_adapter, :notifier

      # Initializes the orchestrator with repository, worker adapter, and notifier instances.
      # Falls back to globally configured adapters if none are provided.
      def initialize(repository: nil, worker_adapter: nil, notifier: nil)
        @repository = repository || Yantra.repository
        @worker_adapter = worker_adapter || Yantra.worker_adapter
        @notifier = notifier || Yantra.notifier

        # Validate dependencies using fully qualified error class name
        unless @repository && @repository.is_a?(Persistence::RepositoryInterface)
          raise Yantra::Errors::ConfigurationError, "Yantra Orchestrator requires a valid persistence repository adapter."
        end
        unless @worker_adapter && @worker_adapter.is_a?(Worker::EnqueuingInterface)
          raise Yantra::Errors::ConfigurationError, "Yantra Orchestrator requires a valid worker enqueuing adapter."
        end
        unless @notifier && @notifier.respond_to?(:publish)
           raise Yantra::Errors::ConfigurationError, "Yantra Orchestrator requires a valid event notifier adapter (responding to #publish)."
        end
      end

      # Starts a workflow if it's in a pending state.
      def start_workflow(workflow_id)
        Yantra.logger.info { "[Orchestrator] Attempting to start workflow #{workflow_id}" }
        update_success = repository.update_workflow_attributes(
          workflow_id,
          { state: StateMachine::RUNNING.to_s, started_at: Time.current },
          expected_old_state: StateMachine::PENDING
        )

        if update_success
          Yantra.logger.info { "[Orchestrator] Workflow #{workflow_id} state set to RUNNING." }
          # Publish yantra.workflow.started event
          begin
            wf_record = repository.find_workflow(workflow_id)
            payload = { workflow_id: workflow_id, klass: wf_record&.klass, started_at: wf_record&.started_at, state: StateMachine::RUNNING }
            @notifier.publish('yantra.workflow.started', payload)
          rescue => e
             Yantra.logger.error { "[Orchestrator] Failed to publish yantra.workflow.started event for #{workflow_id}: #{e.message}" }
          end

          find_and_enqueue_ready_jobs(workflow_id)
          true
        else
          current_workflow = repository.find_workflow(workflow_id)
          if current_workflow.nil?
             Yantra.logger.warn { "[Orchestrator] Workflow #{workflow_id} not found during start attempt." }
          elsif current_workflow.state != StateMachine::PENDING.to_s
             Yantra.logger.warn { "[Orchestrator] Workflow #{workflow_id} cannot start; already in state :#{current_workflow.state}." }
          else
             Yantra.logger.warn { "[Orchestrator] Failed to update workflow #{workflow_id} state to RUNNING (maybe state changed concurrently?)." }
          end
          false
        end
      end

      # Called by the worker job just before executing the step's perform method.
       # Called by the worker job just before executing the step's perform method.
      def step_starting(step_id)
        Yantra.logger.info { "[Orchestrator] Starting job #{step_id}" }
        step = repository.find_step(step_id)
        return false unless step

        allowed_start_states = [StateMachine::ENQUEUED.to_s, StateMachine::RUNNING.to_s]
        unless allowed_start_states.include?(step.state.to_s) # FIX: Compare strings
           Yantra.logger.warn { "[Orchestrator] Step #{step_id} cannot start; not in enqueued or running state (current: #{step.state})." }
           return false
        end

        # Only transition state and publish event if moving from enqueued
        if step.state.to_s == StateMachine::ENQUEUED.to_s # FIX: Compare strings
          update_success = repository.update_step_attributes(
            step_id,
            { state: StateMachine::RUNNING.to_s, started_at: Time.current },
            expected_old_state: StateMachine::ENQUEUED
          )
          unless update_success
             Yantra.logger.warn { "[Orchestrator] Failed to update step #{step_id} state to running before execution (maybe state changed concurrently?)." }
             return false
          end

          # --- Publish yantra.step.started event ONLY after successful transition ---
          begin
            # Fetch again to get the timestamp set by the update
            step_record = repository.find_step(step_id)
            payload = { step_id: step_id, workflow_id: step_record&.workflow_id, klass: step_record&.klass, started_at: step_record&.started_at }
            @notifier.publish('yantra.step.started', payload)
          rescue => e
             Yantra.logger.error { "[Orchestrator] Failed to publish yantra.step.started event for #{step_id}: #{e.message}" }
          end
          # --- END Event Publishing Block ---

        elsif step.state.to_s == StateMachine::RUNNING.to_s
          # State is already running (retry/recovery scenario).
          # Do NOT update state or publish the 'started' event again.
          # Allow the StepJob to proceed with the perform attempt.
          Yantra.logger.info { "[Orchestrator] Step #{step_id} already running, proceeding with execution attempt without state change or start event." }
        end

        true # Allow StepJob#perform to execute
      end

      # Called by the worker job after the step's perform method completes successfully.
      def step_succeeded(step_id, result)
        Yantra.logger.info { "[Orchestrator] Job #{step_id} succeeded." }
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
               Yantra.logger.warn { "[Orchestrator] Could not find step #{step_id} after success update to publish event." }
            end
          rescue => e
             Yantra.logger.error { "[Orchestrator] Failed to publish yantra.step.succeeded event for #{step_id}: #{e.message}" }
          end
          step_finished(step_id) # Proceed to next steps
        else
           Yantra.logger.warn { "[Orchestrator] Failed to update job #{step_id} state to succeeded (maybe already changed?). Still proceeding with step_finished logic." }
           step_finished(step_id)
        end
      end

      # Called by the worker job when a step fails permanently (after retries).
      def step_failed(step_id)
        Yantra.logger.info { "[Orchestrator] Job #{step_id} failed permanently." }
        # Event 'yantra.step.failed' should be published by RetryHandler
        step_finished(step_id)
      end

      # Common logic executed after a step finishes (succeeded or failed).
      def step_finished(finished_step_id)
        Yantra.logger.info { "[Orchestrator] Handling post-finish logic for job #{finished_step_id}." }
        finished_step = repository.find_step(finished_step_id)
        unless finished_step
          Yantra.logger.warn { "[Orchestrator] Cannot find finished step #{finished_step_id} during step_finished." }
          return
        end
        workflow_id = finished_step.workflow_id
        # Convert state string from DB to symbol for internal logic
        finished_state = finished_step.state.to_sym rescue :unknown
        Yantra.logger.debug { "[Orchestrator] step_finished: derived finished_state=#{finished_state.inspect}"}

        process_dependents(finished_step_id, finished_state, workflow_id)
        check_workflow_completion(workflow_id)
      end

      private

      # Finds jobs in the workflow that have no unsatisfied dependencies and enqueues them.
      def find_and_enqueue_ready_jobs(workflow_id)
        ready_step_ids = repository.find_ready_steps(workflow_id)
        Yantra.logger.info { "[Orchestrator] Found ready steps for workflow #{workflow_id}: #{ready_step_ids}" }
        ready_step_ids.each do |step_id|
          enqueue_step(step_id, workflow_id)
        end
      end

      # Enqueues a specific job using the configured worker adapter.
      def enqueue_step(step_id, workflow_id)
        Yantra.logger.info { "[Orchestrator] Entering enqueue_step with: #{workflow_id}: #{step_id}" }
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
             # Publish yantra.step.enqueued event
             begin
               Yantra.logger.info { "[Orchestrator] After update_success: #{workflow_id}: #{step_id}" }
               step_record = repository.find_step(step_id) # Fetch record for timestamp
               Yantra.logger.info { "[Orchestrator] After find_step (2): #{workflow_id}: #{step_id}" }
               payload = { step_id: step_id, workflow_id: workflow_id, klass: step.klass, queue: step.queue, enqueued_at: step_record&.enqueued_at }
               @notifier.publish('yantra.step.enqueued', payload)
             rescue => e
                Yantra.logger.error { "[Orchestrator] Failed to publish yantra.step.enqueued event for #{step_id}: #{e.message}" }
             end
          rescue => e
            Yantra.logger.error { "[Orchestrator] Failed to enqueue job #{step_id} via worker adapter: #{e.message}. Reverting state to pending." }
            repository.update_step_attributes(step_id, { state: StateMachine::PENDING.to_s, enqueued_at: nil })
            repository.set_workflow_has_failures_flag(workflow_id)
          end
        else
           Yantra.logger.warn { "[Orchestrator] Failed to update job #{step_id} state to enqueued before enqueuing." }
        end
      end

      # Checks if all dependencies for a given step are met.
      def dependencies_met?(step_id)
        parent_ids = repository.get_step_dependencies(step_id)
        return true if parent_ids.empty?
        parent_steps = parent_ids.map { |pid| repository.find_step(pid) }.compact
        # Ensure all parents were found AND all succeeded
        # FIX: Compare states as strings
        parent_steps.length == parent_ids.length && parent_steps.all? { |p| p.state.to_s == StateMachine::SUCCEEDED.to_s }
      end

      # Checks if a workflow has completed.
      def check_workflow_completion(workflow_id)
        running_count = repository.running_step_count(workflow_id)
        enqueued_count = repository.enqueued_step_count(workflow_id)
        Yantra.logger.debug { "[Orchestrator] Checking workflow completion for #{workflow_id}. Running: #{running_count}, Enqueued: #{enqueued_count}" }

        if running_count == 0 && enqueued_count == 0
          has_failures = repository.workflow_has_failures?(workflow_id)
          final_state = has_failures ? StateMachine::FAILED : StateMachine::SUCCEEDED
          Yantra.logger.info { "[Orchestrator] Workflow #{workflow_id} appears complete. Setting state to #{final_state}." }

          update_success = repository.update_workflow_attributes(
            workflow_id,
            { state: final_state.to_s, finished_at: Time.current },
            expected_old_state: StateMachine::RUNNING
          )

          if update_success
             Yantra.logger.info { "[Orchestrator] Workflow #{workflow_id} successfully set to #{final_state}." }
            # Publish Event on Workflow Completion
            begin
              wf_record = repository.find_workflow(workflow_id)
              if wf_record
                event_name = has_failures ? 'yantra.workflow.failed' : 'yantra.workflow.succeeded'
                # Use symbol state from constant for consistency in payload if desired, or string from DB
                payload = { workflow_id: workflow_id, klass: wf_record.klass, finished_at: wf_record.finished_at, state: final_state }
                @notifier.publish(event_name, payload)
              else
                 Yantra.logger.warn { "[Orchestrator] Could not find workflow #{workflow_id} after final state update to publish event." }
              end
            rescue => e
               Yantra.logger.error { "[Orchestrator] Failed to publish workflow completion event for #{workflow_id}: #{e.message}" }
            end
          else
             # Log if update failed - check current state
             current_workflow = repository.find_workflow(workflow_id)
             # FIX: Convert DB state to symbol for terminal_state? check
             if current_workflow && StateMachine.terminal_state?(current_workflow.state.to_sym)
                Yantra.logger.warn { "[Orchestrator] Failed to update workflow #{workflow_id} final state to #{final_state}, but it was already in terminal state :#{current_workflow.state}." }
             else
                Yantra.logger.error { "[Orchestrator] Failed to update workflow #{workflow_id} final state to #{final_state}. Current state: :#{current_workflow&.state || 'nil'}." }
             end
          end
        end
      end

      # Processes steps that depend on a finished step.
      def process_dependents(finished_step_id, finished_state, workflow_id)
        Yantra.logger.debug { "ORCH DEBUG: process_dependents called. finished_step_id=#{finished_step_id}, finished_state=#{finished_state.inspect}" }
        dependent_ids = repository.get_step_dependents(finished_step_id)
        return if dependent_ids.empty?

        if finished_state == StateMachine::SUCCEEDED
          Yantra.logger.debug { "ORCH DEBUG: process_dependents: Entered SUCCEEDED block." }
          dependent_ids.each do |dep_id|
            dependent_step = repository.find_step(dep_id)
            # Check state *before* checking dependencies
            # FIX: Compare states as strings
            if dependent_step&.state.to_s == StateMachine::PENDING.to_s
              if dependencies_met?(dep_id)
                enqueue_step(dep_id, workflow_id)
              end
            end
          end
        elsif [StateMachine::FAILED, StateMachine::CANCELLED].include?(finished_state)
          Yantra.logger.debug { "ORCH DEBUG: process_dependents: Entered FAILED/CANCELLED block." }
          cancel_downstream_pending(dependent_ids, workflow_id)
        else
           Yantra.logger.debug { "ORCH DEBUG: process_dependents: finished_state #{finished_state.inspect} did not match SUCCEEDED or FAILED/CANCELLED." }
        end
      end

      # Recursively finds and cancels pending dependents.
      def cancel_downstream_pending(step_ids, workflow_id)
        Yantra.logger.debug { "ORCH DEBUG: cancel_downstream_pending called for step_ids=#{step_ids.inspect}" }
        pending_dependents_to_cancel = []
        step_ids.each do |sid|
          step = repository.find_step(sid)
          # === HYPER-FOCUSED LOGGING ===
          if step

          else

          end
          # =============================
          # FIX: Compare states as strings
          pending_dependents_to_cancel << sid if step&.state.to_s == StateMachine::PENDING.to_s
        end
        # === TARGETED LOGGING ===

        # ========================
        return if pending_dependents_to_cancel.empty? # <<< Should NOT return if comparison works

        Yantra.logger.info { "[Orchestrator] Attempting to bulk cancel #{pending_dependents_to_cancel.size} downstream jobs: #{pending_dependents_to_cancel}." }
        cancelled_count = repository.cancel_steps_bulk(pending_dependents_to_cancel)
        Yantra.logger.info { "[Orchestrator] Bulk cancellation request processed for #{cancelled_count} jobs." }
        next_level_dependents = []
        pending_dependents_to_cancel.each do |cancelled_id|
           # Publish event for each cancelled step
           begin
             payload = { step_id: cancelled_id, workflow_id: workflow_id }
             @notifier.publish('yantra.step.cancelled', payload)
           rescue => e
              Yantra.logger.error { "[Orchestrator] Failed to publish yantra.step.cancelled event for #{cancelled_id}: #{e.message}" }
           end
           # Find next level
           next_level_dependents.concat(repository.get_step_dependents(cancelled_id))
        end
        cancel_downstream_pending(next_level_dependents.uniq, workflow_id) unless next_level_dependents.empty?
      end


    end
  end
end

