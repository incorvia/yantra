# lib/yantra/core/orchestrator.rb

require_relative 'state_machine'
require_relative '../errors'

module Yantra
  module Core
    # Responsible for managing the state transitions and execution flow of a workflow.
    class Orchestrator
      attr_reader :repository, :worker_adapter, :notifier

      # Initializer without respond_to? checks
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
        Yantra.logger.info { "[Orchestrator] Attempting to start workflow #{workflow_id}" } if Yantra.logger
        update_success = repository.update_workflow_attributes(workflow_id, { state: StateMachine::RUNNING.to_s, started_at: Time.current }, expected_old_state: StateMachine::PENDING)
        unless update_success; current_state = repository.find_workflow(workflow_id)&.state || 'not_found'; Yantra.logger.warn { "[Orchestrator] Workflow #{workflow_id} could not be started. Expected state 'pending', found '#{current_state}'." } if Yantra.logger; return false; end
        Yantra.logger.info { "[Orchestrator] Workflow #{workflow_id} state set to RUNNING." } if Yantra.logger
        begin; wf_record = repository.find_workflow(workflow_id); payload = { workflow_id: workflow_id, klass: wf_record&.klass, started_at: wf_record&.started_at }; @notifier&.publish('yantra.workflow.started', payload); rescue => e; Yantra.logger.error { "[Orchestrator] Failed to publish yantra.workflow.started event for #{workflow_id}: #{e.message}" } if Yantra.logger; end
        find_and_enqueue_ready_steps(workflow_id)
        true
      end

      # Called by the worker job just before executing the step's perform method.
      def step_starting(step_id)
        Yantra.logger.info { "[Orchestrator] Starting job #{step_id}" } if Yantra.logger
        step = repository.find_step(step_id)
        return false unless step
        allowed_start_states = [StateMachine::ENQUEUED.to_s, StateMachine::RUNNING.to_s]
        unless allowed_start_states.include?(step.state.to_s); Yantra.logger.warn { "[Orchestrator] Step #{step_id} cannot start; not in enqueued or running state (current: #{step.state})." } if Yantra.logger; return false; end
        if step.state.to_s == StateMachine::ENQUEUED.to_s
          update_success = repository.update_step_attributes(step_id, { state: StateMachine::RUNNING.to_s, started_at: Time.current }, expected_old_state: StateMachine::ENQUEUED)
          unless update_success; Yantra.logger.warn { "[Orchestrator] Failed to update step #{step_id} state to running before execution (maybe state changed concurrently?)." } if Yantra.logger; return false; end
          begin; step_record = repository.find_step(step_id); payload = { step_id: step_id, workflow_id: step_record&.workflow_id, klass: step_record&.klass, started_at: step_record&.started_at }; @notifier&.publish('yantra.step.started', payload); rescue => e; Yantra.logger.error { "[Orchestrator] Failed to publish yantra.step.started event for #{step_id}: #{e.message}" } if Yantra.logger; end
        elsif step.state.to_s == StateMachine::RUNNING.to_s
          Yantra.logger.info { "[Orchestrator] Step #{step_id} already running, proceeding with execution attempt without state change or start event." } if Yantra.logger
        end
        true
      end

      # Called by the worker job after step execution succeeds.
      def step_succeeded(step_id, output)
        Yantra.logger.info { "[Orchestrator] Job #{step_id} succeeded." } if Yantra.logger
        update_success = repository.update_step_attributes(step_id, { state: StateMachine::SUCCEEDED.to_s, finished_at: Time.current }, expected_old_state: StateMachine::RUNNING)
        repository.record_step_output(step_id, output)
        unless update_success; current_state = repository.find_step(step_id)&.state || 'not_found'; Yantra.logger.warn { "[Orchestrator] Failed to update step #{step_id} state to succeeded (expected 'running', found '#{current_state}'). Output recorded anyway." } if Yantra.logger; end
        if update_success; begin; step_record = repository.find_step(step_id); payload = { step_id: step_id, workflow_id: step_record&.workflow_id, klass: step_record&.klass, finished_at: step_record&.finished_at, output: step_record&.output }; @notifier&.publish('yantra.step.succeeded', payload); rescue => e; Yantra.logger.error { "[Orchestrator] Failed to publish yantra.step.succeeded event for #{step_id}: #{e.message}" } if Yantra.logger; end; end
        step_finished(step_id)
      end

      # Common logic after a step finishes
      def step_finished(step_id)
        Yantra.logger.info { "[Orchestrator] Handling post-finish logic for job #{step_id}." } if Yantra.logger
        finished_step = repository.find_step(step_id)
        return unless finished_step
        process_dependents(step_id, finished_step.state.to_sym)
        check_workflow_completion(finished_step.workflow_id)
      end

      private

      # Finds steps whose dependencies are met and enqueues them.
      def find_and_enqueue_ready_steps(workflow_id)
        ready_step_ids = repository.find_ready_steps(workflow_id)
        Yantra.logger.info { "[Orchestrator] Found ready steps for workflow #{workflow_id}: #{ready_step_ids}" } if Yantra.logger
        ready_step_ids.each { |step_id| enqueue_step(step_id) }
      end

      # Refactored enqueue_step
      def enqueue_step(step_id)
        step_record_initial = repository.find_step(step_id)
        return unless step_record_initial
        Yantra.logger.info { "[Orchestrator] Attempting to enqueue step #{step_id} in workflow #{step_record_initial.workflow_id}" } if Yantra.logger
        update_success = repository.update_step_attributes(step_id, { state: StateMachine::ENQUEUED.to_s, enqueued_at: Time.current }, expected_old_state: StateMachine::PENDING)
        unless update_success; current_state = step_record_initial.state || 'unknown'; Yantra.logger.warn { "[Orchestrator] Step #{step_id} could not be marked enqueued. Expected state 'pending', found '#{current_state}'. Skipping enqueue." } if Yantra.logger; return; end
        step_record_for_payload = repository.find_step(step_id); unless step_record_for_payload; Yantra.logger.error { "[Orchestrator] Step #{step_id} not found after successful state update to enqueued!" } if Yantra.logger; return; end
        Yantra.logger.info { "[Orchestrator] Step #{step_id} marked enqueued for Workflow #{step_record_for_payload.workflow_id}." } if Yantra.logger
        begin; payload = { step_id: step_id, workflow_id: step_record_for_payload.workflow_id, klass: step_record_for_payload.klass, queue: step_record_for_payload.queue, enqueued_at: step_record_for_payload.enqueued_at }; @notifier&.publish('yantra.step.enqueued', payload); rescue => e; Yantra.logger.error { "[Orchestrator] Failed to publish yantra.step.enqueued event for #{step_id}: #{e.message}" } if Yantra.logger; end
        begin; worker_adapter.enqueue(step_id, step_record_for_payload.workflow_id, step_record_for_payload.klass, step_record_for_payload.queue); rescue => e; Yantra.logger.error { "[Orchestrator] Failed to enqueue job #{step_id} via worker adapter: #{e.message}. Attempting to mark step as failed." } if Yantra.logger; repository.update_step_attributes(step_id, { state: StateMachine::FAILED.to_s, error: { class: e.class.name, message: "Failed to enqueue: #{e.message}" }, finished_at: Time.current }); repository.set_workflow_has_failures_flag(step_record_for_payload.workflow_id); check_workflow_completion(step_record_for_payload.workflow_id); end
      end

      # Checks dependents of a finished step and enqueues or cancels them.
      def process_dependents(finished_step_id, finished_step_state)
        # Fetch IDs of steps that depend on the one that just finished
        dependents = repository.get_step_dependents(finished_step_id)
        return if dependents.empty? # No dependents, nothing to do

        Yantra.logger.debug { "[Orchestrator] Processing dependents for finished step #{finished_step_id} (state: #{finished_step_state}): #{dependents}" } if Yantra.logger

        # --- Optimization Part 1: Bulk Fetch Dependencies ---
        parent_map = {} # Store { dependent_id => [parent_ids] }
        all_parent_ids_needed = []
        # Check if the repository supports bulk fetching dependencies
        if repository.respond_to?(:get_step_dependencies_multi)
          parent_map = repository.get_step_dependencies_multi(dependents)
          # Ensure all dependents are keys in the map, even if they have no parents
          dependents.each { |dep_id| parent_map[dep_id] ||= [] }
          all_parent_ids_needed = parent_map.values.flatten.uniq
        else
          # Fallback to N+1 if adapter doesn't support bulk fetch
          Yantra.logger.warn {"[Orchestrator] Repository does not support get_step_dependencies_multi, dependency check might be inefficient."} if Yantra.logger
          dependents.each do |dep_id|
             parent_ids = repository.get_step_dependencies(dep_id) # N+1 Call
             parent_map[dep_id] = parent_ids
             all_parent_ids_needed.concat(parent_ids)
          end
          all_parent_ids_needed.uniq!
        end
        # --- End Optimization Part 1 ---

        # --- Optimization Part 2: Bulk Fetch Parent States ---
        parent_states_hash = {}
        if all_parent_ids_needed.any? && repository.respond_to?(:fetch_step_states)
           parent_states_hash = repository.fetch_step_states(all_parent_ids_needed)
        else
           Yantra.logger.warn {"[Orchestrator] Repository does not support fetch_step_states, readiness check might be inefficient."} if Yantra.logger && all_parent_ids_needed.any?
           # Fallback logic within is_ready_to_start? will handle this
        end
        # --- End Optimization Part 2 ---

        # --- Process each dependent ---
        dependents.each do |dep_id|
          if finished_step_state == StateMachine::SUCCEEDED
            # Pass the specific parents for this dependent (from parent_map)
            # and the hash containing all potentially relevant parent states
            parents_for_dep = parent_map.fetch(dep_id, []) # Use fetch with default
            if is_ready_to_start?(dep_id, parents_for_dep, parent_states_hash)
              enqueue_step(dep_id)
            else
               Yantra.logger.debug { "[Orchestrator] Dependent #{dep_id} not ready yet." } if Yantra.logger
            end
          else # Finished step failed or was cancelled
            cancel_downstream_pending(dep_id)
          end
        end
      end


      # Checks if a given step is ready to start
      def is_ready_to_start?(step_id, parent_ids, parent_states_hash)
        step = repository.find_step(step_id)
        return false unless step && step.state.to_sym == StateMachine::PENDING
        return true if parent_ids.empty?
        all_succeeded = parent_ids.all? do |parent_id|
          state = parent_states_hash[parent_id]
          unless state
             Yantra.logger.debug {"[Orchestrator#is_ready_to_start?] Falling back to individual fetch for parent #{parent_id}"} if Yantra.logger
             parent_step = repository.find_step(parent_id)
             state = parent_step&.state
          end
          state == StateMachine::SUCCEEDED.to_s
        end
        all_succeeded
      end

      # Recursively cancels downstream steps that are pending.
      def cancel_downstream_pending(step_id)
        step = repository.find_step(step_id)
        return unless step && step.state.to_sym == StateMachine::PENDING
        Yantra.logger.debug { "[Orchestrator] Recursively cancelling pending step #{step_id}" } if Yantra.logger
        update_success = repository.update_step_attributes(step_id, { state: StateMachine::CANCELLED.to_s, finished_at: Time.current }, expected_old_state: StateMachine::PENDING)
        if update_success
          begin; cancelled_step_record = repository.find_step(step_id); payload = { step_id: step_id, workflow_id: cancelled_step_record&.workflow_id, klass: cancelled_step_record&.klass }; @notifier&.publish('yantra.step.cancelled', payload); rescue => e; Yantra.logger.error { "[Orchestrator] Failed to publish yantra.step.cancelled event during cascade for #{step_id}: #{e.message}" } if Yantra.logger; end
        else
           Yantra.logger.warn { "[Orchestrator] Failed to update state to cancelled for downstream step #{step_id} during cascade." } if Yantra.logger
           return
        end
        repository.get_step_dependents(step_id).each { |dep_id| cancel_downstream_pending(dep_id) }
      end

      # Checks if a workflow has completed and updates its state.
      def check_workflow_completion(workflow_id)
        running_count = repository.running_step_count(workflow_id)
        Yantra.logger.debug { "[Orchestrator] Checking workflow completion for #{workflow_id}. Running count: #{running_count}" } if Yantra.logger
        enqueued_count = repository.enqueued_step_count(workflow_id)
        Yantra.logger.debug { "[Orchestrator] Checking workflow completion for #{workflow_id}. Enqueued count: #{enqueued_count}" } if Yantra.logger

        if running_count.zero? && enqueued_count.zero?
          current_wf = repository.find_workflow(workflow_id)
          return if current_wf.nil? || StateMachine.terminal?(current_wf.state.to_sym)
          has_failures = repository.workflow_has_failures?(workflow_id)
          final_state = has_failures ? StateMachine::FAILED : StateMachine::SUCCEEDED
          finished_at_time = Time.current
          Yantra.logger.info { "[Orchestrator] Workflow #{workflow_id} appears complete. Setting state to #{final_state}." } if Yantra.logger
          update_success = repository.update_workflow_attributes(workflow_id, { state: final_state.to_s, finished_at: finished_at_time }, expected_old_state: StateMachine::RUNNING)
          if update_success
            Yantra.logger.info { "[Orchestrator] Workflow #{workflow_id} successfully set to #{final_state}." } if Yantra.logger
             begin; final_wf_record = repository.find_workflow(workflow_id); event_name = final_state == StateMachine::FAILED ? 'yantra.workflow.failed' : 'yantra.workflow.succeeded'; payload = { workflow_id: workflow_id, klass: final_wf_record&.klass, state: final_state, finished_at: final_wf_record&.finished_at }; @notifier&.publish(event_name, payload); rescue => e; Yantra.logger.error { "[Orchestrator] Failed to publish workflow completion event for #{workflow_id}: #{e.message}" } if Yantra.logger; end
          else
             latest_state = repository.find_workflow(workflow_id)&.state || 'unknown'
             Yantra.logger.warn { "[Orchestrator] Failed to set final state for workflow #{workflow_id} (expected 'running', found '#{latest_state}')." } if Yantra.logger
          end
        end
      end

    end # class Orchestrator
  end # module Core
end # module Yantra
