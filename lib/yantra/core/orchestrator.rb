# lib/yantra/core/orchestrator.rb (Refactored for Bulk Cancellation & step_failed added)

require_relative 'state_machine'
require_relative '../errors'
# Ensure interfaces are loaded if needed for validation/type hints
require_relative '../persistence/repository_interface'
require_relative '../worker/enqueuing_interface'
require_relative '../events/notifier_interface'


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
        log_info "Attempting to start workflow #{workflow_id}"
        update_success = repository.update_workflow_attributes(workflow_id, { state: StateMachine::RUNNING.to_s, started_at: Time.current }, expected_old_state: StateMachine::PENDING)
        unless update_success; current_state = repository.find_workflow(workflow_id)&.state || 'not_found'; log_warn "Workflow #{workflow_id} could not be started. Expected state 'pending', found '#{current_state}'."; return false; end
        log_info "Workflow #{workflow_id} state set to RUNNING."
        publish_workflow_started_event(workflow_id) # Use helper
        find_and_enqueue_ready_steps(workflow_id)
        true
      end

      # Called by the worker job just before executing the step's perform method.
      def step_starting(step_id)
        log_info "Starting job #{step_id}"
        step = repository.find_step(step_id)
        return false unless step
        allowed_start_states = [StateMachine::ENQUEUED.to_s, StateMachine::RUNNING.to_s]
        unless allowed_start_states.include?(step.state.to_s); log_warn "Step #{step_id} cannot start; not in enqueued or running state (current: #{step.state})."; return false; end
        if step.state.to_s == StateMachine::ENQUEUED.to_s
          update_success = repository.update_step_attributes(step_id, { state: StateMachine::RUNNING.to_s, started_at: Time.current }, expected_old_state: StateMachine::ENQUEUED)
          unless update_success; log_warn "Failed to update step #{step_id} state to running before execution (maybe state changed concurrently?)."; return false; end
          publish_step_started_event(step_id) # Use helper
        elsif step.state.to_s == StateMachine::RUNNING.to_s
          log_info "Step #{step_id} already running, proceeding with execution attempt without state change or start event."
        end
        true
      end

      # Called by the worker job after step execution succeeds.
      def step_succeeded(step_id, output)
        log_info "Job #{step_id} succeeded."
        update_success = repository.update_step_attributes(step_id, { state: StateMachine::SUCCEEDED.to_s, finished_at: Time.current }, expected_old_state: StateMachine::RUNNING)
        repository.record_step_output(step_id, output)
        unless update_success; current_state = repository.find_step(step_id)&.state || 'not_found'; log_warn "Failed to update step #{step_id} state to succeeded (expected 'running', found '#{current_state}'). Output recorded anyway."; end
        publish_step_succeeded_event(step_id) if update_success # Use helper
        step_finished(step_id)
      end

      # Handles permanent step failure
      def step_failed(step_id, error_info, expected_old_state: StateMachine::RUNNING) # Add param with default
        log_error "Job #{step_id} failed permanently. Error: #{error_info[:class]} - #{error_info[:message]}"
        finished_at_time = Time.current

        update_success = repository.update_step_attributes(
          step_id,
          {
            state: StateMachine::FAILED.to_s,
            error: error_info,
            finished_at: finished_at_time
          },
          # Use the provided or defaulted expected_old_state
          expected_old_state: expected_old_state
        )

        unless update_success
          current_state = repository.find_step(step_id)&.state || 'not_found'
          log_warn "Failed to update step #{step_id} state to failed (expected '#{expected_old_state}', found '#{current_state}')."
          # Potentially return early or just log if update fails? For now, continue...
        end

        # Set workflow failure flag (idempotent)
        workflow_id = repository.find_step(step_id)&.workflow_id # Fetch fresh in case step_record wasn't passed
        if workflow_id
          repository.set_workflow_has_failures_flag(workflow_id)
        else
          log_error "Cannot find workflow_id for failed step #{step_id} to set failure flag."
        end

        # Publish event only if state transition was successful (or maybe always publish failure?)
        publish_step_failed_event(step_id, error_info, finished_at_time) # Consider if update_success should gate this

        # Trigger downstream processing (cancel dependents, check workflow completion)
        step_finished(step_id)
      end

      # Common logic executed after a step finishes (succeeded, failed, cancelled).
      def step_finished(step_id)
        log_info "Handling post-finish logic for job #{step_id}."
        finished_step = repository.find_step(step_id)
        return unless finished_step # Step might have been deleted

        state_sym = finished_step.state&.to_sym rescue :unknown

        # --- FIXED: Explicitly check for states requiring post-processing ---
        # Check if state is one that signifies the end of this step's execution path.
        # This allows processing dependents/completion for FAILED steps without
        # changing the semantic meaning of TERMINAL_STATES used elsewhere.
        if [StateMachine::SUCCEEDED, StateMachine::FAILED, StateMachine::CANCELLED].include?(state_sym)
          process_dependents(step_id, state_sym)
          check_workflow_completion(finished_step.workflow_id)
        else
          log_warn "Step #{step_id} reached step_finished but is in state '#{state_sym}' which requires no post-processing. Skipping."
        end
        # --- END FIX ---
      end

      private

      # Finds steps whose dependencies are met and enqueues them.
      def find_and_enqueue_ready_steps(workflow_id)
        ready_step_ids = repository.find_ready_steps(workflow_id)
        log_info "Found ready steps for workflow #{workflow_id}: #{ready_step_ids}"
        ready_step_ids.each { |step_id| enqueue_step(step_id) }
      end

      # Enqueues a single step after marking it as ENQUEUED.
      def enqueue_step(step_id)
        step_record_initial = repository.find_step(step_id)
        return unless step_record_initial
        log_info "Attempting to enqueue step #{step_id} in workflow #{step_record_initial.workflow_id}"
        update_success = repository.update_step_attributes(step_id, { state: StateMachine::ENQUEUED.to_s, enqueued_at: Time.current }, expected_old_state: StateMachine::PENDING)
        unless update_success; current_state = step_record_initial.state || 'unknown'; log_warn "Step #{step_id} could not be marked enqueued. Expected state 'pending', found '#{current_state}'. Skipping enqueue."; return; end
        step_record_for_payload = repository.find_step(step_id); unless step_record_for_payload; log_error "Step #{step_id} not found after successful state update to enqueued!"; return; end
        log_info "Step #{step_id} marked enqueued for Workflow #{step_record_for_payload.workflow_id}."
        publish_step_enqueued_event(step_record_for_payload) # Use helper
        enqueue_job_via_adapter(step_record_for_payload) # Use helper
      end

       # Enqueues the job using the worker adapter, handling potential errors.
      def enqueue_job_via_adapter(step)
        worker_adapter.enqueue(step.id, step.workflow_id, step.klass, step.queue)
        log_debug "Successfully enqueued job #{step.id} via worker adapter."
      rescue => e
        log_error "Failed to enqueue job #{step.id} via worker adapter: #{e.message}. Attempting to mark step as failed."
        error_info = { class: e.class.name, message: "Failed to enqueue: #{e.message}", backtrace: e.backtrace&.first(10) }
        # Call step_failed specifying the correct previous state
        step_failed(step.id, error_info, expected_old_state: StateMachine::ENQUEUED) # <--- Specify ENQUEUED
      end

      # --- REFACTORED: process_dependents using bulk cancellation ---
      # Checks dependents of a finished step and enqueues ready ones or cancels downstream on failure.
      def process_dependents(finished_step_id, finished_step_state)
        direct_dependents = repository.get_child_ids(finished_step_id)
        return if direct_dependents.empty?

        log_debug "Processing dependents for finished step #{finished_step_id} (state: #{finished_step_state}): #{direct_dependents}"

        if finished_step_state == StateMachine::SUCCEEDED
          # --- Handle Success: Check readiness and enqueue ---
          parent_map, all_parent_ids_needed = fetch_dependencies_for_steps(direct_dependents)
          ids_to_fetch_states = (direct_dependents + all_parent_ids_needed).uniq
          all_states_hash = fetch_states_for_steps(ids_to_fetch_states)

          direct_dependents.each do |dep_id|
            parents_for_dep = parent_map.fetch(dep_id, [])
            if is_ready_to_start?(dep_id, parents_for_dep, all_states_hash)
              enqueue_step(dep_id)
            else
              log_debug "Dependent #{dep_id} not ready yet."
            end
          end
        # --- FIXED: Check includes FAILED and CANCELLED ---
        elsif [StateMachine::FAILED, StateMachine::CANCELLED].include?(finished_step_state)
          # --- Handle Failure/Cancellation: Bulk cancel downstream ---
          log_warn "Parent step #{finished_step_id} finished with non-success state (#{finished_step_state}). Cancelling downstream pending steps."

          all_pending_descendant_ids = find_all_pending_descendants(direct_dependents)

          if all_pending_descendant_ids.any?
            log_info "Attempting to bulk cancel steps: #{all_pending_descendant_ids}"
            begin
              # Call the bulk cancel method
              update_count = repository.cancel_steps_bulk(all_pending_descendant_ids)

              log_info "Bulk cancel attempted for #{all_pending_descendant_ids.count} steps, repository updated #{update_count} records."

              # Publish events for all steps *intended* for cancellation
              # Assuming cancel_steps_bulk handles the state check internally
              all_pending_descendant_ids.each { |cancelled_id| publish_step_cancelled_event(cancelled_id) }

            rescue NotImplementedError
              log_error "Repository does not implement cancel_steps_bulk! Downstream steps not cancelled."
              # Consider adding fallback to individual cancellation if critical
            rescue => e
              log_error "Error during bulk cancellation for dependents of #{finished_step_id}: #{e.message}"
            end
          else
            log_info "No pending downstream steps found to cancel for dependents of #{finished_step_id}."
          end
        else
            # This case should ideally not be reached if step_finished checks correctly
            log_warn "Step #{finished_step_id} called process_dependents with unexpected state: #{finished_step_state}"
        end
      end
      # --- END REFACTORED ---


      # --- HELPER: Finds all pending descendants ---
      # Performs a graph traversal to find all steps downstream from the initial_step_ids
      # that are currently in a PENDING state.
      # @param initial_step_ids [Array<String>] IDs of the steps to start traversal from.
      # @return [Array<String>] An array of unique IDs of pending descendant steps.
      def find_all_pending_descendants(initial_step_ids)
        pending_descendants = Set.new # Use Set for uniqueness
        queue = initial_step_ids.dup # Steps to check
        visited = Set.new(initial_step_ids) # Avoid cycles and redundant checks

        # Fetch initial states efficiently if possible
        states_to_check = fetch_states_for_steps(initial_step_ids)

        while queue.any?
          current_batch = queue.shift(100) # Process in batches
          current_batch_states = states_to_check.slice(*current_batch)

          # Fetch states for any missing from initial batch fetch (shouldn't happen often)
          missing_state_ids = current_batch - current_batch_states.keys
          if missing_state_ids.any?
              log_warn "Fetching missing states individually in find_all_pending_descendants: #{missing_state_ids}"
              missing_states = fetch_states_for_steps(missing_state_ids)
              current_batch_states.merge!(missing_states)
          end

          next_batch_dependents = []
          current_batch.each do |step_id|
            current_state = current_batch_states[step_id]

            # Only proceed if the step is PENDING
            if current_state == StateMachine::PENDING.to_s
              pending_descendants.add(step_id)
              # Find dependents of this pending step to continue traversal
              # TODO: Optimize using get_parent_ids_multi if available?
              direct_deps = repository.get_parent_ids(step_id)
              direct_deps.each do |dep_id|
                # Add to queue only if not already visited
                if visited.add?(dep_id)
                  next_batch_dependents << dep_id
                end
              end
            else
              # If step is not pending, stop traversing down this path
              log_debug "Skipping descendants of step #{step_id} (state: #{current_state || 'unknown'}) during pending search."
            end
          end

          # Fetch states for the next batch of dependents before adding to queue
          if next_batch_dependents.any?
            states_to_check.merge!(fetch_states_for_steps(next_batch_dependents))
            queue.concat(next_batch_dependents)
          end
        end

        pending_descendants.to_a
      end
      # --- END HELPER ---


      # Fetches dependencies for multiple steps, using bulk operation if available.
      def fetch_dependencies_for_steps(step_ids)
        parent_map = {}
        all_parent_ids = []
        return [parent_map, all_parent_ids] if step_ids.empty? # Handle empty input

        if repository.respond_to?(:get_parent_ids_multi)
          parent_map = repository.get_parent_ids_multi(step_ids)
          # Ensure all requested steps have an entry, even if they have no parents
          step_ids.each { |id| parent_map[id] ||= [] }
          all_parent_ids = parent_map.values.flatten.uniq
        else
          # Fallback to N+1 if adapter doesn't support bulk fetch
          log_warn "Repository does not support get_parent_ids_multi, dependency check might be inefficient."
          step_ids.each do |dep_id|
            parent_ids = repository.get_child_ids(dep_id) # N+1 Call
            parent_map[dep_id] = parent_ids
            all_parent_ids.concat(parent_ids)
          end
          all_parent_ids.uniq!
        end
        [parent_map, all_parent_ids]
      end

      # Fetches states for multiple steps, using bulk operation if available.
      def fetch_states_for_steps(step_ids)
        return {} if step_ids.empty?

        if repository.respond_to?(:fetch_step_states)
            repository.fetch_step_states(step_ids)
        else
            log_warn "Repository does not support fetch_step_states, readiness check might be inefficient."
            {} # Return empty hash, individual checks will happen later
        end
      end

      # Checks if a step is ready to start based on its state and parents' states.
      # Uses pre-fetched states if available.
      def is_ready_to_start?(step_id, parent_ids, all_states_hash)
        # Check dependent's state from the pre-fetched hash
        step_state = all_states_hash[step_id]
        # Must exist in hash and be pending
        return false unless step_state == StateMachine::PENDING.to_s

        # Check parent states using the pre-fetched hash
        return true if parent_ids.empty? # No dependencies, it's ready

        parent_ids.all? do |parent_id|
          state = all_states_hash[parent_id]
          # If a parent state is missing from the hash OR not succeeded, dependent is not ready
          state == StateMachine::SUCCEEDED.to_s
        end
      end


      # Checks if a workflow has completed and updates its state.
      def check_workflow_completion(workflow_id)
        # Ensure workflow_id is present
        return unless workflow_id

        running_count = repository.running_step_count(workflow_id)
        enqueued_count = repository.enqueued_step_count(workflow_id)

        log_debug "Checking workflow completion for #{workflow_id}. Running: #{running_count}, Enqueued: #{enqueued_count}"

        # Workflow is complete if no steps are running or enqueued
        if running_count.zero? && enqueued_count.zero?
          current_wf = repository.find_workflow(workflow_id)

          # Do nothing if workflow not found or already in a terminal state (using the *semantic* definition)
          # Use the StateMachine constant here as intended for cancellation checks etc.
          return if current_wf.nil? || StateMachine.terminal?(current_wf.state.to_sym)

          # Determine final state based on whether any steps failed
          has_failures = repository.workflow_has_failures?(workflow_id)
          final_state = has_failures ? StateMachine::FAILED : StateMachine::SUCCEEDED
          finished_at_time = Time.current

          log_info "Workflow #{workflow_id} appears complete. Setting state to #{final_state}."

          update_success = repository.update_workflow_attributes(
            workflow_id,
            { state: final_state.to_s, finished_at: finished_at_time },
            expected_old_state: StateMachine::RUNNING # Should transition from RUNNING
          )

          if update_success
            log_info "Workflow #{workflow_id} successfully set to #{final_state}."
            publish_workflow_finished_event(workflow_id, final_state) # Use helper
          else
             # Workflow state might have changed concurrently (e.g., manual intervention)
             latest_state = repository.find_workflow(workflow_id)&.state || 'unknown'
             log_warn "Failed to set final state for workflow #{workflow_id} (expected 'running', found '#{latest_state}')."
          end
        end
      end

      # --- Event Publishing Helpers (Refactored for consistency) ---
      # [ ... other helpers remain the same ... ]

      def publish_workflow_started_event(workflow_id)
        wf_record = repository.find_workflow(workflow_id)
        return unless wf_record
        payload = { workflow_id: workflow_id, klass: wf_record.klass, started_at: wf_record.started_at }
        publish_event('yantra.workflow.started', payload)
      end

      def publish_step_started_event(step_id)
        step_record = repository.find_step(step_id)
        return unless step_record
        payload = { step_id: step_id, workflow_id: step_record.workflow_id, klass: step_record.klass, started_at: step_record.started_at }
        publish_event('yantra.step.started', payload)
      end

      def publish_step_succeeded_event(step_id)
        step_record = repository.find_step(step_id)
        return unless step_record
        payload = { step_id: step_id, workflow_id: step_record.workflow_id, klass: step_record.klass, finished_at: step_record.finished_at, output: step_record.output }
        publish_event('yantra.step.succeeded', payload)
      end

      # Event publisher helper for step failure
      def publish_step_failed_event(step_id, error_info, finished_at_time)
          step_record = repository.find_step(step_id) # Fetch fresh record for payload
          return unless step_record
          payload = {
            step_id: step_id,
            workflow_id: step_record.workflow_id,
            klass: step_record.klass,
            state: StateMachine::FAILED.to_s, # Send state as string
            finished_at: finished_at_time,
            error: error_info, # Include the error details
            retries: step_record.retries # Include retry count if available
          }
          publish_event('yantra.step.failed', payload)
      end


      def publish_step_enqueued_event(step_record)
        payload = { step_id: step_record.id, workflow_id: step_record.workflow_id, klass: step_record.klass, queue: step_record.queue, enqueued_at: step_record.enqueued_at }
        publish_event('yantra.step.enqueued', payload)
      end

       def publish_step_cancelled_event(step_id)
        step_record = repository.find_step(step_id) # Fetch fresh record for payload
        return unless step_record
        payload = { step_id: step_id, workflow_id: step_record.workflow_id, klass: step_record.klass }
        publish_event('yantra.step.cancelled', payload)
       end

      def publish_workflow_finished_event(workflow_id, final_state)
        final_wf_record = repository.find_workflow(workflow_id)
        return unless final_wf_record
        event_name = (final_state == StateMachine::FAILED) ? 'yantra.workflow.failed' : 'yantra.workflow.succeeded'
        payload = { workflow_id: workflow_id, klass: final_wf_record.klass, state: final_state.to_s, finished_at: final_wf_record.finished_at }
        publish_event(event_name, payload)
      end

      # Centralized event publishing with error handling.
      def publish_event(event_name, payload)
        @notifier&.publish(event_name, payload)
      rescue => e
        log_error "Failed to publish event '#{event_name}' for payload #{payload.inspect}: #{e.message}"
      end


      # --- Logging Helpers ---
      # (Consolidated logging calls slightly)
      def log_info(message) Yantra.logger&.info { "[Orchestrator] #{message}" } end
      def log_warn(message) Yantra.logger&.warn { "[Orchestrator] #{message}" } end
      def log_error(message) Yantra.logger&.error { "[Orchestrator] #{message}" } end
      def log_debug(message) Yantra.logger&.debug { "[Orchestrator] #{message}" } end

    end # class Orchestrator
  end # module Core
end # module Yantra

