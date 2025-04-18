# lib/yantra/core/orchestrator.rb

require_relative 'state_machine'
require_relative '../errors'

module Yantra
  module Core
    # Responsible for managing the state transitions and execution flow of a workflow.
    class Orchestrator
      attr_reader :repository, :worker_adapter, :notifier

      # Initializes the orchestrator with dependencies.
      def initialize(repository: nil, worker_adapter: nil, notifier: nil)
        @repository = repository || Yantra.repository
        @worker_adapter = worker_adapter || Yantra.worker_adapter
        @notifier = notifier || Yantra.notifier

        validate_dependencies
      end

      # Starts a workflow if it's in a pending state.
      # Returns true if started successfully, false otherwise.
      def start_workflow(workflow_id)
        log_info "Attempting to start workflow #{workflow_id}"

        update_success = repository.update_workflow_attributes(
          workflow_id,
          { state: StateMachine::RUNNING.to_s, started_at: Time.current },
          expected_old_state: StateMachine::PENDING
        )

        unless update_success
          current_state = repository.find_workflow(workflow_id)&.state || 'not_found'
          log_warn "Workflow #{workflow_id} could not be started. Expected state 'pending', found '#{current_state}'."
          return false
        end

        log_info "Workflow #{workflow_id} state set to RUNNING."
        publish_workflow_started_event(workflow_id)
        find_and_enqueue_ready_steps(workflow_id)
        true
      end

      # Called by the worker job just before executing the step's perform method.
      # Returns true if the step is ready to start, false otherwise.
      def step_starting(step_id)
        log_info "Starting job #{step_id}"
        step = repository.find_step(step_id)
        return false unless step # Step not found

        allowed_start_states = [StateMachine::ENQUEUED.to_s, StateMachine::RUNNING.to_s]
        current_state_str = step.state.to_s

        unless allowed_start_states.include?(current_state_str)
          log_warn "Step #{step_id} cannot start; not in enqueued or running state (current: #{current_state_str})."
          return false
        end

        # If enqueued, transition to running and publish event
        if current_state_str == StateMachine::ENQUEUED.to_s
          update_success = repository.update_step_attributes(
            step_id,
            { state: StateMachine::RUNNING.to_s, started_at: Time.current },
            expected_old_state: StateMachine::ENQUEUED
          )

          unless update_success
            log_warn "Failed to update step #{step_id} state to running before execution (maybe state changed concurrently?)."
            return false
          end
          publish_step_started_event(step_id)

        # If already running (e.g., retry), log and proceed without state change/event
        elsif current_state_str == StateMachine::RUNNING.to_s
          log_info "Step #{step_id} already running, proceeding with execution attempt."
        end

        true # Allow execution
      end

      # Called by the worker job after step execution succeeds.
      def step_succeeded(step_id, output)
        log_info "Job #{step_id} succeeded."

        update_success = repository.update_step_attributes(
          step_id,
          { state: StateMachine::SUCCEEDED.to_s, finished_at: Time.current },
          expected_old_state: StateMachine::RUNNING
        )

        repository.record_step_output(step_id, output)

        unless update_success
          current_state = repository.find_step(step_id)&.state || 'not_found'
          log_warn "Failed to update step #{step_id} state to succeeded (expected 'running', found '#{current_state}'). Output recorded anyway."
          # Proceed to step_finished even if state update failed, as output is recorded
        end

        publish_step_succeeded_event(step_id) if update_success
        step_finished(step_id)
      end

      # Common logic executed after a step finishes (succeeded, failed, cancelled).
      def step_finished(step_id)
        log_info "Handling post-finish logic for job #{step_id}."
        finished_step = repository.find_step(step_id)
        return unless finished_step # Should not happen if called after success/failure handling

        process_dependents(step_id, finished_step.state.to_sym)
        check_workflow_completion(finished_step.workflow_id)
      end

      def step_failed(step_id, error_info)
        log_error "Job #{step_id} failed permanently. Error: #{error_info[:class]} - #{error_info[:message]}"
        finished_at_time = Time.current # Capture consistent time

        # Update step state to FAILED, storing error details
        update_success = repository.update_step_attributes(
          step_id,
          {
            state: StateMachine::FAILED.to_s,
            error: error_info, # Store the provided error hash
            finished_at: finished_at_time
          },
          expected_old_state: StateMachine::RUNNING # Should fail from running state
        )

        unless update_success
          current_state = repository.find_step(step_id)&.state || 'not_found'
          log_warn "Failed to update step #{step_id} state to failed (expected 'running', found '#{current_state}')."
          # If update failed, step is likely already in a terminal state.
          # Still call step_finished to ensure workflow consistency checks run.
        end

        # Set workflow failure flag (idempotent)
        # Need workflow_id - fetch step record if update succeeded or if needed regardless
        workflow_id = repository.find_step(step_id)&.workflow_id
        if workflow_id
          repository.set_workflow_has_failures_flag(workflow_id)
        else
          log_error "Cannot find workflow_id for failed step #{step_id} to set failure flag."
        end

        # Publish event only if state transition was successful
        publish_step_failed_event(step_id, error_info, finished_at_time) if update_success

        # Trigger downstream processing (cancel dependents, check workflow completion)
        step_finished(step_id)
      end

      private

      # Validates the presence and type/interface of required dependencies.
      def validate_dependencies
        unless @repository&.is_a?(Persistence::RepositoryInterface)
          raise Yantra::Errors::ConfigurationError, "Yantra Orchestrator requires a valid persistence repository adapter."
        end
        unless @worker_adapter&.is_a?(Worker::EnqueuingInterface)
          raise Yantra::Errors::ConfigurationError, "Yantra Orchestrator requires a valid worker enqueuing adapter."
        end
        unless @notifier&.respond_to?(:publish)
           raise Yantra::Errors::ConfigurationError, "Yantra Orchestrator requires a valid event notifier adapter (responding to #publish)."
        end
      end

      # Finds steps whose dependencies are met and enqueues them.
      def find_and_enqueue_ready_steps(workflow_id)
        ready_step_ids = repository.find_ready_steps(workflow_id)
        log_info "Found ready steps for workflow #{workflow_id}: #{ready_step_ids}"
        ready_step_ids.each { |step_id| enqueue_step(step_id) }
      end

      # Enqueues a single step after marking it as ENQUEUED.
      def enqueue_step(step_id)
        step_record_initial = repository.find_step(step_id)
        return unless step_record_initial # Step might have been processed/deleted concurrently

        log_info "Attempting to enqueue step #{step_id} in workflow #{step_record_initial.workflow_id}"

        update_success = repository.update_step_attributes(
          step_id,
          { state: StateMachine::ENQUEUED.to_s, enqueued_at: Time.current },
          expected_old_state: StateMachine::PENDING
        )

        unless update_success
          current_state = step_record_initial.state || 'unknown' # Use initial state if available
          log_warn "Step #{step_id} could not be marked enqueued. Expected state 'pending', found '#{current_state}'. Skipping enqueue."
          return
        end

        # Fetch again to get enqueued_at timestamp for event/job
        step_record_for_job = repository.find_step(step_id)
        unless step_record_for_job
          log_error "Step #{step_id} not found after successful state update to enqueued!"
          return # Cannot proceed without step record
        end

        log_info "Step #{step_id} marked enqueued for Workflow #{step_record_for_job.workflow_id}."
        publish_step_enqueued_event(step_record_for_job)
        enqueue_job_via_adapter(step_record_for_job)
      end

      # Enqueues the job using the worker adapter, handling potential errors.
      def enqueue_job_via_adapter(step)
        worker_adapter.enqueue(step.id, step.workflow_id, step.klass, step.queue)
        log_debug "Successfully enqueued job #{step.id} via worker adapter."
      rescue => e
        log_error "Failed to enqueue job #{step.id} via worker adapter: #{e.message}. Attempting to mark step as failed."
        # Attempt to mark the step as failed since it couldn't be enqueued
        repository.update_step_attributes(
          step.id,
          {
            state: StateMachine::FAILED.to_s,
            error: { class: e.class.name, message: "Failed to enqueue: #{e.message}" },
            finished_at: Time.current
          }
          # No expected_old_state here, force fail state if enqueue fails
        )
        repository.set_workflow_has_failures_flag(step.workflow_id)
        # Need to check workflow completion again after this failure
        check_workflow_completion(step.workflow_id)
      end

      # Checks dependents of a finished step and enqueues or cancels them.
      def process_dependents(finished_step_id, finished_step_state)
        dependent_ids = repository.get_step_dependents(finished_step_id)
        return if dependent_ids.empty?

        log_debug "Processing dependents for finished step #{finished_step_id} (state: #{finished_step_state}): #{dependent_ids}"

        parent_map, all_parent_ids = fetch_dependencies_for_steps(dependent_ids)
        all_relevant_step_ids = (dependent_ids + all_parent_ids).uniq
        all_states_hash = fetch_states_for_steps(all_relevant_step_ids)

        dependent_ids.each do |dep_id|
          process_single_dependent(dep_id, finished_step_state, parent_map, all_states_hash)
        end
      end

      # Fetches dependencies for multiple steps, using bulk operation if available.
      def fetch_dependencies_for_steps(step_ids)
        parent_map = {}
        all_parent_ids = []

        if repository.respond_to?(:get_step_dependencies_multi)
          parent_map = repository.get_step_dependencies_multi(step_ids)
          # Ensure all requested steps have an entry, even if they have no parents
          step_ids.each { |id| parent_map[id] ||= [] }
          all_parent_ids = parent_map.values.flatten.uniq
        else
          log_warn "Repository does not support get_step_dependencies_multi, dependency check might be inefficient."
          step_ids.each do |dep_id|
             parent_ids = repository.get_step_dependencies(dep_id) # N+1 Call
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

      # Processes a single dependent step based on the finished parent's state.
      def process_single_dependent(dependent_id, finished_parent_state, parent_map, all_states_hash)
        if finished_parent_state == StateMachine::SUCCEEDED
          parent_ids_for_dep = parent_map.fetch(dependent_id, [])
          if ready_to_start?(dependent_id, parent_ids_for_dep, all_states_hash)
            enqueue_step(dependent_id)
          else
            log_debug "Dependent #{dependent_id} not ready yet."
          end
        else # Parent failed or was cancelled
          # Pass state hash to avoid re-fetching in cancel_downstream_pending
          cancel_downstream_pending(dependent_id, all_states_hash)
        end
      end

      # Checks if a step is ready to start based on its state and parents' states.
      # Uses pre-fetched states if available.
      def ready_to_start?(step_id, parent_ids, all_states_hash)
        # Check dependent's own state first. Must be PENDING.
        step_state = all_states_hash[step_id]
        # If state wasn't pre-fetched, fetch it now (fallback)
        step_state ||= repository.find_step(step_id)&.state&.to_s

        return false unless step_state == StateMachine::PENDING.to_s

        # If no parents, it's ready.
        return true if parent_ids.empty?

        # Check parent states. All must be SUCCEEDED.
        parent_ids.all? do |parent_id|
          parent_state = all_states_hash[parent_id]
          # If state wasn't pre-fetched, fetch it now (fallback)
          parent_state ||= repository.find_step(parent_id)&.state&.to_s

          parent_state == StateMachine::SUCCEEDED.to_s
        end
      end

      # Recursively cancels downstream steps that are PENDING.
      # Uses pre-fetched states if available (`all_states_hash`).
      def cancel_downstream_pending(step_id, all_states_hash = {})
        step_state = all_states_hash[step_id]
        # If state wasn't pre-fetched, fetch it individually.
        step_state ||= repository.find_step(step_id)&.state&.to_s

        # Only proceed if the step is currently PENDING.
        return unless step_state == StateMachine::PENDING.to_s

        log_debug "Recursively cancelling pending step #{step_id}"

        update_success = repository.update_step_attributes(
          step_id,
          { state: StateMachine::CANCELLED.to_s, finished_at: Time.current },
          expected_old_state: StateMachine::PENDING
        )

        unless update_success
          # Log warning if state update failed (e.g., state changed concurrently).
          log_warn "Failed to update state to cancelled for downstream step #{step_id} during cascade."
          # Do not recurse if the update failed, the state is no longer PENDING.
          return
        end

        # Publish event only after successful cancellation.
        publish_step_cancelled_event(step_id)

        # Recurse to its dependents *after* successful cancellation.
        repository.get_step_dependents(step_id).each do |dep_id|
          # Pass the hash down for efficiency.
          cancel_downstream_pending(dep_id, all_states_hash)
        end

      # General rescue block to catch unexpected errors during cancellation cascade.
      rescue => e
         log_error "ERROR inside cancel_downstream_pending(#{step_id}): #{e.class} - #{e.message}\n#{e.backtrace.first(5).join("\n")}"
         # Re-raise the exception after logging to ensure it's not swallowed.
         raise e
      end

      # Checks if a workflow has completed (no running or enqueued steps)
      # and updates its state to SUCCEEDED or FAILED.
      def check_workflow_completion(workflow_id)
        running_count = repository.running_step_count(workflow_id)
        enqueued_count = repository.enqueued_step_count(workflow_id)

        log_debug "Checking workflow completion for #{workflow_id}. Running: #{running_count}, Enqueued: #{enqueued_count}"

        # Workflow is complete if no steps are running or enqueued
        if running_count.zero? && enqueued_count.zero?
          current_wf = repository.find_workflow(workflow_id)

          # Do nothing if workflow not found or already in a terminal state
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
            publish_workflow_finished_event(workflow_id, final_state)
          else
             # Workflow state might have changed concurrently (e.g., manual intervention)
             latest_state = repository.find_workflow(workflow_id)&.state || 'unknown'
             log_warn "Failed to set final state for workflow #{workflow_id} (expected 'running', found '#{latest_state}')."
          end
        end
      end

      # --- Event Publishing Helpers ---

      def publish_workflow_started_event(workflow_id)
        wf_record = repository.find_workflow(workflow_id)
        return unless wf_record # Guard against race condition where workflow deleted
        payload = {
          workflow_id: workflow_id,
          klass: wf_record.klass,
          started_at: wf_record.started_at
        }
        publish_event('yantra.workflow.started', payload)
      end

      def publish_step_started_event(step_id)
        step_record = repository.find_step(step_id)
        return unless step_record
        payload = {
          step_id: step_id,
          workflow_id: step_record.workflow_id,
          klass: step_record.klass,
          started_at: step_record.started_at
        }
        publish_event('yantra.step.started', payload)
      end

      def publish_step_succeeded_event(step_id)
        step_record = repository.find_step(step_id)
        return unless step_record
        payload = {
          step_id: step_id,
          workflow_id: step_record.workflow_id,
          klass: step_record.klass,
          finished_at: step_record.finished_at,
          output: step_record.output # Assumes output is loaded here
        }
        publish_event('yantra.step.succeeded', payload)
      end

      def publish_step_enqueued_event(step_record)
        # Accepts record directly as it was just fetched in enqueue_step
        payload = {
          step_id: step_record.id,
          workflow_id: step_record.workflow_id,
          klass: step_record.klass,
          queue: step_record.queue,
          enqueued_at: step_record.enqueued_at
        }
        publish_event('yantra.step.enqueued', payload)
      end

       def publish_step_cancelled_event(step_id)
        step_record = repository.find_step(step_id) # Fetch fresh record for payload
        return unless step_record
        payload = {
          step_id: step_id,
          workflow_id: step_record.workflow_id,
          klass: step_record.klass
          # finished_at is set, but maybe not crucial for this event type
        }
        publish_event('yantra.step.cancelled', payload)
      end

      def publish_workflow_finished_event(workflow_id, final_state)
        final_wf_record = repository.find_workflow(workflow_id)
        return unless final_wf_record
        event_name = (final_state == StateMachine::FAILED) ?
          'yantra.workflow.failed' : 'yantra.workflow.succeeded'
        payload = {
          workflow_id: workflow_id,
          klass: final_wf_record.klass,
          state: final_state.to_s, # Send state as string
          finished_at: final_wf_record.finished_at
        }
        publish_event(event_name, payload)
      end

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

      # Centralized event publishing with error handling.
      def publish_event(event_name, payload)
        @notifier&.publish(event_name, payload)
      rescue => e
        log_error "Failed to publish event '#{event_name}' for payload #{payload.inspect}: #{e.message}"
        # Decide if failure to publish event should be critical? Currently just logs.
      end


      # --- Logging Helpers ---

      def log_info(message)
        Yantra.logger&.info { "[Orchestrator] #{message}" }
      end

      def log_warn(message)
        Yantra.logger&.warn { "[Orchestrator] #{message}" }
      end

      def log_error(message)
        Yantra.logger&.error { "[Orchestrator] #{message}" }
      end

      def log_debug(message)
        Yantra.logger&.debug { "[Orchestrator] #{message}" }
      end

    end # class Orchestrator
  end # module Core
end # module Yantra
