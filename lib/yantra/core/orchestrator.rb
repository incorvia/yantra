# lib/yantrsetup_mocha_mocks_and_orchestratora/core/orchestrator.rb

require_relative 'state_machine'
require_relative '../errors'
require_relative '../persistence/repository_interface'
require_relative '../worker/enqueuing_interface'
require_relative '../events/notifier_interface'
require_relative 'step_enqueuing_service' # <<< CHANGED: Require the new service

module Yantra
  module Core
    # Orchestrates the workflow lifecycle based on step completion events.
    # It interacts with the repository, worker adapter, and notifier.
    class Orchestrator
      attr_reader :repository, :worker_adapter, :notifier, :step_enqueuer # <<< CHANGED: Added step_enqueuer

      # Initializes the orchestrator with necessary adapters.
      # Falls back to globally configured adapters if none are provided.
      # Instantiates the StepEnqueuingService.
      def initialize(repository: nil, worker_adapter: nil, notifier: nil)
        @repository     = repository || Yantra.repository
        @worker_adapter = worker_adapter || Yantra.worker_adapter
        @notifier       = notifier || Yantra.notifier

        # <<< CHANGED: Instantiate the StepEnqueuingService >>>
        @step_enqueuer = StepEnqueuingService.new(
          repository: @repository,
          worker_adapter: @worker_adapter,
          notifier: @notifier
        )
        # <<< END CHANGED >>>

        # Validation checks
        unless @repository.is_a?(Persistence::RepositoryInterface)
          raise Yantra::Errors::ConfigurationError, "Orchestrator requires a valid persistence adapter."
        end
        unless @worker_adapter.is_a?(Worker::EnqueuingInterface)
          raise Yantra::Errors::ConfigurationError, "Orchestrator requires a valid worker adapter."
        end
        unless @notifier.respond_to?(:publish)
           # Allowing notifier to be optional if StepEnqueuingService handles nil notifier
           log_warn "[Orchestrator] Notifier is missing or invalid. Events may not be published by StepEnqueuingService."
        end
      end

      # Starts a workflow by setting its state to running and enqueuing initial ready steps.
      #
      # @param workflow_id [String] The ID of the workflow to start.
      # @return [Boolean] True if successfully started, false otherwise.
      def start_workflow(workflow_id)
        log_info "Starting workflow #{workflow_id}"
        updated = repository.update_workflow_attributes(
          workflow_id,
          { state: StateMachine::RUNNING.to_s, started_at: Time.current },
          expected_old_state: StateMachine::PENDING
        )

        unless updated
          current = repository.find_workflow(workflow_id)&.state || 'unknown'
          log_warn "Failed to start workflow #{workflow_id}, expected 'pending' but found '#{current}'"
          return false
        end

        publish_workflow_started_event(workflow_id)
        # <<< CHANGED: Call refactored method >>>
        find_and_enqueue_initial_steps(workflow_id)
        true
      end

      # Marks a step as starting its execution.
      # Ensures the step is in a valid state to start (enqueued or already running).
      #
      # @param step_id [String] The ID of the step starting.
      # @return [Boolean] True if successfully marked as starting or already running, false otherwise.
      def step_starting(step_id)
        log_info "Starting step #{step_id}"
        step = repository.find_step(step_id)
        return false unless step

        # Allow idempotency: if already running, it's okay.
        allowed = [StateMachine::ENQUEUED.to_s, StateMachine::RUNNING.to_s]
        unless allowed.include?(step.state.to_s)
          log_warn "Step #{step_id} in invalid start state: #{step.state}"
          return false
        end

        # Only update state and publish event if transitioning from ENQUEUED
        if step.state.to_s == StateMachine::ENQUEUED.to_s
          updated = repository.update_step_attributes(
            step_id,
            { state: StateMachine::RUNNING.to_s, started_at: Time.current },
            expected_old_state: StateMachine::ENQUEUED
          )


          if updated
            log_debug "Step #{step_id} successfully updated to RUNNING." # Optional debug log
            publish_step_started_event(step_id)
          else
            log_warn "Could not transition step #{step_id} to RUNNING (update returned false)."
            # Decide on return value. Current code returns true below.
            # Consider if returning false here makes more sense.
          end
        end

        true
      end

      # Marks a step as succeeded, records its output, and processes dependents.
      #
      # @param step_id [String] The ID of the step that succeeded.
      # @param output [Hash, Object] The output data produced by the step.
      # @return [void]
      def step_succeeded(step_id, output)
        log_info "Step #{step_id} succeeded"
        updated = repository.update_step_attributes(
          step_id,
          { state: StateMachine::SUCCEEDED.to_s, finished_at: Time.current },
          expected_old_state: StateMachine::RUNNING
        )

        # Record output regardless of state transition success (might have finished already)
        repository.record_step_output(step_id, output)

        # Publish event only if state transition was successful
        publish_step_succeeded_event(step_id) if updated

        # Process dependents and check workflow completion
        step_finished(step_id)
      end

      # Marks a step as failed, records error info, sets workflow failure flag, and processes dependents.
      #
      # @param step_id [String] The ID of the step that failed.
      # @param error_info [Hash] Information about the error ({ class:, message:, backtrace: }).
      # @param expected_old_state [Symbol] The state the step was expected to be in before failing.
      # @return [void]
      def step_failed(step_id, error_info, expected_old_state: StateMachine::RUNNING)
        log_error "Step #{step_id} failed: #{error_info[:class]} - #{error_info[:message]}"
        finished_at = Time.current

        updated = repository.update_step_attributes(
          step_id,
          {
            state: StateMachine::FAILED.to_s,
            error: error_info,
            finished_at: finished_at
          },
          expected_old_state: expected_old_state
        )

        unless updated
          current = repository.find_step(step_id)&.state || 'unknown'
          log_warn "Step #{step_id} could not be marked FAILED (found '#{current}')"
          # Proceed anyway to ensure dependents are handled and failure flag is set
        end

        # Set workflow failure flag (idempotent)
        workflow_id = repository.find_step(step_id)&.workflow_id

        if workflow_id
          update_success = repository.update_workflow_attributes(
            workflow_id,
            { has_failures: true }
            # No expected_old_state needed here, just ensure the flag is set
          )
          unless update_success
            log_warn "[Orchestrator] Failed to set has_failures flag for workflow #{workflow_id} via update_workflow_attributes."
          end
        end

        # Publish event and process dependents
        publish_step_failed_event(step_id, error_info, finished_at)
        step_finished(step_id)
      end

      # Common logic executed after a step reaches a terminal state (succeeded, failed, cancelled).
      # Processes dependent steps and checks for overall workflow completion.
      #
      # @param step_id [String] The ID of the step that finished.
      # @return [void]
      def step_finished(step_id)
        log_info "Step #{step_id} finished processing by worker"
        step = repository.find_step(step_id) # Mock raises PersistenceError here in the test
        unless step
          log_warn "Step #{step_id} not found when processing finish, maybe deleted?"
          return
        end

        workflow_id = step.workflow_id
        current_state = step.state.to_sym

        case current_state
        when StateMachine::SUCCEEDED, StateMachine::FAILED, StateMachine::CANCELLED
          process_dependents(step_id, current_state, workflow_id)
          check_workflow_completion(workflow_id)
        else
          log_warn "Step #{step_id} reported finished but state is '#{step.state}', skipping downstream logic."
        end
      rescue StandardError => e
        log_error("Error during step_finished for step #{step_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
        # Re-raise the exception so the caller knows something went wrong
        raise e # <<< ADDED re-raise
      end


      private

      # --- <<< CHANGED: Renamed and Refactored using StepEnqueuingService >>> ---
      # Finds initially ready steps for a workflow and delegates their enqueuing.
      #
      # @param workflow_id [String] The ID of the workflow.
      # @return [void]
      def find_and_enqueue_initial_steps(workflow_id)
        ready_step_ids = repository.find_ready_steps(workflow_id) # Find initially ready steps
        log_info "Initially ready steps for workflow #{workflow_id}: #{ready_step_ids.inspect}"
        return if ready_step_ids.empty?

        # Delegate to the enqueuing service
        begin
           log_debug "[Orchestrator] Delegating initial enqueue to StepEnqueuingService for steps: #{ready_step_ids.inspect}"
           count = @step_enqueuer.call(
             workflow_id: workflow_id,
             step_ids_to_attempt: ready_step_ids
           )
           log_info "[Orchestrator] StepEnqueuingService reported #{count} initial steps successfully enqueued."
        rescue StandardError => e
            log_error "[Orchestrator] Error during initial step enqueue via StepEnqueuingService for workflow #{workflow_id}: #{e.class} - #{e.message}"
            # Consider impact on workflow state if initial steps fail to enqueue
        end
      end
      # --- <<< END CHANGED >>> ---


      # --- <<< REMOVED OLD HELPERS >>> ---
      # def enqueue_step(step_id) ... END
      # def publish_bulk_enqueued_event(...) ... END
      # def publish_step_enqueued_event(step) ... END
      # --- <<< END REMOVED HELPERS >>> ---


      # --- <<< CHANGED: Refactored process_dependents >>> ---
      # Processes steps dependent on a finished step.
      # If the finished step succeeded, identifies ready dependents and delegates enqueuing.
      # If the finished step failed or was cancelled, cancels downstream dependents.
      #
      # @param finished_step_id [String] The ID of the step that just finished.
      # @param finished_state [Symbol] The final state of the finished step.
      # @param workflow_id [String] The ID of the workflow these steps belong to.
      # @return [void]
      def process_dependents(finished_step_id, finished_state, workflow_id)
        dependents_ids = repository.get_dependent_ids(finished_step_id)
        return if dependents_ids.empty?

        log_debug "[Orchestrator] Processing dependents for #{finished_step_id} in workflow #{workflow_id}: #{dependents_ids.inspect}"

        if finished_state == StateMachine::SUCCEEDED
          # --- Find ready steps and delegate to StepEnqueuingService ---
          parent_map, all_parents = fetch_dependencies_for_steps(dependents_ids)
          states = fetch_states_for_steps(dependents_ids + all_parents)

          ready_step_ids = []
          dependents_ids.each do |step_id|
             next unless states[step_id.to_s] == StateMachine::PENDING.to_s
             parents = parent_map[step_id] || []
             if is_ready_to_start?(step_id, parents, states)
                ready_step_ids << step_id
             else
                log_debug "[Orchestrator] Step #{step_id} not ready yet."
             end
          end

          if ready_step_ids.any?
             log_info "[Orchestrator] Steps ready to enqueue after #{finished_step_id} finished: #{ready_step_ids.inspect}"
             begin
                # Delegate the actual enqueuing, state update, and event publish
                log_debug "[Orchestrator] Delegating dependent enqueue to StepEnqueuingService for steps: #{ready_step_ids.inspect}"
                count = @step_enqueuer.call(
                   workflow_id: workflow_id,
                   step_ids_to_attempt: ready_step_ids
                )
                log_info "[Orchestrator] StepEnqueuingService reported #{count} dependent steps successfully enqueued."
             rescue StandardError => e
                 log_error "[Orchestrator] Error during dependent step enqueue via StepEnqueuingService: #{e.class} - #{e.message}"
             end
          else
              log_debug "[Orchestrator] No dependent steps became ready after #{finished_step_id} finished."
          end
          # --- End Delegation ---
        else
          # Failure/Cancellation Cascade (pass workflow_id if needed)
          cancel_downstream_dependents(workflow_id, dependents_ids, finished_step_id, finished_state)
        end
      end
      # --- <<< END CHANGED >>> ---

      # --- <<< REMOVED process_successful_dependents METHOD >>> ---
      # def process_successful_dependents(workflow_id, potential_step_ids) ... END
      # --- <<< END REMOVAL >>> ---


      # --- Other private methods remain ---

      # Cancels pending descendants of a failed/cancelled step.
      # @param workflow_id [String] Workflow ID for context (optional, depends on implementation)
      # @param initial_step_ids [Array<String>] The direct dependents of the failed step.
      # @param failed_step_id [String] The ID of the step that failed/was cancelled.
      # @param state [Symbol] The state of the step that triggered cancellation (:failed or :cancelled).
      def cancel_downstream_dependents(workflow_id, initial_step_ids, failed_step_id, state)
        log_warn "[Orchestrator] Cancelling downstream steps of #{failed_step_id} (state: #{state})"
        # Assuming find_all_pending_descendants correctly finds steps to cancel
        descendants_to_cancel_ids = find_all_pending_descendants(initial_step_ids)
        return if descendants_to_cancel_ids.empty?

        log_info "[Orchestrator] Bulk cancelling #{descendants_to_cancel_ids.size} steps: #{descendants_to_cancel_ids.inspect}"

        begin
          # Use bulk cancellation method from repository
          cancelled_count = repository.cancel_steps_bulk(descendants_to_cancel_ids)
          log_info "[Orchestrator] Repository reported #{cancelled_count} steps cancelled."

          # Publish individual cancelled events
          descendants_to_cancel_ids.each { |id| publish_step_cancelled_event(id) }
        rescue NotImplementedError
          log_error "[Orchestrator] Repository does not implement cancel_steps_bulk. Cannot cancel downstream steps efficiently."
        rescue Yantra::Errors::PersistenceError => e
           log_error "[Orchestrator] Persistence error during bulk cancellation: #{e.message}"
        rescue => e
          log_error "[Orchestrator] Unexpected error cancelling steps: #{e.class} - #{e.message}"
        end
      end

      # Finds all descendant steps that are still in a pending state, starting from a set of initial steps.
      # Uses breadth-first traversal.
      # @param initial_step_ids [Array<String>] The starting set of step IDs.
      # @return [Array<String>] An array of pending descendant step IDs.
      def find_all_pending_descendants(initial_step_ids)
        pending_descendants = Set.new
        queue = initial_step_ids.dup
        visited = Set.new(initial_step_ids)

        # Limit iterations to prevent infinite loops in case of cycles (though DAGs shouldn't have them)
        max_iterations = 10000 # Adjust as needed
        iterations = 0

        while !queue.empty? && iterations < max_iterations
          iterations += 1
          # Process in batches to avoid huge memory usage for states/dependencies
          current_batch_ids = queue.shift(100) # Process 100 at a time

          # Fetch states only for the current batch
          batch_states = fetch_states_for_steps(current_batch_ids)

          # Find direct dependents for the current batch
          # Assuming get_dependent_ids_bulk exists for efficiency, otherwise loop
          batch_dependents_map = {}
          batch_dependents_map = repository.get_dependent_ids_bulk(current_batch_ids)
          # Ensure all queried IDs have an entry in the map
          current_batch_ids.each { |id| batch_dependents_map[id] ||= [] }

          current_batch_ids.each do |step_id|
            # Check the fetched state for the current step
            if batch_states[step_id.to_s] == StateMachine::PENDING.to_s
              pending_descendants << step_id # Add to list if pending

              # Find its direct children and add to queue if not visited
              direct_dependents = batch_dependents_map[step_id] || []
              direct_dependents.each do |dependent_id|
                if visited.add?(dependent_id) # add? returns nil if already present
                  queue << dependent_id
                end
              end
            end
          end
        end # end while loop

        if iterations >= max_iterations
            log_error "[Orchestrator] find_all_pending_descendants exceeded max iterations, potential cycle or very deep graph?"
        end

        pending_descendants.to_a
      end


      # Fetches dependency information (parent IDs) for multiple steps efficiently.
      # @param step_ids [Array<String>] An array of step IDs.
      # @return [Array(Hash, Array)] A tuple containing:
      #   - parent_map [Hash{String => Array<String>}] Maps step ID to array of its parent IDs.
      #   - all_parents [Array<String>] A unique list of all parent IDs found.
      def fetch_dependencies_for_steps(step_ids)
        return [{}, []] if step_ids.nil? || step_ids.empty?
        unique_ids = step_ids.uniq

        parent_map = {}
        all_parents = []

        # Prefer bulk method if available
        if repository.respond_to?(:get_dependencies_ids_bulk)
          parent_map = repository.get_dependencies_ids_bulk(unique_ids)
          # Ensure all queried steps have an entry, even if empty
          unique_ids.each { |id| parent_map[id] ||= [] }
          all_parents = parent_map.values.flatten.uniq
        else
          # Fallback to individual calls (less efficient)
          log_warn "[Orchestrator] Repository does not implement get_dependencies_ids_bulk, fetching dependencies individually."
          unique_ids.each do |id|
            parents = repository.get_dependencies_ids(id) # Assuming singular version exists
            parent_map[id] = parents
            all_parents.concat(parents)
          end
          all_parents.uniq!
        end

        [parent_map, all_parents]
      end

      # Fetches the current states for multiple steps efficiently.
      # @param step_ids [Array<String>] An array of step IDs.
      # @return [Hash{String => String}] A hash mapping step ID to its state string.
      def fetch_states_for_steps(step_ids)
        return {} if step_ids.nil? || step_ids.empty?
        unique_ids = step_ids.uniq

        if repository.respond_to?(:fetch_step_states)
          repository.fetch_step_states(unique_ids)
        else
          # Fallback (inefficient) - Log warning
          log_warn "[Orchestrator] Repository does not implement fetch_step_states. Cannot efficiently check readiness/cancellation."
          # Return empty hash or try individual finds? Returning empty might break logic.
          # Let's try individual finds with a warning, though it's N+1.
          states = {}
          unique_ids.each do |id|
             step = repository.find_step(id)
             states[id] = step&.state.to_s if step
          end
          states
        end
      rescue Yantra::Errors::PersistenceError => e
         log_error "[Orchestrator] Failed to fetch step states for IDs #{unique_ids.inspect}: #{e.message}"
         {} # Return empty hash on error
      end

      # Checks if a step is ready to start based on its state and its parents' states.
      # @param step_id [String] The ID of the step to check.
      # @param parent_ids [Array<String>] An array of IDs of the step's direct parents.
      # @param states_map [Hash{String => String}] A pre-fetched map of step IDs to their state strings.
      # @return [Boolean] True if the step is ready, false otherwise.
      def is_ready_to_start?(step_id, parent_ids, states_map)
        # Already checked if step_id itself is PENDING before calling this in process_dependents

        # Check if all parents have succeeded
        parent_ids.all? { |pid| states_map[pid.to_s] == StateMachine::SUCCEEDED.to_s }
      end

      # Checks if a workflow has completed (no running or enqueued steps) and updates its state.
      # @param workflow_id [String] The ID of the workflow to check.
      # @return [void]
      def check_workflow_completion(workflow_id)
        return unless workflow_id

        # Check counts efficiently
        running_count = repository.running_step_count(workflow_id)
        enqueued_count = repository.enqueued_step_count(workflow_id)

        # Exit if any steps are still active
        return unless running_count.zero? && enqueued_count.zero?

        # Re-fetch workflow to check current state before potentially marking finished
        wf = repository.find_workflow(workflow_id)
        return if wf.nil? || StateMachine.terminal?(wf.state.to_sym)

        # Determine final state based on failure flag
        final_state = repository.workflow_has_failures?(workflow_id) ? StateMachine::FAILED : StateMachine::SUCCEEDED

        # Atomically update state from RUNNING to the final state
        log_info "[Orchestrator] Attempting to mark workflow #{workflow_id} as #{final_state}"
        success = repository.update_workflow_attributes(
          workflow_id,
          { state: final_state.to_s, finished_at: Time.current },
          expected_old_state: StateMachine::RUNNING # Can only finish from RUNNING state
        )

        if success
          log_info "[Orchestrator] Workflow #{workflow_id} marked as #{final_state}."
          publish_workflow_finished_event(workflow_id, final_state)
        else
          # This might happen in race conditions or if workflow was cancelled/failed concurrently
          log_warn "[Orchestrator] Failed to mark workflow #{workflow_id} as #{final_state}. Expected RUNNING state."
        end
      rescue StandardError => e
          log_error("[Orchestrator] Error during check_workflow_completion for #{workflow_id}: #{e.class} - #{e.message}")
      end

      # --- Event Publishing Helpers ---
      # (Removed publish_step_enqueued_event and publish_bulk_enqueued_event)

      def publish_event(name, payload)
        # Add defensiveness around notifier call
        return unless notifier&.respond_to?(:publish)
        notifier.publish(name, payload)
      rescue => e
        log_error "Failed to publish event #{name}: #{e.message}"
      end

      def publish_workflow_started_event(workflow_id)
        wf = repository.find_workflow(workflow_id)
        return unless wf
        publish_event('yantra.workflow.started', {
          workflow_id: workflow_id, klass: wf.klass, started_at: wf.started_at
        })
      end

      def publish_workflow_finished_event(workflow_id, state)
         wf = repository.find_workflow(workflow_id)
         return unless wf
         event = state == StateMachine::FAILED ? 'yantra.workflow.failed' : 'yantra.workflow.succeeded'
         publish_event(event, {
           workflow_id: workflow_id, klass: wf.klass, state: state.to_s, finished_at: wf.finished_at
         })
      end

      def publish_step_started_event(step_id)
         step = repository.find_step(step_id)
         return unless step
         publish_event('yantra.step.started', {
           step_id: step_id, workflow_id: step.workflow_id, klass: step.klass, started_at: step.started_at
         })
      end

      def publish_step_succeeded_event(step_id)
         step = repository.find_step(step_id)
         return unless step
         publish_event('yantra.step.succeeded', {
           step_id: step_id, workflow_id: step.workflow_id, klass: step.klass,
           finished_at: step.finished_at, output: step.output
         })
      end

      def publish_step_failed_event(step_id, error_info, finished_at)
         step = repository.find_step(step_id)
         return unless step
         publish_event('yantra.step.failed', {
           step_id: step_id, workflow_id: step.workflow_id, klass: step.klass,
           error: error_info, finished_at: finished_at, state: StateMachine::FAILED.to_s,
           retries: step.respond_to?(:retries) ? step.retries : 0 # Handle if retries field doesn't exist
         })
      end

      def publish_step_cancelled_event(step_id)
         step = repository.find_step(step_id)
         return unless step
         publish_event('yantra.step.cancelled', {
           step_id: step_id, workflow_id: step.workflow_id, klass: step.klass
         })
      end

      # --- Logging helpers ---
      def log_info(msg);  Yantra.logger&.info  { "[Orchestrator] #{msg}" } end
      def log_warn(msg);  Yantra.logger&.warn  { "[Orchestrator] #{msg}" } end
      def log_error(msg); Yantra.logger&.error { "[Orchestrator] #{msg}" } end
      def log_debug(msg); Yantra.logger&.debug { "[Orchestrator] #{msg}" } end

    end # class Orchestrator
  end # module Core
end # module Yantra
