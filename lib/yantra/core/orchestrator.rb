# lib/yantra/core/orchestrator.rb
# frozen_string_literal: true

require_relative 'state_machine'
require_relative '../errors'
require_relative '../persistence/repository_interface'
require_relative '../worker/enqueuing_interface'
require_relative '../events/notifier_interface'
require_relative 'step_enqueuer'
require_relative 'dependent_processor'
require_relative 'state_transition_service' # Require the new service

module Yantra
  module Core
    # Orchestrates the lifecycle of workflows and steps, handling state transitions,
    # dependency checks, and triggering background job execution.
    # Uses DependentProcessor and StateTransitionService.
    class Orchestrator
      attr_reader :repository, :worker_adapter, :notifier, :step_enqueuer,
                  :dependent_processor, :transition_service # Added transition_service

      def initialize(repository: nil, worker_adapter: nil, notifier: nil)
        @repository     = repository     || Yantra.repository
        @worker_adapter = worker_adapter || Yantra.worker_adapter
        @notifier       = notifier       || Yantra.notifier
        @logger         = Yantra.logger  || Logger.new(IO::NULL)

        validate_dependencies

        # --- Instantiate Services ---
        @transition_service = StateTransitionService.new(
          repository: @repository,
          logger: @logger
        )
        @step_enqueuer = StepEnqueuer.new(
          repository: @repository,
          worker_adapter: @worker_adapter,
          notifier: @notifier,
          logger: @logger
        )
        @dependent_processor = DependentProcessor.new(
          repository: @repository,
          step_enqueuer: @step_enqueuer,
          logger: @logger
        )
        # --- End Instantiate ---
      end

      # Starts a pending workflow.
      def start_workflow(workflow_id)
        log_info "Starting workflow #{workflow_id}"
        # --- MODIFIED: Use Transition Service ---
        updated = repository.update_workflow_attributes(
          workflow_id,
          { state: StateMachine::RUNNING.to_s, started_at: Time.current },
          expected_old_state: StateMachine::PENDING
        )
        # --- END MODIFIED ---

        unless updated
          # Log is handled within transition_service on failure
          return false
        end

        publish_workflow_started_event(workflow_id)
        enqueue_initial_steps(workflow_id)
        true
      end

      # Marks a step as starting its execution.
      def step_starting(step_id)
        log_info "Starting step #{step_id}"
        step = repository.find_step(step_id)
        unless step
            log_warn "Step #{step_id} not found when attempting to start."
            return false
        end

        current_state_sym = step.state.to_sym

        # Check if the step is in a state allowed to start
        unless StateMachine.can_start?(current_state_sym)
          log_warn "Step #{step_id} in invalid start state: #{current_state_sym}"
          return false
        end

        # If already running, it's an idempotent success
        return true if current_state_sym == StateMachine::RUNNING

        updated = transition_service.transition_step(
          step_id,
          StateMachine::RUNNING,
          # Pass the actual current state as expectation for optimistic lock
          expected_old_state: current_state_sym,
          extra_attrs: { started_at: Time.current }
        )

        if updated
          publish_step_started_event(step_id)
          true
        else
          # Log handled by transition_service, check if it's now running anyway
          (repository.find_step(step_id)&.state.to_s == StateMachine::RUNNING.to_s)
        end
        # --- END MODIFIED ---
      end

      # Handles the successful completion of a step's core logic.
      def handle_post_processing(step_id)
        log_info "Handling post-processing for succeeded step #{step_id}"
        step = repository.find_step(step_id)
        unless step
          log_error "Step #{step_id} not found during handle_post_processing."
          raise Yantra::Errors::StepNotFound, "Step #{step_id} not found for post-processing"
        end

        # --- MODIFIED: Use StateMachine constant ---
        unless step.state.to_sym == StateMachine::POST_PROCESSING
            log_warn "handle_post_processing called for step #{step_id} but state is '#{step.state}' (expected POST_PROCESSING). Skipping."
            return
        end
        # --- END MODIFIED ---

        workflow_id = step.workflow_id

        # Delegate dependent processing (enqueueing successors)
        log_debug "Delegating successor processing for step #{step_id} to DependentProcessor."
        @dependent_processor.process_successors(
          finished_step_id: step_id,
          workflow_id: workflow_id
        )

        # If dependent_processor succeeded without raising:
        finalize_step_succeeded(step_id) # Pass output for potential recording
        check_workflow_completion(workflow_id)

      rescue Yantra::Errors::EnqueueFailed => e
        log_warn "Dependent processing for step #{step_id} failed with retryable enqueue error: #{e.message}. Step remains POST_PROCESSING. Job will retry."
        raise e
      rescue StandardError => e
        log_error("Unexpected error during handle_post_processing for step #{step_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
        handle_post_processing_failure(step_id, e)
      end

      # Marks a step as failed permanently.
      def step_failed(step_id, error_info, expected_old_state: StateMachine::RUNNING)
        log_error "Step #{step_id} failed permanently: #{error_info[:class]} - #{error_info[:message]}"
        finished_at = Time.current

        # --- MODIFIED: Use Transition Service ---
        updated = transition_service.transition_step(
          step_id,
          StateMachine::FAILED,
          expected_old_state: expected_old_state, # Use passed expectation
          extra_attrs: { error: error_info, finished_at: finished_at }
        )
        # --- END MODIFIED ---

        # Log handled by transition_service if update failed due to state mismatch
        # log_failure_state_update(step_id) unless updated # Removed

        # Always set flag and publish event based on intent, even if state update failed
        set_workflow_failure_flag(step_id)
        publish_step_failed_event(step_id, error_info, finished_at)

        # Trigger Dependent Cancellation & Completion Check
        process_failure_cascade_and_check_completion(step_id)

      rescue StandardError => e # Catch errors during this method's own processing
        log_error("Error during step_failed processing for step #{step_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      end


      private

      # --- REMOVED attempt_transition_to_running (logic moved to step_starting/transition_service) ---

      # Finalizes a step after successful post-processing.
      def finalize_step_succeeded(step_id)
        log_info "Finalizing step #{step_id} as SUCCEEDED."
        # --- MODIFIED: Use Transition Service ---
        success = transition_service.transition_step(
          step_id,
          StateMachine::SUCCEEDED,
          expected_old_state: StateMachine::POST_PROCESSING,
          extra_attrs: { finished_at: Time.current }
        )
        # --- END MODIFIED ---

        if success
          publish_step_succeeded_event(step_id)
        else
          # Log handled by transition_service
          log_error "Failed to finalize step #{step_id} to SUCCEEDED."
        end
      end

      # Handles failure during post-processing.
      def handle_post_processing_failure(step_id, error)
         log_error "Handling failure during post-processing for step #{step_id}."
         error_info = format_error(error)
         finished_at = Time.current

         # --- MODIFIED: Use Transition Service ---
         updated = transition_service.transition_step(
            step_id,
            StateMachine::FAILED,
            expected_old_state: StateMachine::POST_PROCESSING,
            extra_attrs: { error: error_info, finished_at: finished_at }
         )
         # --- END MODIFIED ---

         # Log handled by transition_service if update failed due to state mismatch
         # log_failure_state_update(step_id) unless updated # Removed

         # Always set flag and publish event based on intent
         set_workflow_failure_flag(step_id)
         publish_step_failed_event(step_id, error_info, finished_at)

         # Trigger Dependent Cancellation & Completion Check
         process_failure_cascade_and_check_completion(step_id)

      rescue StandardError => e # Catch errors during failure handling itself
        log_error("Error during handle_post_processing_failure for step #{step_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      end

      # Helper for failure cascade + completion check
      def process_failure_cascade_and_check_completion(step_id)
        step = repository.find_step(step_id)
        if step
          workflow_id = step.workflow_id
          log_debug "Delegating failure cascade processing for step #{step_id} to DependentProcessor."
          cancelled_step_ids = @dependent_processor.process_failure_cascade(
            finished_step_id: step_id,
            workflow_id: workflow_id
          )
          (cancelled_step_ids || []).each { |id| publish_step_cancelled_event(id) }
          check_workflow_completion(workflow_id)
        else
           log_error "Step #{step_id} not found after failure, cannot process dependents or check completion."
        end
      rescue StandardError => e
        log_error("Error during process_failure_cascade_and_check_completion for step #{step_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      end


      # Formats an exception for storage
      def format_error(error)
         { class: error.class.name, message: error.message, backtrace: error.backtrace&.first(10) }
      end

      # Validates essential collaborators.
      def validate_dependencies
        unless @repository
          raise Yantra::Errors::ConfigurationError, "Orchestrator requires a valid persistence adapter."
        end
        unless @worker_adapter
          raise Yantra::Errors::ConfigurationError, "Orchestrator requires a valid worker adapter."
        end
        unless @notifier&.respond_to?(:publish)
          log_warn "Notifier is missing or invalid. Events may not be published."
        end
      end

      # --- REMOVED log_failure_state_update (Handled by transition service) ---

      # Sets the has_failures flag on the workflow containing the step.
      def set_workflow_failure_flag(step_id)
        workflow_id = repository.find_step(step_id)&.workflow_id
        return unless workflow_id
        success = repository.update_workflow_attributes(workflow_id, { has_failures: true })
        log_warn "Failed to set has_failures for workflow #{workflow_id}." unless success
      rescue => e
        log_error "Error setting failure flag for workflow containing step #{step_id}: #{e.message}"
      end

      # Enqueues steps ready to run at workflow start.
      def enqueue_initial_steps(workflow_id)
        # 1. Get all PENDING steps for the workflow
        pending_steps = repository.list_steps(workflow_id: workflow_id, status: :pending)
        return if pending_steps.empty?
        pending_step_ids = pending_steps.map(&:id)

        # 2. Get the dependencies for these pending steps
        #    (Handle potential nil return from bulk method)
        dependencies_map = repository.get_dependency_ids_bulk(pending_step_ids) || {}
        pending_step_ids.each { |id| dependencies_map[id] ||= [] } # Ensure entry for all

        # 3. Filter to find steps with NO dependencies
        initial_step_ids = pending_step_ids.select { |id| dependencies_map[id].empty? }

        log_info "Initially enqueueable steps for workflow #{workflow_id}: #{initial_step_ids.inspect}"
        return if initial_step_ids.empty?

        # 4. Call StepEnqueuer
        step_enqueuer.call(workflow_id: workflow_id, step_ids_to_attempt: initial_step_ids)
      rescue StandardError => e
        log_error "Error enqueuing initial steps for #{workflow_id}: #{e.class} - #{e.message}"
      end

      # Checks if workflow is complete and updates state accordingly.
      def check_workflow_completion(workflow_id)
        return unless workflow_id

        # Use the StateMachine constant for non-terminal states
        non_terminal_states = StateMachine::NON_TERMINAL_STATES
        if repository.has_steps_in_states?(workflow_id: workflow_id, states: non_terminal_states)
          log_debug "Workflow #{workflow_id} not complete yet (steps found in states: #{non_terminal_states.inspect})."
          return
        end

        wf = repository.find_workflow(workflow_id)
        return if wf.nil? || [StateMachine::SUCCEEDED, StateMachine::FAILED, StateMachine::CANCELLED].include?(wf.state.to_sym)

        final_state = repository.workflow_has_failures?(workflow_id) ? StateMachine::FAILED : StateMachine::SUCCEEDED

        # --- MODIFIED: Use Transition Service ---
        success = repository.update_workflow_attributes(
          workflow_id,
          { state: final_state.to_s, finished_at: Time.current },
          expected_old_state: StateMachine::RUNNING
        )
        # --- END MODIFIED ---

        if success
          log_info "Workflow #{workflow_id} marked as #{final_state}."
          publish_workflow_finished_event(workflow_id, final_state)
        else
          # Log handled by transition_service
          log_warn "Failed to mark workflow #{workflow_id} as #{final_state}."
        end
      rescue => e
        log_error "Error during check_workflow_completion for #{workflow_id}: #{e.class} - #{e.message}"
      end

      # --- Event Publishing Helpers (Restored Full Format) ---
      def publish_event(name, payload)
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
          step_id: step_id, workflow_id: step.workflow_id, klass: step.klass, finished_at: step.finished_at, output: step.output
        })
      end

      def publish_step_failed_event(step_id, error_info, finished_at)
        step = repository.find_step(step_id)
        return unless step
        publish_event('yantra.step.failed', {
          step_id: step_id, workflow_id: step.workflow_id, klass: step.klass, error: error_info, finished_at: finished_at, state: StateMachine::FAILED.to_s, retries: step.respond_to?(:retries) ? step.retries : 0
        })
      end

      def publish_step_cancelled_event(step_id)
        step = repository.find_step(step_id)
        return unless step
        publish_event('yantra.step.cancelled', {
          step_id: step_id, workflow_id: step.workflow_id, klass: step.klass
        })
      end
      # --- End Event Publishing Helpers ---

      # --- Logging Helpers ---
      def log_info(msg);  @logger&.info  { "[Orchestrator] #{msg}" } end
      def log_warn(msg);  @logger&.warn  { "[Orchestrator] #{msg}" } end
      def log_error(msg); @logger&.error { "[Orchestrator] #{msg}" } end
      def log_debug(msg); @logger&.debug { "[Orchestrator] #{msg}" } end

    end # class Orchestrator
  end # module Core
end # module Yantra

