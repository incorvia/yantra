# lib/yantra/core/orchestrator.rb
# frozen_string_literal: true

require_relative 'state_machine'
require_relative '../errors'
require_relative '../persistence/repository_interface'
require_relative '../worker/enqueuing_interface'
require_relative '../events/notifier_interface'
require_relative 'step_enqueuer'
require_relative 'dependent_processor'

module Yantra
  module Core
    # Orchestrates the lifecycle of workflows and steps, handling state transitions,
    # dependency checks, and triggering background job execution.
    class Orchestrator
      attr_reader :repository, :worker_adapter, :notifier, :step_enqueuer, :dependent_processor

      def initialize(repository: nil, worker_adapter: nil, notifier: nil)
        @repository     = repository     || Yantra.repository
        @worker_adapter = worker_adapter || Yantra.worker_adapter
        @notifier       = notifier       || Yantra.notifier
        @logger         = Yantra.logger  || Logger.new(IO::NULL)

        validate_dependencies

        @step_enqueuer = StepEnqueuer.new(
          repository: @repository,
          worker_adapter: @worker_adapter,
          notifier: @notifier
        )

        @dependent_processor = DependentProcessor.new(
          repository: @repository,
          step_enqueuer: @step_enqueuer,
          logger: @logger
        )
      end

      # Starts a pending workflow.
      def start_workflow(workflow_id)
        log_info "Starting workflow #{workflow_id}"
        updated = repository.update_workflow_attributes(
          workflow_id,
          { state: StateMachine::RUNNING.to_s, started_at: Time.current },
          expected_old_state: StateMachine::PENDING
        )

        unless updated
          current = repository.find_workflow(workflow_id)&.state || 'unknown'
          log_warn "Failed to start workflow #{workflow_id}, expected 'pending' state but found '#{current}'."
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
        return false unless step

        allowed_states = [StateMachine::ENQUEUED.to_s, StateMachine::RUNNING.to_s]
        unless allowed_states.include?(step.state.to_s)
          log_warn "Step #{step_id} in invalid start state: #{step.state}"
          return false
        end

        if step.state.to_s == StateMachine::ENQUEUED.to_s
          updated = repository.update_step_attributes(
            step_id,
            { state: StateMachine::RUNNING.to_s, started_at: Time.current },
            expected_old_state: StateMachine::ENQUEUED
          )

          if updated
            # log_debug "Step #{step_id} successfully updated to RUNNING." # Removed debug log
            publish_step_started_event(step_id)
          else
            log_warn "Could not transition step #{step_id} to RUNNING (update returned false)."
          end
        end
        true
      end

      # Marks a step as succeeded.
      def step_succeeded(step_id, output)
        log_info "Step #{step_id} succeeded"
        updated = repository.update_step_attributes(
          step_id,
          { state: StateMachine::SUCCEEDED.to_s, finished_at: Time.current },
          expected_old_state: StateMachine::RUNNING
        )
        repository.update_step_output(step_id, output)
        publish_step_succeeded_event(step_id) if updated
        step_finished(step_id)
      end

      # Marks a step as failed permanently.
      def step_failed(step_id, error_info, expected_old_state: StateMachine::RUNNING)
        log_error "Step #{step_id} failed permanently: #{error_info[:class]} - #{error_info[:message]}"
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
        log_failure_state_update(step_id) unless updated
        set_workflow_failure_flag(step_id)
        publish_step_failed_event(step_id, error_info, finished_at)
        step_finished(step_id)
      end

      # Handles processing after a step reaches a state requiring downstream evaluation.
      def step_finished(step_id)
        log_info "Step #{step_id} finished processing by worker"
        step = repository.find_step(step_id)
        unless step
          log_warn("Step #{step_id} not found when processing finish.")
          return
        end

        finished_state = step.state.to_sym
        workflow_id = step.workflow_id

        case finished_state
        when StateMachine::SUCCEEDED, StateMachine::FAILED, StateMachine::CANCELLED
          # log_debug "Step #{step_id} reached state #{finished_state}, processing dependents..." # Removed debug log
          cancelled_step_ids = @dependent_processor.call(
            finished_step_id: step_id,
            finished_state: finished_state,
            workflow_id: workflow_id
          )

          if cancelled_step_ids&.any?
            log_info "Publishing cancellation events for steps: #{cancelled_step_ids.inspect}"
            cancelled_step_ids.each { |cancelled_id| publish_step_cancelled_event(cancelled_id) }
          end

          check_workflow_completion(workflow_id)
        else
          log_warn "Step #{step_id} reported finished but state is '#{step.state}', skipping dependent processing."
        end

      rescue StandardError => e
        log_error("Error during step_finished for step #{step_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
        raise e
      end

      # Utility for formatting benchmark output (consider removing if not used externally)
      def format_benchmark(label, measurement)
        "#{label}: #{measurement.real.round(4)}s real, #{measurement.total.round(4)}s cpu"
      end

      private

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

      # Logs warning if marking step failed was unsuccessful.
      def log_failure_state_update(step_id)
        log_warn "Step #{step_id} could not be marked FAILED (update returned false)."
      end

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
        step_ids = repository.list_ready_steps(workflow_id: workflow_id)
        log_info "Initially ready steps for workflow #{workflow_id}: #{step_ids.inspect}"
        return if step_ids.empty?
        step_enqueuer.call(workflow_id: workflow_id, step_ids_to_attempt: step_ids)
      rescue StandardError => e
        log_error "Error enqueuing initial steps for #{workflow_id}: #{e.class} - #{e.message}"
      end

      # Checks if workflow is complete and updates state accordingly.
      def check_workflow_completion(workflow_id)
        return unless workflow_id
        has_running = repository.running_step_count(workflow_id) > 0
        has_enqueued = repository.enqueued_step_count(workflow_id) > 0
        return if has_running || has_enqueued # Not complete if steps are active

        wf = repository.find_workflow(workflow_id)
        # Check if already finished or not found
        return if wf.nil? || [StateMachine::SUCCEEDED, StateMachine::FAILED, StateMachine::CANCELLED].include?(wf.state.to_sym)

        final_state = repository.workflow_has_failures?(workflow_id) ? StateMachine::FAILED : StateMachine::SUCCEEDED
        success = repository.update_workflow_attributes(
          workflow_id,
          { state: final_state.to_s, finished_at: Time.current },
          expected_old_state: StateMachine::RUNNING
        )

        if success
          log_info "Workflow #{workflow_id} marked as #{final_state}."
          publish_workflow_finished_event(workflow_id, final_state)
        else
          current_state = repository.find_workflow(workflow_id)&.state || 'unknown'
          log_warn "Failed to mark workflow #{workflow_id} as #{final_state}. Expected RUNNING state, found '#{current_state}'."
        end
      rescue => e
        log_error "Error during check_workflow_completion for #{workflow_id}: #{e.class} - #{e.message}"
      end

      # --- Event Publishing Helpers ---
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

      # --- Logging Helpers ---
      def log_info(msg);  @logger&.info  { "[Orchestrator] #{msg}" } end
      def log_warn(msg);  @logger&.warn  { "[Orchestrator] #{msg}" } end
      def log_error(msg); @logger&.error { "[Orchestrator] #{msg}" } end
      def log_debug(msg); @logger&.debug { "[Orchestrator] #{msg}" } end

    end # class Orchestrator
  end # module Core
end # module Yantra
