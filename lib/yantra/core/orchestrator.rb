# frozen_string_literal: true

require_relative 'state_machine'
require_relative '../errors'
require_relative '../persistence/repository_interface'
require_relative '../worker/enqueuing_interface'
require_relative '../events/notifier_interface'
require_relative 'step_enqueuer'
require_relative 'dependent_processor'
require_relative 'state_transition_service'

module Yantra
  module Core
    class Orchestrator
      attr_reader :repository, :worker_adapter, :notifier, :step_enqueuer,
                  :dependent_processor, :transition_service

      def initialize(repository: nil, worker_adapter: nil, notifier: nil)
        @repository     = repository     || Yantra.repository
        @worker_adapter = worker_adapter || Yantra.worker_adapter
        @notifier       = notifier       || Yantra.notifier
        @logger         = Yantra.logger  || Logger.new(IO::NULL)

        validate_dependencies

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
      end

      def start_workflow(workflow_id)
        log_info "Starting workflow #{workflow_id}"
        updated = repository.update_workflow_attributes(
          workflow_id,
          { state: StateMachine::RUNNING.to_s, started_at: Time.current },
          expected_old_state: StateMachine::PENDING
        )

        return false unless updated

        publish_workflow_started_event(workflow_id)
        enqueue_initial_steps(workflow_id)
        true
      end

      def step_starting(step_id)
        log_info "Starting step #{step_id}"
        step = find_step_for_start!(step_id)
        return false unless startable_state?(step)
        return true if already_running?(step)

        transitioned = transition_step_to_running(step_id, step)
        return true if transitioned

        running_after_retry?(step_id)
      end

      def handle_post_processing(step_id)
        log_info "Handling post-processing for succeeded step #{step_id}"
        step = load_step_for_post_processing!(step_id)
        process_step_dependents(step)
        finalize_step_succeeded(step.id)
        check_workflow_completion(step.workflow_id)
      rescue Yantra::Errors::EnqueueFailed => e
        log_warn "Dependent processing for step #{step_id} failed with enqueue error: #{e.message}. Step remains POST_PROCESSING. Will retry."
        raise e
      rescue StandardError => e
        log_error("Unexpected error during post-processing for step #{step_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
        handle_post_processing_failure(step_id, e)
      end

      def step_failed(step_id, error_info, expected_old_state: StateMachine::RUNNING)
        log_error "Step #{step_id} failed permanently: #{error_info[:class]} - #{error_info[:message]}"
        finished_at = Time.current

        transition_step_to_failed(step_id, error_info, expected_old_state, finished_at)
        mark_workflow_as_failed(step_id)
        publish_step_failed_event(step_id, error_info, finished_at)
        process_failure_dependents_and_check_completion(step_id)
      rescue StandardError => e
        log_error("Error during step_failed processing for step #{step_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      end

      private

      def transition_step_to_running(step_id, step)
        allowed_start_states = [StateMachine::SCHEDULING, StateMachine::ENQUEUED]
        transition_service.transition_step(
          step_id,
          StateMachine::RUNNING,
          expected_old_state: allowed_start_states, # Pass the array
          extra_attrs: { started_at: Time.current }
        ).tap { |success| publish_step_started_event(step_id) if success }
      end

      def transition_step_to_failed(step_id, error_info, expected_old_state, finished_at)
        transition_service.transition_step(
          step_id,
          StateMachine::FAILED,
          expected_old_state: expected_old_state,
          extra_attrs: { error: error_info, finished_at: finished_at }
        )
      end

      def finalize_step_succeeded(step_id)
        log_info "Finalizing step #{step_id} as SUCCEEDED."
        success = transition_service.transition_step(
          step_id,
          StateMachine::SUCCEEDED,
          expected_old_state: StateMachine::POST_PROCESSING,
          extra_attrs: { finished_at: Time.current }
        )

        success ? publish_step_succeeded_event(step_id) : log_error("Failed to finalize step #{step_id} to SUCCEEDED.")
      end

      def handle_post_processing_failure(step_id, error)
        log_error "Handling failure during post-processing for step #{step_id}."
        error_info = format_error(error)
        finished_at = Time.current

        transition_service.transition_step(
          step_id,
          StateMachine::FAILED,
          expected_old_state: StateMachine::POST_PROCESSING,
          extra_attrs: { error: error_info, finished_at: finished_at }
        )

        mark_workflow_as_failed(step_id)
        publish_step_failed_event(step_id, error_info, finished_at)
        process_failure_dependents_and_check_completion(step_id)
      rescue StandardError => e
        log_error("Error during handle_post_processing_failure for step #{step_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      end

      def process_failure_dependents_and_check_completion(step_id)
        step = repository.find_step(step_id)
        return log_error "Step #{step_id} not found after failure, cannot process dependents or check completion." unless step

        log_debug "Processing dependent cancellations for step #{step.id}"

        cancelled_ids = dependent_processor.process_failure_cascade(
          finished_step_id: step.id,
          workflow_id: step.workflow_id
        )

        cancelled_ids&.each { |id| publish_step_cancelled_event(id) }
        check_workflow_completion(step.workflow_id)
      rescue => e
        log_error("Error during failure-dependent handling for step #{step_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      end

      def format_error(error)
        { class: error.class.name, message: error.message, backtrace: error.backtrace&.first(10) }
      end

      def validate_dependencies
        raise Yantra::Errors::ConfigurationError, "Orchestrator requires a valid persistence adapter." unless @repository
        raise Yantra::Errors::ConfigurationError, "Orchestrator requires a valid worker adapter." unless @worker_adapter
        log_warn "Notifier is missing or invalid. Events may not be published." unless @notifier&.respond_to?(:publish)
      end

      def mark_workflow_as_failed(step_id)
        workflow_id = repository.find_step(step_id)&.workflow_id
        return unless workflow_id

        success = repository.update_workflow_attributes(workflow_id, { has_failures: true })
        log_warn "Failed to set has_failures for workflow #{workflow_id}." unless success
      rescue => e
        log_error "Error setting failure flag for workflow containing step #{step_id}: #{e.message}"
      end

      def enqueue_initial_steps(workflow_id)
        pending_steps = repository.list_steps(workflow_id: workflow_id, status: :pending)
        return if pending_steps.empty?

        pending_step_ids = pending_steps.map(&:id)
        dependencies_map = repository.get_dependency_ids_bulk(pending_step_ids) || {}
        pending_step_ids.each { |id| dependencies_map[id] ||= [] }

        initial_step_ids = pending_step_ids.select { |id| dependencies_map[id].empty? }
        log_info "Initially enqueueable steps for workflow #{workflow_id}: #{initial_step_ids.inspect}"
        return if initial_step_ids.empty?

        step_enqueuer.call(workflow_id: workflow_id, step_ids_to_attempt: initial_step_ids)
      rescue Yantra::Errors::EnqueueFailed => e
        raise e
      rescue StandardError => e
        log_error "Error enqueuing initial steps for #{workflow_id}: #{e.class} - #{e.message}"
      end

      def check_workflow_completion(workflow_id)
        return unless workflow_id
        return log_debug "Workflow #{workflow_id} not complete yet (steps still in progress)." if steps_in_progress?(workflow_id)

        wf = repository.find_workflow(workflow_id)
        return if workflow_completed?(wf)

        final_state = determine_final_workflow_state(workflow_id)
        update_and_finalize_workflow(workflow_id, final_state)
      rescue => e
        log_error "Error during check_workflow_completion for #{workflow_id}: #{e.class} - #{e.message}"
      end

      def load_step_for_post_processing!(step_id)
        step = repository.find_step(step_id)
        raise Yantra::Errors::StepNotFound, "Step #{step_id} not found for post-processing" unless step

        unless step.state.to_sym == StateMachine::POST_PROCESSING
          raise Yantra::Errors::InvalidStepState, "Cannot post-process step in state #{step.state}"
        end

        step
      end

      def process_step_dependents(step)
        log_debug "Processing dependents for step #{step.id}"
        dependent_processor.process_successors(
          finished_step_id: step.id,
          workflow_id: step.workflow_id
        )
      end

      def steps_in_progress?(workflow_id)
        wip_states = StateMachine::WORK_IN_PROGRESS_STATES
        repository.has_steps_in_states?(workflow_id: workflow_id, states: wip_states)
      end

      def workflow_completed?(workflow)
        return true if workflow.nil?
        terminal_states = [StateMachine::SUCCEEDED, StateMachine::FAILED, StateMachine::CANCELLED]
        terminal_states.include?(workflow.state.to_sym)
      end

      def determine_final_workflow_state(workflow_id)
        repository.workflow_has_failures?(workflow_id) ? StateMachine::FAILED : StateMachine::SUCCEEDED
      end

      def update_and_finalize_workflow(workflow_id, final_state)
        updated = repository.update_workflow_attributes(
          workflow_id,
          { state: final_state.to_s, finished_at: Time.current },
          expected_old_state: StateMachine::RUNNING
        )

        if updated
          log_info "Workflow #{workflow_id} marked as #{final_state}."
          publish_workflow_finished_event(workflow_id, final_state)
        else
          log_warn "Failed to mark workflow #{workflow_id} as #{final_state}."
        end
      end

      def find_step_for_start!(step_id)
        step = repository.find_step(step_id)
        log_warn "Step #{step_id} not found when attempting to start." unless step
        step
      end

      def startable_state?(step)
        state_sym = step.state.to_sym
        raise Yantra::Errors::OrchestrationError, "Step #{step.id} in invalid state for execution: #{step.state}." unless StateMachine.eligible_for_perform?(state_sym)
        true
      end

      def already_running?(step)
        step.state.to_sym == StateMachine::RUNNING
      end

      def running_after_retry?(step_id)
        repository.find_step(step_id)&.state.to_s == StateMachine::RUNNING.to_s
      end

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

      def log_info(msg);  @logger&.info  { "[Orchestrator] #{msg}" } end
      def log_warn(msg);  @logger&.warn  { "[Orchestrator] #{msg}" } end
      def log_error(msg); @logger&.error { "[Orchestrator] #{msg}" } end
      def log_debug(msg); @logger&.debug { "[Orchestrator] #{msg}" } end
    end
  end
end

