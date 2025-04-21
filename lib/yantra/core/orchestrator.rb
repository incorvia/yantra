require_relative 'state_machine'
require_relative '../errors'
require_relative '../persistence/repository_interface'
require_relative '../worker/enqueuing_interface'
require_relative '../events/notifier_interface'
require_relative 'step_enqueuer'

module Yantra
  module Core
    class Orchestrator
      attr_reader :repository, :worker_adapter, :notifier, :step_enqueuer

      def initialize(repository: nil, worker_adapter: nil, notifier: nil)
        @repository     = repository     || Yantra.repository
        @worker_adapter = worker_adapter || Yantra.worker_adapter
        @notifier       = notifier       || Yantra.notifier

        @step_enqueuer = StepEnqueuer.new(
          repository: @repository,
          worker_adapter: @worker_adapter,
          notifier: @notifier
        )

        validate_dependencies
      end

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
        enqueue_initial_steps(workflow_id)

        true
      end

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
            log_debug "Step #{step_id} successfully updated to RUNNING."
            publish_step_started_event(step_id)
          else
            log_warn "Could not transition step #{step_id} to RUNNING."
          end
        end

        true
      end

      def step_succeeded(step_id, output)
        log_info "Step #{step_id} succeeded"

        updated = repository.update_step_attributes(
          step_id,
          { state: StateMachine::SUCCEEDED.to_s, finished_at: Time.current },
          expected_old_state: StateMachine::RUNNING
        )

        repository.record_step_output(step_id, output)
        publish_step_succeeded_event(step_id) if updated

        step_finished(step_id)
      end

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

        log_failure_state_update(step_id) unless updated
        set_workflow_failure_flag(step_id)
        publish_step_failed_event(step_id, error_info, finished_at)
        step_finished(step_id)
      end

      def format_benchmark(label, measurement)
        "#{label}: #{measurement.real.round(4)}s real, #{measurement.total.round(4)}s cpu"
      end

      def step_finished(step_id)
        log_info "Step #{step_id} finished processing by worker"

        step = repository.find_step(step_id)
        return log_warn("Step #{step_id} not found when processing finish.") unless step

        case step.state.to_sym
        when StateMachine::SUCCEEDED, StateMachine::FAILED, StateMachine::CANCELLED
            process_dependents(step.id, step.state.to_sym, step.workflow_id)
            check_workflow_completion(step.workflow_id)
        else
          log_warn "Step #{step_id} reported finished but state is '#{step.state}', skipping."
        end
      rescue StandardError => e
        log_error("Error during step_finished for step #{step_id}: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
        raise e
      end

      private

      def validate_dependencies
        unless @repository.is_a?(Persistence::RepositoryInterface)
          raise Yantra::Errors::ConfigurationError, "Orchestrator requires a valid persistence adapter."
        end

        unless @worker_adapter.is_a?(Worker::EnqueuingInterface)
          raise Yantra::Errors::ConfigurationError, "Orchestrator requires a valid worker adapter."
        end

        unless @notifier.respond_to?(:publish)
          log_warn "Notifier is missing or invalid. Events may not be published."
        end
      end

      def log_failure_state_update(step_id)
        current = repository.find_step(step_id)&.state || 'unknown'
        log_warn "Step #{step_id} could not be marked FAILED (found '#{current}')"
      end

      def set_workflow_failure_flag(step_id)
        workflow_id = repository.find_step(step_id)&.workflow_id
        return unless workflow_id

        success = repository.update_workflow_attributes(
          workflow_id,
          { has_failures: true }
        )

        log_warn "Failed to set has_failures for workflow #{workflow_id}." unless success
      end

      def enqueue_initial_steps(workflow_id)
        step_ids = repository.find_ready_steps(workflow_id)
        log_info "Initially ready steps for workflow #{workflow_id}: #{step_ids.inspect}"
        return if step_ids.empty?

        step_enqueuer.call(workflow_id: workflow_id, step_ids_to_attempt: step_ids)
      rescue StandardError => e
        log_error "Error enqueuing initial steps for #{workflow_id}: #{e.class} - #{e.message}"
      end

      def process_dependents(finished_step_id, finished_state, workflow_id)
        dependents_ids = []
        dependents_ids += repository.get_dependent_ids(finished_step_id)
        return if dependents_ids.empty?

        log_debug "Processing dependents for #{finished_step_id} in workflow #{workflow_id}: #{dependents_ids.inspect}"

        if finished_state == StateMachine::SUCCEEDED
          parent_map, all_parents = fetch_dependencies_for_steps(dependents_ids)
          states = fetch_states_for_steps(dependents_ids + all_parents)

          ready_step_ids = []
          ready_step_ids += dependents_ids.select do |step_id|
            states[step_id.to_s] == StateMachine::PENDING.to_s &&
              is_ready_to_start?(step_id, parent_map[step_id] || [], states)
          end

          if ready_step_ids.any?
            log_info "Steps ready to enqueue after #{finished_step_id} finished: #{ready_step_ids.inspect}"
            step_enqueuer.call(workflow_id: workflow_id, step_ids_to_attempt: ready_step_ids)
            puts format_benchmark("Fan-Out (step_enqueuer.call -> enqueue N)", measurement)
          else
            log_debug "No dependent steps became ready after #{finished_step_id} finished."
          end
        else
          cancel_downstream_dependents(workflow_id, dependents_ids, finished_step_id, finished_state)
        end
      rescue StandardError => e
        log_error "Error during dependent step enqueue: #{e.class} - #{e.message}"
      end

      def cancel_downstream_dependents(workflow_id, initial_step_ids, failed_step_id, state)
        log_warn "Cancelling downstream steps of #{failed_step_id} (state: #{state})"

        descendants_to_cancel_ids = find_all_pending_descendants(initial_step_ids)
        return if descendants_to_cancel_ids.empty?

        log_info "Bulk cancelling #{descendants_to_cancel_ids.size} steps: #{descendants_to_cancel_ids.inspect}"

        cancelled_count = repository.cancel_steps_bulk(descendants_to_cancel_ids)
        log_info "Repository reported #{cancelled_count} steps cancelled."

        descendants_to_cancel_ids.each { |id| publish_step_cancelled_event(id) }
      rescue => e
        log_error "Unexpected error cancelling steps: #{e.class} - #{e.message}"
      end

      def find_all_pending_descendants(initial_step_ids)
        pending_descendants = Set.new
        queue = initial_step_ids.dup
        visited = Set.new(initial_step_ids)

        max_iterations = 10_000
        iterations = 0

        while !queue.empty? && iterations < max_iterations
          iterations += 1
          current_batch_ids = queue.shift(100)
          batch_states = fetch_states_for_steps(current_batch_ids)
          batch_dependents_map = repository.get_dependent_ids_bulk(current_batch_ids)
          current_batch_ids.each { |id| batch_dependents_map[id] ||= [] }

          current_batch_ids.each do |step_id|
            if batch_states[step_id.to_s] == StateMachine::PENDING.to_s
              pending_descendants << step_id
              batch_dependents_map[step_id].each do |dependent_id|
                queue << dependent_id if visited.add?(dependent_id)
              end
            end
          end
        end

        log_error "Exceeded max iterations in find_all_pending_descendants" if iterations >= max_iterations
        pending_descendants.to_a
      end

      def fetch_dependencies_for_steps(step_ids)
        return [{}, []] if step_ids.nil? || step_ids.empty?

        unique_ids = step_ids.uniq
        parent_map = {}
        all_parents = []

        if repository.respond_to?(:get_dependencies_ids_bulk)
          parent_map = repository.get_dependencies_ids_bulk(unique_ids)
          unique_ids.each { |id| parent_map[id] ||= [] }
          all_parents = parent_map.values.flatten.uniq
        else
          log_warn "Repository does not implement get_dependencies_ids_bulk"
          unique_ids.each do |id|
            parents = repository.get_dependencies_ids(id)
            parent_map[id] = parents
            all_parents.concat(parents)
          end
          all_parents.uniq!
        end

        [parent_map, all_parents]
      end

      def fetch_states_for_steps(step_ids)
        return {} if step_ids.nil? || step_ids.empty?

        unique_ids = step_ids.uniq
        return repository.fetch_step_states(unique_ids) if repository.respond_to?(:fetch_step_states)

        log_warn "Repository does not implement fetch_step_states"
        states = {}
        unique_ids.each do |id|
          step = repository.find_step(id)
          states[id] = step&.state.to_s if step
        end
        states
      rescue => e
        log_error "Failed to fetch step states: #{e.message}"
        {}
      end

      def is_ready_to_start?(step_id, parent_ids, states_map)
        parent_ids.all? { |pid| states_map[pid.to_s] == StateMachine::SUCCEEDED.to_s }
      end

      def check_workflow_completion(workflow_id)
        return unless workflow_id

        return unless repository.running_step_count(workflow_id).zero? &&
                      repository.enqueued_step_count(workflow_id).zero?

        wf = repository.find_workflow(workflow_id)
        return if wf.nil? || StateMachine.terminal?(wf.state.to_sym)

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
          log_warn "Failed to mark workflow #{workflow_id} as #{final_state}. Expected RUNNING state."
        end
      rescue => e
        log_error "Error during check_workflow_completion for #{workflow_id}: #{e.class} - #{e.message}"
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
          workflow_id: workflow_id,
          klass: wf.klass,
          started_at: wf.started_at
        })
      end

      def publish_workflow_finished_event(workflow_id, state)
        wf = repository.find_workflow(workflow_id)
        return unless wf

        event = state == StateMachine::FAILED ? 'yantra.workflow.failed' : 'yantra.workflow.succeeded'

        publish_event(event, {
          workflow_id: workflow_id,
          klass: wf.klass,
          state: state.to_s,
          finished_at: wf.finished_at
        })
      end

      def publish_step_started_event(step_id)
        step = repository.find_step(step_id)
        return unless step

        publish_event('yantra.step.started', {
          step_id: step_id,
          workflow_id: step.workflow_id,
          klass: step.klass,
          started_at: step.started_at
        })
      end

      def publish_step_succeeded_event(step_id)
        step = repository.find_step(step_id)
        return unless step

        publish_event('yantra.step.succeeded', {
          step_id: step_id,
          workflow_id: step.workflow_id,
          klass: step.klass,
          finished_at: step.finished_at,
          output: step.output
        })
      end

      def publish_step_failed_event(step_id, error_info, finished_at)
        step = repository.find_step(step_id)
        return unless step

        publish_event('yantra.step.failed', {
          step_id: step_id,
          workflow_id: step.workflow_id,
          klass: step.klass,
          error: error_info,
          finished_at: finished_at,
          state: StateMachine::FAILED.to_s,
          retries: step.respond_to?(:retries) ? step.retries : 0
        })
      end

      def publish_step_cancelled_event(step_id)
        step = repository.find_step(step_id)
        return unless step

        publish_event('yantra.step.cancelled', {
          step_id: step_id,
          workflow_id: step.workflow_id,
          klass: step.klass
        })
      end

      def log_info(msg);  Yantra.logger&.info  { "[Orchestrator] #{msg}" } end
      def log_warn(msg);  Yantra.logger&.warn  { "[Orchestrator] #{msg}" } end
      def log_error(msg); Yantra.logger&.error { "[Orchestrator] #{msg}" } end
      def log_debug(msg); Yantra.logger&.debug { "[Orchestrator] #{msg}" } end
    end
  end
end

