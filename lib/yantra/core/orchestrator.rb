# lib/yantra/core/orchestrator.rb

require_relative 'state_machine'
require_relative '../errors'
require_relative '../persistence/repository_interface'
require_relative '../worker/enqueuing_interface'
require_relative '../events/notifier_interface'

module Yantra
  module Core
    class Orchestrator
      attr_reader :repository, :worker_adapter, :notifier

      def initialize(repository: nil, worker_adapter: nil, notifier: nil)
        @repository     = repository || Yantra.repository
        @worker_adapter = worker_adapter || Yantra.worker_adapter
        @notifier       = notifier || Yantra.notifier

        unless @repository.is_a?(Persistence::RepositoryInterface)
          raise Yantra::Errors::ConfigurationError, "Orchestrator requires a valid persistence adapter."
        end

        unless @worker_adapter.is_a?(Worker::EnqueuingInterface)
          raise Yantra::Errors::ConfigurationError, "Orchestrator requires a valid worker adapter."
        end

        unless @notifier.respond_to?(:publish)
          raise Yantra::Errors::ConfigurationError, "Orchestrator requires a notifier responding to #publish."
        end
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
        find_and_enqueue_ready_steps(workflow_id)
        true
      end

      def step_starting(step_id)
        log_info "Starting step #{step_id}"
        step = repository.find_step(step_id)
        return false unless step

        allowed = [StateMachine::ENQUEUED.to_s, StateMachine::RUNNING.to_s]
        unless allowed.include?(step.state.to_s)
          log_warn "Step #{step_id} in invalid start state: #{step.state}"
          return false
        end

        if step.state.to_s == StateMachine::ENQUEUED.to_s
          updated = repository.update_step_attributes(
            step_id,
            { state: StateMachine::RUNNING.to_s, started_at: Time.current },
            expected_old_state: StateMachine::ENQUEUED
          )

          unless updated
            log_warn "Could not transition step #{step_id} to RUNNING"
            return false
          end

          publish_step_started_event(step_id)
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

        unless updated
          current = repository.find_step(step_id)&.state || 'unknown'
          log_warn "Step #{step_id} could not be marked FAILED (found '#{current}')"
        end

        workflow_id = repository.find_step(step_id)&.workflow_id
        repository.set_workflow_has_failures_flag(workflow_id) if workflow_id

        publish_step_failed_event(step_id, error_info, finished_at)
        step_finished(step_id)
      end

      def step_finished(step_id)
        log_info "Step #{step_id} finished"
        step = repository.find_step(step_id)
        return unless step

        workflow_id = step.workflow_id

        case step.state.to_sym
        when StateMachine::SUCCEEDED, StateMachine::FAILED, StateMachine::CANCELLED
          process_dependents(step_id, step.state.to_sym, workflow_id)
          check_workflow_completion(step.workflow_id)
        else
          log_warn "Step #{step_id} in state '#{step.state}', skipping downstream logic"
        end
      end

      private

      def find_and_enqueue_ready_steps(workflow_id)
        ready = repository.find_ready_steps(workflow_id)
        log_info "Ready steps for workflow #{workflow_id}: #{ready.inspect}"
        ready.each { |step_id| enqueue_step(step_id) }
      end

      def enqueue_step(step_id)
        step = repository.find_step(step_id)
        return unless step

        updated = repository.update_step_attributes(
          step_id,
          { state: StateMachine::ENQUEUED.to_s, enqueued_at: Time.current },
          expected_old_state: StateMachine::PENDING
        )

        return unless updated

        step = repository.find_step(step_id)
        publish_step_enqueued_event(step)
        enqueue_job_via_adapter(step)
      end

      def enqueue_job_via_adapter(step)
        worker_adapter.enqueue(step.id, step.workflow_id, step.klass, step.queue)
        log_debug "Step #{step.id} enqueued"
      rescue => e
        log_error "Failed to enqueue step #{step.id}: #{e.message}"
        error_info = {
          class: e.class.name,
          message: e.message,
          backtrace: e.backtrace&.first(10)
        }
        step_failed(step.id, error_info, expected_old_state: StateMachine::ENQUEUED)
      end

      def process_dependents(finished_step_id, finished_state, workflow_id)
        dependents_ids = repository.get_dependent_ids(finished_step_id) # Fetch IDs of children
        return if dependents_ids.empty?

        log_debug "Processing dependents for #{finished_step_id}: #{dependents_ids.inspect}"

        if finished_state == StateMachine::SUCCEEDED
          # --- Delegate to the optimized method ---
          process_successful_dependents(workflow_id, dependents_ids) # Pass finished_step_id for context
        else
          # Failure/Cancellation Cascade
          cancel_downstream_dependents(dependents_ids, finished_step_id, finished_state)
        end
      end

      def process_successful_dependents(workflow_id, potential_step_ids)
        # Fetch parent info and states map upfront
        parent_map, all_parents = fetch_dependencies_for_steps(potential_step_ids)
        states = fetch_states_for_steps(potential_step_ids + all_parents) # Needed for parent checks

        # 1. Identify ready steps using the states map
        ready_step_ids = []
        potential_step_ids.each do |step_id|
          next unless states[step_id.to_s] == StateMachine::PENDING.to_s
          parents = parent_map[step_id] || []
          if is_ready_to_start?(step_id, parents, states)
            ready_step_ids << step_id
          else
            log_debug "Step #{step_id} not ready yet."
          end
        end
        return if ready_step_ids.empty?
        log_info "Steps ready to enqueue: #{ready_step_ids.inspect}"

        # 2. Bulk fetch data for the identified ready steps
        begin
          ready_steps = repository.find_steps(ready_step_ids) # No state filter needed
          ready_steps_map = ready_steps.index_by(&:id)
        rescue Yantra::Errors::PersistenceError => e
          log_error "Failed to bulk fetch ready steps (#{ready_step_ids.inspect}): #{e.message}"
          return
        end

        # 3. Attempt to enqueue each ready step
        successfully_enqueued_ids = []

        ready_step_ids.each do |step_id|
          step = ready_steps_map[step_id]
          unless step
            log_warn "Could not find pre-fetched step data for #{step_id}, skipping enqueue."
            next
          end
          unless step.state.to_sym == StateMachine::PENDING
            log_warn "Step #{step_id} state was no longer PENDING (#{step.state}) just before enqueue, skipping."
            next
          end

          begin
            enqueue_job_via_adapter(step)
            successfully_enqueued_ids << step_id # Collect only IDs
          rescue StandardError => e
            log_error "Failed to enqueue step #{step_id} via worker adapter: #{e.message}"
          end
        end

        # 4. Bulk update state ONLY for successfully enqueued steps
        if successfully_enqueued_ids.any?
          log_info "Bulk updating state to ENQUEUED for steps: #{successfully_enqueued_ids.inspect}"
          update_attributes = { enqueued_at: Time.current }
          update_success = repository.bulk_update_steps(
            successfully_enqueued_ids,
            { state: StateMachine::ENQUEUED }.merge(update_attributes)
          )
          unless update_success
            log_warn "Bulk update to enqueued might have failed or reported issues for steps: #{successfully_enqueued_ids.inspect}"
          end

          # --- 5. Publish ONE bulk event ---
          # Fetch workflow_id efficiently (e.g., from one of the steps or the finished_step_id)
          if workflow_id # Check if workflow_id is present
            begin
              publish_bulk_enqueued_event(workflow_id, successfully_enqueued_ids)
            rescue StandardError => e
              log_error("Error occurred during publish_bulk_enqueued_event: #{e.message}")
            end
          else
            log_warn "Workflow ID was nil when trying to publish bulk enqueued event."
          end
          # --- END Bulk Event Publishing ---

        else
          log_info("No steps were successfully enqueued in this batch.") if ready_step_ids.any?
        end
      end

      def cancel_downstream_dependents(initial_step_ids, failed_step_id, state)
        log_warn "Cancelling downstream of #{failed_step_id} (state: #{state})"
        descendants = find_all_pending_descendants(initial_step_ids)
        return if descendants.empty?

        log_info "Bulk cancelling #{descendants.size} steps"

        begin
          count = repository.cancel_steps_bulk(descendants)
          log_info "Cancelled #{count} steps"
          descendants.each { |id| publish_step_cancelled_event(id) }
        rescue NotImplementedError
          log_error "Repository does not implement cancel_steps_bulk"
        rescue => e
          log_error "Error cancelling steps: #{e.message}"
        end
      end

      def find_all_pending_descendants(initial_step_ids)
        pending = Set.new
        visited = Set.new(initial_step_ids)
        queue = initial_step_ids.dup
        states = fetch_states_for_steps(initial_step_ids)

        until queue.empty?
          current = queue.shift(100)
          batch_states = states.slice(*current)
          missing = current - batch_states.keys

          unless missing.empty?
            missing_states = fetch_states_for_steps(missing)
            batch_states.merge!(missing_states)
          end

          next_batch = []

          current.each do |step_id|
            if batch_states[step_id] == StateMachine::PENDING.to_s
              pending << step_id
              deps = repository.get_dependencies_ids(step_id)
              deps.each do |dep|
                next if visited.include?(dep)

                visited << dep
                next_batch << dep
              end
            end
          end

          unless next_batch.empty?
            states.merge!(fetch_states_for_steps(next_batch))
            queue.concat(next_batch)
          end
        end

        pending.to_a
      end

      def fetch_dependencies_for_steps(step_ids)
        parent_map = {}
        all_parents = []

        if repository.respond_to?(:get_dependencies_ids_bulk)
          parent_map = repository.get_dependencies_ids_bulk(step_ids)
          step_ids.each { |id| parent_map[id] ||= [] }
          all_parents = parent_map.values.flatten.uniq
        else
          step_ids.each do |id|
            parents = repository.get_dependent_ids(id)
            parent_map[id] = parents
            all_parents.concat(parents)
          end
          all_parents.uniq!
        end

        [parent_map, all_parents]
      end

      def fetch_states_for_steps(step_ids)
        return {} if step_ids.empty?

        if repository.respond_to?(:fetch_step_states)
          repository.fetch_step_states(step_ids)
        else
          log_warn "fetch_step_states not supported"
          {}
        end
      end

      def is_ready_to_start?(step_id, parent_ids, states)
        return false unless states[step_id] == StateMachine::PENDING.to_s
        return true if parent_ids.empty?

        parent_ids.all? { |pid| states[pid] == StateMachine::SUCCEEDED.to_s }
      end

      def check_workflow_completion(workflow_id)
        return unless workflow_id

        running = repository.running_step_count(workflow_id)
        enqueued = repository.enqueued_step_count(workflow_id)

        return unless running.zero? && enqueued.zero?

        wf = repository.find_workflow(workflow_id)
        return if wf.nil? || StateMachine.terminal?(wf.state.to_sym)

        final = repository.workflow_has_failures?(workflow_id) ? StateMachine::FAILED : StateMachine::SUCCEEDED
        success = repository.update_workflow_attributes(
          workflow_id,
          { state: final.to_s, finished_at: Time.current },
          expected_old_state: StateMachine::RUNNING
        )

        if success
          publish_workflow_finished_event(workflow_id, final)
        end
      end

      def publish_event(name, payload)
        notifier&.publish(name, payload)
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
          workflow_id:  workflow_id,
          klass:        wf.klass,
          state:        state.to_s,
          finished_at:  wf.finished_at
        })
      end

      def publish_step_started_event(step_id)
        step = repository.find_step(step_id)
        return unless step

        publish_event('yantra.step.started', {
          step_id:     step_id,
          workflow_id: step.workflow_id,
          klass:       step.klass,
          started_at:  step.started_at
        })
      end

      def publish_step_succeeded_event(step_id)
        step = repository.find_step(step_id)
        return unless step

        publish_event('yantra.step.succeeded', {
          step_id:     step_id,
          workflow_id: step.workflow_id,
          klass:       step.klass,
          finished_at: step.finished_at,
          output:      step.output
        })
      end

      def publish_step_failed_event(step_id, error_info, finished_at)
        step = repository.find_step(step_id)
        return unless step

        publish_event('yantra.step.failed', {
          step_id:     step_id,
          workflow_id: step.workflow_id,
          klass:       step.klass,
          error:       error_info,
          finished_at: finished_at,
          state:       StateMachine::FAILED.to_s,
          retries:     step.retries
        })
      end

      def publish_step_enqueued_event(step)
        publish_event('yantra.step.enqueued', {
          step_id:     step.id,
          workflow_id: step.workflow_id,
          klass:       step.klass,
          queue:       step.queue,
          enqueued_at: step.enqueued_at
        })
      end

      def publish_bulk_enqueued_event(workflow_id, successfully_enqueued_ids)
        publish_event('yantra.step.bulk_enqueued', {
          workflow_id:               workflow_id,
          enqueued_ids: successfully_enqueued_ids
        }) 
      end

      def publish_step_cancelled_event(step_id)
        step = repository.find_step(step_id)
        return unless step

        publish_event('yantra.step.cancelled', {
          step_id:     step_id,
          workflow_id: step.workflow_id,
          klass:       step.klass
        })
      end

      def log_info(msg);  Yantra.logger&.info  { "[Orchestrator] #{msg}" } end
      def log_warn(msg);  Yantra.logger&.warn  { "[Orchestrator] #{msg}" } end
      def log_error(msg); Yantra.logger&.error { "[Orchestrator] #{msg}" } end
      def log_debug(msg); Yantra.logger&.debug { "[Orchestrator] #{msg}" } end
    end
  end
end

