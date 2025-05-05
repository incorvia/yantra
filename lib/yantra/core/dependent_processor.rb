# lib/yantra/core/dependent_processor.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require 'set'

module Yantra
  module Core
    class DependentProcessor
      attr_reader :repository, :step_enqueuer, :logger

      def initialize(repository:, step_enqueuer:, logger: Yantra.logger)
        @repository = repository or raise ArgumentError, "DependentProcessor requires a repository"
        @step_enqueuer = step_enqueuer or raise ArgumentError, "DependentProcessor requires a step_enqueuer"
        @logger = logger || Logger.new(IO::NULL)
      end

      def process_successors(finished_step_id:, workflow_id:)
        dependents = fetch_direct_dependents(finished_step_id)
        return if dependents.empty?

        log_debug "Processing successors for step #{finished_step_id}..."
        parent_map, step_records_map = fetch_dependency_and_step_data(dependents)
        enqueueable = select_ready_dependents(dependents, parent_map, step_records_map)

        if enqueueable.any?
          log_info "Steps ready to enqueue after #{finished_step_id}: #{enqueueable.inspect}"
          step_enqueuer.call(workflow_id: workflow_id, step_ids_to_attempt: enqueueable)
        else
          log_debug "No direct dependents became ready after #{finished_step_id}."
        end
      rescue Yantra::Errors::EnqueueFailed => e
        log_warn "StepEnqueuer failed for successors of #{finished_step_id}: #{e.message}"
        raise
      rescue => e
        log_error format_error("Error during successor processing", finished_step_id, e)
        raise
      end

      def process_failure_cascade(finished_step_id:, workflow_id:)
        initial = fetch_direct_dependents(finished_step_id)
        return [] if initial.empty?

        log_warn "Initiating cancellation cascade from failed/cancelled step #{finished_step_id}"
        cancellable_ids = find_cancellable_descendants(initial)
        return [] if cancellable_ids.empty?

        log_info "Cancelling #{cancellable_ids.size} steps: #{cancellable_ids.inspect}"
        now = Time.current
        cancel_attrs = {
          state: StateMachine::CANCELLED.to_s,
          finished_at: now,
          updated_at: now
        }

        count = repository.bulk_update_steps(cancellable_ids, cancel_attrs)
        log_info "Repository reported #{count} steps cancelled."
        cancellable_ids
      rescue => e
        log_error format_error("Error during failure cascade", finished_step_id, e)
        raise
      end

      private

      def fetch_direct_dependents(step_id)
        repository.get_dependent_ids(step_id) || []
      end

      def fetch_dependency_and_step_data(dependent_ids)
        parent_map = repository.get_dependency_ids_bulk(dependent_ids) || {}
        dependent_ids.each { |id| parent_map[id] ||= [] }

        step_ids = (dependent_ids + parent_map.values.flatten).uniq
        records = repository.find_steps(step_ids) || []
        step_map = records.index_by { |s| s.id.to_s }

        [parent_map, step_map]
      rescue Yantra::Errors::PersistenceError => e
        log_error "Failed to fetch dependency/step data: #{e.message}"
        [{}, {}]
      end

      def select_ready_dependents(dependents, parent_map, step_map)
        dependents.select do |id|
          step = step_map[id.to_s]
          next false unless step

          StateMachine.is_enqueue_candidate_state?(step.state.to_sym) &&
            prerequisites_met?(parent_map[id.to_s], step_map)
        end
      end

      def prerequisites_met?(prerequisite_ids, step_map)
        return true if prerequisite_ids.nil? || prerequisite_ids.empty?

        prerequisite_ids.all? do |id|
          step = step_map[id.to_s]
          step && StateMachine.prerequisite_met?(step.state.to_sym)
        end
      end

      def find_cancellable_descendants(initial_ids)
        visited = Set.new(initial_ids)
        queue = initial_ids.dup
        cancellable = Set.new
        max_iterations = 10_000
        iterations = 0

        while queue.any? && iterations < max_iterations
          batch_ids = queue.shift(100)
          iterations += 1

          step_map = (repository.find_steps(batch_ids) || []).index_by(&:id)
          dependent_map = repository.get_dependent_ids_bulk(batch_ids.map(&:to_s)) || {}
          batch_ids.each { |id| dependent_map[id.to_s] ||= [] }

          batch_ids.each do |id|
            step = step_map[id]
            next unless step

            if StateMachine.is_cancellable_state?(step.state.to_sym)
              cancellable << id
              dependent_map[id.to_s].each do |child_id|
                queue << child_id if child_id.present? && visited.add?(child_id)
              end
            end
          end
        end

        log_error "Exceeded max iteration limit (#{max_iterations}) in find_cancellable_descendants." if iterations >= max_iterations
        cancellable.to_a
      end

      def format_error(prefix, step_id, exception)
        "#{prefix} for #{step_id}: #{exception.class} - #{exception.message}\n" \
        "#{exception.backtrace&.first(10)&.join("\n")}"
      end

      def log_debug(msg); logger.debug { "[DependentProcessor] #{msg}" }; end
      def log_info(msg);  logger.info  { "[DependentProcessor] #{msg}" }; end
      def log_warn(msg);  logger.warn  { "[DependentProcessor] #{msg}" }; end
      def log_error(msg); logger.error { "[DependentProcessor] #{msg}" }; end
    end
  end
end

