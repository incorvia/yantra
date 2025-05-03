# lib/yantra/core/dependent_processor.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require 'set'

module Yantra
  module Core
    # Processes successors or descendants of a finished step.
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
        enqueueable = filter_enqueueable_dependents(dependents, parent_map, step_records_map)

        if enqueueable.any?
          log_info "Steps ready to enqueue after #{finished_step_id}: #{enqueueable.inspect}"
          step_enqueuer.call(workflow_id: workflow_id, step_ids_to_attempt: enqueueable)
        else
          log_debug "No direct dependents became ready after #{finished_step_id}."
        end
      rescue => e
        log_error "Error during successor processing for #{finished_step_id}: #{e.class} - #{e.message}\n#{e.backtrace&.first(10)&.join("\n")}"
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
        cancel_attrs = { state: StateMachine::CANCELLED.to_s, finished_at: now, updated_at: now }
        count = repository.bulk_update_steps(cancellable_ids, cancel_attrs)
        log_info "Repository reported #{count} steps cancelled."

        cancellable_ids
      rescue => e
        log_error "Error during failure cascade from #{finished_step_id}: #{e.class} - #{e.message}\n#{e.backtrace&.first(10)&.join("\n")}"
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

      def filter_enqueueable_dependents(dependents, parent_map, step_map)
        dependents.select do |id|
          step = step_map[id.to_s]
          next false unless step
          StateMachine.is_enqueue_candidate_state?(step.state.to_sym, step.enqueued_at) &&
            prerequisites_met?(parent_map[id], step_map)
        end
      end

      def prerequisites_met?(ids, step_map)
        return true if ids.nil? || ids.empty?
        ids.all? do |id|
          step = step_map[id.to_s]
          step && StateMachine.prerequisite_met?(step.state.to_sym)
        end
      end

      def find_cancellable_descendants(initial_ids)
        visited = Set.new(initial_ids)
        queue = initial_ids.dup
        cancellable = Set.new
        max = 10_000
        i = 0

        while queue.any? && i < max
          batch = queue.shift(100)
          i += 1

          step_map = (repository.find_steps(batch) || []).index_by(&:id)
          dep_map = repository.get_dependent_ids_bulk(batch) || {}
          batch.each { |id| dep_map[id] ||= [] }

          batch.each do |id|
            step = step_map[id] || repository.find_step(id)
            next unless step

            if StateMachine.is_cancellable_state?(step.state.to_sym, step.enqueued_at)
              cancellable << id
              dep_map[id].each { |child_id| queue << child_id if visited.add?(child_id) }
            end
          end
        end

        log_error "Exceeded max iteration limit in find_cancellable_descendants." if i >= max
        cancellable.to_a
      end

      def log_debug(msg); logger.debug { "[DependentProcessor] #{msg}" }; end
      def log_info(msg);  logger.info  { "[DependentProcessor] #{msg}" }; end
      def log_warn(msg);  logger.warn  { "[DependentProcessor] #{msg}" }; end
      def log_error(msg); logger.error { "[DependentProcessor] #{msg}" }; end
    end
  end
end

