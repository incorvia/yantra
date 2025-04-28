# lib/yantra/core/dependent_processor.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require 'set'

module Yantra
  module Core
    # Processes dependent steps after a step finishes.
    class DependentProcessor
      attr_reader :repository, :step_enqueuer, :logger

      def initialize(repository:, step_enqueuer:, logger: Yantra.logger)
        @repository = repository
        @step_enqueuer = step_enqueuer
        @logger = logger || Logger.new(IO::NULL)

        # Basic validation of collaborators
        unless @repository
          raise ArgumentError, "DependentProcessor requires a repository"
        end
        unless @step_enqueuer&.respond_to?(:call)
          raise ArgumentError, "DependentProcessor requires a step_enqueuer"
        end
      end

      # Main entry point to process dependents.
      # Returns array of cancelled step IDs if cancellation occurred, nil otherwise.
      # Re-raises errors encountered during processing.
      def call(finished_step_id:, finished_state:, workflow_id:)
        @finished_step_id = finished_step_id
        @finished_state = finished_state.to_sym
        @workflow_id = workflow_id
        @dependents_ids = nil
        @parent_map = nil
        @states = nil

        @dependents_ids = fetch_direct_dependents
        return nil if @dependents_ids.empty?

        # log_debug "Processing dependents for #{@finished_step_id} (State: #{@finished_state})..." # Removed debug

        if @finished_state == StateMachine::SUCCEEDED
          process_dependents_on_success
          return nil # No cancellations
        else
          return cancel_downstream_dependents(@dependents_ids, @finished_step_id, @finished_state)
        end

      rescue StandardError => e
        log_error "Error during dependent processing for step #{@finished_step_id}: #{e.class} - #{e.message}\n#{e.backtrace&.first(10)&.join("\n")}"
        raise e # Re-raise errors
      end

      private

      # --- Success Path Helpers ---

      def process_dependents_on_success
        fetch_dependency_data_for_dependents
        ready_step_ids = find_ready_dependents

        if ready_step_ids.any?
          # log_info "Steps ready to enqueue after #{@finished_step_id} finished: #{ready_step_ids.inspect}"
          @step_enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: ready_step_ids)
        # else # Removed debug log for no ready dependents
        #   log_debug "No direct dependents became ready after #{@finished_step_id} finished."
        end
      end

      def fetch_direct_dependents
        repository.get_dependent_ids(@finished_step_id)
      end

      def fetch_dependency_data_for_dependents
        @parent_map = repository.get_dependency_ids_bulk(@dependents_ids) || {}
        @dependents_ids.each { |id| @parent_map[id] ||= [] }
        all_involved_step_ids = (@dependents_ids + @parent_map.values.flatten).uniq
        @states = fetch_states_for_steps(all_involved_step_ids)
      end

      def find_ready_dependents
        @dependents_ids.select do |step_id|
          # Use StateMachine method to check if the step itself is in a valid state to start
          can_be_enqueued = StateMachine.can_enqueue?(@states[step_id.to_s]&.to_sym) # Pass symbol

          # Use StateMachine method to check if all prerequisites are met
          prereqs = @parent_map[step_id] || []
          all_prereqs_met = are_all_prerequisites_met?(prereqs) # Renamed helper

          can_be_enqueued && all_prereqs_met
        end
      end

      def are_all_prerequisites_met?(prerequisite_ids) # Renamed helper
        return true if prerequisite_ids.empty?
        prerequisite_ids.all? do |prereq_id|
          # Use StateMachine method to check prerequisite state
          StateMachine.prerequisite_met?(@states[prereq_id.to_s]&.to_sym) # Pass symbol
        end
      end

      def fetch_states_for_steps(step_ids)
        return {} if step_ids.nil? || step_ids.empty?
        unique_ids = step_ids.uniq

        if repository.respond_to?(:get_step_states)
          result = repository.get_step_states(unique_ids) || {}
          return result.transform_keys(&:to_s)
        end

        log_warn "Repository does not implement bulk get_step_states, falling back."
        states = {}
        unique_ids.each do |id|
          step = repository.find_step(id) # N+1 potential
          states[id.to_s] = step&.state.to_s if step
        end
        states
      rescue => e
        log_error "Failed to fetch step states: #{e.message}"
        {}
      end

      # --- Failure/Cancellation Path Helpers ---

      def cancel_downstream_dependents(initial_step_ids, failed_step_id, state)
        log_warn "Cancelling downstream steps of #{failed_step_id} (state: #{state})"
        descendants_to_cancel_ids = find_all_pending_descendants(initial_step_ids)
        return [] if descendants_to_cancel_ids.empty?

        # log_info "Bulk cancelling #{descendants_to_cancel_ids.size} pending descendant steps: #{descendants_to_cancel_ids.inspect}"
        cancelled_count = repository.bulk_cancel_steps(descendants_to_cancel_ids)
        log_info "Repository reported #{cancelled_count} steps cancelled."
        return descendants_to_cancel_ids
      rescue => e
        log_error "Unexpected error cancelling steps: #{e.class} - #{e.message}\n#{e.backtrace&.first(5)&.join("\n")}"
        raise e
      end

      def find_all_pending_descendants(initial_step_ids)
        pending_descendants = Set.new
        queue = initial_step_ids.dup
        visited = Set.new(initial_step_ids)
        max_iterations = 10_000
        iterations = 0
        fetch_states_in_bulk = repository.respond_to?(:get_step_states)
        fetch_dependents_in_bulk = repository.respond_to?(:get_dependent_ids_bulk)

        while !queue.empty? && iterations < max_iterations
          iterations += 1
          current_batch_ids = queue.shift(100)
          batch_states = fetch_states_in_bulk ? fetch_states_for_steps(current_batch_ids) : {}
          batch_dependents_map = fetch_dependents_in_bulk ? (repository.get_dependent_ids_bulk(current_batch_ids) || {}) : {}
          current_batch_ids.each { |id| batch_dependents_map[id] ||= [] unless fetch_dependents_in_bulk }

          current_batch_ids.each do |step_id|
            current_state = fetch_states_in_bulk ? batch_states[step_id.to_s] : (repository.find_step(step_id)&.state.to_s)
            if current_state == StateMachine::PENDING.to_s
              pending_descendants << step_id
              dependents = fetch_dependents_in_bulk ? batch_dependents_map[step_id] : repository.get_dependent_ids(step_id)
              (dependents || []).each { |dep_id| queue << dep_id if visited.add?(dep_id) }
            end
          end
        end

        log_error "Exceeded max iterations (#{max_iterations}) in find_all_pending_descendants." if iterations >= max_iterations
        pending_descendants.to_a
      end

      # --- Logging Helpers ---
      def log_debug(message)
        logger.debug { "[DependentProcessor] #{message}" }
      end
      def log_info(message)
        logger.info { "[DependentProcessor] #{message}" }
      end
      def log_warn(message)
        logger.warn { "[DependentProcessor] #{message}" }
      end
      def log_error(message)
        logger.error { "[DependentProcessor] #{message}" }
      end

    end # class DependentProcessor
  end # module Core
end # module Yantra

