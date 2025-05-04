# lib/yantra/core/dependent_processor.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require 'set'
require 'logger' # Ensure logger is available

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
        # Filter dependents based on state (PENDING or SCHEDULING) and met prerequisites
        enqueueable = filter_enqueueable_dependents(dependents, parent_map, step_records_map)

        if enqueueable.any?
          log_info "Steps ready to enqueue after #{finished_step_id}: #{enqueueable.inspect}"
          # Pass the identified steps to the StepEnqueuer
          step_enqueuer.call(workflow_id: workflow_id, step_ids_to_attempt: enqueueable)
        else
          log_debug "No direct dependents became ready after #{finished_step_id}."
        end
      rescue Yantra::Errors::EnqueueFailed => e
        # Log and re-raise EnqueueFailed so the calling job can retry
        log_warn "StepEnqueuer failed for successors of #{finished_step_id}: #{e.message}"
        raise e
      rescue => e
        log_error "Error during successor processing for #{finished_step_id}: #{e.class} - #{e.message}\n#{e.backtrace&.first(10)&.join("\n")}"
        raise # Re-raise other unexpected errors
      end

      def process_failure_cascade(finished_step_id:, workflow_id:)
        initial = fetch_direct_dependents(finished_step_id)
        return [] if initial.empty?

        log_warn "Initiating cancellation cascade from failed/cancelled step #{finished_step_id}"

        cancellable_ids = find_cancellable_descendants(initial)
        return [] if cancellable_ids.empty?

        log_info "Cancelling #{cancellable_ids.size} steps: #{cancellable_ids.inspect}"
        now = Time.current
        # Use CANCELLED state from StateMachine
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
        # Ensure all dependents have an entry in the map, even if empty
        dependent_ids.each { |id| parent_map[id] ||= [] }
        # Fetch records for dependents and all their parents
        step_ids = (dependent_ids + parent_map.values.flatten).uniq
        records = repository.find_steps(step_ids) || []
        # Create a hash for quick lookup by ID (ensure keys are strings if IDs are strings)
        step_map = records.index_by { |s| s.id.to_s }
        [parent_map, step_map]
      rescue Yantra::Errors::PersistenceError => e
        log_error "Failed to fetch dependency/step data: #{e.message}"
        [{}, {}] # Return empty maps on error
      end

      # Filters dependents to find those ready for an enqueue attempt.
      def filter_enqueueable_dependents(dependents, parent_map, step_map)
        dependents.select do |id|
          step = step_map[id.to_s]
          next false unless step # Skip if step record wasn't found

          # --- MODIFIED: Call is_enqueue_candidate_state? with only the state ---
          # This helper now checks if state is PENDING or SCHEDULING.
          is_candidate = StateMachine.is_enqueue_candidate_state?(step.state.to_sym)
          # --- END MODIFICATION ---

          # Check if prerequisites are met
          prereqs_ok = prerequisites_met?(parent_map[id.to_s], step_map) # Ensure key is string if needed

          # Log reasons for skipping if helpful for debugging
          # log_debug "Checking step #{id}: State=#{step.state}, Candidate=#{is_candidate}, Prereqs=#{prereqs_ok}"

          # Step is enqueueable if it's a candidate state AND prerequisites are met
          is_candidate && prereqs_ok
        end
      end

      # Checks if all prerequisite steps (by ID) are in a 'met' state.
      def prerequisites_met?(prerequisite_ids, step_map)
        return true if prerequisite_ids.nil? || prerequisite_ids.empty?

        prerequisite_ids.all? do |id|
          step = step_map[id.to_s] # Ensure key is string if needed
          # Prerequisite is met if the step record exists and its state is considered met
          step && StateMachine.prerequisite_met?(step.state.to_sym)
        end
      end

      # Finds all descendants eligible for cancellation.
      def find_cancellable_descendants(initial_ids)
        visited = Set.new(initial_ids)
        queue = initial_ids.dup
        cancellable = Set.new
        max_iterations = 10_000 # Safety break
        iterations = 0

        while queue.any? && iterations < max_iterations
          # Process in batches to avoid loading too many steps at once
          batch_ids = queue.shift(100) # Adjust batch size as needed
          iterations += 1

          # Fetch step records and their direct dependents for the batch
          step_map = (repository.find_steps(batch_ids) || []).index_by(&:id)
          # Ensure keys are strings if IDs are strings
          dependent_map = repository.get_dependent_ids_bulk(batch_ids.map(&:to_s)) || {}
          batch_ids.each { |id| dependent_map[id.to_s] ||= [] } # Ensure entry for all

          batch_ids.each do |id|
            step = step_map[id]
            next unless step # Skip if step wasn't found (e.g., deleted)

            # --- MODIFIED: Use updated is_cancellable_state? ---
            # Check if the step is in a state eligible for cancellation
            if StateMachine.is_cancellable_state?(step.state.to_sym)
              cancellable << id
              # Add children to queue only if they are valid IDs and not visited
              dependent_map[id.to_s].each do |child_id|
                # --- ADDED CHECK: Ensure child_id is not blank ---
                queue << child_id if child_id.present? && visited.add?(child_id)
                # --- END ADDED CHECK ---
              end
            else
              # Log if a step is skipped because it's not cancellable
              # log_debug "Skipping cancellation for step #{id}: State is #{step.state}"
            end
          end
        end

        log_error "Exceeded max iteration limit (#{max_iterations}) in find_cancellable_descendants." if iterations >= max_iterations
        cancellable.to_a
      end

      # Logging helpers
      def log_debug(msg); logger.debug { "[DependentProcessor] #{msg}" }; end
      def log_info(msg);  logger.info  { "[DependentProcessor] #{msg}" }; end
      def log_warn(msg);  logger.warn  { "[DependentProcessor] #{msg}" }; end
      def log_error(msg); logger.error { "[DependentProcessor] #{msg}" }; end

    end # class DependentProcessor
  end # module Core
end # module Yantra

