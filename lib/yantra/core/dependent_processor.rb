# lib/yantra/core/dependent_processor.rb (New File)
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require 'set' # Needed for Set used in find_all_pending_descendants

module Yantra
  module Core
    # Service class responsible for processing the dependent steps
    # after a single step finishes execution. It determines which
    # dependents are now ready to be enqueued or need to be cancelled.
    class DependentProcessor
      # Dependencies injected during initialization
      attr_reader :repository, :step_enqueuer, :logger

      def initialize(repository:, step_enqueuer:, logger: Yantra.logger)
        @repository = repository
        @step_enqueuer = step_enqueuer
        @logger = logger || Logger.new(IO::NULL) # Ensure logger exists

        unless repository # Add checks for required collaborators
          raise ArgumentError, "DependentProcessor requires a repository"
        end
        unless step_enqueuer&.respond_to?(:call)
          raise ArgumentError, "DependentProcessor requires a step_enqueuer"
        end
      end

      # Public interface to process dependents
      # @param finished_step_id [String] ID of the step that just finished.
      # @param finished_state [Symbol] Final state of the finished step (:succeeded, :failed, :cancelled).
      # @param workflow_id [String] ID of the workflow instance.
      # @return [Array<String>, nil] Returns array of cancelled step IDs if cancellation occurred, nil otherwise.
      # @raise [StandardError] Re-raises errors encountered during processing.
      def call(finished_step_id:, finished_state:, workflow_id:)
        # Store context in instance variables for helper methods
        @finished_step_id = finished_step_id
        @finished_state = finished_state.to_sym # Ensure symbol
        @workflow_id = workflow_id

        # Reset intermediate state for this call
        @dependents_ids = nil
        @parent_map = nil
        @states = nil

        # Fetch direct dependents
        @dependents_ids = fetch_direct_dependents
        return nil if @dependents_ids.empty? # Nothing to process

        log_debug "Processing dependents for #{@finished_step_id} (State: #{@finished_state}) in workflow #{@workflow_id}: #{@dependents_ids.inspect}"

        # Handle based on the finished step's state
        if @finished_state == StateMachine::SUCCEEDED
          process_dependents_on_success
          return nil # No cancellations occurred
        else
          # Failure or cancellation requires cancelling downstream
          # Return the list of IDs that were targeted for cancellation
          return cancel_downstream_dependents(@dependents_ids, @finished_step_id, @finished_state)
        end

      rescue StandardError => e
        # Log errors occurring during the dependent processing itself
        log_error "Error during dependent processing for step #{@finished_step_id}: #{e.class} - #{e.message}\n#{e.backtrace&.first(10)&.join("\n")}"
        # --- MODIFIED: Re-raise the error ---
        # Swallowing the error here prevents the Orchestrator from knowing
        # that processing failed, potentially leaving the workflow in a bad state.
        raise e
        # --- END MODIFIED ---
      end

      private

      # --- Helper Methods for Success Path ---

      def process_dependents_on_success
        # Fetch necessary data: dependencies of dependents and relevant states
        fetch_dependency_data_for_dependents

        # Identify which dependents are now ready
        ready_step_ids = find_ready_dependents

        if ready_step_ids.any?
          log_info "Steps ready to enqueue after #{@finished_step_id} finished: #{ready_step_ids.inspect}"
          # Delegate enqueuing to the StepEnqueuer service
          @step_enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: ready_step_ids)
        else
          log_debug "No direct dependents became ready after #{@finished_step_id} finished."
        end
      end

      # Fetches the direct dependents of the finished step
      def fetch_direct_dependents
        repository.get_dependent_ids(@finished_step_id)
      end

      # Fetches the prerequisite map and all relevant step states
      def fetch_dependency_data_for_dependents
        # Fetch map of { dependent_id => [prereq_id1, prereq_id2] }
        # Ensure parent_map is a hash even if bulk method returns nil
        @parent_map = repository.get_dependency_ids_bulk(@dependents_ids) || {}
        # Ensure every dependent has an entry, even if empty list of parents
        @dependents_ids.each { |id| @parent_map[id] ||= [] }


        # Collect all unique step IDs involved (dependents + all their parents)
        all_involved_step_ids = (@dependents_ids + @parent_map.values.flatten).uniq

        # Fetch the states for all involved steps in one go
        @states = fetch_states_for_steps(all_involved_step_ids)
      end

      # Filters the direct dependents to find those now ready
      def find_ready_dependents
        @dependents_ids.select do |step_id|
          # Check if the dependent itself is PENDING
          is_pending = (@states[step_id.to_s] == StateMachine::PENDING.to_s)

          # Check if all its prerequisites are SUCCEEDED
          prereqs = @parent_map[step_id] || [] # Use the fetched parent map
          all_prereqs_succeeded = are_all_prerequisites_succeeded?(prereqs)

          is_pending && all_prereqs_succeeded
        end
      end

      # Checks if all prerequisites for a single step are succeeded based on fetched states
      def are_all_prerequisites_succeeded?(prerequisite_ids)
        return true if prerequisite_ids.empty? # No prerequisites means it's ready

        prerequisite_ids.all? do |prereq_id|
          @states[prereq_id.to_s] == StateMachine::SUCCEEDED.to_s
        end
      end

      # Bulk fetches states, includes fallback logic (as before)
      def fetch_states_for_steps(step_ids)
        return {} if step_ids.nil? || step_ids.empty?
        unique_ids = step_ids.uniq

        # Prefer bulk operation if available
        if repository.respond_to?(:get_step_states)
          # Ensure the result is a hash with string keys for consistency
          result = repository.get_step_states(unique_ids) || {}
          return result.transform_keys(&:to_s)
        end

        # Fallback with warning (potential N+1)
        log_warn "Repository does not implement bulk get_step_states, falling back to individual find_step calls."
        states = {}
        unique_ids.each do |id|
          step = repository.find_step(id) # N+1 potential
          states[id.to_s] = step&.state.to_s if step # Store as string keys for consistency
        end
        states
      rescue => e
        log_error "Failed to fetch step states: #{e.message}"
        {} # Return empty hash on error
      end

      # --- Helper Methods for Failure/Cancellation Path ---

      # Cancels all pending downstream steps starting from a set of initial dependents.
      # @return [Array<String>] List of step IDs that were targeted for cancellation.
      def cancel_downstream_dependents(initial_step_ids, failed_step_id, state)
        log_warn "Cancelling downstream steps of #{failed_step_id} (state: #{state})"

        descendants_to_cancel_ids = find_all_pending_descendants(initial_step_ids)

        return [] if descendants_to_cancel_ids.empty? # Return empty if nothing to cancel

        log_info "Bulk cancelling #{descendants_to_cancel_ids.size} pending descendant steps: #{descendants_to_cancel_ids.inspect}"

        # Perform the bulk cancellation
        cancelled_count = repository.cancel_steps_bulk(descendants_to_cancel_ids)
        log_info "Repository reported #{cancelled_count} steps cancelled."

        # Return the list of IDs that were cancelled (or attempted)
        return descendants_to_cancel_ids

      rescue => e
        # Log and re-raise errors during cancellation itself
        log_error "Unexpected error cancelling steps: #{e.class} - #{e.message}\n#{e.backtrace&.first(5)&.join("\n")}"
        # --- MODIFIED: Re-raise the error ---
        raise e
        # --- END MODIFIED ---
      end

      # Performs a breadth-first search to find all descendants that are still PENDING.
      # This is necessary for cascading cancellation.
      def find_all_pending_descendants(initial_step_ids)
        pending_descendants = Set.new
        queue = initial_step_ids.dup # Start queue with immediate dependents
        visited = Set.new(initial_step_ids) # Keep track of visited nodes

        max_iterations = 10_000 # Safety break for large graphs or cycles
        iterations = 0

        # Use bulk operations where possible for efficiency
        fetch_states_in_bulk = repository.respond_to?(:get_step_states)
        fetch_dependents_in_bulk = repository.respond_to?(:get_dependent_ids_bulk)

        while !queue.empty? && iterations < max_iterations
          iterations += 1
          # Process in batches to potentially optimize DB calls
          current_batch_ids = queue.shift(100) # Process up to 100 at a time

          # Fetch states and dependents for the current batch
          batch_states = fetch_states_in_bulk ?
                           fetch_states_for_steps(current_batch_ids) :
                           {} # Will fetch individually if bulk not supported
          batch_dependents_map = fetch_dependents_in_bulk ?
                                   (repository.get_dependent_ids_bulk(current_batch_ids) || {}) :
                                   {} # Will fetch individually if bulk not supported

          # Ensure all IDs have an entry in the maps, even if empty
          current_batch_ids.each do |id|
            batch_dependents_map[id] ||= [] unless fetch_dependents_in_bulk
          end

          current_batch_ids.each do |step_id|
            # Fetch state individually if bulk wasn't used
            current_state = fetch_states_in_bulk ?
                              batch_states[step_id.to_s] :
                              (repository.find_step(step_id)&.state.to_s) # Individual fetch

            # If the step is PENDING, add it to the cancel list and queue its children
            if current_state == StateMachine::PENDING.to_s
              pending_descendants << step_id

              # Get dependents for this specific step
              dependents = fetch_dependents_in_bulk ?
                             batch_dependents_map[step_id] :
                             repository.get_dependent_ids(step_id) # Individual fetch

              (dependents || []).each do |dependent_id|
                # Add to queue only if not visited before
                queue << dependent_id if visited.add?(dependent_id)
              end
            end
          end
        end

        if iterations >= max_iterations
            log_error "Exceeded max iterations (#{max_iterations}) in find_all_pending_descendants. Potential cycle or very large graph."
        end

        pending_descendants.to_a
      end

      # --- Logging Helpers ---
      # (Assumes logger responds to debug, info, warn, error)
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

