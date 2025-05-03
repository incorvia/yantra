# lib/yantra/core/dependent_processor.rb
# frozen_string_literal: true

require_relative '../errors'
require_relative 'state_machine'
require 'set'

module Yantra
  module Core
    # Service class responsible for processing the dependent steps
    # after a single step finishes execution. It either enqueues
    # ready successors or cancels pending descendants.
    class DependentProcessor
      attr_reader :repository, :step_enqueuer, :logger

      def initialize(repository:, step_enqueuer:, logger: Yantra.logger)
        @repository = repository
        @step_enqueuer = step_enqueuer
        @logger = logger || Logger.new(IO::NULL)

        unless @repository
          raise ArgumentError, "DependentProcessor requires a repository"
        end
        unless @step_enqueuer&.respond_to?(:call)
          raise ArgumentError, "DependentProcessor requires a step_enqueuer"
        end
      end

      # Finds and enqueues direct dependents that are now ready because the
      # finished_step_id succeeded.
      def process_successors(finished_step_id:, workflow_id:)
        @finished_step_id = finished_step_id
        @workflow_id = workflow_id
        @dependents_ids = nil
        @parent_map = nil
        @step_records_map = nil

        @dependents_ids = fetch_direct_dependents
        return if @dependents_ids.empty?

        log_debug "Processing successors for succeeded step #{@finished_step_id}..."

        fetch_dependency_and_step_data_for_dependents
        enqueueable_step_ids = find_enqueueable_dependents

        if enqueueable_step_ids.any?
          log_info "Steps ready to enqueue after #{@finished_step_id} succeeded: #{enqueueable_step_ids.inspect}"
          @step_enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: enqueueable_step_ids)
        else
          log_debug "No direct dependents became ready after #{@finished_step_id} succeeded."
        end

      rescue StandardError => e
        log_error "Error during successor processing for step #{@finished_step_id}: #{e.class} - #{e.message}\n#{e.backtrace&.first(10)&.join("\n")}"
        raise e
      end

      # Finds and cancels all cancellable descendants of a step that failed or was cancelled.
      def process_failure_cascade(finished_step_id:, workflow_id:)
        @finished_step_id = finished_step_id
        @workflow_id = workflow_id
        @dependents_ids = nil

        initial_dependents = fetch_direct_dependents
        return [] if initial_dependents.empty?

        log_warn "Initiating cancellation cascade downstream from failed/cancelled step #{@finished_step_id}"

        descendants_to_cancel_ids = find_all_cancellable_descendants(initial_dependents)
        return [] if descendants_to_cancel_ids.empty?

        log_info "Bulk cancelling #{descendants_to_cancel_ids.size} cancellable descendant steps: #{descendants_to_cancel_ids.inspect}"
        cancel_attrs = {
          state: StateMachine::CANCELLED.to_s,
          finished_at: Time.current,
          updated_at: Time.current
        }
        cancelled_count = repository.bulk_update_steps(descendants_to_cancel_ids, cancel_attrs)
        log_info "Repository reported #{cancelled_count} steps cancelled."

        return descendants_to_cancel_ids

      rescue StandardError => e
        log_error "Error during failure cascade processing for step #{@finished_step_id}: #{e.class} - #{e.message}\n#{e.backtrace&.first(10)&.join("\n")}"
        raise e
      end


      private

      # --- Helper methods for Success Path ---

      def fetch_direct_dependents
        repository.get_dependent_ids(@finished_step_id) || []
      end

      def fetch_dependency_and_step_data_for_dependents
        return unless @dependents_ids&.any?
        @parent_map = repository.get_dependency_ids_bulk(@dependents_ids) || {}
        @dependents_ids.each { |id| @parent_map[id] ||= [] }
        all_involved_step_ids = (@dependents_ids + @parent_map.values.flatten).uniq
        found_steps = repository.find_steps(all_involved_step_ids) || []
        # Store full records keyed by string ID
        @step_records_map = found_steps.index_by { |step| step.id.to_s }
      rescue Yantra::Errors::PersistenceError => e
         log_error "Failed to fetch dependency/step data for dependents of #{@finished_step_id}: #{e.message}"
         @parent_map = {}
         @step_records_map = {}
      end

      def find_enqueueable_dependents
        return [] unless @dependents_ids && @step_records_map && @parent_map
        @dependents_ids.select do |step_id|
          step_record = @step_records_map[step_id.to_s]
          next false unless step_record

          enqueued_at = step_record.enqueued_at
          is_candidate = StateMachine.is_enqueue_candidate_state?(step_record.state.to_sym, enqueued_at)

          # Check if all prerequisites are met
          prereqs = @parent_map[step_id] || []
          all_prereqs_met = are_all_prerequisites_succeeded?(prereqs)

          is_candidate && all_prereqs_met
        end
      end

      def are_all_prerequisites_succeeded?(prerequisite_ids)
        return true if prerequisite_ids.empty?
        return false unless @step_records_map
        prerequisite_ids.all? do |prereq_id|
          prereq_record = @step_records_map[prereq_id.to_s]
          # Prerequisite met only if record exists and state is SUCCEEDED
          prereq_record && StateMachine.prerequisite_met?(prereq_record.state.to_sym)
        end
      end


      # --- Helper Methods for Failure/Cancellation Path ---

      # Performs a breadth-first search to find all descendants that are in a truly cancellable state.
      def find_all_cancellable_descendants(initial_step_ids)
        cancellable_descendants = Set.new
        queue = initial_step_ids.dup
        visited = Set.new(initial_step_ids)
        max_iterations = 10_000
        iterations = 0

        # Use bulk operations where possible for efficiency
        # Note: Need to fetch state AND enqueued_at if checking AWAITING_EXECUTION state
        fetch_bulk = repository.respond_to?(:find_steps) # Check for bulk find method
        fetch_dependents_in_bulk = repository.respond_to?(:get_dependent_ids_bulk)

        while !queue.empty? && iterations < max_iterations
          iterations += 1
          current_batch_ids = queue.shift(100)

          # Fetch necessary data for this batch
          batch_records_map = fetch_bulk ?
                               (repository.find_steps(current_batch_ids) || []).index_by(&:id) :
                               {} # Will fetch individually if bulk not supported
          batch_dependents_map = fetch_dependents_in_bulk ?
                                   (repository.get_dependent_ids_bulk(current_batch_ids) || {}) :
                                   {} # Will fetch individually if bulk not supported
          current_batch_ids.each { |id| batch_dependents_map[id] ||= [] unless fetch_dependents_in_bulk }

          current_batch_ids.each do |step_id|
            # Fetch record individually if bulk wasn't used or didn't find it
            step_record = batch_records_map[step_id] || (repository.find_step(step_id) unless fetch_bulk)
            next unless step_record # Skip if step somehow disappeared

            enqueued_at_ts = step_record.respond_to?(:enqueued_at) ? step_record.enqueued_at : nil
            if StateMachine.is_cancellable_state?(step_record.state.to_sym, enqueued_at_ts)
              cancellable_descendants << step_id

              # Get dependents for this specific step
              dependents = fetch_dependents_in_bulk ?
                             batch_dependents_map[step_id] :
                             repository.get_dependent_ids(step_id) # Individual fetch

              (dependents || []).each do |dep_id|
                # Add to queue only if not visited before
                queue << dep_id if visited.add?(dep_id)
              end
            # else: Step is running, succeeded, failed, cancelled - cannot be cancelled now
            end
          end
        end

        log_error "Exceeded max iterations (#{max_iterations}) in find_all_cancellable_descendants." if iterations >= max_iterations
        cancellable_descendants.to_a
      end

      # --- Logging Helpers ---
      def log_debug(message); logger.debug { "[DependentProcessor] #{message}" }; end
      def log_info(message);  logger.info  { "[DependentProcessor] #{message}" }; end
      def log_warn(message);  logger.warn  { "[DependentProcessor] #{message}" }; end
      def log_error(message); logger.error { "[DependentProcessor] #{message}" }; end

    end # class DependentProcessor
  end # module Core
end # module Yantra
