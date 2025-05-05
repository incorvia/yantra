# lib/yantra/core/graph_utils.rb
# frozen_string_literal: true

require 'set'
require 'logger'
require_relative '../errors' # For potential error handling if needed

module Yantra
  module Core
    # Provides utility functions for traversing the step dependency graph.
    module GraphUtils
      extend self # Make methods available as module functions (e.g., GraphUtils.find_descendants...)

      DEFAULT_BATCH_SIZE = 100
      MAX_ITERATIONS = 10_000 # Safety break for large graphs or cycles

      # Performs a breadth-first search starting from given node IDs, collecting
      # the IDs of nodes (steps) whose state matches the condition provided
      # by the state_check_block.
      #
      # @param start_node_ids [Array<String>] IDs of the initial steps to start traversal from.
      # @param repository [Object] An object implementing the RepositoryInterface,
      #   specifically #find_steps and #get_dependent_ids_bulk.
      # @param include_starting_nodes [Boolean] If true, checks the state of the
      #   start_node_ids themselves. If false (default), only checks descendants.
      # @param batch_size [Integer] How many nodes to process per database query batch.
      # @param logger [Logger] Logger instance for debugging/errors.
      # @yield [Symbol] The state symbol of a step. Block should return true if the state matches the desired condition.
      # @return [Array<String>] An array of step IDs that matched the state condition.
      def find_descendants_matching_state(start_node_ids, repository:, include_starting_nodes: false, batch_size: DEFAULT_BATCH_SIZE, logger: Yantra.logger, &state_check_block)
        unless block_given?
          logger&.error "[GraphUtils] No state_check_block provided to find_descendants_matching_state."
          return []
        end
        return [] if start_node_ids.nil? || start_node_ids.empty?

        # Use Sets for efficient lookup and uniqueness
        visited = Set.new
        queue = []
        matching_ids = Set.new
        iterations = 0

        # --- Initial Node Handling ---
        if include_starting_nodes
          # If checking start nodes, fetch them first
          start_nodes = repository.find_steps(start_node_ids) || []
          start_nodes.each do |node|
            visited.add(node.id.to_s) # Mark as visited
            if state_check_block.call(node.state.to_sym)
              matching_ids.add(node.id.to_s)
              queue << node.id.to_s # Add matching start nodes to queue for descendant check
            end
          end
        else
          # If only checking descendants, mark start nodes visited and add to queue
          start_node_ids.each do |id|
            str_id = id.to_s
            visited.add(str_id)
            queue << str_id
          end
        end
        # --- End Initial Node Handling ---


        # --- Breadth-First Traversal ---
        while queue.any? && iterations < MAX_ITERATIONS
          iterations += 1
          # Process nodes in batches
          current_batch_parent_ids = queue.shift(batch_size)

          # Get direct dependents for the current batch of parents
          # Ensure keys/values are consistent (e.g., strings)
          dependent_map = repository.get_dependent_ids_bulk(current_batch_parent_ids.map(&:to_s)) || {}
          all_direct_dependent_ids = dependent_map.values.flatten.uniq.compact.map(&:to_s)

          next if all_direct_dependent_ids.empty?

          # Find the state of these direct dependents
          # Fetch only those not already visited to potentially reduce load
          ids_to_fetch = all_direct_dependent_ids - visited.to_a
          next if ids_to_fetch.empty?

          dependent_steps = (repository.find_steps(ids_to_fetch) || []).index_by { |s| s.id.to_s }

          ids_to_fetch.each do |dep_id|
            visited.add(dep_id) # Mark visited now

            step = dependent_steps[dep_id]
            next unless step # Skip if step record wasn't found

            # Check if the dependent step's state matches the condition
            if state_check_block.call(step.state.to_sym)
              matching_ids.add(dep_id)
              # Add to queue ONLY if it matched, to continue traversal down this path
              queue << dep_id
            end
          end
        end
        # --- End Traversal ---

        if iterations >= MAX_ITERATIONS
           logger&.error "[GraphUtils] Exceeded max iteration limit (#{MAX_ITERATIONS}) in find_descendants_matching_state. Graph might be too large or contain cycles."
        end

        matching_ids.to_a # Return as array

      rescue Yantra::Errors::PersistenceError => e
        logger&.error "[GraphUtils] PersistenceError during graph traversal: #{e.message}"
        [] # Return empty on DB errors
      rescue => e
        logger&.error "[GraphUtils] Unexpected error during graph traversal: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}"
        [] # Return empty on other errors
      end

    end # module GraphUtils
  end # module Core
end # module Yantra

