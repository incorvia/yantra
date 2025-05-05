# lib/yantra/core/graph_utils.rb
# frozen_string_literal: true

require 'set'
require 'logger'
require_relative '../errors'

module Yantra
  module Core
    # Provides utility functions for traversing the step dependency graph.
    module GraphUtils
      extend self

      DEFAULT_BATCH_SIZE = 100
      MAX_ITERATIONS = 10_000

      # Performs a breadth-first search starting from given node IDs, collecting
      # the IDs of nodes (steps) whose state matches the condition provided
      # by the state_check_block.
      #
      # NOTE: This implementation only continues traversal through steps whose state matches
      # the given condition. That is usually sufficient, because the orchestrator guarantees
      # that a failed step blocks all dependents from running — so descendants will remain
      # in a state like :pending or :cancelled. If this assumption is ever broken (e.g., in
      # partial retries or concurrent state transitions), traversal logic may need to allow
      # walking through non-matching nodes.
      #
      # @param start_node_ids [Array<String>] Starting step IDs.
      # @param repository [#find_steps, #get_dependent_ids_bulk] Source of step and edge data.
      # @param include_starting_nodes [Boolean] Whether to apply the matcher to start_node_ids.
      # @param batch_size [Integer] Number of nodes to process per DB batch.
      # @param logger [Logger] Optional logger.
      # @yield [Symbol] Block to match step state (e.g., ->(state) { state == :cancelled })
      # @return [Array<String>] IDs of matching nodes.
      def find_descendants_matching_state(start_node_ids, repository:, include_starting_nodes: false, batch_size: DEFAULT_BATCH_SIZE, logger: Yantra.logger, &state_check_block)
        unless block_given?
          logger&.error "[GraphUtils] No state_check_block provided to find_descendants_matching_state."
          return []
        end

        return [] if start_node_ids.nil? || start_node_ids.empty?

        visited = Set.new
        queue = []
        matching_ids = Set.new
        iterations = 0

        # --- Initial Node Handling ---
        if include_starting_nodes
          start_nodes = repository.find_steps(start_node_ids) || []
          start_nodes.each do |node|
            node_id = node.id.to_s
            visited.add(node_id)
            if state_check_block.call(node.state.to_sym)
              matching_ids.add(node_id)
              queue << node_id
            end
          end
        else
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
          current_batch_parent_ids = queue.shift(batch_size)

          dependent_map = repository.get_dependent_ids_bulk(current_batch_parent_ids) || {}
          all_direct_dependent_ids = dependent_map.values.flatten.uniq.compact.map(&:to_s)
          next if all_direct_dependent_ids.empty?

          ids_to_fetch = all_direct_dependent_ids - visited.to_a
          next if ids_to_fetch.empty?

          dependent_steps = (repository.find_steps(ids_to_fetch) || []).index_by { |s| s.id.to_s }

          ids_to_fetch.each do |dep_id|
            visited.add(dep_id)
            step = dependent_steps[dep_id]
            next unless step

            if state_check_block.call(step.state.to_sym)
              matching_ids.add(dep_id)
              queue << dep_id # NOTE: traversal continues only if matched
            end
          end
        end
        # --- End Traversal ---

        if iterations >= MAX_ITERATIONS
          logger&.error "[GraphUtils] Exceeded max iteration limit (#{MAX_ITERATIONS}) in find_descendants_matching_state. Graph might be too large or contain cycles."
        end

        matching_ids.to_a

      rescue Yantra::Errors::PersistenceError => e
        logger&.error "[GraphUtils] PersistenceError during graph traversal: #{e.message}"
        []
      rescue => e
        logger&.error "[GraphUtils] Unexpected error during graph traversal: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}"
        []
      end
    end
  end
end

