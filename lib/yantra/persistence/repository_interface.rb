module Yantra
  module Persistence
    # Defines the contract for persistence adapters.
    # Adapters must implement all these methods using consistent naming.
    # Convention:
    # - find_: Retrieve single record(s) by ID(s).
    # - list_: Retrieve collection of records by criteria.
    # - get_: Retrieve specific attributes or associated IDs.
    # - create_: Persist new record(s).
    # - update_: Modify existing record(s).
    # - delete_: Remove record(s).
    # - add_: Create associations.
    # - Other specific verbs (increment_, cancel_, etc.) as needed.
    module RepositoryInterface

      # === Workflow Methods ===

      # --- Read ---
      def find_workflow(workflow_id); raise NotImplementedError; end
      def list_workflows(status: nil, limit: 50, offset: 0); raise NotImplementedError; end
      def workflow_has_failures?(workflow_id); raise NotImplementedError; end

      # --- Write ---
      def create_workflow(workflow_instance); raise NotImplementedError; end
      def create_child_workflow(workflow_instance, parent_workflow_id, idempotency_key: nil); raise NotImplementedError; end
      def update_workflow_attributes(workflow_id, attributes_hash, expected_old_state: nil); raise NotImplementedError; end
      def delete_workflow(workflow_id); raise NotImplementedError; end
      def delete_expired_workflows(cutoff_timestamp); raise NotImplementedError; end
      
      # --- Parent-Child Workflow Methods ---
      def find_child_workflows(parent_workflow_id); raise NotImplementedError; end
      def find_existing_idempotency_keys(parent_workflow_id, potential_keys); raise NotImplementedError; end

      # === Step Methods ===

      # --- Read ---
      def find_step(step_id); raise NotImplementedError; end
      def find_steps(step_ids); raise NotImplementedError; end # Find multiple specific steps by their IDs
      def list_steps(workflow_id:, status: nil); raise NotImplementedError; end # Renamed from get_workflow_steps
      def get_step_states(step_ids); raise NotImplementedError; end # Renamed from fetch_step_states
      def get_step_outputs(step_ids); raise NotImplementedError; end # Renamed from fetch_step_outputs
      def has_steps_in_states?(workflow_id:, states:); raise NotImplementedError; end

      # --- Write ---
      def create_step(step_instance); raise NotImplementedError; end # Renamed from persist_step
      def create_steps_bulk(step_instances_array); raise NotImplementedError; end # Renamed from persist_steps_bulk

        # Updates attributes for a single step record.
      # Uses optimistic locking based on expected_old_state if provided.
      #
      # @param step_id [String] The ID of the step to update.
      # @param attributes_hash [Hash] Hash of attributes to update.
      # @param expected_old_state [Symbol, Array<Symbol>, nil]
      #   If a Symbol, updates only if the current state matches.
      #   If an Array of Symbols, updates only if the current state is one of the states in the array.
      #   If nil, updates without checking the current state (use with caution).
      # @return [Boolean] true if the update was successful (1 row affected), false otherwise.
      # @raise [Yantra::Errors::PersistenceError] on underlying database errors.
      def update_step_attributes(step_id, attributes_hash, expected_old_state: nil); raise NotImplementedError; end
      # WARNING: This method does NOT guard against race conditions.
# Do NOT use it for `state` transitions where correctness depends on current state.
# For atomic state transitions, use `bulk_transition_steps` instead.

      def bulk_update_steps(step_ids, attributes_hash); raise NotImplementedError; end # Updated param name
      def bulk_upsert_steps(updates_array); raise NotImplementedError; end
      def increment_step_retries(step_id); raise NotImplementedError; end
      def increment_step_executions(step_id); raise NotImplementedError; end
      def update_step_output(step_id, output); raise NotImplementedError; end # Renamed from record_step_output
      def update_step_error(step_id, error_hash); raise NotImplementedError; end # Renamed from record_step_error, updated param name
    
      # Performs an atomic bulk update on step records, transitioning them to a new state.
      # Only steps currently in `expected_old_state` will be updated.
      #
      # NOTE: This is intended for critical state transitions. Must be atomic.
      # If any steps are not in the expected state, they will be skipped.
      #
      # @param step_ids [Array<String>] IDs of steps to transition
      # @param attributes_hash [Hash] Attributes to apply (must include :state)
      # @param expected_old_state [Symbol] Only steps currently in this state will be updated
      # @return [Array<String>] IDs of steps that were successfully updated
      def bulk_transition_steps(step_ids, attributes_hash, expected_old_state:)
        raise NotImplementedError
      end

      # === Dependency Methods ===

      # --- Read ---
      def get_dependency_ids(step_id); raise NotImplementedError; end # Renamed from get_dependencies_ids
      def get_dependency_ids_bulk(step_ids); raise NotImplementedError; end # Renamed from get_dependencies_ids_bulk
      def get_dependent_ids(step_id); raise NotImplementedError; end
      def get_dependent_ids_bulk(step_ids); raise NotImplementedError; end

      # --- Write ---
      def add_step_dependency(step_id, dependency_step_id); raise NotImplementedError; end
      def add_step_dependencies_bulk(dependency_links_array); raise NotImplementedError; end

    end
  end
end
