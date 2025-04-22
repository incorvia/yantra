# lib/yantra/persistence/repository_interface.rb

module Yantra
  module Persistence
    # Defines the contract for persistence adapters.
    # Adapters must implement all these methods.
    module RepositoryInterface
      # Implement methods for Workflow CRUD and state management
      def find_workflow(workflow_id); raise NotImplementedError; end
      def create_workflow(workflow_instance); raise NotImplementedError; end
      def update_workflow_attributes(workflow_id, attributes_hash, expected_old_state: nil); raise NotImplementedError; end
      def workflow_has_failures?(workflow_id); raise NotImplementedError; end

      # Implement methods for Step CRUD and state management
      def enqueued_step_count(workflow_id); raise NotImplementedError; end # Added for potential use
      def fetch_step_states(step_ids); raise NotImplementedError; end
      def find_step(step_id); raise NotImplementedError; end
      def find_steps(step_ids); raise NotImplementedError; end
      def get_workflow_steps(workflow_id, status: nil); raise NotImplementedError; end
      def increment_step_retries(step_id); raise NotImplementedError; end
      def create_step(step_instance); raise NotImplementedError; end
      def create_steps_bulk(step_instances_array); raise NotImplementedError; end
      def record_step_error(step_id, error); raise NotImplementedError; end
      def record_step_output(step_id, output); raise NotImplementedError; end
      def running_step_count(workflow_id); raise NotImplementedError; end
      def update_step_attributes(step_id, attributes_hash, expected_old_state: nil); raise NotImplementedError; end

      # Implement methods for Dependencies
      def add_step_dependencies_bulk(dependency_links_array); raise NotImplementedError; end
      def add_step_dependency(step_id, dependency_step_id); raise NotImplementedError; end
      def find_ready_steps(workflow_id); raise NotImplementedError; end
      def get_dependencies_ids(step_id); raise NotImplementedError; end
      def get_dependencies_ids_bulk(step_ids); raise NotImplementedError; end
      def get_dependent_ids(step_id); raise NotImplementedError; end
      def get_dependent_ids_bulk(step_ids); raise NotImplementedError; end

      # Implement methods for Bulk Operations / Cleanup
      def bulk_update_steps(step_ids, attributes); raise NotImplementedError; end
      def cancel_steps_bulk(step_ids); raise NotImplementedError; end
      def delete_expired_workflows(cutoff_timestamp); raise NotImplementedError; end
      def delete_workflow(workflow_id); raise NotImplementedError; end
      def fetch_step_outputs(step_ids); raise NotImplementedError; end
      def list_workflows(status: nil, limit: 50, offset: 0); raise NotImplementedError; end
    end
  end
end

