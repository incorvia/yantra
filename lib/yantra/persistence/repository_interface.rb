# lib/yantra/persistence/repository_interface.rb

module Yantra
  module Persistence
    module RepositoryInterface
      # --- Workflow Methods ---
      def find_workflow(workflow_id); raise NotImplementedError; end
      def persist_workflow(workflow_instance); raise NotImplementedError; end
      def update_workflow_attributes(workflow_id, attributes_hash, expected_old_state: nil); raise NotImplementedError; end
      def set_workflow_has_failures_flag(workflow_id); raise NotImplementedError; end
      def workflow_has_failures?(workflow_id); raise NotImplementedError; end

      # --- Step Methods ---
      def find_step(step_id); raise NotImplementedError; end
      def persist_step(step_instance); raise NotImplementedError; end
      def persist_steps_bulk(step_instances_array); raise NotImplementedError; end
      def update_step_attributes(step_id, attributes_hash, expected_old_state: nil); raise NotImplementedError; end
      def running_step_count(workflow_id); raise NotImplementedError; end
      def enqueued_step_count(workflow_id); raise NotImplementedError; end
      def get_workflow_steps(workflow_id, status: nil); raise NotImplementedError; end
      def increment_step_retries(step_id); raise NotImplementedError; end
      def record_step_output(step_id, output); raise NotImplementedError; end
      def record_step_error(step_id, error); raise NotImplementedError; end

      # --- Dependency Methods ---
      def add_step_dependency(step_id, dependency_step_id); raise NotImplementedError; end
      def add_step_dependencies_bulk(dependency_links_array); raise NotImplementedError; end
      def get_step_dependencies(step_id); raise NotImplementedError; end # Returns IDs of steps this step depends on (parents)
      def get_step_dependents(step_id); raise NotImplementedError; end # Returns IDs of steps that depend on this step (children)
      def find_ready_steps(workflow_id); raise NotImplementedError; end

      # --- Bulk Operations ---
      def cancel_steps_bulk(step_ids); raise NotImplementedError; end

      # --- Listing/Cleanup Methods ---
      def list_workflows(status: nil, limit: 50, offset: 0); raise NotImplementedError; end
      def delete_workflow(workflow_id); raise NotImplementedError; end
      def delete_expired_workflows(cutoff_timestamp); raise NotImplementedError; end

      # --- Pipelining Methods --- NEW! ---
      def fetch_step_outputs(step_ids); raise NotImplementedError; end
    end
  end
end

