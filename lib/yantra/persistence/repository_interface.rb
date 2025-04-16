# lib/yantra/persistence/repository_interface.rb

module Yantra
  module Persistence
    module RepositoryInterface

      # ... (persist_workflow, find_workflow, etc. - keep existing methods) ...
      def find_workflow(workflow_id)
        raise NotImplementedError
      end

      def persist_workflow(workflow_instance)
        raise NotImplementedError
      end

      def update_workflow_attributes(workflow_id, attributes_hash, expected_old_state: nil)
        raise NotImplementedError
      end

      def set_workflow_has_failures_flag(workflow_id)
        raise NotImplementedError
      end

      def workflow_has_failures?(workflow_id)
        raise NotImplementedError
      end

      # --- Job Methods ---

      def persist_step(step_instance)
        raise NotImplementedError # Keep single persist
      end

      def persist_steps_bulk(step_instances_array)
        raise NotImplementedError # Keep bulk persist
      end

      def find_step(step_id)
        raise NotImplementedError
      end

      def update_step_attributes(step_id, attributes_hash, expected_old_state: nil)
        raise NotImplementedError
      end

      def cancel_jobs_bulk(step_ids)
        raise NotImplementedError, "#{self.class.name}#cancel_jobs_bulk is not implemented"
      end

      def running_step_count(workflow_id)
        raise NotImplementedError
      end

      # --- NEW METHOD ---
      # Counts jobs currently in the 'enqueued' state for a workflow.
      # @param workflow_id [String] The UUID of the workflow.
      # @return [Integer] The number of enqueued jobs.
      def enqueued_step_count(workflow_id)
        raise NotImplementedError, "#{self.class.name}#enqueued_step_count is not implemented"
      end
      # --- END NEW METHOD ---

      def get_workflow_steps(workflow_id, status: nil)
        raise NotImplementedError
      end

      def increment_step_retries(step_id)
        raise NotImplementedError
      end

      def record_step_output(step_id, output)
        raise NotImplementedError
      end

      def record_step_error(step_id, error)
        raise NotImplementedError
      end

      # --- Dependency Methods ---
      # ... (add_step_dependency, add_step_dependencies_bulk, get_step_dependencies, get_step_dependents remain the same) ...

      def add_step_dependency(step_id, dependency_step_id)
        raise NotImplementedError
      end

      def add_step_dependencies_bulk(dependency_links_array)
         raise NotImplementedError
      end

      def get_step_dependencies(step_id)
        raise NotImplementedError
      end

      def get_step_dependents(step_id)
        raise NotImplementedError
      end

      def find_ready_jobs(workflow_id)
        raise NotImplementedError
      end

      # --- Listing/Cleanup Methods ---
      # ... (list_workflows, delete_workflow, delete_expired_workflows remain the same) ...

      def list_workflows(status: nil, limit: 50, offset: 0)
        raise NotImplementedError
      end

      def delete_workflow(workflow_id)
        raise NotImplementedError
      end

      def delete_expired_workflows(cutoff_timestamp)
        raise NotImplementedError
      end

    end
  end
end

