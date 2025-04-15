# lib/yantra/persistence/repository_interface.rb

module Yantra
  module Persistence
    module RepositoryInterface

      # ... (persist_workflow, find_workflow, etc. - keep existing methods) ...

      # --- Job Methods ---

      def persist_job(job_instance)
        raise NotImplementedError # Keep single persist
      end

      def persist_jobs_bulk(job_instances_array)
        raise NotImplementedError # Keep bulk persist
      end

      def find_job(job_id)
        raise NotImplementedError
      end

      def update_job_attributes(job_id, attributes_hash, expected_old_state: nil)
        raise NotImplementedError
      end

      # --- NEW METHOD ---
      # Updates multiple jobs to the 'cancelled' state efficiently.
      # Should only cancel jobs that are in a cancellable state (e.g., pending, enqueued, running).
      # @param job_ids [Array<String>] An array of job UUIDs to cancel.
      # @return [Integer] The number of jobs actually updated/cancelled.
      def cancel_jobs_bulk(job_ids)
        raise NotImplementedError, "#{self.class.name}#cancel_jobs_bulk is not implemented"
      end
      # --- END NEW METHOD ---

      def running_job_count(workflow_id)
        raise NotImplementedError
      end

      def get_workflow_jobs(workflow_id, status: nil)
        raise NotImplementedError
      end

      def increment_job_retries(job_id)
        raise NotImplementedError
      end

      def record_job_output(job_id, output)
        raise NotImplementedError
      end

      def record_job_error(job_id, error)
        raise NotImplementedError
      end

      # --- Dependency Methods ---
      # ... (add_job_dependency, add_job_dependencies_bulk, get_job_dependencies, get_job_dependents remain the same) ...

      def add_job_dependency(job_id, dependency_job_id)
        raise NotImplementedError
      end

      def add_job_dependencies_bulk(dependency_links_array)
         raise NotImplementedError
      end

      def get_job_dependencies(job_id)
        raise NotImplementedError
      end

      def get_job_dependents(job_id)
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

