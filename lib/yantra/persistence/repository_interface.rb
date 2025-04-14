# lib/yantra/persistence/repository_interface.rb

module Yantra
  module Persistence
    # Defines the contract for persistence adapters used by Yantra.
    # Concrete adapters (e.g., RedisAdapter, ActiveRecordAdapter) must
    # implement all of these methods.
    #
    # Adapters handle the specific details of interacting with the chosen backend
    # (Redis, PostgreSQL via ActiveRecord, etc.).
    module RepositoryInterface

      # --- Workflow Methods ---

      # Persists a new workflow instance.
      # @param workflow_instance [Yantra::Workflow] An instance of a Yantra::Workflow subclass.
      # @return [Boolean] true if successful.
      # @raise [Yantra::Errors::PersistenceError] if persistence fails.
      def persist_workflow(workflow_instance)
        raise NotImplementedError, "#{self.class.name}#persist_workflow is not implemented"
      end

      # Finds a workflow by its ID.
      # @param workflow_id [String] The UUID of the workflow.
      # @return [Object, nil] A representation of the workflow (e.g., AR record, Struct, Hash) or nil if not found.
      #   The exact return type may depend on the adapter, but should contain necessary status info.
      def find_workflow(workflow_id)
        raise NotImplementedError, "#{self.class.name}#find_workflow is not implemented"
      end

      # Updates attributes of a workflow, optionally checking the expected current state.
      # @param workflow_id [String] The UUID of the workflow.
      # @param attributes_hash [Hash] A hash of attributes to update (e.g., { state: 'running', started_at: Time.current }).
      # @param expected_old_state [Symbol, String, nil] If provided, only update if the current state matches this.
      # @return [Boolean] true if the update was successful, false otherwise (e.g., not found or state mismatch).
      def update_workflow_attributes(workflow_id, attributes_hash, expected_old_state: nil)
        raise NotImplementedError, "#{self.class.name}#update_workflow_attributes is not implemented"
      end

      # Atomically sets the 'has_failures' flag to true for a workflow.
      # @param workflow_id [String] The UUID of the workflow.
      # @return [Boolean] true if the flag was set (or already true), false if workflow not found.
      def set_workflow_has_failures_flag(workflow_id)
        raise NotImplementedError, "#{self.class.name}#set_workflow_has_failures_flag is not implemented"
      end

      # Checks if the 'has_failures' flag is true for a workflow.
      # @param workflow_id [String] The UUID of the workflow.
      # @return [Boolean] true if the flag is set, false otherwise.
      def workflow_has_failures?(workflow_id)
        raise NotImplementedError, "#{self.class.name}#workflow_has_failures? is not implemented"
      end

      # --- Job Methods ---

      # Persists a new job instance.
      # @param job_instance [Yantra::Job] An instance of a Yantra::Job subclass.
      # @return [Boolean] true if successful.
      # @raise [Yantra::Errors::PersistenceError] if persistence fails.
      def persist_job(job_instance)
        raise NotImplementedError, "#{self.class.name}#persist_job is not implemented"
      end

      # Persists multiple new job instances in bulk for efficiency.
      # @param job_instances_array [Array<Yantra::Job>] An array of Yantra::Job subclass instances.
      # @return [Boolean] true if successful.
      # @raise [Yantra::Errors::PersistenceError] if bulk persistence fails.
      def persist_jobs_bulk(job_instances_array)
        raise NotImplementedError, "#{self.class.name}#persist_jobs_bulk is not implemented"
      end

      # Finds a job by its ID.
      # @param job_id [String] The UUID of the job.
      # @return [Object, nil] A representation of the job (e.g., AR record, Struct, Hash) or nil if not found.
      def find_job(job_id)
        raise NotImplementedError, "#{self.class.name}#find_job is not implemented"
      end

      # Updates attributes of a job, optionally checking the expected current state.
      # @param job_id [String] The UUID of the job.
      # @param attributes_hash [Hash] A hash of attributes to update (e.g., { state: 'running', started_at: Time.current }).
      # @param expected_old_state [Symbol, String, nil] If provided, only update if the current state matches this.
      # @return [Boolean] true if the update was successful, false otherwise.
      def update_job_attributes(job_id, attributes_hash, expected_old_state: nil)
        raise NotImplementedError, "#{self.class.name}#update_job_attributes is not implemented"
      end

      # Gets the count of jobs currently in the 'running' state for a given workflow.
      # Must be implemented efficiently by adapters (e.g., using indexes or dedicated counters).
      # @param workflow_id [String] The UUID of the workflow.
      # @return [Integer] The count of running jobs.
      def running_job_count(workflow_id)
        raise NotImplementedError, "#{self.class.name}#running_job_count is not implemented"
      end

      # Retrieves jobs associated with a workflow, optionally filtered by status.
      # @param workflow_id [String] The UUID of the workflow.
      # @param status [Symbol, String, nil] If provided, filter jobs by this state.
      # @return [Array<Object>] An array of job representations (e.g., AR records, Structs, Hashes).
      def get_workflow_jobs(workflow_id, status: nil)
        raise NotImplementedError, "#{self.class.name}#get_workflow_jobs is not implemented"
      end

      # Increments the retry count for a job.
      # @param job_id [String] The UUID of the job.
      # @return [Boolean] true if successful, false if job not found.
      def increment_job_retries(job_id)
        raise NotImplementedError, "#{self.class.name}#increment_job_retries is not implemented"
      end

      # Records the output of a successfully completed job.
      # @param job_id [String] The UUID of the job.
      # @param output [Object] The output data (should be serializable).
      # @return [Boolean] true if successful, false if job not found.
      def record_job_output(job_id, output)
        raise NotImplementedError, "#{self.class.name}#record_job_output is not implemented"
      end

      # Records error information for a failed job.
      # @param job_id [String] The UUID of the job.
      # @param error [Exception] The exception object caught during execution.
      # @return [Boolean] true if successful, false if job not found.
      def record_job_error(job_id, error)
        raise NotImplementedError, "#{self.class.name}#record_job_error is not implemented"
      end

      # --- Dependency Methods ---

      # Adds a single dependency relationship between two jobs.
      # @param job_id [String] The UUID of the job that depends on another.
      # @param dependency_job_id [String] The UUID of the job that must complete first.
      # @return [Boolean] true if successful or if the dependency already existed.
      # @raise [Yantra::Errors::PersistenceError] if persistence fails for other reasons.
      def add_job_dependency(job_id, dependency_job_id)
        raise NotImplementedError, "#{self.class.name}#add_job_dependency is not implemented"
      end

      # Adds multiple dependency relationships in bulk.
      # @param dependency_links_array [Array<Hash>] An array where each hash
      #   represents a link, e.g., [{ job_id: j1, depends_on_job_id: p1 }, ...].
      # @return [Boolean] true if successful.
      # @raise [Yantra::Errors::PersistenceError] if bulk persistence fails.
      def add_job_dependencies_bulk(dependency_links_array)
         raise NotImplementedError, "#{self.class.name}#add_job_dependencies_bulk is not implemented"
      end

      # Gets the IDs of jobs that the given job depends on (prerequisites).
      # @param job_id [String] The UUID of the job.
      # @return [Array<String>] An array of prerequisite job UUIDs.
      def get_job_dependencies(job_id)
        raise NotImplementedError, "#{self.class.name}#get_job_dependencies is not implemented"
      end

      # Gets the IDs of jobs that depend on the given job.
      # @param job_id [String] The UUID of the job.
      # @return [Array<String>] An array of dependent job UUIDs.
      def get_job_dependents(job_id)
        raise NotImplementedError, "#{self.class.name}#get_job_dependents is not implemented"
      end

      # Finds jobs within a workflow that are ready to run (pending with succeeded dependencies).
      # Adapters should implement this efficiently if possible, though complex logic
      # might also reside in the Orchestrator using simpler adapter methods.
      # @param workflow_id [String] The UUID of the workflow.
      # @return [Array<String>] An array of job UUIDs ready to be enqueued.
      def find_ready_jobs(workflow_id)
        raise NotImplementedError, "#{self.class.name}#find_ready_jobs is not implemented"
      end

      # --- Listing/Cleanup Methods ---

      # Lists workflows, optionally filtered by status, with pagination.
      # @param status [Symbol, String, nil] Filter by state if provided.
      # @param limit [Integer] Maximum number of workflows to return.
      # @param offset [Integer] Number of workflows to skip for pagination.
      # @return [Array<Object>] An array of workflow representations.
      def list_workflows(status: nil, limit: 50, offset: 0)
        raise NotImplementedError, "#{self.class.name}#list_workflows is not implemented"
      end

      # Deletes a workflow and its associated jobs/data.
      # @param workflow_id [String] The UUID of the workflow to delete.
      # @return [Boolean] true if a workflow was found and deleted, false otherwise.
      def delete_workflow(workflow_id)
        raise NotImplementedError, "#{self.class.name}#delete_workflow is not implemented"
      end

      # Deletes workflows that finished before a given timestamp.
      # @param cutoff_timestamp [Time, DateTime] Delete workflows finished before this time.
      # @return [Integer] The number of workflows deleted.
      def delete_expired_workflows(cutoff_timestamp)
        raise NotImplementedError, "#{self.class.name}#delete_expired_workflows is not implemented"
      end

    end
  end
end

