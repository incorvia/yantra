# lib/yantra/persistence/repository_interface.rb

module Yantra
  module Persistence
    # Defines the contract for persistence adapters.
    # Concrete adapters (e.g., RedisAdapter, SqlAdapter) must implement these methods.
    # This ensures the core Yantra logic can interact with any backend
    # without knowing its specific implementation details.
    module RepositoryInterface

      # == Workflow Persistence Methods ==

      # Persists a new workflow instance or updates an existing one.
      # @param workflow_instance [Yantra::Workflow] The workflow object to persist.
      # @return [Boolean] true if successful.
      def persist_workflow(workflow_instance)
        raise NotImplementedError, "#{self.class.name} must implement #persist_workflow"
      end

      # Finds and reconstructs a workflow instance by its ID.
      # @param workflow_id [String] The UUID of the workflow.
      # @return [Yantra::Workflow, nil] The reconstructed workflow object or nil if not found.
      def find_workflow(workflow_id)
        raise NotImplementedError, "#{self.class.name} must implement #find_workflow"
      end

      # Atomically updates the state of a workflow.
      # Should ideally support conditional updates based on expected_old_state.
      # @param workflow_id [String] The UUID of the workflow.
      # @param new_state [Symbol] The new state symbol.
      # @param expected_old_state [Symbol, nil] If provided, only update if current state matches this.
      # @return [Boolean] true if the update was successful.
      def update_workflow_state(workflow_id, new_state, expected_old_state: nil)
        raise NotImplementedError, "#{self.class.name} must implement #update_workflow_state"
      end

      # Lists workflows, potentially filtered by status, with pagination.
      # @param status [Symbol, nil] Filter by workflow state (e.g., :running, :failed).
      # @param limit [Integer] Maximum number of workflows to return.
      # @param offset [Integer] Number of workflows to skip (for pagination).
      # @return [Array<Hash>] An array of workflow data hashes (suitable for light display or reconstruction).
      def list_workflows(status: nil, limit: 50, offset: 0)
         raise NotImplementedError, "#{self.class.name} must implement #list_workflows"
      end

      # Deletes a workflow and potentially its associated jobs/data.
      # @param workflow_id [String] The UUID of the workflow to delete.
      # @return [Boolean] true if successful.
      def delete_workflow(workflow_id)
         raise NotImplementedError, "#{self.class.name} must implement #delete_workflow"
      end

      # == Job Persistence Methods ==

      # Persists a new job instance or updates an existing one.
      # @param job_instance [Yantra::Job] The job object to persist.
      # @return [Boolean] true if successful.
      def persist_job(job_instance)
        raise NotImplementedError, "#{self.class.name} must implement #persist_job"
      end

      # Finds and reconstructs a job instance by its ID.
      # @param job_id [String] The UUID of the job.
      # @return [Yantra::Job, nil] The reconstructed job object or nil if not found.
      def find_job(job_id)
        raise NotImplementedError, "#{self.class.name} must implement #find_job"
      end

      # Atomically updates the state of a job.
      # Should ideally support conditional updates based on expected_old_state.
      # @param job_id [String] The UUID of the job.
      # @param new_state [Symbol] The new state symbol.
      # @param expected_old_state [Symbol, nil] If provided, only update if current state matches this.
      # @return [Boolean] true if the update was successful.
      def update_job_state(job_id, new_state, expected_old_state: nil)
        raise NotImplementedError, "#{self.class.name} must implement #update_job_state"
      end

      # Retrieves jobs associated with a specific workflow, optionally filtered by status.
      # @param workflow_id [String] The UUID of the workflow.
      # @param status [Symbol, nil] Filter by job state (e.g., :running, :failed).
      # @return [Array<Hash>] An array of job data hashes.
      def get_workflow_jobs(workflow_id, status: nil)
         raise NotImplementedError, "#{self.class.name} must implement #get_workflow_jobs"
      end

      # == Dependency Management Methods ==

      # Records that job_id depends on dependency_job_id.
      # @param job_id [String] The dependent job's UUID.
      # @param dependency_job_id [String] The UUID of the job it depends on.
      # @return [Boolean] true if successful.
      def add_job_dependency(job_id, dependency_job_id)
        raise NotImplementedError, "#{self.class.name} must implement #add_job_dependency"
      end

      # Gets the IDs of jobs that the specified job depends on.
      # @param job_id [String] The UUID of the job.
      # @return [Array<String>] An array of dependency job UUIDs.
      def get_job_dependencies(job_id)
        raise NotImplementedError, "#{self.class.name} must implement #get_job_dependencies"
      end

      # Gets the IDs of jobs that depend on the specified job.
      # @param job_id [String] The UUID of the job.
      # @return [Array<String>] An array of dependent job UUIDs.
      def get_job_dependents(job_id)
        raise NotImplementedError, "#{self.class.name} must implement #get_job_dependents"
      end

      # == Orchestration Support Methods ==

      # Finds jobs within a workflow whose dependencies have all successfully completed.
      # @param workflow_id [String] The UUID of the workflow.
      # @return [Array<Yantra::Job>] An array of reconstructed Job objects ready to run.
      def find_ready_jobs(workflow_id)
        raise NotImplementedError, "#{self.class.name} must implement #find_ready_jobs"
      end

      # == Job Execution Data Methods ==

      # Atomically increments the retry count for a job.
      # @param job_id [String] The UUID of the job.
      # @return [Integer] The new retry count.
      def increment_job_retries(job_id)
        raise NotImplementedError, "#{self.class.name} must implement #increment_job_retries"
      end

      # Records the output of a successfully completed job.
      # @param job_id [String] The UUID of the job.
      # @param output [Object] The output data (should be serializable, e.g., Hash, String).
      # @return [Boolean] true if successful.
      def record_job_output(job_id, output)
        raise NotImplementedError, "#{self.class.name} must implement #record_job_output"
      end

      # Records error information for a failed job.
      # @param job_id [String] The UUID of the job.
      # @param error [Hash] Error details (e.g., { class:, message:, backtrace: }).
      # @return [Boolean] true if successful.
      def record_job_error(job_id, error)
        raise NotImplementedError, "#{self.class.name} must implement #record_job_error"
      end

      # Optional: Add methods for queue management if needed by the orchestrator,
      # e.g., adding job_id to a ready queue.
      # def add_to_ready_queue(queue_name, job_id)
      #   raise NotImplementedError, ...
      # end
      # def fetch_from_ready_queue(queue_name)
      #   raise NotImplementedError, ...
      # end

    end
  end
end

