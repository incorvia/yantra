# lib/yantra/persistence/active_record/adapter.rb

# Ensure ActiveRecord and internal models are loaded. This might happen
# via Bundler's autoloading or explicit requires elsewhere.
# require 'active_record'
# require_relative 'workflow_record'
# require_relative 'job_record'
# require_relative 'job_dependency_record'

# Require the interface this adapter implements
require_relative '../repository_interface'

module Yantra
  module Persistence
    module ActiveRecord
      # Implements the RepositoryInterface using ActiveRecord.
      # Acts as a bridge between the generic Yantra persistence contract
      # and the specific ActiveRecord models (WorkflowRecord, JobRecord, etc.).
      # This class is instantiated dynamically by Yantra.repository based on configuration.
      class Adapter
        include Yantra::Persistence::RepositoryInterface

        # Optional: Initialize with configuration if needed
        # def initialize(config = Yantra.configuration)
        #   @config = config
        # end

        # --- Workflow Methods ---

        # Finds a workflow by its ID.
        # @param workflow_id [String] The UUID of the workflow.
        # @return [WorkflowRecord, nil] The ActiveRecord model instance or nil if not found.
        #   Note: Consider returning a standardized DTO/Struct.
        def find_workflow(workflow_id)
          WorkflowRecord.find_by(id: workflow_id)
        end

        # Persists a new workflow instance.
        # @param workflow_instance [Yantra::Workflow] An instance of a Yantra::Workflow subclass.
        # @return [Boolean] true if successful.
        # @raise [Yantra::Errors::PersistenceError] if persistence fails.
        def persist_workflow(workflow_instance)
          # Extract attributes from the workflow instance and set initial state
          # Note: kwargs are available on the instance but not persisted separately here.
          WorkflowRecord.create!(
            id: workflow_instance.id,
            klass: workflow_instance.klass.to_s, # Use the stored @klass attribute
            arguments: workflow_instance.arguments,
            # kwargs: workflow_instance.kwargs, # Removed: No kwargs column in schema
            state: 'pending', # Set initial state explicitly
            globals: workflow_instance.globals,
            has_failures: false # Ensure default is set
            # Timestamps (created_at, updated_at) handled by ActiveRecord
          )
          true # Indicate success
        rescue ::ActiveRecord::RecordInvalid => e
          # Wrap AR validation errors in a Yantra-specific error.
          raise Yantra::Errors::PersistenceError, "Failed to persist workflow: #{e.message}"
        end

        # Updates attributes of a workflow, optionally checking the expected current state.
        # @param workflow_id [String] The UUID of the workflow.
        # @param attributes_hash [Hash] A hash of attributes to update.
        # @param expected_old_state [Symbol, String, nil] If provided, only update if the current state matches this.
        # @return [Boolean] true if the update was successful, false otherwise.
        def update_workflow_attributes(workflow_id, attributes_hash, expected_old_state: nil)
          workflow = WorkflowRecord.find_by(id: workflow_id)
          return false unless workflow

          if expected_old_state && workflow.state != expected_old_state.to_s
            return false # State mismatch
          end
          # Perform the update with the provided attributes.
          workflow.update(attributes_hash)
        end

        # Atomically sets the 'has_failures' flag to true for a workflow.
        # @param workflow_id [String] The UUID of the workflow.
        # @return [Boolean] true if the flag was set, false if workflow not found.
        def set_workflow_has_failures_flag(workflow_id)
          updated_count = WorkflowRecord.where(id: workflow_id).update_all(has_failures: true)
          updated_count > 0
        end

        # Checks if the 'has_failures' flag is true for a workflow.
        # @param workflow_id [String] The UUID of the workflow.
        # @return [Boolean] true if the flag is set, false otherwise.
        def workflow_has_failures?(workflow_id)
          WorkflowRecord.where(id: workflow_id).pick(:has_failures) || false
        end

        # --- Job Methods ---

        # Finds a job by its ID.
        # @param job_id [String] The UUID of the job.
        # @return [JobRecord, nil] The ActiveRecord model instance or nil if not found.
        def find_job(job_id)
          JobRecord.find_by(id: job_id)
        end

        # Persists a new job instance.
        # @param job_instance [Yantra::Job] An instance of a Yantra::Job subclass.
        # @return [Boolean] true if successful.
        # @raise [Yantra::Errors::PersistenceError] if persistence fails.
        def persist_job(job_instance)
          # Extract attributes from the job instance and set initial state
          JobRecord.create!(
            id: job_instance.id,
            workflow_id: job_instance.workflow_id,
            klass: job_instance.klass.to_s, # Use stored @klass
            arguments: job_instance.arguments,
            state: 'pending', # Set initial state explicitly
            queue: job_instance.queue_name, # Assumes method exists
            is_terminal: job_instance.terminal? || false, # Assumes method exists
            retries: 0 # Ensure default
            # Timestamps handled by ActiveRecord.
          )
          true
        rescue ::ActiveRecord::RecordInvalid => e
          raise Yantra::Errors::PersistenceError, "Failed to persist job: #{e.message}"
        end

        # Updates attributes of a job, optionally checking the expected current state.
        # @param job_id [String] The UUID of the job.
        # @param attributes_hash [Hash] A hash of attributes to update.
        # @param expected_old_state [Symbol, String, nil] If provided, only update if the current state matches this.
        # @return [Boolean] true if the update was successful, false otherwise.
        def update_job_attributes(job_id, attributes_hash, expected_old_state: nil)
          job = JobRecord.find_by(id: job_id)
          return false unless job

          if expected_old_state && job.state != expected_old_state.to_s
            return false # State mismatch
          end
          # Perform the update with the provided attributes.
          job.update(attributes_hash)
        end

        # Gets the count of jobs currently in the 'running' state for a given workflow.
        # @param workflow_id [String] The UUID of the workflow.
        # @return [Integer] The count of running jobs.
        def running_job_count(workflow_id)
          JobRecord.where(workflow_id: workflow_id, state: 'running').count
        end

        # Retrieves jobs associated with a workflow, optionally filtered by status.
        # @param workflow_id [String] The UUID of the workflow.
        # @param status [Symbol, String, nil] If provided, filter jobs by this state.
        # @return [Array<JobRecord>] An array of JobRecord instances.
        def get_workflow_jobs(workflow_id, status: nil)
          scope = JobRecord.where(workflow_id: workflow_id)
          scope = scope.with_state(status) if status
          scope.to_a
        end

        # Increments the retry count for a job.
        # @param job_id [String] The UUID of the job.
        # @return [Boolean] true if successful, false if job not found.
        def increment_job_retries(job_id)
          updated_count = JobRecord.where(id: job_id).update_all("retries = COALESCE(retries, 0) + 1")
          updated_count > 0
        end

        # Records the output of a successfully completed job.
        # @param job_id [String] The UUID of the job.
        # @param output [Object] The output data (should be serializable to JSON).
        # @return [Boolean] true if successful, false if job not found.
        def record_job_output(job_id, output)
          updated_count = JobRecord.where(id: job_id).update_all(output: output)
          updated_count > 0
        end

        # Records error information for a failed job.
        # @param job_id [String] The UUID of the job.
        # @param error [Exception] The exception object caught during execution.
        # @return [Boolean] true if successful, false if job not found.
        def record_job_error(job_id, error)
          error_data = { class: error.class.name, message: error.message, backtrace: error.backtrace&.first(10) }
          updated_count = JobRecord.where(id: job_id).update_all(error: error_data)
          updated_count > 0
        end

        # --- Dependency Methods ---

        # Adds a single dependency relationship between two jobs.
        # @param job_id [String] The UUID of the job that depends on another.
        # @param dependency_job_id [String] The UUID of the job that must complete first.
        # @return [Boolean] true if successful or if the dependency already existed.
        # @raise [Yantra::Errors::PersistenceError] if persistence fails for other reasons.
        def add_job_dependency(job_id, dependency_job_id)
          JobDependencyRecord.create!(job_id: job_id, depends_on_job_id: dependency_job_id)
          true
        rescue ::ActiveRecord::RecordInvalid, ::ActiveRecord::RecordNotUnique => e
           is_duplicate = e.is_a?(::ActiveRecord::RecordNotUnique) ||
                          (e.is_a?(::ActiveRecord::RecordInvalid) && e.record.errors.details.any? { |_field, errors| errors.any? { |err| err[:error] == :taken } })
           raise Yantra::Errors::PersistenceError, "Failed to add dependency: #{e.message}" unless is_duplicate
           true # Already exists
        end

        # Adds multiple dependency relationships in bulk using insert_all for efficiency.
        # @param dependency_links_array [Array<Hash>] An array where each hash
        #   represents a link, e.g., [{ job_id: j1_id, depends_on_job_id: p1_id }, ...].
        #   Keys must match the column names in 'yantra_job_dependencies'.
        # @return [Boolean] true if successful (or if array was empty).
        # @raise [Yantra::Errors::PersistenceError] if bulk persistence fails (e.g., DB constraints).
        def add_job_dependencies_bulk(dependency_links_array)
          return true if dependency_links_array.nil? || dependency_links_array.empty?
          begin
            # Assuming job_id and depends_on_job_id are the only columns needed
            # and timestamps are handled by DB defaults or aren't present.
            JobDependencyRecord.insert_all(dependency_links_array)
            true
          rescue ::ActiveRecord::RecordNotUnique => e
            # Handle unique constraint violation - potentially means some links already existed
            # or there were duplicates in the input array. Depending on desired behavior,
            # could log this, ignore it, or re-raise specific error.
            # Re-raising for now as it indicates a potential issue with input data or state.
            raise Yantra::Errors::PersistenceError, "Bulk dependency insert failed due to unique constraint: #{e.message}. Check for duplicates or pre-existing links."
          rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
            # Catch other potential DB or AR errors during bulk insert.
            raise Yantra::Errors::PersistenceError, "Bulk dependency insert failed: #{e.message}"
          end
        end

        # Gets the IDs of jobs that the given job depends on (prerequisites).
        # @param job_id [String] The UUID of the job.
        # @return [Array<String>] An array of prerequisite job UUIDs.
        def get_job_dependencies(job_id)
          record = JobRecord.find_by(id: job_id)
          record ? record.dependencies.pluck(:id) : []
        end

        # Gets the IDs of jobs that depend on the given job.
        # @param job_id [String] The UUID of the job.
        # @return [Array<String>] An array of dependent job UUIDs.
        def get_job_dependents(job_id)
          record = JobRecord.find_by(id: job_id)
          record ? record.dependents.pluck(:id) : []
        end

        # Finds jobs within a workflow that are ready to run (pending with succeeded dependencies).
        # @param workflow_id [String] The UUID of the workflow.
        # @return [Array<String>] An array of job UUIDs ready to be enqueued.
        def find_ready_jobs(workflow_id)
          ready_job_ids = []
          JobRecord.includes(:dependencies).where(workflow_id: workflow_id, state: 'pending').find_each do |job|
             next if job.dependencies.empty?
             all_deps_succeeded = job.dependencies.all? { |dep| dep.state == 'succeeded' }
             ready_job_ids << job.id if all_deps_succeeded
          end
          ready_job_ids
        end

        # --- Listing/Cleanup Methods ---

        # Lists workflows, optionally filtered by status, with pagination.
        # @param status [Symbol, String, nil] Filter by state if provided.
        # @param limit [Integer] Maximum number of workflows to return.
        # @param offset [Integer] Number of workflows to skip for pagination.
        # @return [Array<WorkflowRecord>] An array of WorkflowRecord instances.
        def list_workflows(status: nil, limit: 50, offset: 0)
          scope = WorkflowRecord.order(created_at: :desc).limit(limit).offset(offset)
          scope = scope.with_state(status) if status
          scope.to_a
        end

        # Deletes a workflow and its associated jobs (due to dependent: :destroy).
        # @param workflow_id [String] The UUID of the workflow to delete.
        # @return [Boolean] true if a workflow was found and deleted, false otherwise.
        def delete_workflow(workflow_id)
          workflow = WorkflowRecord.find_by(id: workflow_id)
          deleted = workflow&.destroy
          !deleted.nil?
        end

        # Deletes workflows that finished before a given timestamp.
        # @param cutoff_timestamp [Time, DateTime] Delete workflows finished before this time.
        # @return [Integer] The number of workflows deleted.
        def delete_expired_workflows(cutoff_timestamp)
          deleted_workflows = WorkflowRecord.where("finished_at < ?", cutoff_timestamp)
          count = deleted_workflows.count
          deleted_workflows.destroy_all
          count
        end

      end
    end
  end
end

