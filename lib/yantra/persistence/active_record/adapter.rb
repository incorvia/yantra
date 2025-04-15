# lib/yantra/persistence/active_record/adapter.rb

# Ensure ActiveRecord and internal models are loaded.
# require 'active_record'
# require_relative 'workflow_record'
# require_relative 'job_record'
# require_relative 'job_dependency_record'

require_relative '../repository_interface' # Make sure this path is correct

module Yantra
  module Persistence
    module ActiveRecord
      class Adapter
        include Yantra::Persistence::RepositoryInterface

        # --- Workflow Methods ---

        def find_workflow(workflow_id)
          WorkflowRecord.find_by(id: workflow_id)
        end

        def persist_workflow(workflow_instance)
          WorkflowRecord.create!(
            id: workflow_instance.id,
            klass: workflow_instance.klass.to_s,
            arguments: workflow_instance.arguments,
            # kwargs: workflow_instance.kwargs, # Not persisted
            state: 'pending',
            globals: workflow_instance.globals,
            has_failures: false
          )
          true
        rescue ::ActiveRecord::RecordInvalid => e
          raise Yantra::Errors::PersistenceError, "Failed to persist workflow: #{e.message}"
        end

        def update_workflow_attributes(workflow_id, attributes_hash, expected_old_state: nil)
          workflow = WorkflowRecord.find_by(id: workflow_id)
          return false unless workflow
          if expected_old_state && workflow.state != expected_old_state.to_s
            return false
          end
          workflow.update(attributes_hash)
        end

        def set_workflow_has_failures_flag(workflow_id)
          updated_count = WorkflowRecord.where(id: workflow_id).update_all(has_failures: true)
          updated_count > 0
        end

        def workflow_has_failures?(workflow_id)
          WorkflowRecord.where(id: workflow_id).pick(:has_failures) || false
        end

        # --- Job Methods ---

        def find_job(job_id)
          JobRecord.find_by(id: job_id)
        end

        # Persists a single new job instance. USE WITH CAUTION - prefer bulk.
        def persist_job(job_instance)
          JobRecord.create!(
            id: job_instance.id,
            workflow_id: job_instance.workflow_id,
            klass: job_instance.klass.to_s,
            arguments: job_instance.arguments,
            state: 'pending',
            queue: job_instance.queue_name, # Assumes method exists on job_instance
            is_terminal: job_instance.terminal?, # Assumes method exists on job_instance
            retries: 0
          )
          true
        rescue ::ActiveRecord::RecordInvalid => e
          raise Yantra::Errors::PersistenceError, "Failed to persist job: #{e.message}"
        end

        # Persists multiple new job instances in bulk using insert_all for efficiency.
        # @param job_instances_array [Array<Yantra::Job>] An array of Yantra::Job subclass instances.
        # @return [Boolean] true if successful (or if array was empty).
        # @raise [Yantra::Errors::PersistenceError] if bulk persistence fails.
        def persist_jobs_bulk(job_instances_array)
          return true if job_instances_array.nil? || job_instances_array.empty?

          # Prepare array of hashes for insert_all, extracting data from job instances
          current_time = Time.current # Use a single timestamp for consistency
          records_to_insert = job_instances_array.map do |job_instance|
            {
              id: job_instance.id,
              workflow_id: job_instance.workflow_id,
              klass: job_instance.klass.to_s,
              arguments: job_instance.arguments,
              state: 'pending', # Initial state for all jobs
              queue: job_instance.queue_name, # Assumes method exists
              is_terminal: job_instance.terminal?, # Assumes method exists
              retries: 0,
              created_at: current_time, # Set timestamps explicitly for insert_all
              updated_at: current_time
              # enqueued_at, started_at, finished_at default to NULL initially
            }
          end

          begin
            # Use insert_all for efficient bulk insertion. Bypasses AR callbacks/validations.
            JobRecord.insert_all(records_to_insert)
            true # Return true assuming insert_all raises on critical failure
          rescue ::ActiveRecord::RecordNotUnique => e
            # Should not happen if UUID generation is correct
            raise Yantra::Errors::PersistenceError, "Bulk job insert failed due to unique constraint (ID conflict?): #{e.message}"
          rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
            # Catch other potential DB or AR errors during bulk insert.
            raise Yantra::Errors::PersistenceError, "Bulk job insert failed: #{e.message}"
          end
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
          job.update(attributes_hash)
        end

        # Updates multiple jobs to the 'cancelled' state efficiently using update_all.
        # Only cancels jobs in cancellable states.
        # @param job_ids [Array<String>] An array of job UUIDs to cancel.
        # @return [Integer] The number of jobs actually updated/cancelled.
        def cancel_jobs_bulk(job_ids)
           return 0 if job_ids.nil? || job_ids.empty?

           cancellable_states = [
             Yantra::Core::StateMachine::PENDING.to_s,
             Yantra::Core::StateMachine::ENQUEUED.to_s,
             Yantra::Core::StateMachine::RUNNING.to_s
           ]

           # Use update_all to efficiently update matching jobs
           updated_count = JobRecord.where(id: job_ids, state: cancellable_states).update_all(
             state: Yantra::Core::StateMachine::CANCELLED.to_s,
             finished_at: Time.current # Set finished time on cancellation
             # updated_at is usually handled automatically by update_all if table has it
           )
           updated_count # Return the number of rows affected
        rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
           # Catch potential DB or AR errors during bulk update.
           raise Yantra::Errors::PersistenceError, "Bulk job cancellation failed: #{e.message}"
        end

        def running_job_count(workflow_id)
          JobRecord.where(workflow_id: workflow_id, state: 'running').count
        end

        def get_workflow_jobs(workflow_id, status: nil)
          scope = JobRecord.where(workflow_id: workflow_id)
          scope = scope.with_state(status) if status
          scope.to_a
        end

        def increment_job_retries(job_id)
          updated_count = JobRecord.where(id: job_id).update_all("retries = COALESCE(retries, 0) + 1")
          updated_count > 0
        end

        def record_job_output(job_id, output)
          updated_count = JobRecord.where(id: job_id).update_all(output: output)
          updated_count > 0
        end

        def record_job_error(job_id, error)
          error_data = { class: error.class.name, message: error.message, backtrace: error.backtrace&.first(10) }
          updated_count = JobRecord.where(id: job_id).update_all(error: error_data)
          updated_count > 0
        end

        def add_job_dependency(job_id, dependency_job_id)
          JobDependencyRecord.create!(job_id: job_id, depends_on_job_id: dependency_job_id)
          true
        rescue ::ActiveRecord::RecordInvalid, ::ActiveRecord::RecordNotUnique => e
           is_duplicate = e.is_a?(::ActiveRecord::RecordNotUnique) ||
                          (e.is_a?(::ActiveRecord::RecordInvalid) && e.record.errors.details.any? { |_field, errors| errors.any? { |err| err[:error] == :taken } })
           raise Yantra::Errors::PersistenceError, "Failed to add dependency: #{e.message}" unless is_duplicate
           true # Already exists
        end

        def add_job_dependencies_bulk(dependency_links_array)
          return true if dependency_links_array.nil? || dependency_links_array.empty?
          begin
            JobDependencyRecord.insert_all(dependency_links_array)
            true
          rescue ::ActiveRecord::RecordNotUnique => e
            raise Yantra::Errors::PersistenceError, "Bulk dependency insert failed due to unique constraint: #{e.message}. Check for duplicates or pre-existing links."
          rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
            raise Yantra::Errors::PersistenceError, "Bulk dependency insert failed: #{e.message}"
          end
        end

        def get_job_dependencies(job_id)
          record = JobRecord.find_by(id: job_id)
          record ? record.dependencies.pluck(:id) : []
        end

        def get_job_dependents(job_id)
          record = JobRecord.find_by(id: job_id)
          record ? record.dependents.pluck(:id) : []
        end

        def find_ready_jobs(workflow_id)
          ready_job_ids = []
          JobRecord.includes(:dependencies).where(workflow_id: workflow_id, state: 'pending').find_each do |job|
             next if job.dependencies.empty?
             all_deps_succeeded = job.dependencies.all? { |dep| dep.state == 'succeeded' }
             ready_job_ids << job.id if all_deps_succeeded
          end
          ready_job_ids
        end

        def list_workflows(status: nil, limit: 50, offset: 0)
          scope = WorkflowRecord.order(created_at: :desc).limit(limit).offset(offset)
          scope = scope.with_state(status) if status
          scope.to_a
        end

        def delete_workflow(workflow_id)
          workflow = WorkflowRecord.find_by(id: workflow_id)
          deleted = workflow&.destroy
          !deleted.nil?
        end

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

