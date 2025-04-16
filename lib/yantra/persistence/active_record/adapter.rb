# lib/yantra/persistence/active_record/adapter.rb

require_relative '../repository_interface'
require_relative '../../core/state_machine' # Needed for state constants
require_relative 'workflow_record' # Ensure models are required
require_relative 'job_record'
require_relative 'job_dependency_record'


module Yantra
  module Persistence
    module ActiveRecord
      class Adapter
        include Yantra::Persistence::RepositoryInterface

        # --- Workflow Methods ---
        # ... (find_workflow, persist_workflow, etc. remain the same) ...
        def find_workflow(workflow_id)
          WorkflowRecord.find_by(id: workflow_id)
        end

        def persist_workflow(workflow_instance)
          WorkflowRecord.create!(
            id: workflow_instance.id, klass: workflow_instance.klass.to_s,
            arguments: workflow_instance.arguments, state: 'pending',
            globals: workflow_instance.globals, has_failures: false
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
        # ... (find_job, persist_job, etc. remain the same) ...
        def find_job(job_id)
          JobRecord.find_by(id: job_id)
        end

        def persist_job(job_instance)
          JobRecord.create!(
            id: job_instance.id, workflow_id: job_instance.workflow_id,
            klass: job_instance.klass.to_s, arguments: job_instance.arguments,
            state: 'pending', queue: job_instance.queue_name,
            is_terminal: job_instance.terminal?, retries: 0
          )
          true
        rescue ::ActiveRecord::RecordInvalid => e
          raise Yantra::Errors::PersistenceError, "Failed to persist job: #{e.message}"
        end

        def persist_jobs_bulk(job_instances_array)
          return true if job_instances_array.nil? || job_instances_array.empty?
          current_time = Time.current
          records_to_insert = job_instances_array.map do |job_instance|
            { id: job_instance.id, workflow_id: job_instance.workflow_id,
              klass: job_instance.klass.to_s, arguments: job_instance.arguments,
              state: 'pending', queue: job_instance.queue_name,
              is_terminal: job_instance.terminal?, retries: 0,
              created_at: current_time, updated_at: current_time }
          end
          begin
            JobRecord.insert_all(records_to_insert)
            true
          rescue ::ActiveRecord::RecordNotUnique => e
            raise Yantra::Errors::PersistenceError, "Bulk job insert failed due to unique constraint (ID conflict?): #{e.message}"
          rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
            raise Yantra::Errors::PersistenceError, "Bulk job insert failed: #{e.message}"
          end
        end

        def update_job_attributes(job_id, attributes_hash, expected_old_state: nil)
          job = JobRecord.find_by(id: job_id)
          return false unless job
          if expected_old_state && job.state != expected_old_state.to_s
            return false
          end
          job.update(attributes_hash)
        end

        def running_job_count(workflow_id)
          JobRecord.where(workflow_id: workflow_id, state: 'running').count
        end

        # --- IMPLEMENTED: enqueued_job_count ---
        def enqueued_job_count(workflow_id)
          JobRecord.where(workflow_id: workflow_id, state: 'enqueued').count
        end
        # --- END IMPLEMENTED ---

        def get_workflow_jobs(workflow_id, status: nil)
          scope = JobRecord.where(workflow_id: workflow_id)
          scope = scope.with_state(status) if status
          scope.to_a
        end

        def increment_job_retries(job_id)
          # Use COALESCE for safety if retries column could be NULL
          updated_count = JobRecord.where(id: job_id).update_all("retries = COALESCE(retries, 0) + 1")
          updated_count > 0
        end

        def record_job_output(job_id, output)
          # Convert output to JSON string if your column type requires it
          # output_json = output.to_json
          updated_count = JobRecord.where(id: job_id).update_all(output: output)
          updated_count > 0
        end

        def record_job_error(job_id, error)
          # Format error object into hash for JSON storage
          error_data = { class: error.class.name, message: error.message, backtrace: error.backtrace&.first(10) }
          # Convert hash to JSON string if your column type requires it
          # error_json = error_data.to_json
          updated_count = JobRecord.where(id: job_id).update_all(error: error_data)
          updated_count > 0
        end

        # --- Dependency Methods ---
        # ... (add_job_dependency, etc. remain the same) ...
        def add_job_dependency(job_id, dependency_job_id)
          JobDependencyRecord.create!(job_id: job_id, depends_on_job_id: dependency_job_id)
          true
        rescue ::ActiveRecord::RecordInvalid, ::ActiveRecord::RecordNotUnique => e
           is_duplicate = e.is_a?(::ActiveRecord::RecordNotUnique) || (e.is_a?(::ActiveRecord::RecordInvalid) && e.record.errors.details.any? { |_field, errors| errors.any? { |err| err[:error] == :taken } })
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
            if job.dependencies.empty? || job.dependencies.all? { |dep| dep.state == 'succeeded' }
              ready_job_ids << job.id
            end
          end
          ready_job_ids
        end

        # --- Bulk Cancellation Method ---
        # ... (remains the same) ...
        def cancel_jobs_bulk(job_ids)
           return 0 if job_ids.nil? || job_ids.empty?
           cancellable_states = [
             Yantra::Core::StateMachine::PENDING.to_s,
             Yantra::Core::StateMachine::ENQUEUED.to_s,
             Yantra::Core::StateMachine::RUNNING.to_s # Should running be cancelled by this? Maybe not. Let's remove for now.
           ].freeze # Define as constant
           # Revisit: Should cancel_jobs_bulk cancel RUNNING jobs?
           # Let's assume NO for now, aligning with cancel_workflow logic.
           cancellable_states_query = [
             Yantra::Core::StateMachine::PENDING.to_s,
             Yantra::Core::StateMachine::ENQUEUED.to_s
           ]
           updated_count = JobRecord.where(id: job_ids, state: cancellable_states_query).update_all(
             state: Yantra::Core::StateMachine::CANCELLED.to_s,
             finished_at: Time.current # Or Time.now.utc
           )
           updated_count
        rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
           raise Yantra::Errors::PersistenceError, "Bulk job cancellation failed: #{e.message}"
        end

        # --- Listing/Cleanup Methods ---
        # ... (list_workflows, delete_workflow, delete_expired_workflows remain the same) ...
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

