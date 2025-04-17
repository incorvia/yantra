# lib/yantra/persistence/active_record/adapter.rb

require_relative '../repository_interface'
require_relative '../../core/state_machine' # Needed for state constants
require_relative 'workflow_record' # Ensure models are required
require_relative 'step_record'
require_relative 'step_dependency_record'
require 'json' # For error formatting if needed


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
          # --- BEGIN DEBUG LOGGING ---
          puts "DEBUG: [AR::Adapter#update_workflow_attributes] ID: #{workflow_id}, Attrs: #{attributes_hash.inspect}, Expected State: #{expected_old_state.inspect}"
          # --- END DEBUG LOGGING ---
          workflow = WorkflowRecord.find_by(id: workflow_id)
          unless workflow
            puts "DEBUG: [AR::Adapter#update_workflow_attributes] Workflow not found." # DEBUG
            return false
          end

          # Convert expected_old_state symbol to string for comparison
          expected_state_str = expected_old_state&.to_s
          actual_state_str = workflow.state

          # --- BEGIN DEBUG LOGGING ---
          puts "DEBUG: [AR::Adapter#update_workflow_attributes] Actual State: #{actual_state_str.inspect}"
          # --- END DEBUG LOGGING ---

          if expected_state_str && actual_state_str != expected_state_str
            puts "WARN: [AR::Adapter] State mismatch for workflow #{workflow_id}. Expected: #{expected_state_str}, Actual: #{actual_state_str}" # DEBUG
            return false
          end

          # Ensure state in attributes_hash is also a string if passed
          attributes_hash[:state] = attributes_hash[:state].to_s if attributes_hash.key?(:state)

          update_result = workflow.update(attributes_hash)
          # --- BEGIN DEBUG LOGGING ---
          puts "DEBUG: [AR::Adapter#update_workflow_attributes] Update Result: #{update_result.inspect}"
          unless update_result
             puts "DEBUG: [AR::Adapter#update_workflow_attributes] Errors: #{workflow.errors.full_messages.inspect}" if workflow.errors.any?
          end
          # --- END DEBUG LOGGING ---
          update_result # Returns true on success, false on failure
        end

        def set_workflow_has_failures_flag(workflow_id)
          updated_count = WorkflowRecord.where(id: workflow_id).update_all(has_failures: true)
          updated_count > 0
        end

        def workflow_has_failures?(workflow_id)
          WorkflowRecord.where(id: workflow_id).pick(:has_failures) || false
        end

        # --- Step Methods ---
        def find_step(step_id)
          StepRecord.find_by(id: step_id)
        end

        def persist_step(step_instance)
          # Ensure klass is converted to string for persistence
          klass_name = step_instance.klass.is_a?(Class) ? step_instance.klass.to_s : step_instance.klass.to_s

          StepRecord.create!(
            id: step_instance.id, workflow_id: step_instance.workflow_id,
            klass: klass_name, arguments: step_instance.arguments,
            state: 'pending', queue: step_instance.queue_name,
            retries: 0
          )
          true
        rescue ::ActiveRecord::RecordInvalid => e
          raise Yantra::Errors::PersistenceError, "Failed to persist step: #{e.message}"
        end

        def persist_steps_bulk(step_instances_array)
          return true if step_instances_array.nil? || step_instances_array.empty?
          current_time = Time.current
          records_to_insert = step_instances_array.map do |step_instance|
             # Ensure klass is converted to string for persistence
             klass_name = step_instance.klass.is_a?(Class) ? step_instance.klass.to_s : step_instance.klass.to_s
            {
              id: step_instance.id,
              workflow_id: step_instance.workflow_id,
              klass: klass_name,
              arguments: step_instance.arguments,
              state: 'pending',
              queue: step_instance.queue_name,
              retries: 0,
              created_at: current_time,
              updated_at: current_time
            }
          end
          begin
            StepRecord.insert_all(records_to_insert)
            true
          rescue ::ActiveRecord::RecordNotUnique => e
            raise Yantra::Errors::PersistenceError, "Bulk job insert failed due to unique constraint (ID conflict?): #{e.message}"
          rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
            raise Yantra::Errors::PersistenceError, "Bulk job insert failed: #{e.message}"
          end
        end

        def update_step_attributes(step_id, attributes_hash, expected_old_state: nil)
          # --- BEGIN DEBUG LOGGING ---
          puts "DEBUG: [AR::Adapter#update_step_attributes] ID: #{step_id}, Attrs: #{attributes_hash.inspect}, Expected State: #{expected_old_state.inspect}"
          # --- END DEBUG LOGGING ---
          step_record = StepRecord.find_by(id: step_id) # Renamed variable
          unless step_record
            puts "DEBUG: [AR::Adapter#update_step_attributes] Step not found." # DEBUG
            return false
          end

          # Convert expected_old_state symbol to string for comparison
          expected_state_str = expected_old_state&.to_s
          actual_state_str = step_record.state

          # --- BEGIN DEBUG LOGGING ---
          puts "DEBUG: [AR::Adapter#update_step_attributes] Actual State: #{actual_state_str.inspect}"
          # --- END DEBUG LOGGING ---

          if expected_state_str && actual_state_str != expected_state_str
            puts "WARN: [AR::Adapter] State mismatch for step #{step_id}. Expected: #{expected_state_str}, Actual: #{actual_state_str}" # DEBUG
            return false
          end

          # Ensure state in attributes_hash is also a string if passed
          attributes_hash[:state] = attributes_hash[:state].to_s if attributes_hash.key?(:state)

          update_result = step_record.update(attributes_hash)
          # --- BEGIN DEBUG LOGGING ---
          puts "DEBUG: [AR::Adapter#update_step_attributes] Update Result: #{update_result.inspect}"
          unless update_result
             puts "DEBUG: [AR::Adapter#update_step_attributes] Errors: #{step_record.errors.full_messages.inspect}" if step_record.errors.any?
          end
          # --- END DEBUG LOGGING ---
          update_result # Returns true on success, false on failure
        end


        def running_step_count(workflow_id)
          StepRecord.where(workflow_id: workflow_id, state: 'running').count
        end

        def enqueued_step_count(workflow_id)
          StepRecord.where(workflow_id: workflow_id, state: 'enqueued').count
        end

        def get_workflow_steps(workflow_id, status: nil)
          scope = StepRecord.where(workflow_id: workflow_id)
          # Assuming StepRecord has a `with_state` scope or similar
          # If not, implement state filtering directly:
          scope = scope.where(state: status.to_s) if status
          scope.order(:created_at).to_a # Add ordering for consistency
        end

        def increment_step_retries(step_id)
          updated_count = StepRecord.where(id: step_id).update_all("retries = COALESCE(retries, 0) + 1")
          updated_count > 0
        end

        def record_step_output(step_id, output)
          # Consider JSON serialization if output is complex and column is text/jsonb
          # output_data = output.to_json rescue output # Basic fallback
          updated_count = StepRecord.where(id: step_id).update_all(output: output)
          updated_count > 0
        end

        def record_step_error(step_id, error)
          # Handle case where error might not be an Exception object
          if error.is_a?(Exception)
            error_data = { class: error.class.name, message: error.message, backtrace: error.backtrace&.first(10) }
          elsif error.is_a?(Hash) # Allow passing pre-formatted hash
            error_data = error.slice(:class, :message, :backtrace)
          else
            error_data = { class: error.class.name, message: error.to_s }
          end
          # Consider JSON serialization if column is text/jsonb
          # error_json = error_data.to_json rescue error_data.inspect
          updated_count = StepRecord.where(id: step_id).update_all(error: error_data)
          updated_count > 0
        end

        # --- Dependency Methods ---
        def add_step_dependency(step_id, dependency_step_id)
          StepDependencyRecord.create!(step_id: step_id, depends_on_step_id: dependency_step_id)
          true
        rescue ::ActiveRecord::RecordInvalid, ::ActiveRecord::RecordNotUnique => e
           is_duplicate = e.is_a?(::ActiveRecord::RecordNotUnique) || (e.is_a?(::ActiveRecord::RecordInvalid) && e.record.errors.details.any? { |_field, errors| errors.any? { |err| err[:error] == :taken } })
           # Only raise if it's not a duplicate constraint violation
           raise Yantra::Errors::PersistenceError, "Failed to add dependency: #{e.message}" unless is_duplicate
           true # Return true if it already exists (idempotent)
        end

        def add_step_dependencies_bulk(dependency_links_array)
          return true if dependency_links_array.nil? || dependency_links_array.empty?
          begin
            # Use ignore option for databases that support it (like PostgreSQL with ON CONFLICT DO NOTHING)
            # This makes the operation idempotent if links already exist.
            # Note: `insert_all` uniqueness option might vary slightly by Rails version.
            # For basic idempotency, rescuing RecordNotUnique might be needed if `ignore` isn't available/suitable.
            StepDependencyRecord.insert_all(dependency_links_array) # Add ", unique_by: [:step_id, :depends_on_step_id]" if needed and supported
            true
          rescue ::ActiveRecord::RecordNotUnique # Catch if unique constraint fails (means link existed)
             puts "INFO: [AR::Adapter#add_step_dependencies_bulk] Some dependency links already existed (ignored)."
             true # Treat as success if links already exist
          rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
            raise Yantra::Errors::PersistenceError, "Bulk dependency insert failed: #{e.message}"
          end
        end

        def get_step_dependencies(step_id)
          # Use StepRecord association if defined, otherwise query StepDependencyRecord
          record = StepRecord.find_by(id: step_id)
          # Assuming `has_many :dependency_records` association exists on StepRecord
          # And `belongs_to :dependency_step, class_name: 'StepRecord', foreign_key: 'depends_on_step_id'` on StepDependencyRecord
          # record ? record.dependency_records.pluck(:depends_on_step_id) : []
          # Fallback to direct query if associations aren't set up:
          StepDependencyRecord.where(step_id: step_id).pluck(:depends_on_step_id)
        end

        def get_step_dependents(step_id)
          # Use StepRecord association if defined, otherwise query StepDependencyRecord
          # record = StepRecord.find_by(id: step_id)
          # Assuming `has_many :dependent_records, class_name: 'StepDependencyRecord', foreign_key: 'depends_on_step_id'`
          # record ? record.dependent_records.pluck(:step_id) : []
          # Fallback to direct query:
          StepDependencyRecord.where(depends_on_step_id: step_id).pluck(:step_id)
        end

        def find_ready_steps(workflow_id)
          ready_step_ids = []
          # Find pending jobs and eagerly load their dependencies' states
          StepRecord.includes(:dependencies) # Assumes `has_many :dependencies, through: :dependency_records` etc.
                    .where(workflow_id: workflow_id, state: 'pending')
                    .find_each do |step_record|
            # Check if all dependencies (parents) are succeeded
            # The `step_record.dependencies` here should refer to the parent StepRecord objects
            if step_record.dependencies.empty? || step_record.dependencies.all? { |dep| dep.state == Yantra::Core::StateMachine::SUCCEEDED.to_s }
              ready_step_ids << step_record.id
            end
          end
          ready_step_ids
        end

        # --- Pipelining Methods ---
        def fetch_step_outputs(step_ids)
          return {} if step_ids.nil? || step_ids.empty?
          begin
            StepRecord.where(id: step_ids).pluck(:id, :output).to_h
          rescue ::ActiveRecord::ActiveRecordError => e
            raise Yantra::Errors::PersistenceError, "Failed to fetch step outputs: #{e.message}"
          end
        end


        # --- Bulk Cancellation Method ---
        def cancel_steps_bulk(step_ids)
           return 0 if step_ids.nil? || step_ids.empty?
           cancellable_states_query = [
             Yantra::Core::StateMachine::PENDING.to_s,
             Yantra::Core::StateMachine::ENQUEUED.to_s
           ]
           updated_count = StepRecord.where(id: step_ids, state: cancellable_states_query).update_all(
             state: Yantra::Core::StateMachine::CANCELLED.to_s,
             finished_at: Time.current
           )
           updated_count
        rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
           raise Yantra::Errors::PersistenceError, "Bulk job cancellation failed: #{e.message}"
        end

        # --- Listing/Cleanup Methods ---
        def list_workflows(status: nil, limit: 50, offset: 0)
          scope = WorkflowRecord.order(created_at: :desc).limit(limit).offset(offset)
          # scope = scope.with_state(status) if status # Assumes scope on WorkflowRecord
          scope = scope.where(state: status.to_s) if status
          scope.to_a
        end

        def delete_workflow(workflow_id)
          # Ensure dependent records (steps, dependencies) are handled by DB constraints or model callbacks
          workflow = WorkflowRecord.find_by(id: workflow_id)
          deleted = workflow&.destroy # Use destroy to trigger callbacks/associations
          !deleted.nil? && deleted.destroyed?
        end

        def delete_expired_workflows(cutoff_timestamp)
          # Assumes `dependent: :delete_all` or similar on WorkflowRecord associations,
          # or foreign keys with ON DELETE CASCADE in the database schema.
          deleted_workflows = WorkflowRecord.where("finished_at < ?", cutoff_timestamp)
          count = deleted_workflows.count # Get count before destroying
          deleted_workflows.destroy_all if count > 0 # Avoid query if nothing to delete
          count
        end

      end
    end
  end
end

