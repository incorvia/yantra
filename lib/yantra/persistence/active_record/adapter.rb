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
          expected_state_str = expected_old_state&.to_s
          actual_state_str = workflow.state
          if expected_state_str && actual_state_str != expected_state_str
            Yantra.logger.warn { "[AR::Adapter] State mismatch for workflow #{workflow_id}. Expected: #{expected_state_str}, Actual: #{actual_state_str}" } if Yantra.logger
            return false
          end
          attributes_hash[:state] = attributes_hash[:state].to_s if attributes_hash.key?(:state)
          workflow.update(attributes_hash) # Returns true on success, false on failure
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
            klass_name = step_instance.klass.is_a?(Class) ? step_instance.klass.to_s : step_instance.klass.to_s
            { id: step_instance.id, workflow_id: step_instance.workflow_id, klass: klass_name,
              arguments: step_instance.arguments, state: 'pending', queue: step_instance.queue_name,
              retries: 0, created_at: current_time, updated_at: current_time }
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
          step_record = StepRecord.find_by(id: step_id)
          return false unless step_record
          expected_state_str = expected_old_state&.to_s
          actual_state_str = step_record.state
          if expected_state_str && actual_state_str != expected_state_str
            Yantra.logger.warn { "[AR::Adapter] State mismatch for step #{step_id}. Expected: #{expected_state_str}, Actual: #{actual_state_str}" } if Yantra.logger
            return false
          end
          attributes_hash[:state] = attributes_hash[:state].to_s if attributes_hash.key?(:state)
          step_record.update(attributes_hash) # Returns true on success, false on failure
        end


        def running_step_count(workflow_id)
          StepRecord.where(workflow_id: workflow_id, state: 'running').count
        end

        def enqueued_step_count(workflow_id)
          StepRecord.where(workflow_id: workflow_id, state: 'enqueued').count
        end

        def get_workflow_steps(workflow_id, status: nil)
          scope = StepRecord.where(workflow_id: workflow_id)
          scope = scope.where(state: status.to_s) if status
          scope.order(:created_at).to_a
        end

        def increment_step_retries(step_id)
          # Use Arel for atomic increment to avoid race conditions
          step_table = StepRecord.arel_table
          update_manager = ::Arel::UpdateManager.new
          update_manager.table(step_table)
          update_manager.set([[step_table[:retries], ::Arel::Nodes::Addition.new(step_table[:retries], 1)]])
          update_manager.where(step_table[:id].eq(step_id))
          # Execute the update and check rows affected
          updated_count = StepRecord.connection.update(update_manager)
          updated_count > 0
        rescue => e
          Yantra.logger.error { "[AR Adapter] Failed increment_step_retries for #{step_id}: #{e.message}" } if Yantra.logger
          false # Indicate failure
        end

        def record_step_output(step_id, output)
          step = StepRecord.find_by(id: step_id)
          return false unless step
          # Use standard update, which invokes model type casting (native or serialize)
          step.update(output: output) # Returns true on success, false on validation failure
        end

        # --- CORRECTED: Use find + update instead of update_all ---
        def record_step_error(step_id, error)
          step = StepRecord.find_by(id: step_id)
          # Return error hash if step not found, consistent with returning hash on success
          return { class: 'PersistenceError', message: "Step not found: #{step_id}" } unless step

          if error.is_a?(Exception)
            error_data = { class: error.class.name, message: error.message, backtrace: error.backtrace&.first(10) }
          elsif error.is_a?(Hash)
            # Ensure keys are consistent if hash is passed directly
            error_data = {
              class: error[:class] || error['class'],
              message: error[:message] || error['message'],
              backtrace: error[:backtrace] || error['backtrace']
            }.compact
          else
            error_data = { class: error.class.name, message: error.to_s }
          end

          # Use standard update, which invokes model type casting (native or serialize)
          update_success = step.update(error: error_data)
          # Log if update failed? Optional.
          # Yantra.logger.warn { "[AR Adapter] Failed to update error for step #{step_id}" } if !update_success && Yantra.logger

          # Return the formatted error data hash
          error_data
        rescue => e
          Yantra.logger.error { "[AR Adapter] Failed record_step_error for #{step_id}: #{e.message}" } if Yantra.logger
          { class: 'PersistenceError', message: "Failed to record original error: #{e.message}" }
        end



        # --- END UPDATE ---

        # --- Dependency Methods ---
        def add_step_dependency(step_id, dependency_step_id)
          StepDependencyRecord.create!(step_id: step_id, depends_on_step_id: dependency_step_id)
          true
        rescue ::ActiveRecord::RecordInvalid, ::ActiveRecord::RecordNotUnique => e
          is_duplicate = e.is_a?(::ActiveRecord::RecordNotUnique) || (e.is_a?(::ActiveRecord::RecordInvalid) && e.record.errors.details.any? { |_field, errors| errors.any? { |err| err[:error] == :taken } })
          raise Yantra::Errors::PersistenceError, "Failed to add dependency: #{e.message}" unless is_duplicate
          true
        end

        def add_step_dependencies_bulk(dependency_links_array)
          return true if dependency_links_array.nil? || dependency_links_array.empty?
          begin
            # Consider adding unique_by constraint if supported and needed for idempotency
            StepDependencyRecord.insert_all(dependency_links_array)
            true
          rescue ::ActiveRecord::RecordNotUnique
            Yantra.logger.info { "[AR::Adapter#add_step_dependencies_bulk] Some dependency links already existed (ignored)." } if Yantra.logger
            true
          rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
            raise Yantra::Errors::PersistenceError, "Bulk dependency insert failed: #{e.message}"
          end
        end

        def get_dependencies_ids(step_id)
          StepDependencyRecord.where(step_id: step_id).pluck(:depends_on_step_id)
        end

        def get_dependent_ids(step_id)
          StepDependencyRecord.where(depends_on_step_id: step_id).pluck(:step_id)
        end

        def get_dependencies_ids_bulk(step_ids)
          return {} if step_ids.nil? || step_ids.empty? || step_ids.all?(&:nil?)
          # Fetch pairs of [step_id, depends_on_step_id]
          links = StepDependencyRecord.where(step_id: step_ids).pluck(:step_id, :depends_on_step_id)
          # Group by step_id and map to get the array of parent IDs
          links.group_by(&:first).transform_values { |pairs| pairs.map(&:last) }
        rescue ::ActiveRecord::ActiveRecordError => e
          Yantra.logger.error { "[AR Adapter] Failed get_dependencies_ids_bulk for IDs #{step_ids.inspect}: #{e.message}" } if Yantra.logger
          {} # Return empty hash on error
        end

        def find_ready_steps(workflow_id)
          # Find IDs of all steps for the workflow
          all_step_ids = StepRecord.where(workflow_id: workflow_id).pluck(:id)
          return [] if all_step_ids.empty?

          # Find IDs of steps that have dependencies within this workflow
          steps_with_deps_ids = StepDependencyRecord.where(step_id: all_step_ids).pluck(:step_id).uniq

          # Find IDs of steps whose dependencies are NOT yet succeeded
          incomplete_deps = StepDependencyRecord
            .joins("INNER JOIN yantra_steps AS dependent_steps ON yantra_step_dependencies.step_id = dependent_steps.id")
            .joins("INNER JOIN yantra_steps AS prerequisite_steps ON yantra_step_dependencies.depends_on_step_id = prerequisite_steps.id")
            .where(dependent_steps: { workflow_id: workflow_id, state: 'pending' }) # Only check pending steps
            .where.not(prerequisite_steps: { state: 'succeeded' }) # Where parent is NOT succeeded
            .pluck(:step_id).uniq # Get IDs of pending steps with incomplete parents

          # Find all pending steps for the workflow
          pending_step_ids = StepRecord.where(workflow_id: workflow_id, state: 'pending').pluck(:id)

          # Ready steps are:
          # 1. Pending steps that have NO dependencies within the workflow
          # 2. Pending steps whose dependencies ARE ALL succeeded (i.e., not in incomplete_deps)
          ready_step_ids = pending_step_ids - incomplete_deps

          ready_step_ids
        end


        # --- Pipelining Methods ---
        def fetch_step_outputs(step_ids)
          return {} if step_ids.nil? || step_ids.empty?
          begin
            # Assuming 'output' column stores JSON or serialized data
            outputs = StepRecord.where(id: step_ids).pluck(:id, :output)
            # Attempt to parse JSON if output is string, handle potential errors
            outputs.to_h do |id, out_data|
              parsed_output = if out_data.is_a?(String)
                                begin; JSON.parse(out_data); rescue JSON::ParserError; out_data; end
                              else
                                out_data # Assume already parsed by AR or is simple type
                              end
              [id, parsed_output]
            end
          rescue ::ActiveRecord::ActiveRecordError => e
            raise Yantra::Errors::PersistenceError, "Failed to fetch step outputs: #{e.message}"
          end
        end


        # --- Bulk Cancellation Method ---
        def cancel_steps_bulk(step_ids)
          return 0 if step_ids.nil? || step_ids.empty?
          # Only cancel steps that are currently pending or enqueued
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
          scope = scope.where(state: status.to_s) if status
          scope.to_a
        end

        def delete_workflow(workflow_id)
          # Relies on `dependent: :destroy` on WorkflowRecord association or DB cascade
          workflow = WorkflowRecord.find_by(id: workflow_id)
          deleted = workflow&.destroy
          !deleted.nil? && deleted.destroyed?
        end

        def delete_expired_workflows(cutoff_timestamp)
          # Relies on `dependent: :destroy` or DB cascade
          deleted_workflows = WorkflowRecord.where("finished_at < ?", cutoff_timestamp)
          count = deleted_workflows.count
          deleted_workflows.destroy_all if count > 0
          count
        end

        def fetch_step_states(step_ids)
          return {} if step_ids.nil? || step_ids.empty? || step_ids.all?(&:nil?) # Handle empty/nil cases
          # Use pluck for efficiency, converting results to a hash
          StepRecord.where(id: step_ids).pluck(:id, :state).to_h
        rescue ::ActiveRecord::ActiveRecordError => e
          Yantra.logger.error { "[AR Adapter] Failed fetch_step_states for IDs #{step_ids.inspect}: #{e.message}" } if Yantra.logger
          {} # Return empty hash on error
        end

      end
    end
  end
end

