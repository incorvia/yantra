# lib/yantra/persistence/active_record/adapter.rb

require_relative '../repository_interface'
require_relative '../../core/state_machine' # Needed for state constants
require_relative 'workflow_record'
require_relative 'step_record'
require_relative 'step_dependency_record'
require 'json' # For error/output formatting if needed
require 'active_record' # Ensure ActiveRecord is loaded

module Yantra
  module Persistence
    module ActiveRecord
      # ActiveRecord adapter implementing the RepositoryInterface.
      # Provides persistence logic for workflows, steps, and dependencies
      # using ActiveRecord models mapped to database tables.
      class Adapter
        include Yantra::Persistence::RepositoryInterface

        # ========================================================
        # Workflow Methods
        # ========================================================

        # @see Yantra::Persistence::RepositoryInterface#find_workflow
        def find_workflow(workflow_id)
          WorkflowRecord.find_by(id: workflow_id)
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error finding workflow #{workflow_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error finding workflow: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#create_workflow
        def create_workflow(workflow_instance)
          WorkflowRecord.create!(
            id: workflow_instance.id,
            klass: workflow_instance.klass.to_s,
            arguments: workflow_instance.arguments,
            state: Yantra::Core::StateMachine::PENDING.to_s,
            globals: workflow_instance.globals,
            has_failures: false
          )
          true
        rescue ::ActiveRecord::RecordInvalid, ::ActiveRecord::RecordNotUnique => e
          log_error { "Failed to persist workflow #{workflow_instance.id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Failed to persist workflow: #{e.message}"
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Database error persisting workflow #{workflow_instance.id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Database error persisting workflow: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#update_workflow_attributes
        def update_workflow_attributes(workflow_id, attributes_hash, expected_old_state: nil)
          workflow = WorkflowRecord.find_by(id: workflow_id)
          return false unless workflow
          if expected_old_state
            expected_state_str = expected_old_state.to_s
            actual_state_str = workflow.state
            if actual_state_str != expected_state_str
              log_warn { "[AR::Adapter] State mismatch for workflow #{workflow_id}. Expected: #{expected_state_str}, Actual: #{actual_state_str}" }
              return false
            end
          end
          update_attrs = attributes_hash.dup
          if update_attrs.key?(:state) && update_attrs[:state].is_a?(Symbol)
             update_attrs[:state] = update_attrs[:state].to_s
          end
          workflow.update(update_attrs)
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error updating workflow attributes for #{workflow_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error updating workflow: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#workflow_has_failures?
        def workflow_has_failures?(workflow_id)
          WorkflowRecord.where(id: workflow_id).pick(:has_failures) || false
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error checking failure status for workflow #{workflow_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error checking workflow failure status: #{e.message}"
        end

        # ========================================================
        # Step Methods
        # ========================================================

        # @see Yantra::Persistence::RepositoryInterface#find_step
        def find_step(step_id)
          StepRecord.find_by(id: step_id)
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error finding step #{step_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error finding step: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#find_steps
        def find_steps(step_ids, state: nil)
           return [] if step_ids.nil? || step_ids.empty?
          unique_ids = step_ids.uniq
          query = StepRecord.where(id: unique_ids)
          if state
             query = query.where(state: state.to_s)
          end
          query.to_a
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error finding steps #{unique_ids.inspect}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Finding steps failed: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#persist_step
        def persist_step(step_instance)
          klass_name = step_instance.klass.is_a?(Class) ? step_instance.klass.to_s : step_instance.klass.to_s
          StepRecord.create!(
            id: step_instance.id,
            workflow_id: step_instance.workflow_id,
            klass: klass_name,
            arguments: step_instance.arguments,
            state: Yantra::Core::StateMachine::PENDING.to_s,
            queue: step_instance.queue_name,
            retries: 0
          )
          true
        rescue ::ActiveRecord::RecordInvalid, ::ActiveRecord::RecordNotUnique => e
          log_error { "Failed to persist step #{step_instance.id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Failed to persist step: #{e.message}"
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Database error persisting step #{step_instance.id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Database error persisting step: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#persist_steps_bulk
        def persist_steps_bulk(step_instances_array)
          return true if step_instances_array.nil? || step_instances_array.empty?
          current_time = Time.current
          records_to_insert = step_instances_array.map do |step|
            {
              id: step.id,
              workflow_id: step.workflow_id,
              klass: step.klass.is_a?(Class) ? step.klass.to_s : step.klass.to_s,
              arguments: step.arguments,
              state: Yantra::Core::StateMachine::PENDING.to_s,
              queue: step.queue_name,
              retries: 0,
              created_at: current_time,
              updated_at: current_time
            }
          end
          StepRecord.insert_all!(records_to_insert)
          true
        rescue ::ActiveRecord::RecordNotUnique => e
          log_error { "Bulk step insert failed due to unique constraint: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Bulk step insert failed due to unique constraint (ID conflict?): #{e.message}"
        rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
          log_error { "Bulk step insert failed: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Bulk step insert failed: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#update_step_attributes
        def update_step_attributes(step_id, attributes_hash, expected_old_state: nil)
          step_record = StepRecord.find_by(id: step_id)
          return false unless step_record
          if expected_old_state
            expected_state_str = expected_old_state.to_s
            actual_state_str = step_record.state
            if actual_state_str != expected_state_str
              log_warn { "[AR::Adapter] State mismatch for step #{step_id}. Expected: #{expected_state_str}, Actual: #{actual_state_str}" }
              return false
            end
          end
          update_attrs = attributes_hash.dup
          if update_attrs.key?(:state) && update_attrs[:state].is_a?(Symbol)
            update_attrs[:state] = update_attrs[:state].to_s
          end
          step_record.update(update_attrs)
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error updating step attributes for #{step_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error updating step: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#get_workflow_steps
        def get_workflow_steps(workflow_id, status: nil)
          scope = StepRecord.where(workflow_id: workflow_id)
          scope = scope.where(state: status.to_s) if status
          scope.order(:created_at).to_a
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error getting steps for workflow #{workflow_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error getting workflow steps: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#increment_step_retries
        def increment_step_retries(step_id)
          updated_count = StepRecord.update_counters(step_id, retries: 1)
          updated_count > 0
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "[AR Adapter] Failed increment_step_retries for #{step_id}: #{e.message}" }
          false
        end

        # @see Yantra::Persistence::RepositoryInterface#record_step_output
        def record_step_output(step_id, output)
          step = StepRecord.find_by(id: step_id)
          return false unless step
          step.update(output: output)
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error recording output for step #{step_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error recording step output: #{e.message}"
        end

        # <<< CHANGED: Reverted record_step_error to original logic >>>
        # @see Yantra::Persistence::RepositoryInterface#record_step_error
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
          log_warn { "[AR Adapter] Failed to update error for step #{step_id}" } if !update_success && Yantra.logger

          # Return the formatted error data hash (Original Behavior)
          error_data
        rescue => e # Rescue StandardError as original likely did
          log_error { "[AR Adapter] Failed record_step_error for #{step_id}: #{e.message}" }
          # Return error hash on failure (Original Behavior)
          { class: 'PersistenceError', message: "Failed to record original error: #{e.message}" }
        end
        # <<< END CHANGED >>>

        # @see Yantra::Persistence::RepositoryInterface#fetch_step_states
        def fetch_step_states(step_ids)
          return {} if step_ids.nil? || step_ids.empty? || step_ids.all?(&:nil?)
          unique_ids = step_ids.uniq
          StepRecord.where(id: unique_ids).pluck(:id, :state).to_h
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "[AR Adapter] Failed fetch_step_states for IDs #{unique_ids.inspect}: #{e.message}" }
          {}
        end

        # @see Yantra::Persistence::RepositoryInterface#running_step_count
        def running_step_count(workflow_id)
          StepRecord.where(workflow_id: workflow_id, state: Yantra::Core::StateMachine::RUNNING.to_s).count
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error counting running steps for workflow #{workflow_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error counting running steps: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#enqueued_step_count
        def enqueued_step_count(workflow_id)
          StepRecord.where(workflow_id: workflow_id, state: Yantra::Core::StateMachine::ENQUEUED.to_s).count
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error counting enqueued steps for workflow #{workflow_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error counting enqueued steps: #{e.message}"
        end

        # ========================================================
        # Dependency Methods
        # ========================================================

        # @see Yantra::Persistence::RepositoryInterface#add_step_dependency
        def add_step_dependency(step_id, dependency_step_id)
          # ... (Implementation as before) ...
          StepDependencyRecord.create!(step_id: step_id, depends_on_step_id: dependency_step_id)
          true
        rescue ::ActiveRecord::RecordInvalid, ::ActiveRecord::RecordNotUnique => e
          is_duplicate = e.is_a?(::ActiveRecord::RecordNotUnique) ||
                         (e.is_a?(::ActiveRecord::RecordInvalid) && e.record.errors.details.any? { |_field, errors| errors.any? { |err| err[:error] == :taken } })
          raise Yantra::Errors::PersistenceError, "Failed to add dependency: #{e.message}" unless is_duplicate
          true
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Database error adding dependency #{dependency_step_id} -> #{step_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Database error adding dependency: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#add_step_dependencies_bulk
        def add_step_dependencies_bulk(dependency_links_array)
          # ... (Implementation as before) ...
          return true if dependency_links_array.nil? || dependency_links_array.empty?
          StepDependencyRecord.insert_all!(dependency_links_array)
          true
        rescue ::ActiveRecord::RecordNotUnique
          log_info { "[AR::Adapter#add_step_dependencies_bulk] Some dependency links already existed (ignored)." }
          true
        rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
          log_error { "Bulk dependency insert failed: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Bulk dependency insert failed: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#find_ready_steps
        def find_ready_steps(workflow_id)
          all_step_ids = StepRecord.where(workflow_id: workflow_id).pluck(:id)
          return [] if all_step_ids.empty?

          incomplete_deps = StepDependencyRecord
            .joins("INNER JOIN yantra_steps AS prerequisites ON prerequisites.id = yantra_step_dependencies.depends_on_step_id")
            .where(step_id: all_step_ids)
          # Corrected: Use SQL string condition for the aliased table
            .where.not("prerequisites.state = ?", Yantra::Core::StateMachine::SUCCEEDED.to_s)
            .pluck(:step_id).uniq

          pending_step_ids = StepRecord.where(workflow_id: workflow_id, state: Yantra::Core::StateMachine::PENDING.to_s).pluck(:id)
          ready_step_ids = pending_step_ids - incomplete_deps
          ready_step_ids
        rescue ::ActiveRecord::ActiveRecordError => e
          # Use Yantra.logger for consistency if defined, otherwise fallback
          logger = defined?(Yantra.logger) && Yantra.logger ? Yantra.logger : Logger.new(STDOUT)
          logger.error { "[Persistence] Error finding ready steps for workflow #{workflow_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error finding ready steps: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#get_dependencies_ids
        def get_dependencies_ids(step_id)
          # ... (Implementation as before) ...
          StepDependencyRecord.where(step_id: step_id).pluck(:depends_on_step_id)
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error getting dependencies for step #{step_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error getting dependencies: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#get_dependencies_ids_bulk
        def get_dependencies_ids_bulk(step_ids)
          # ... (Implementation as before) ...
          return {} if step_ids.nil? || step_ids.empty? || step_ids.all?(&:nil?)
          unique_ids = step_ids.uniq
          links = StepDependencyRecord.where(step_id: unique_ids).pluck(:step_id, :depends_on_step_id)
          dependencies_map = links.group_by(&:first).transform_values { |pairs| pairs.map(&:last) }
          unique_ids.each { |id| dependencies_map[id] ||= [] }
          dependencies_map
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "[AR Adapter] Failed get_dependencies_ids_bulk for IDs #{unique_ids.inspect}: #{e.message}" }
          {}
        end

        # @see Yantra::Persistence::RepositoryInterface#get_dependent_ids
        def get_dependent_ids(step_id)
          # ... (Implementation as before) ...
          StepDependencyRecord.where(depends_on_step_id: step_id).pluck(:step_id)
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error getting dependents for step #{step_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error getting dependents: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#get_dependent_ids_bulk
        def get_dependent_ids_bulk(step_ids)
          # ... (Implementation as before) ...
          return {} if step_ids.nil? || step_ids.empty? || step_ids.all?(&:nil?)
          unique_ids = step_ids.uniq
          links = StepDependencyRecord
                    .where(depends_on_step_id: unique_ids)
                    .select(:step_id, :depends_on_step_id)
          dependents_map = links.group_by(&:depends_on_step_id).transform_values do |records|
            records.map(&:step_id)
          end
          unique_ids.each { |id| dependents_map[id] ||= [] }
          dependents_map
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error finding bulk dependents for steps #{unique_ids.inspect}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Finding bulk dependents failed: #{e.message}"
        end

        # ========================================================
        # Bulk Operations / Cleanup / Other Methods
        # ========================================================

        # @see Yantra::Persistence::RepositoryInterface#bulk_update_steps
        def bulk_update_steps(step_ids, attributes)
          # ... (Implementation as before) ...
          return true if step_ids.nil? || step_ids.empty?
          return true if attributes.nil? || attributes.empty?
          update_attrs = attributes.dup
          if update_attrs.key?(:state) && update_attrs[:state].is_a?(Symbol)
            update_attrs[:state] = update_attrs[:state].to_s
          end
          update_attrs[:updated_at] = Time.current unless update_attrs.key?(:updated_at)
          StepRecord.where(id: step_ids).update_all(update_attrs)
        rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
          log_error { "Bulk step update failed for IDs #{step_ids.inspect}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Bulk step update failed: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#cancel_steps_bulk
        def cancel_steps_bulk(step_ids)
          # ... (Implementation as before) ...
          return 0 if step_ids.nil? || step_ids.empty?
          cancellable_states = [
            Yantra::Core::StateMachine::PENDING.to_s,
            Yantra::Core::StateMachine::ENQUEUED.to_s
          ]
          updated_count = StepRecord.where(id: step_ids, state: cancellable_states).update_all(
            state: Yantra::Core::StateMachine::CANCELLED.to_s,
            finished_at: Time.current,
            updated_at: Time.current
          )
          updated_count
        rescue ::ActiveRecord::StatementInvalid, ::ActiveRecord::ActiveRecordError => e
          log_error { "Bulk step cancellation failed for IDs #{step_ids.inspect}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Bulk job cancellation failed: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#delete_workflow
        def delete_workflow(workflow_id)
          # ... (Implementation as before) ...
          workflow = WorkflowRecord.find_by(id: workflow_id)
          deleted_record = workflow&.destroy
          !deleted_record.nil? && deleted_record.destroyed?
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error deleting workflow #{workflow_id}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error deleting workflow: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#delete_expired_workflows
        def delete_expired_workflows(cutoff_timestamp)
          # ... (Implementation as before) ...
          deleted_count = WorkflowRecord.where("finished_at < ?", cutoff_timestamp).delete_all
          deleted_count
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error deleting expired workflows older than #{cutoff_timestamp}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error deleting expired workflows: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#fetch_step_outputs
        def fetch_step_outputs(step_ids)
          # ... (Implementation as before) ...
          return {} if step_ids.nil? || step_ids.empty? || step_ids.all?(&:nil?)
          unique_ids = step_ids.uniq
          outputs = StepRecord.where(id: unique_ids).pluck(:id, :output)
          outputs.to_h
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Failed to fetch step outputs for IDs #{unique_ids.inspect}: #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Failed to fetch step outputs: #{e.message}"
        end

        # @see Yantra::Persistence::RepositoryInterface#list_workflows
        def list_workflows(status: nil, limit: 50, offset: 0)
          # ... (Implementation as before) ...
          scope = WorkflowRecord.order(created_at: :desc).limit(limit).offset(offset)
          scope = scope.where(state: status.to_s) if status
          scope.to_a
        rescue ::ActiveRecord::ActiveRecordError => e
          log_error { "Error listing workflows (status: #{status}, limit: #{limit}, offset: #{offset}): #{e.message}" }
          raise Yantra::Errors::PersistenceError, "Error listing workflows: #{e.message}"
        end

        private

        # Simple logging helpers (use block form for potential performance)
        def log_info(&block);  Yantra.logger&.info("[AR::Adapter]", &block) end
        def log_warn(&block);  Yantra.logger&.warn("[AR::Adapter]", &block) end
        def log_error(&block); Yantra.logger&.error("[AR::Adapter]", &block) end
        def log_debug(&block); Yantra.logger&.debug("[AR::Adapter]", &block) end

      end # class Adapter
    end # module ActiveRecord
  end # module Persistence
end # module Yantra

