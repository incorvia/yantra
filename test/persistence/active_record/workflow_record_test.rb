# test/persistence/active_record/workflow_record_test.rb

# Require the main test helper which sets up AR, loads schema, etc.
require "test_helper"

# Explicitly require the ActiveRecord models needed for these tests.
# These requires will raise LoadError if run without AR dev dependencies,
# aligning with the decision to fail explicitly in that case.
require "yantra/persistence/active_record/workflow_record"
require "yantra/persistence/active_record/step_record"
# require "yantra/persistence/active_record/step_dependency_record" # Add if needed by tests in this file

module Yantra
  module Persistence
    module ActiveRecord
      # Test class for the WorkflowRecord model.
      # Inherits from the base class defined in test_helper.
      class WorkflowRecordTest < YantraActiveRecordTestCase # Ensure YantraActiveRecordTestCase is defined in test_helper

        # Basic test to ensure connection works and table exists.
        def test_database_connection_and_schema_loaded
          # Use ::ActiveRecord::Base to reference the top-level constant
          assert ::ActiveRecord::Base.connected?, "ActiveRecord should be connected"
          assert ::ActiveRecord::Base.connection.table_exists?(:yantra_workflows), "yantra_workflows table should exist"
          assert ::ActiveRecord::Base.connection.column_exists?(:yantra_workflows, :klass), "klass column should exist"
        end

        # "Hello World": Can we create and retrieve a record?
        def test_can_create_and_find_workflow
          # Arrange: Define attributes for a new workflow
          workflow_id = SecureRandom.uuid # Generate a UUID
          attributes = {
            id: workflow_id,
            klass: "MyHelloWorldWorkflow",
            state: "pending",
            has_failures: false
            # Add other non-null columns if your schema requires them
          }

          # Act: Create the record using the AR model directly.
          created_workflow = WorkflowRecord.create!(attributes)

          # Assert: Verify creation was successful
          refute_nil created_workflow, "WorkflowRecord should have been created"
          assert created_workflow.persisted?, "WorkflowRecord should be persisted"
          assert_equal workflow_id, created_workflow.id
          assert_equal "MyHelloWorldWorkflow", created_workflow.klass
          assert_equal "pending", created_workflow.state

          # Act: Find the record by its ID
          found_workflow = WorkflowRecord.find(workflow_id)

          # Assert: Verify finding was successful
          refute_nil found_workflow, "Should find the created workflow by ID"
          assert_equal workflow_id, found_workflow.id
          assert_equal "MyHelloWorldWorkflow", found_workflow.klass
        end

        # Test associations
        def test_workflow_has_jobs_association
          # Arrange
          # Ensure StepRecord is defined before using it
          skip "StepRecord model not defined" unless defined?(Yantra::Persistence::ActiveRecord::StepRecord)
          workflow = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "running")

          # Act & Assert
          assert_respond_to workflow, :step_records, "WorkflowRecord should respond to step_records"
          assert_empty workflow.step_records, "A new workflow should have no jobs initially"

          # Arrange: Add a job
          job = StepRecord.create!(id: SecureRandom.uuid, workflow_record: workflow, klass: "Job", state: "pending")

          # Assert: Association works (use reload to ensure data is fresh from DB)
          assert_includes workflow.step_records.reload, job, "Workflow should include the added job"
        end

        # --- Scope Tests ---
        # Verify that the defined scopes filter correctly.
        def test_state_scopes
          # Arrange: Create workflows with different states
          wf_running1 = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "running")
          wf_running2 = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "running")
          wf_pending = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "pending")
          wf_failed = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "failed")
          wf_succeeded = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "succeeded")

          # Assert: Check each scope
          assert_equal [wf_running1.id, wf_running2.id].sort, WorkflowRecord.running.pluck(:id).sort
          assert_equal [wf_failed.id], WorkflowRecord.failed.pluck(:id)
          assert_equal [wf_succeeded.id], WorkflowRecord.succeeded.pluck(:id)
          assert_equal [wf_pending.id], WorkflowRecord.pending.pluck(:id)
          assert_equal [wf_pending.id], WorkflowRecord.with_state(:pending).pluck(:id)
          assert_equal [wf_running1.id, wf_running2.id].sort, WorkflowRecord.with_state("running").pluck(:id).sort
        end

        # --- Validation Tests (Examples - Add if validations uncommented in model) ---

        # def test_validation_requires_klass
        #   # Assumes validates :klass, presence: true is active
        #   workflow = WorkflowRecord.new(id: SecureRandom.uuid, state: "pending")
        #   refute workflow.valid?, "Workflow should be invalid without klass"
        #   assert_includes workflow.errors[:klass], "can't be blank"
        # end

        # def test_validation_requires_valid_state
        #   # Assumes validates :state, inclusion: { in: %w[...] } is active
        #   workflow = WorkflowRecord.new(id: SecureRandom.uuid, klass: "Wf", state: "invalid_state")
        #   refute workflow.valid?, "Workflow should be invalid with invalid state"
        #   assert_includes workflow.errors[:state], "is not a valid state"
        # end

        # Add more tests here for other validations, associations, scopes etc.

      end
    end
  end
end

