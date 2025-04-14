# test/persistence/active_record/workflow_record_test.rb

# Require the main test helper which sets up AR, loads schema, etc.
require "test_helper"

# Explicitly require the ActiveRecord models needed for these tests.
# This ensures the constants are defined before the test class uses them.
# These requires will raise LoadError if run without AR dev dependencies,
# aligning with the decision to fail explicitly in that case.
require "yantra/persistence/active_record/workflow_record"
require "yantra/persistence/active_record/job_record"
# require "yantra/persistence/active_record/job_dependency_record" # Add if needed by tests in this file

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
          # Use the model class constant directly now that it's required.
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
          # Ensure JobRecord is defined (it should be due to require above)
          workflow = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "running")

          # Act & Assert
          assert_respond_to workflow, :job_records, "WorkflowRecord should respond to job_records"
          assert_empty workflow.job_records, "A new workflow should have no jobs initially"

          # Arrange: Add a job
          job = JobRecord.create!(id: SecureRandom.uuid, workflow_record: workflow, klass: "Job", state: "pending")

          # Assert: Association works (use reload to ensure data is fresh from DB)
          assert_includes workflow.job_records.reload, job, "Workflow should include the added job"
        end

        # Add more tests here for validations, associations, scopes etc.

      end
    end
  end
end

