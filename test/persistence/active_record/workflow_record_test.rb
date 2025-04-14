# test/persistence/active_record/workflow_record_test.rb

# Require the main test helper which sets up AR, loads schema, etc.
require "test_helper"

# Conditionally define tests only if ActiveRecord and the necessary base class
# were successfully loaded in the test_helper. This prevents load errors if
# AR gems are missing when running the full suite.
if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Persistence::ActiveRecord::WorkflowRecord)

  module Yantra
    module Persistence
      module ActiveRecord
        # Test class for the WorkflowRecord model.
        # Inherits from the base class defined in test_helper to get
        # the automatic skipping logic if AR is not loaded.
        class WorkflowRecordTest < YantraActiveRecordTestCase

          # Basic test to ensure connection works and table exists.
          def test_database_connection_and_schema_loaded
            # Checks if the connection is active and the table exists.
            assert ActiveRecord::Base.connected?, "ActiveRecord should be connected"
            assert ActiveRecord::Base.connection.table_exists?(:yantra_workflows), "yantra_workflows table should exist"
            # You could also check for specific columns if needed:
            # assert ActiveRecord::Base.connection.column_exists?(:yantra_workflows, :klass), "klass column should exist"
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

            # Act: Create the record using the AR model directly
            created_workflow = nil
            # assert_nothing_raised ensures no exceptions happen during creation
            assert_nothing_raised do
              created_workflow = WorkflowRecord.create!(attributes)
            end

            # Assert: Verify creation was successful
            refute_nil created_workflow, "WorkflowRecord should have been created"
            assert created_workflow.persisted?, "WorkflowRecord should be persisted"
            assert_equal workflow_id, created_workflow.id
            assert_equal "MyHelloWorldWorkflow", created_workflow.klass
            assert_equal "pending", created_workflow.state

            # Act: Find the record by its ID
            found_workflow = nil
            assert_nothing_raised do
              found_workflow = WorkflowRecord.find(workflow_id)
            end

            # Assert: Verify finding was successful
            refute_nil found_workflow, "Should find the created workflow by ID"
            assert_equal workflow_id, found_workflow.id
            assert_equal "MyHelloWorldWorkflow", found_workflow.klass
          end

          # Add more tests here for validations, associations, scopes etc.
          # For example:
          # def test_workflow_requires_klass
          #   workflow = WorkflowRecord.new(state: 'pending', id: SecureRandom.uuid)
          #   refute workflow.valid?, "Workflow should be invalid without klass"
          #   assert_includes workflow.errors[:klass], "can't be blank"
          # end

        end
      end
    end
  end

else
  # Optional: Define a placeholder test that just skips if AR isn't loaded,
  # preventing "no tests found" errors if this is the only file.
  class WorkflowRecordPlaceholderTest < Minitest::Test
    def test_ar_workflow_tests_skipped
      skip "Skipping WorkflowRecord tests because ActiveRecord is not available."
    end
  end
end

