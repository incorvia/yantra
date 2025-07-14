# test/parent_child_workflow_test.rb

require "test_helper"

# Explicitly require the files needed for these tests
if AR_LOADED # Only require if AR itself was loaded successfully
  require "yantra/client"
  require "yantra/workflow"
  require "yantra/step"
  require "yantra/persistence/active_record/workflow_record"
  require "yantra/persistence/active_record/step_record"
  require "yantra/persistence/active_record/step_dependency_record"
end

# --- Define Dummy Classes for Testing ---

# A simple step for testing child workflow creation
class ChildWorkflowTestStep < Yantra::Step
  def perform(data: nil)
    # Test steps don't need complex logic, just need to exist
  end
end

# A simple workflow for testing child workflow creation
class ChildWorkflowTestWorkflow < Yantra::Workflow
  def perform(user_id:, report_type: 'default')
    run ChildWorkflowTestStep, name: :fetch_data, params: { data: user_id }
    run ChildWorkflowTestStep, name: :process_data, params: { data: report_type }, after: :fetch_data
  end
end

# A simple workflow for testing child workflows
class ChildWorkflowTestChildWorkflow < Yantra::Workflow
  def perform(order_id:, sku:)
    run ChildWorkflowTestStep, name: :process_order, params: { order_id: order_id, sku: sku }
  end
end

# --- Parent-Child Workflow Tests ---

module Yantra
  # Check if base class and models were loaded before defining tests
  if defined?(YantraActiveRecordTestCase) && AR_LOADED

    class ParentChildWorkflowTest < YantraActiveRecordTestCase

      # Ensure the correct adapter is configured before each test in this file
      def setup
        super # Run base class setup (skip checks, DB cleaning)
        Yantra.configure { |c| c.persistence_adapter = :active_record }
        # Reset repository memoization to ensure the correct adapter is used
        Yantra.instance_variable_set(:@repository, nil)
      end

      def test_create_child_workflow_persists_with_parent_relationship
        # Arrange
        parent_workflow_id = Client.create_workflow(ChildWorkflowTestWorkflow, user_id: 123, report_type: 'summary')
        child_idempotency_key = "meal-short-456-ABC123"

        # Act
        child_workflow_id = Client.create_workflow(
          ChildWorkflowTestChildWorkflow,
          parent_workflow_id: parent_workflow_id,
          idempotency_key: child_idempotency_key,
          order_id: 456,
          sku: "ABC123"
        )

        # Assert: Child workflow was created correctly
        assert child_workflow_id.is_a?(String) && !child_workflow_id.empty?, "Should return a child workflow ID string"
        
        child_wf_record = Persistence::ActiveRecord::WorkflowRecord.find_by(id: child_workflow_id)
        refute_nil child_wf_record, "Child WorkflowRecord should be created in DB"
        assert_equal "ChildWorkflowTestChildWorkflow", child_wf_record.klass
        assert_equal "pending", child_wf_record.state
        assert_equal parent_workflow_id, child_wf_record.parent_workflow_id
        assert_equal child_idempotency_key, child_wf_record.idempotency_key

        # Assert: Parent workflow relationship is correct
        parent_wf_record = Persistence::ActiveRecord::WorkflowRecord.find_by(id: parent_workflow_id)
        refute_nil parent_wf_record, "Parent WorkflowRecord should exist"
        assert_equal 1, parent_wf_record.child_workflows.count, "Parent should have one child workflow"
        assert_equal child_workflow_id, parent_wf_record.child_workflows.first.id

        # Assert: Child workflow steps were created
        child_step_records = Persistence::ActiveRecord::StepRecord.where(workflow_id: child_workflow_id)
        assert_equal 1, child_step_records.count, "Child workflow should have one step"
        assert_equal "ChildWorkflowTestStep", child_step_records.first.klass
      end

      def test_create_child_workflow_without_idempotency_key
        # Arrange
        parent_workflow_id = Client.create_workflow(ChildWorkflowTestWorkflow, user_id: 123)

        # Act
        child_workflow_id = Client.create_workflow(
          ChildWorkflowTestChildWorkflow,
          parent_workflow_id: parent_workflow_id,
          order_id: 456,
          sku: "ABC123"
        )

        # Assert
        child_wf_record = Persistence::ActiveRecord::WorkflowRecord.find_by(id: child_workflow_id)
        refute_nil child_wf_record
        assert_equal parent_workflow_id, child_wf_record.parent_workflow_id
        assert_nil child_wf_record.idempotency_key, "Idempotency key should be nil when not provided"
      end

      def test_create_child_workflow_raises_error_for_invalid_parent_id
        # Act & Assert
        error = assert_raises(ArgumentError) do
          Client.create_workflow(
            ChildWorkflowTestChildWorkflow,
            parent_workflow_id: "", # Empty parent ID
            order_id: 456,
            sku: "ABC123"
          )
        end
        assert_match(/parent_workflow_id must be a non-empty string/, error.message)

        error = assert_raises(ArgumentError) do
          Client.create_workflow(
            ChildWorkflowTestChildWorkflow,
            parent_workflow_id: nil, # Nil parent ID
            order_id: 456,
            sku: "ABC123"
          )
        end
        assert_match(/parent_workflow_id must be a non-empty string/, error.message)
      end

      def test_create_child_workflow_raises_error_for_invalid_workflow_class
        # Arrange
        parent_workflow_id = Client.create_workflow(ChildWorkflowTestWorkflow, user_id: 123)

        # Act & Assert
        error = assert_raises(ArgumentError) do
          Client.create_workflow(
            String, # Invalid class
            parent_workflow_id: parent_workflow_id,
            order_id: 456,
            sku: "ABC123"
          )
        end
        assert_match(/must be a subclass of Yantra::Workflow/, error.message)
      end

      def test_find_child_workflows_returns_all_children
        # Arrange
        parent_workflow_id = Client.create_workflow(ChildWorkflowTestWorkflow, user_id: 123)
        
        # Create multiple child workflows
        child1_id = Client.create_workflow(
          ChildWorkflowTestChildWorkflow,
          parent_workflow_id: parent_workflow_id,
          idempotency_key: "key1",
          order_id: 1,
          sku: "SKU1"
        )
        
        child2_id = Client.create_workflow(
          ChildWorkflowTestChildWorkflow,
          parent_workflow_id: parent_workflow_id,
          idempotency_key: "key2",
          order_id: 2,
          sku: "SKU2"
        )

        # Act
        child_workflows = Client.find_child_workflows(parent_workflow_id)

        # Assert
        assert_equal 2, child_workflows.count, "Should find 2 child workflows"
        child_ids = child_workflows.map(&:id)
        assert_includes child_ids, child1_id
        assert_includes child_ids, child2_id
        assert_equal parent_workflow_id, child_workflows.first.parent_workflow_id
        assert_equal parent_workflow_id, child_workflows.last.parent_workflow_id
      end

      def test_find_child_workflows_returns_empty_array_for_nonexistent_parent
        # Act
        child_workflows = Client.find_child_workflows("nonexistent-parent-id")

        # Assert
        assert_equal [], child_workflows, "Should return empty array for nonexistent parent"
      end

      def test_find_existing_idempotency_keys_returns_existing_keys
        # Arrange
        parent_workflow_id = Client.create_workflow(ChildWorkflowTestWorkflow, user_id: 123)
        
        # Create child workflows with specific idempotency keys
        Client.create_workflow(
          ChildWorkflowTestChildWorkflow,
          parent_workflow_id: parent_workflow_id,
          idempotency_key: "existing-key-1",
          order_id: 1,
          sku: "SKU1"
        )
        
        Client.create_workflow(
          ChildWorkflowTestChildWorkflow,
          parent_workflow_id: parent_workflow_id,
          idempotency_key: "existing-key-2",
          order_id: 2,
          sku: "SKU2"
        )

        # Act
        potential_keys = ["existing-key-1", "new-key-1", "existing-key-2", "new-key-2"]
        existing_keys = Client.find_existing_idempotency_keys(parent_workflow_id, potential_keys)

        # Assert
        assert_equal 2, existing_keys.count, "Should find 2 existing keys"
        assert_includes existing_keys, "existing-key-1"
        assert_includes existing_keys, "existing-key-2"
        refute_includes existing_keys, "new-key-1"
        refute_includes existing_keys, "new-key-2"
      end

      def test_find_existing_idempotency_keys_returns_empty_array_for_nonexistent_parent
        # Act
        existing_keys = Client.find_existing_idempotency_keys("nonexistent-parent-id", ["key1", "key2"])

        # Assert
        assert_equal [], existing_keys, "Should return empty array for nonexistent parent"
      end

      def test_find_existing_idempotency_keys_returns_empty_array_for_empty_potential_keys
        # Arrange
        parent_workflow_id = Client.create_workflow(ChildWorkflowTestWorkflow, user_id: 123)

        # Act
        existing_keys = Client.find_existing_idempotency_keys(parent_workflow_id, [])

        # Assert
        assert_equal [], existing_keys, "Should return empty array for empty potential keys"
      end

      def test_idempotency_key_uniqueness_constraint
        # Arrange
        parent_workflow_id = Client.create_workflow(ChildWorkflowTestWorkflow, user_id: 123)
        idempotency_key = "unique-key-123"

        # Create first child workflow with the key
        Client.create_workflow(
          ChildWorkflowTestChildWorkflow,
          parent_workflow_id: parent_workflow_id,
          idempotency_key: idempotency_key,
          order_id: 1,
          sku: "SKU1"
        )

        # Act & Assert: Try to create another child workflow with the same key
        error = assert_raises(Yantra::Errors::PersistenceError) do
          Client.create_workflow(
            ChildWorkflowTestChildWorkflow,
            parent_workflow_id: parent_workflow_id,
            idempotency_key: idempotency_key, # Same key
            order_id: 2,
            sku: "SKU2"
          )
        end
        assert_match(/Failed to persist/, error.message)
      end

      def test_parent_child_relationship_through_active_record_associations
        # Arrange
        parent_workflow_id = Client.create_workflow(ChildWorkflowTestWorkflow, user_id: 123)
        
        child_workflow_id = Client.create_workflow(
          ChildWorkflowTestChildWorkflow,
          parent_workflow_id: parent_workflow_id,
          idempotency_key: "test-key",
          order_id: 456,
          sku: "ABC123"
        )

        # Act
        parent_record = Persistence::ActiveRecord::WorkflowRecord.find(parent_workflow_id)
        child_record = Persistence::ActiveRecord::WorkflowRecord.find(child_workflow_id)

        # Assert: Parent can access children
        assert_equal 1, parent_record.child_workflows.count
        assert_equal child_workflow_id, parent_record.child_workflows.first.id

        # Assert: Child can access parent
        refute_nil child_record.parent_workflow
        assert_equal parent_workflow_id, child_record.parent_workflow.id

        # Assert: Child can access parent's attributes
        assert_equal "ChildWorkflowTestWorkflow", child_record.parent_workflow.klass
      end

      def test_child_workflow_inherits_parent_globals
        # Arrange
        parent_globals = { tenant_id: "tenant-123", user_id: "user-456" }
        parent_workflow_id = Client.create_workflow(
          ChildWorkflowTestWorkflow, 
          user_id: 123, 
          globals: parent_globals
        )

        # Act
        child_workflow_id = Client.create_workflow(
          ChildWorkflowTestChildWorkflow,
          parent_workflow_id: parent_workflow_id,
          idempotency_key: "test-key",
          order_id: 456,
          sku: "ABC123"
        )

        # Assert: Child workflow has its own globals (not inherited from parent)
        child_record = Persistence::ActiveRecord::WorkflowRecord.find(child_workflow_id)
        parent_record = Persistence::ActiveRecord::WorkflowRecord.find(parent_workflow_id)
        
        # Child should have empty globals by default (unless explicitly set)
        assert_equal({}, child_record.globals)
        # Convert parent_globals to string keys for comparison since JSONB stores them as strings
        expected_parent_globals = parent_globals.transform_keys(&:to_s)
        assert_equal expected_parent_globals, parent_record.globals
      end
    end
  end
end 