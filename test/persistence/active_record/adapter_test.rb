# test/persistence/active_record/adapter_test.rb
require "test_helper"

# Explicitly require the files needed for these tests
if AR_LOADED
  require "yantra/persistence/active_record/adapter"
  require "yantra/persistence/active_record/workflow_record"
  require "yantra/persistence/active_record/step_record"
  require "yantra/persistence/active_record/step_dependency_record"
  require "yantra/core/state_machine"
  require "yantra/errors"
end

module Yantra
  module Persistence
    module ActiveRecord
      # Check if base class and models were loaded before defining tests
      if defined?(YantraActiveRecordTestCase) && AR_LOADED

        # Tests for the ActiveRecord persistence adapter
        class AdapterTest < YantraActiveRecordTestCase

          def setup
            super # Ensure DB cleaning happens via base class setup
            # Instantiate the adapter directly for testing its methods
            @adapter = Adapter.new
            @workflow = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "running")
          end

          # --- Tests for cancel_steps_bulk ---

          def test_cancel_steps_bulk_cancels_only_cancellable_states
            # Arrange: Create jobs in various states
            step_pending = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job", state: "pending")
            step_enqueued = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job", state: "enqueued")
            step_running = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job", state: "running")
            step_succeeded = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job", state: "succeeded", finished_at: Time.current)
            step_failed = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job", state: "failed", finished_at: Time.current)
            step_cancelled_already = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job", state: "cancelled", finished_at: Time.current)

            step_ids_to_cancel = [
              step_pending.id,
              step_enqueued.id,
              step_running.id,
              step_succeeded.id, # Should be ignored by WHERE clause
              step_failed.id,    # Should be ignored by WHERE clause (no longer strictly terminal, but not cancellable)
              step_cancelled_already.id  # Should be ignored by WHERE clause
            ]

            # Act: Call the method under test
            updated_count = @adapter.cancel_steps_bulk(step_ids_to_cancel)

            # Assert: Check return value (count of *updated* records)
            assert_equal 3, updated_count, "Should return count of updated (cancellable) jobs"

            # Assert: Verify states of jobs in the database
            assert_equal "cancelled", step_pending.reload.state
            assert_equal "cancelled", step_enqueued.reload.state
            assert_equal "cancelled", step_running.reload.state
            refute_nil step_pending.finished_at, "Pending job should now have finished_at set"
            refute_nil step_enqueued.finished_at, "Enqueued job should now have finished_at set"
            refute_nil step_running.finished_at, "Running job should now have finished_at set"

            # Assert: Verify non-cancellable jobs were untouched
            assert_equal "succeeded", step_succeeded.reload.state
            assert_equal "failed", step_failed.reload.state
            assert_equal "cancelled", step_cancelled_already.reload.state # Was already cancelled
            # Check timestamps didn't change for untouched records
            assert_in_delta step_succeeded.finished_at, step_succeeded.reload.finished_at, 0.01
            assert_in_delta step_failed.finished_at, step_failed.reload.finished_at, 0.01
            assert_in_delta step_cancelled_already.finished_at, step_cancelled_already.reload.finished_at, 0.01
          end

          def test_cancel_steps_bulk_handles_empty_array
            assert_equal 0, @adapter.cancel_steps_bulk([]), "Should return 0 for empty array"
          end

          def test_cancel_steps_bulk_handles_nil_input
            assert_equal 0, @adapter.cancel_steps_bulk(nil), "Should return 0 for nil input"
          end

          def test_cancel_steps_bulk_handles_non_existent_ids
            # Arrange: Create one valid job
            step_pending = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job", state: "pending")
            non_existent_id = SecureRandom.uuid

            # Act: Call with a mix of valid and invalid IDs
            updated_count = @adapter.cancel_steps_bulk([step_pending.id, non_existent_id])

            # Assert: Only the existing job in a cancellable state should be updated
            assert_equal 1, updated_count
            assert_equal "cancelled", step_pending.reload.state
          end

          def test_cancel_steps_bulk_raises_persistence_error_on_db_error
             # Arrange
             step_pending = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job", state: "pending")
             step_ids = [step_pending.id]

             # Mock update_all on the relation object that `where` returns
             mock_relation = Minitest::Mock.new
             # Expectation: call :update_all, return value is irrelevant (nil), raise error in block.
             mock_relation.expect(:update_all, nil) do |*args| # Accept any args passed to update_all
               # Use :: to ensure we raise the top-level ActiveRecord error
               raise ::ActiveRecord::StatementInvalid, "DB Update Error" # <<< FIXED HERE
             end

             # Stub the 'where' call on StepRecord class to return our mock relation
             StepRecord.stub(:where, mock_relation) do
                # Act & Assert
                error = assert_raises(Yantra::Errors::PersistenceError) do
                   @adapter.cancel_steps_bulk(step_ids)
                end
                # Check the error message includes the original error
                assert_match(/Bulk job cancellation failed: DB Update Error/, error.message)
             end
             # Verify the mock relation had update_all called on it
             mock_relation.verify
          end

          # TODO: Add Tests for Other Adapter Methods
          # - persist_steps_bulk (success, empty, error cases)
          # - find_ready_steps (various dependency scenarios)
          # - update_step_attributes / update_workflow_attributes error handling?
          # - list_workflows pagination/filtering
          # - delete_workflow / delete_expired_workflows

        end

      end # if defined?
    end
  end
end

