# test/persistence/active_record/adapter_test.rb
require "test_helper"

# Explicitly require the files needed for these tests
if AR_LOADED
  require "yantra/persistence/active_record/adapter"
  require "yantra/persistence/active_record/workflow_record"
  require "yantra/persistence/active_record/job_record"
  require "yantra/persistence/active_record/job_dependency_record"
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

          # --- Tests for cancel_jobs_bulk ---

          # --- UPDATED: Changed expected count from 3 to 2 ---
          def test_cancel_jobs_bulk_cancels_only_cancellable_states
            # Arrange: Create jobs in various states
            job_pending = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobP", state: "pending")
            job_enqueued = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobE", state: "enqueued")
            job_running = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobR", state: "running") # This one won't be cancelled now
            job_succeeded = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobS", state: "succeeded", finished_at: Time.current)
            job_failed = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobF", state: "failed", finished_at: Time.current)
            job_cancelled_already = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobC", state: "cancelled", finished_at: Time.current)

            job_ids_to_cancel = [
              job_pending.id,
              job_enqueued.id,
              job_running.id, # Included in list, but adapter logic should skip it
              job_succeeded.id,
              job_failed.id,
              job_cancelled_already.id
            ]

            # Act: Call the method under test
            updated_count = @adapter.cancel_jobs_bulk(job_ids_to_cancel)

            # Assert: Check return value (count of *updated* records)
            # Expecting only pending and enqueued to be updated now.
            assert_equal 2, updated_count, "Should return count of updated (pending/enqueued) jobs" # <<< CHANGED FROM 3 to 2

            # Assert: Verify states of jobs in the database
            assert_equal "cancelled", job_pending.reload.state
            assert_equal "cancelled", job_enqueued.reload.state
            assert_equal "running", job_running.reload.state # <<< Running job should NOT be cancelled
            refute_nil job_pending.finished_at, "Pending job should now have finished_at set"
            refute_nil job_enqueued.finished_at, "Enqueued job should now have finished_at set"
            assert_nil job_running.finished_at, "Running job finished_at should remain nil" # <<< Added check

            # Assert: Verify non-cancellable jobs were untouched
            assert_equal "succeeded", job_succeeded.reload.state
            assert_equal "failed", job_failed.reload.state
            assert_equal "cancelled", job_cancelled_already.reload.state
            # Check timestamps didn't change for untouched records
            assert_in_delta job_succeeded.finished_at, job_succeeded.reload.finished_at, 0.01
            assert_in_delta job_failed.finished_at, job_failed.reload.finished_at, 0.01
            assert_in_delta job_cancelled_already.finished_at, job_cancelled_already.reload.finished_at, 0.01
          end
          # --- END UPDATED TEST ---


          def test_cancel_jobs_bulk_handles_empty_array
            assert_equal 0, @adapter.cancel_jobs_bulk([]), "Should return 0 for empty array"
          end

          def test_cancel_jobs_bulk_handles_nil_input
            assert_equal 0, @adapter.cancel_jobs_bulk(nil), "Should return 0 for nil input"
          end

          def test_cancel_jobs_bulk_handles_non_existent_ids
            # Arrange: Create one valid job
            job_pending = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job", state: "pending")
            non_existent_id = SecureRandom.uuid

            # Act: Call with a mix of valid and invalid IDs
            updated_count = @adapter.cancel_jobs_bulk([job_pending.id, non_existent_id])

            # Assert: Only the existing job in a cancellable state should be updated
            assert_equal 1, updated_count
            assert_equal "cancelled", job_pending.reload.state
          end

          def test_cancel_jobs_bulk_raises_persistence_error_on_db_error
             # Arrange
             job_pending = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job", state: "pending")
             job_ids = [job_pending.id]

             # Mock update_all on the relation object that `where` returns
             mock_relation = Minitest::Mock.new
             # Expectation: call :update_all, return value is irrelevant (nil), raise error in block.
             mock_relation.expect(:update_all, nil) do |*args| # Accept any args passed to update_all
               # Use :: to ensure we raise the top-level ActiveRecord error
               raise ::ActiveRecord::StatementInvalid, "DB Update Error"
             end

             # Stub the 'where' call on JobRecord class to return our mock relation
             JobRecord.stub(:where, mock_relation) do
                # Act & Assert
                error = assert_raises(Yantra::Errors::PersistenceError) do
                   @adapter.cancel_jobs_bulk(job_ids)
                end
                # Check the error message includes the original error
                assert_match(/Bulk job cancellation failed: DB Update Error/, error.message)
             end
             # Verify the mock relation had update_all called on it
             mock_relation.verify
          end

          # TODO: Add Tests for Other Adapter Methods

        end

      end # if defined?
    end
  end
end

