# test/worker/active_job/async_job_test.rb
require "test_helper"
require "ostruct" # For creating simple mock objects

# Conditionally load ActiveJob related files
if AR_LOADED # Using AR_LOADED as proxy for full dev env
  require "yantra/worker/active_job/async_job"
  require "yantra/core/orchestrator"
  require "yantra/core/state_machine"
  require "yantra/errors"
  require "yantra/job" # Need base Yantra::Job
  require "minitest/mock"
end

# --- Dummy User Job Classes ---
class AsyncSuccessJob < Yantra::Job
  def perform(data:, multiplier: 1)
    "Success output with #{data * multiplier}"
  end
end

class AsyncFailureJob < Yantra::Job
  # NOTE: This perform only expects :data
  def perform(data:)
    # This will raise ArgumentError if called with :multiplier
  end
end

module Yantra
  module Worker
    module ActiveJob
      # Run tests only if ActiveJob components could be loaded/defined
      if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Worker::ActiveJob::AsyncJob)

        class AsyncJobTest < YantraActiveRecordTestCase

          def setup
            super
            @mock_repo = Minitest::Mock.new
            @mock_orchestrator_instance = Minitest::Mock.new

            @job_id = SecureRandom.uuid
            @workflow_id = SecureRandom.uuid
            @job_klass_name = "AsyncSuccessJob"
            @arguments = { data: 10, multiplier: 2 } # Note: multiplier will cause ArgumentError in AsyncFailureJob

            @mock_job_record = OpenStruct.new(
              id: @job_id,
              workflow_id: @workflow_id,
              klass: @job_klass_name,
              state: :enqueued,
              arguments: @arguments.transform_keys(&:to_s),
              queue_name: 'default'
            )

            @async_job_instance = AsyncJob.new
            @async_job_instance.executions = 1 # Default executions to 1
          end

          def teardown
            @mock_repo.verify
            @mock_orchestrator_instance.verify
            super
          end

          # Helper to run perform within the stubbed context
          def run_perform_with_stubs(&block)
             Yantra.stub(:repository, @mock_repo) do
               Yantra::Core::Orchestrator.stub(:new, @mock_orchestrator_instance) do
                  yield
               end
             end
          end


          # --- Test Perform Method ---

          def test_perform_success_path
            # Arrange
            expected_output = "Success output with 20"
            @mock_job_record.state = :enqueued

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expect(:job_starting, true, [@job_id])
              @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
              @mock_orchestrator_instance.expect(:job_succeeded, nil, [@job_id, expected_output])

              # Act
              @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
            end
          end

          def test_perform_user_job_failure_path_allows_retry
            # Arrange: Simulate first failure (attempt 1 < max 3)
            failing_job_klass_name = "AsyncFailureJob"
            @mock_job_record.klass = failing_job_klass_name
            @mock_job_record.state = :enqueued
            @async_job_instance.executions = 1

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expect(:job_starting, true, [@job_id])
              @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
              # Expect job_failed to be called on orchestrator.
              # The block should now re-raise the error it receives to simulate
              # the RetryHandler allowing a retry.
              @mock_orchestrator_instance.expect(:job_failed, nil) do |jid, err, exec_count|
                 # Verify args first
                 valid_args = ( jid == @job_id && err.is_a?(ArgumentError) && exec_count == 1 )
                 # If args are valid, re-raise the error to simulate retry path
                 raise err if valid_args
                 # Return value of block indicates if args matched expectation
                 valid_args
              end

              # Act & Assert: Expect the error to propagate because the mock re-raises it
              assert_raises(ArgumentError) do
                @async_job_instance.perform(@job_id, @workflow_id, failing_job_klass_name)
              end
            end
          end

          def test_perform_user_job_failure_path_reaches_max_attempts
            # Arrange: Simulate final failure (attempt 3 >= max 3)
            failing_job_klass_name = "AsyncFailureJob"
            @mock_job_record.klass = failing_job_klass_name
            # State should be :running if this is a retry attempt handled by backend
            @mock_job_record.state = :running
            @async_job_instance.executions = 3 # Set attempt number to max

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expect(:job_starting, true, [@job_id])
              @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
              # Expect job_failed to be called on orchestrator.
              # The block should NOT re-raise the error in this case, as the
              # real RetryHandler would handle permanent failure internally.
              @mock_orchestrator_instance.expect(:job_failed, nil) do |jid, err, exec_count|
                 jid == @job_id && err.is_a?(ArgumentError) && exec_count == 3
                 # Do not raise err here
              end

              # Act
              # Perform should NOT raise an error now, as RetryHandler handles final failure
              @async_job_instance.perform(@job_id, @workflow_id, failing_job_klass_name)

            end # Stubs are removed here
            # Verify mocks ensures job_failed was called correctly
          end


          def test_perform_does_nothing_if_job_is_terminal
             run_perform_with_stubs do
               # Arrange: find_job returns job in a terminal state
               @mock_job_record.state = :succeeded
               # Expect job_starting to be called, and it should return false
               @mock_orchestrator_instance.expect(:job_starting, false, [@job_id])
               # --> Expect NO other calls

               # Act
               @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
             end
          end

          def test_perform_does_nothing_if_job_not_found
             run_perform_with_stubs do
               # Arrange: job_starting returns false because find_job returns nil
               @mock_orchestrator_instance.expect(:job_starting, false, [@job_id])

               # Act
               @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
             end
          end

          def test_perform_does_nothing_if_update_to_running_fails
             run_perform_with_stubs do
               # Arrange: job_starting returns false because repo update failed
               @mock_orchestrator_instance.expect(:job_starting, false, [@job_id])

               # Act
               @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
             end
          end

          # TODO: Add test for NameError when user job class cannot be found (JobDefinitionError)
          # TODO: Add test for ArgumentError when user perform has wrong signature (covered by failure test now)
          # TODO: Add tests for retry logic with different max_attempts config

        end

      end # if defined?
    end
  end
end

