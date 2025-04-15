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
  # Require RetryHandler as it's now instantiated directly in AsyncJob's rescue block
  require "yantra/worker/retry_handler"
end

# --- Dummy User Job Classes ---
class AsyncSuccessJob < Yantra::Job
  def perform(data:, multiplier: 1); "Success output with #{data * multiplier}"; end
end
class AsyncFailureJob < Yantra::Job
  def perform(data:); raise ArgumentError, "unknown keyword: :multiplier"; end # Simulate failure
end

module Yantra
  module Worker
    module ActiveJob
      if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Worker::ActiveJob::AsyncJob)

        class AsyncJobTest < YantraActiveRecordTestCase

          def setup
            super
            @mock_repo = Minitest::Mock.new
            @mock_orchestrator_instance = Minitest::Mock.new

            @job_id = SecureRandom.uuid
            @workflow_id = SecureRandom.uuid
            @job_klass_name = "AsyncSuccessJob"
            @arguments = { data: 10, multiplier: 2 }

            @mock_job_record = OpenStruct.new(
              id: @job_id, workflow_id: @workflow_id, klass: @job_klass_name,
              state: :enqueued, arguments: @arguments.transform_keys(&:to_s)
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
              @mock_repo.expect(:find_job, @mock_job_record, [@job_id]) # Needed after job_starting
              # Expect job_succeeded to be called on orchestrator
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
              # Expect RetryHandler to be called, which then calls repo.increment_job_retries
              @mock_repo.expect(:increment_job_retries, true, [@job_id])
              # --> Expect NO call to orchestrator job_succeeded or job_finished

              # Act & Assert: Expect the original error (ArgumentError) to be re-raised by RetryHandler
              assert_raises(ArgumentError) do
                @async_job_instance.perform(@job_id, @workflow_id, failing_job_klass_name)
              end
            end
          end

          def test_perform_user_job_failure_path_reaches_max_attempts
            # Arrange: Simulate final failure (attempt 3 >= max 3)
            failing_job_klass_name = "AsyncFailureJob"
            @mock_job_record.klass = failing_job_klass_name
            @mock_job_record.state = :running # Assume it was running from previous attempt
            @async_job_instance.executions = 3

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expect(:job_starting, true, [@job_id])
              @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
              # Expect RetryHandler to be called, which then calls repo methods for permanent failure
              @mock_repo.expect(:update_job_attributes, true) do |jid, attrs, opts| # Update to :failed
                 jid == @job_id && attrs[:state] == Yantra::Core::StateMachine::FAILED.to_s && opts == { expected_old_state: :running }
              end
              @mock_repo.expect(:record_job_error, true) do |jid, err| # Record error
                 jid == @job_id && err.is_a?(ArgumentError)
              end
              @mock_repo.expect(:set_workflow_has_failures_flag, true, [@workflow_id]) # Set flag
              # Expect AsyncJob to call orchestrator.job_finished AFTER handler returns :failed
              @mock_orchestrator_instance.expect(:job_finished, nil, [@job_id])

              # Act
              # Perform should NOT raise an error now, as RetryHandler handles final failure
              @async_job_instance.perform(@job_id, @workflow_id, failing_job_klass_name)

            end
          end


          def test_perform_does_nothing_if_job_is_terminal
             run_perform_with_stubs do
               # Arrange: job_starting returns false because state is terminal
               @mock_orchestrator_instance.expect(:job_starting, false, [@job_id])
               # --> Expect NO other calls

               # Act
               @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
             end
          end

          def test_perform_does_nothing_if_job_not_found
             run_perform_with_stubs do
               # Arrange: job_starting returns false because repo.find_job returns nil
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

