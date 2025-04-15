# --- test/worker/active_job/async_job_test.rb ---
# (Update mock expectations and definition error test)

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
  # Simulate failure
  def perform(data:); raise ArgumentError, "unknown keyword: :multiplier"; end
end

module Yantra
  module Worker
    module ActiveJob
      if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Worker::ActiveJob::AsyncJob)

        class AsyncJobTest < YantraActiveRecordTestCase

          def setup
            super
            # <<< --- EXPLICIT CONFIGURATION --- >>>
            # Ensure max attempts default is known for tests
            Yantra.configure { |c| c.default_max_job_attempts = 3 }
            # <<< ------------------------------ >>>

            @mock_repo = Minitest::Mock.new
            @mock_orchestrator_instance = Minitest::Mock.new

            @job_id = SecureRandom.uuid
            @workflow_id = SecureRandom.uuid
            @job_klass_name = "AsyncSuccessJob"
            @arguments = { data: 10, multiplier: 2 }

            # Add retries property for mock job record
            @mock_job_record = OpenStruct.new(
              id: @job_id, workflow_id: @workflow_id, klass: @job_klass_name,
              state: :running, # Assume job_starting succeeded for most tests here
              arguments: @arguments.transform_keys(&:to_s),
              retries: 0 # Start retries at 0
            )

            @async_job_instance = AsyncJob.new
            @async_job_instance.executions = 1 # Default executions to 1 for most tests
          end

          def teardown
            @mock_repo.verify
            @mock_orchestrator_instance.verify
            # Reset Yantra config if needed
            Yantra::Configuration.reset! if Yantra::Configuration.respond_to?(:reset!)
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
            # Assume job_starting succeeded before perform is called internally
            @mock_job_record.state = :running

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expect(:job_starting, true, [@job_id])
              @mock_repo.expect(:find_job, @mock_job_record, [@job_id]) # Needed by perform logic
              # Expect job_succeeded to be called on orchestrator
              @mock_orchestrator_instance.expect(:job_succeeded, nil, [@job_id, expected_output])

              # Act
              @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
            end
          end

          # <<< --- ADJUSTED RETRY TEST --- >>>
          def test_perform_user_job_failure_path_allows_retry
            # Arrange: Simulate first failure (attempt 1 < max 3)
            failing_job_klass_name = "AsyncFailureJob"
            @mock_job_record.klass = failing_job_klass_name
            @mock_job_record.state = :running # Assume job_starting succeeded
            @async_job_instance.executions = 1 # Explicitly set attempt number

            # Mock user class lookup if needed, though const_get might work if class defined
            # Object.stub(:const_get, AsyncFailureJob) do ... end

            run_perform_with_stubs do
                # Expectations
                @mock_orchestrator_instance.expect(:job_starting, true, [@job_id])
                @mock_repo.expect(:find_job, @mock_job_record, [@job_id])

                # --- Corrected Expectations for Retry Path ---
                # Expect RetryHandler calls methods in `prepare_for_retry!`
                @mock_repo.expect(:increment_job_retries, true, [@job_id])
                # --- Expect error OBJECT ---
                @mock_repo.expect(:record_job_error, true) do |jid, err_object|
                   jid == @job_id && err_object.is_a?(ArgumentError) # Check received error class
                end
                # --- END FIX ---
                # --- DO NOT Expect permanent failure calls ---
                # --- DO NOT Expect orchestrator.job_finished ---

                # Act & Assert: Expect the original error (ArgumentError) to be re-raised by RetryHandler
                error = assert_raises(ArgumentError) do
                  @async_job_instance.perform(@job_id, @workflow_id, failing_job_klass_name)
                end
                assert_match(/unknown keyword: :multiplier/, error.message)
             end # End run_perform_with_stubs
          end
          # <<< --- END ADJUSTED RETRY TEST --- >>>


          # <<< --- ADJUSTED MAX ATTEMPTS TEST --- >>>
          def test_perform_user_job_failure_path_reaches_max_attempts
            # Arrange: Simulate final failure (attempt 3 >= max 3)
            failing_job_klass_name = "AsyncFailureJob"
            @mock_job_record.klass = failing_job_klass_name
            @mock_job_record.state = :running # Assume state from previous attempt
            @async_job_instance.executions = 3 # Set to max attempts

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expect(:job_starting, true, [@job_id])
              @mock_repo.expect(:find_job, @mock_job_record, [@job_id])

              # --- Corrected Expectations for Permanent Failure Path ---
              # Expect RetryHandler calls methods in `fail_permanently!`
              @mock_repo.expect(:update_job_attributes, true) do |jid, attrs, opts|
                 jid == @job_id &&
                 attrs[:state] == Yantra::Core::StateMachine::FAILED.to_s &&
                 attrs[:finished_at].is_a?(Time) && # Check finished_at is set
                 opts == { expected_old_state: :running }
              end
              # --- Expect error OBJECT ---
              @mock_repo.expect(:record_job_error, true) do |jid, err_object|
                 jid == @job_id && err_object.is_a?(ArgumentError) # Check received error class
              end
              # --- END FIX ---
              @mock_repo.expect(:set_workflow_has_failures_flag, true, [@workflow_id])
              # --- DO NOT Expect increment_job_retries ---

              # Expect AsyncJob to call orchestrator.job_finished AFTER handler returns :failed
              @mock_orchestrator_instance.expect(:job_finished, nil, [@job_id])

              # Act
              # Perform should NOT raise an error now, as RetryHandler handles final failure
              @async_job_instance.perform(@job_id, @workflow_id, failing_job_klass_name)
            end
          end
          # <<< --- END ADJUSTED MAX ATTEMPTS TEST --- >>>


          def test_perform_does_nothing_if_job_starting_returns_false
             run_perform_with_stubs do
               # Arrange: job_starting returns false
               @mock_orchestrator_instance.expect(:job_starting, false, [@job_id])
               # --> Expect NO other calls (repo.find_job etc.)

               # Act
               @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
             end
          end

          def test_perform_raises_persistence_error_if_job_not_found_after_starting
             run_perform_with_stubs do
               # Arrange
               @mock_orchestrator_instance.expect(:job_starting, true, [@job_id])
               @mock_repo.expect(:find_job, nil, [@job_id]) # Simulate job disappearing

               # Act & Assert
               error = assert_raises(Yantra::Errors::JobNotFound) do # Check specific error
                  @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
               end
               assert_match(/Job record #{@job_id} not found after starting/, error.message)
             end
          end

          # --- Test Definition Error (Revised Expectations) ---
          def test_perform_raises_job_definition_error_if_klass_invalid
            invalid_klass_name = "NonExistentJobClass"
            # Need job record for the initial find_job call
            @mock_job_record.klass = invalid_klass_name

            run_perform_with_stubs do
               # Expectations
               @mock_orchestrator_instance.expect(:job_starting, true, [@job_id])
               @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
               # --- DO NOT Expect RetryHandler calls (increment_job_retries, etc.) ---
               # --- DO NOT Expect orchestrator.job_finished or job_succeeded ---

               # Act & Assert: Expect JobDefinitionError to be raised directly by AsyncJob
               error = assert_raises(Yantra::Errors::JobDefinitionError) do
                  # Pass the invalid name to perform
                  @async_job_instance.perform(@job_id, @workflow_id, invalid_klass_name)
               end
               # Check the error message raised by AsyncJob's specific rescue
               assert_match(/Class #{invalid_klass_name} could not be loaded/, error.message)
            end
          end
          # --- END Revised Test ---

        end
      end # if defined?
    end
  end
end

