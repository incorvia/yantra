# --- test/worker/active_job/async_step_test.rb ---
# (Update mock expectations and definition error test)

require "test_helper"
require "ostruct" # For creating simple mock objects

# Conditionally load ActiveJob related files
if AR_LOADED # Using AR_LOADED as proxy for full dev env
  require "yantra/worker/active_job/async_job"
  require "yantra/core/orchestrator"
  require "yantra/core/state_machine"
  require "yantra/errors"
  require "yantra/step" # Need base Yantra::Step
  require "minitest/mock"
  # Require RetryHandler as it's now instantiated directly in AsyncJob's rescue block
  require "yantra/worker/retry_handler"
end

# --- Dummy User Job Classes ---
class AsyncSuccessJob < Yantra::Step
  def perform(data:, multiplier: 1); "Success output with #{data * multiplier}"; end
end
class AsyncFailureJob < Yantra::Step
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
            Yantra.configure { |c| c.default_max_step_attempts = 3 }
            # <<< ------------------------------ >>>

            @mock_repo = Minitest::Mock.new
            @mock_orchestrator_instance = Minitest::Mock.new

            @step_id = SecureRandom.uuid
            @workflow_id = SecureRandom.uuid
            @step_klass_name = "AsyncSuccessJob"
            @arguments = { data: 10, multiplier: 2 }

            # Add retries property for mock job record
            @mock_step_record = OpenStruct.new(
              id: @step_id, workflow_id: @workflow_id, klass: @step_klass_name,
              state: :running, # Assume step_starting succeeded for most tests here
              arguments: @arguments.transform_keys(&:to_s),
              retries: 0 # Start retries at 0
            )

            @async_step_instance = AsyncJob.new
            @async_step_instance.executions = 1 # Default executions to 1 for most tests
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
            # Assume step_starting succeeded before perform is called internally
            @mock_step_record.state = :running

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expect(:step_starting, true, [@step_id])
              @mock_repo.expect(:find_step, @mock_step_record, [@step_id]) # Needed by perform logic
              # Expect step_succeeded to be called on orchestrator
              @mock_orchestrator_instance.expect(:step_succeeded, nil, [@step_id, expected_output])

              # Act
              @async_step_instance.perform(@step_id, @workflow_id, @step_klass_name)
            end
          end

          # <<< --- ADJUSTED RETRY TEST --- >>>
          def test_perform_user_step_failure_path_allows_retry
            # Arrange: Simulate first failure (attempt 1 < max 3)
            failing_step_klass_name = "AsyncFailureJob"
            @mock_step_record.klass = failing_step_klass_name
            @mock_step_record.state = :running # Assume step_starting succeeded
            @async_step_instance.executions = 1 # Explicitly set attempt number

            # Mock user class lookup if needed, though const_get might work if class defined
            # Object.stub(:const_get, AsyncFailureJob) do ... end

            run_perform_with_stubs do
                # Expectations
                @mock_orchestrator_instance.expect(:step_starting, true, [@step_id])
                @mock_repo.expect(:find_step, @mock_step_record, [@step_id])

                # --- Corrected Expectations for Retry Path ---
                # Expect RetryHandler calls methods in `prepare_for_retry!`
                @mock_repo.expect(:increment_step_retries, true, [@step_id])
                # --- Expect error OBJECT ---
                @mock_repo.expect(:record_step_error, true) do |jid, err_object|
                   jid == @step_id && err_object.is_a?(ArgumentError) # Check received error class
                end
                # --- END FIX ---
                # --- DO NOT Expect permanent failure calls ---
                # --- DO NOT Expect orchestrator.step_finished ---

                # Act & Assert: Expect the original error (ArgumentError) to be re-raised by RetryHandler
                error = assert_raises(ArgumentError) do
                  @async_step_instance.perform(@step_id, @workflow_id, failing_step_klass_name)
                end
                assert_match(/unknown keyword: :multiplier/, error.message)
             end # End run_perform_with_stubs
          end
          # <<< --- END ADJUSTED RETRY TEST --- >>>


          # <<< --- ADJUSTED MAX ATTEMPTS TEST --- >>>
          def test_perform_user_step_failure_path_reaches_max_attempts
            # Arrange: Simulate final failure (attempt 3 >= max 3)
            failing_step_klass_name = "AsyncFailureJob"
            @mock_step_record.klass = failing_step_klass_name
            @mock_step_record.state = :running # Assume state from previous attempt
            @async_step_instance.executions = 3 # Set to max attempts

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expect(:step_starting, true, [@step_id])
              @mock_repo.expect(:find_step, @mock_step_record, [@step_id])

              # --- Corrected Expectations for Permanent Failure Path ---
              # Expect RetryHandler calls methods in `fail_permanently!`
              @mock_repo.expect(:update_step_attributes, true) do |jid, attrs, opts|
                 jid == @step_id &&
                 attrs[:state] == Yantra::Core::StateMachine::FAILED.to_s &&
                 attrs[:finished_at].is_a?(Time) && # Check finished_at is set
                 opts == { expected_old_state: :running }
              end
              # --- Expect error OBJECT ---
              @mock_repo.expect(:record_step_error, true) do |jid, err_object|
                 jid == @step_id && err_object.is_a?(ArgumentError) # Check received error class
              end
              # --- END FIX ---
              @mock_repo.expect(:set_workflow_has_failures_flag, true, [@workflow_id])
              # --- DO NOT Expect increment_step_retries ---

              # Expect AsyncJob to call orchestrator.step_finished AFTER handler returns :failed
              @mock_orchestrator_instance.expect(:step_finished, nil, [@step_id])

              # Act
              # Perform should NOT raise an error now, as RetryHandler handles final failure
              @async_step_instance.perform(@step_id, @workflow_id, failing_step_klass_name)
            end
          end
          # <<< --- END ADJUSTED MAX ATTEMPTS TEST --- >>>


          def test_perform_does_nothing_if_step_starting_returns_false
             run_perform_with_stubs do
               # Arrange: step_starting returns false
               @mock_orchestrator_instance.expect(:step_starting, false, [@step_id])
               # --> Expect NO other calls (repo.find_step etc.)

               # Act
               @async_step_instance.perform(@step_id, @workflow_id, @step_klass_name)
             end
          end

          def test_perform_raises_persistence_error_if_step_not_found_after_starting
             run_perform_with_stubs do
               # Arrange
               @mock_orchestrator_instance.expect(:step_starting, true, [@step_id])
               @mock_repo.expect(:find_step, nil, [@step_id]) # Simulate job disappearing

               # Act & Assert
               error = assert_raises(Yantra::Errors::StepNotFound) do # Check specific error
                  @async_step_instance.perform(@step_id, @workflow_id, @step_klass_name)
               end
               assert_match(/Job record #{@step_id} not found after starting/, error.message)
             end
          end

          # --- Test Definition Error (Revised Expectations) ---
          def test_perform_raises_step_definition_error_if_klass_invalid
            invalid_klass_name = "NonExistentStepClass"
            # Need job record for the initial find_step call
            @mock_step_record.klass = invalid_klass_name

            run_perform_with_stubs do
               # Expectations
               @mock_orchestrator_instance.expect(:step_starting, true, [@step_id])
               @mock_repo.expect(:find_step, @mock_step_record, [@step_id])
               # --- DO NOT Expect RetryHandler calls (increment_step_retries, etc.) ---
               # --- DO NOT Expect orchestrator.step_finished or step_succeeded ---

               # Act & Assert: Expect StepDefinitionError to be raised directly by AsyncJob
               error = assert_raises(Yantra::Errors::StepDefinitionError) do
                  # Pass the invalid name to perform
                  @async_step_instance.perform(@step_id, @workflow_id, invalid_klass_name)
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

