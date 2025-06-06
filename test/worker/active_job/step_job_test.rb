# test/worker/active_job/step_job_test.rb

require "test_helper"
require "ostruct" # For creating simple mock objects
require "mocha/minitest" # Use Mocha for mocking

# Conditionally load ActiveJob related files
if AR_LOADED # Using AR_LOADED as proxy for full dev env
  require "yantra/worker/active_job/step_job"
  require "yantra/core/orchestrator"
  require "yantra/core/state_machine"
  require "yantra/core/step_executor" # <<< Require StepExecutor
  require "yantra/errors"
  require "yantra/step" # Need base Yantra::Step
  require "yantra/worker/retry_handler"
  require 'active_job/test_helper'
end

# --- Dummy User Job Classes ---
class AsyncSuccessJob < Yantra::Step
  def initialize(**args); super; end
  def perform(data:, multiplier: 1); "Success output with #{data * multiplier}"; end
end
class AsyncFailureJob < Yantra::Step
  def initialize(**args); super; end
  def perform(data:, **_options)
    raise StandardError, "Job failed intentionally";
  end
end

module Yantra
  module Worker
    module ActiveJob
      if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Worker::ActiveJob::StepJob)

        class StepJobTest < YantraActiveRecordTestCase

          def setup
            super
            Yantra.configure { |c| c.default_step_options[:retries] = 3 }

            @mock_repository = mock('repository')
            @mock_orchestrator = mock('orchestrator')
            @mock_notifier = mock('notifier')
            @mock_worker_adapter = mock('worker_adapter') # Needed for Orchestrator init

            # --- <<< CHANGED: Updated Stubs >>> ---
            # Stub global accessors
            Yantra.stubs(:repository).returns(@mock_repository)
            Yantra.stubs(:notifier).returns(@mock_notifier)
            Yantra.stubs(:worker_adapter).returns(@mock_worker_adapter) # Stub this too

            # Stub Orchestrator.new to return OUR mock when StepJob -> StepExecutor calls it
            Yantra::Core::Orchestrator.stubs(:new).returns(@mock_orchestrator)

            # Add stubs needed by StepExecutor's initializer validation checks
            # These are called when StepJob#step_executor calls StepExecutor.new
            @mock_repository.stubs(:respond_to?).with(:update_step_attributes).returns(true)

            @mock_repository.stubs(:respond_to?).with(:find_step).returns(true)
            @mock_repository.stubs(:respond_to?).with(:update_step_error).returns(true)
            # Add stubs for other checks if StepExecutor initializer has them
            # @mock_orchestrator.stubs(:respond_to?).with(:...)
            # @mock_notifier.stubs(:respond_to?).with(:...)
            @mock_orchestrator.stubs(:respond_to?).with(:step_starting).returns(true)
            @mock_orchestrator.stubs(:respond_to?).with(:handle_post_processing).returns(true)
            @mock_orchestrator.stubs(:respond_to?).with(:step_failed).returns(true)
            # --- <<< END CHANGED >>> ---

            @step_id = SecureRandom.uuid
            @workflow_id = SecureRandom.uuid
            @step_klass_name = "AsyncSuccessJob"
            @arguments = { data: 10, multiplier: 2 }

            @mock_step_record = OpenStruct.new(
              id: @step_id, workflow_id: @workflow_id, klass: @step_klass_name,
              state: :running, # Use symbol or string based on what StepExecutor expects
              arguments: @arguments.transform_keys(&:to_s),
              retries: 0,
              max_attempts: 4,
              queue: 'default_queue'
            )

            @async_job_instance = StepJob.new
          end

          def teardown
            Yantra::Configuration.reset! if defined?(Yantra::Configuration) && Yantra::Configuration.respond_to?(:reset!)
            super
          end

          # --- Test Perform Method ---

          # --- <<< CHANGED: Test now verifies StepExecutor is called >>> ---
          def test_perform_success_path
            # Arrange
            # Mock the step_executor helper to return a mock executor
            mock_executor = mock('step_executor')
            @async_job_instance.stubs(:step_executor).returns(mock_executor)

            # Expect the execute method to be called on the mock executor
            mock_executor.expects(:execute).with(
              step_id: @step_id,
              workflow_id: @workflow_id,
              step_klass_name: @step_klass_name,
            ).returns(nil) # Or whatever execute returns on success

            # Act
            @async_job_instance.perform(@step_id, @workflow_id, @step_klass_name)

            # Assert: Verification happens in teardown via Mocha expects
          end
          # --- <<< END CHANGED >>> ---

          # --- <<< CHANGED: Test now verifies StepExecutor is called >>> ---
          def test_perform_user_step_failure_path_allows_retry
            # Arrange
            failing_step_klass_name = "AsyncFailureJob"
            expected_error_class = StandardError

            # Mock the step_executor helper
            mock_executor = mock('step_executor')
            @async_job_instance.stubs(:step_executor).returns(mock_executor)

            # Expect execute to be called and simulate it raising the user error
            mock_executor.expects(:execute)
              .with(
                step_id: @step_id,
                workflow_id: @workflow_id,
                step_klass_name: failing_step_klass_name,
              )
              .raises(expected_error_class, "Job failed intentionally")

            # Act & Assert: Expect the error raised by the executor to propagate
            error = assert_raises(expected_error_class) do
              @async_job_instance.perform(@step_id, @workflow_id, failing_step_klass_name)
            end
            assert_match(/Job failed intentionally/, error.message)

            # Note: We no longer test the *internal* behavior of the RetryHandler
            # here. That should be done in StepExecutor tests or RetryHandler tests.
            # This test now just verifies StepJob calls the executor and handles errors.
          end
          # --- <<< END CHANGED >>> ---

          # --- <<< CHANGED: Test now verifies StepExecutor is called >>> ---
          def test_perform_user_step_failure_path_reaches_max_attempts
            # Arrange
            failing_step_klass_name = "AsyncFailureJob"

            # Mock the step_executor helper
            mock_executor = mock('step_executor')
            @async_job_instance.stubs(:step_executor).returns(mock_executor)

            # Expect execute to be called and simulate it raising the user error
            mock_executor.expects(:execute)
              .with(
                step_id: @step_id,
                workflow_id: @workflow_id,
                step_klass_name: failing_step_klass_name,
              )
              .raises(StandardError, "Job failed intentionally") # Simulate error on last attempt

            # Act & Assert: Expect the error to propagate
            error = assert_raises(StandardError) do
              @async_job_instance.perform(@step_id, @workflow_id, failing_step_klass_name)
            end
            assert_match(/Job failed intentionally/, error.message)

            # Note: We don't verify orchestrator.step_failed here anymore.
            # That interaction happens *inside* the StepExecutor/RetryHandler,
            # which should have its own tests.
          end
          # --- <<< END CHANGED >>> ---

          # --- <<< CHANGED: Test now verifies StepExecutor is called >>> ---
          def test_perform_does_nothing_if_step_starting_returns_false
            # Arrange
            # Provide a mock step record as `StepExecutor` fetches it before calling `step_starting`
            mock_step_record = OpenStruct.new(
              id: @step_id,
              workflow_id: @workflow_id,
              klass: @step_klass_name,
              state: :pending,
              performed_at: nil,
              arguments: {},
              retries: 0,
              max_attempts: 3
            )

            # Let real executor run, but control orchestrator and repository behavior
            @mock_repository.stubs(:find_step).with(@step_id).returns(mock_step_record)
            @mock_orchestrator.expects(:step_starting).with(@step_id).returns(false)

            # The following should never happen since step_starting returned false
            @mock_repository.expects(:update_step_attributes).never
            @mock_orchestrator.expects(:step_failed).never
            @mock_orchestrator.expects(:step_succeeded).never

            # Act
            @async_job_instance.perform(@step_id, @workflow_id, @step_klass_name)
          end


          # --- <<< END CHANGED >>> ---


          # --- <<< CHANGED: Test now verifies StepExecutor is called >>> ---
          def test_perform_raises_persistence_error_if_step_not_found_after_starting
            # Arrange
            # Mock the step_executor helper
            mock_executor = mock('step_executor')
            @async_job_instance.stubs(:step_executor).returns(mock_executor)

            # Expect execute to be called and simulate it raising StepNotFound
            mock_executor.expects(:execute)
              .with(
                step_id: @step_id,
                workflow_id: @workflow_id,
                step_klass_name: @step_klass_name,
              )
              .raises(Yantra::Errors::StepNotFound, "Step record #{@step_id} not found after starting.")

            # Act & Assert
            error = assert_raises(Yantra::Errors::StepNotFound) do
              @async_job_instance.perform(@step_id, @workflow_id, @step_klass_name)
            end
            assert_match(/Step record #{@step_id} not found after starting/, error.message)
          end
          # --- <<< END CHANGED >>> ---


          # --- <<< CHANGED: Test now verifies StepExecutor is called >>> ---
          def test_perform_raises_step_definition_error_if_klass_invalid
            # Arrange
            invalid_klass_name = "NonExistentStepClass"
            # Mock the step_executor helper
            mock_executor = mock('step_executor')
            @async_job_instance.stubs(:step_executor).returns(mock_executor)

            # Expect execute to be called and simulate it raising StepDefinitionError
            mock_executor.expects(:execute)
              .with(
                step_id: @step_id,
                workflow_id: @workflow_id,
                step_klass_name: invalid_klass_name,
              )
              .raises(Yantra::Errors::StepDefinitionError, "Class #{invalid_klass_name} could not be loaded")

            # Act & Assert: Expect StepDefinitionError
            error = assert_raises(Yantra::Errors::StepDefinitionError) do
              @async_job_instance.perform(@step_id, @workflow_id, invalid_klass_name)
            end
            assert_match(/Class #{invalid_klass_name} could not be loaded/, error.message)

            # Note: We no longer check orchestrator.step_failed here,
            # as that logic is inside StepExecutor and tested separately.
          end
          # --- <<< END CHANGED >>> ---

        end
      end # if defined?
    end
  end
end


