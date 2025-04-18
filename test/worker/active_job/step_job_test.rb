# test/worker/active_job/step_job_test.rb

require "test_helper"
require "ostruct" # For creating simple mock objects
require "mocha/minitest" # Use Mocha for mocking

# Conditionally load ActiveJob related files
if AR_LOADED # Using AR_LOADED as proxy for full dev env
  require "yantra/worker/active_job/step_job"
  require "yantra/core/orchestrator"
  require "yantra/core/state_machine"
  require "yantra/errors"
  require "yantra/step" # Need base Yantra::Step
  require "yantra/worker/retry_handler"
  require 'active_job/test_helper' # Require the helper for ActiveJob base functionality if needed
end

# --- Dummy User Job Classes ---
class AsyncSuccessJob < Yantra::Step
  # Need initializer accepting keywords now due to base class change
  def initialize(**args); super; end
  def perform(data:, multiplier: 1); "Success output with #{data * multiplier}"; end
end
class AsyncFailureJob < Yantra::Step
  # Need initializer accepting keywords now due to base class change
  def initialize(**args); super; end
  # Accept args to avoid ArgumentError, then raise intended error
  def perform(data:, **_options)
    raise StandardError, "Job failed intentionally";
  end
end
# Dummy class for invalid klass test
# class NonExistentStepClass; end # Not needed, test uses string

module Yantra
  module Worker
    module ActiveJob
      if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Worker::ActiveJob::StepJob)

        class StepJobTest < YantraActiveRecordTestCase
          # --- REMOVED: Unnecessary include ---
          # This test class doesn't directly use methods like perform_enqueued_jobs
          # include ActiveJob::TestHelper
          # --- END REMOVED ---

          def setup
            super
            Yantra.configure { |c| c.default_step_options[:retries] = 3 } # Default 4 attempts

            # --- Use Mocha Mocks Consistently ---
            @mock_repository = mock('repository')
            @mock_orchestrator = mock('orchestrator')
            @mock_notifier = mock('notifier') # Needed by RetryHandler

            # Stub global accessors/constructors
            Yantra.stubs(:repository).returns(@mock_repository)
            Yantra.stubs(:notifier).returns(@mock_notifier)
            # Stub Orchestrator.new to return our mock when StepJob calls its helper
            Yantra::Core::Orchestrator.stubs(:new).returns(@mock_orchestrator)
            # --- End Mocha Setup ---

            @step_id = SecureRandom.uuid
            @workflow_id = SecureRandom.uuid
            @step_klass_name = "AsyncSuccessJob"
            @arguments = { data: 10, multiplier: 2 }

            # Use OpenStruct for mock data record (can be replaced with stubbing if preferred)
            @mock_step_record = OpenStruct.new(
              id: @step_id, workflow_id: @workflow_id, klass: @step_klass_name,
              state: :running, # Start as running for simplicity in some tests
              arguments: @arguments.transform_keys(&:to_s),
              retries: 0,
              max_attempts: 4, # Based on default retries = 3
              queue: 'default_queue'
              # Ensure it responds to methods needed by StepJob/RetryHandler
            )

            @async_job_instance = StepJob.new
            # Set executions count directly for testing retry logic
            @async_job_instance.executions = 0 # AJ executions are 0-based
          end

          def teardown
            # Mocha teardown happens automatically via mocha/minitest
            Yantra::Configuration.reset! if defined?(Yantra::Configuration) && Yantra::Configuration.respond_to?(:reset!)
            super
          end

          # --- Test Perform Method ---

          def test_perform_success_path
            # Arrange
            expected_output = "Success output with 20"
            @mock_step_record.state = :running # Assume starting succeeded

            # Expectations
            @mock_orchestrator.expects(:step_starting).with(@step_id).returns(true)
            # Expect first find_step call
            @mock_repository.expects(:find_step).with(@step_id).returns(@mock_step_record)
            # Expect orchestrator success call
            @mock_orchestrator.expects(:step_succeeded).with(@step_id, expected_output)

            # Act
            @async_job_instance.perform(@step_id, @workflow_id, @step_klass_name)

            # Assert: Verification happens in teardown
          end

          def test_perform_user_step_failure_path_allows_retry
            # Arrange: Simulate first failure (attempt 1 < max 4)
            failing_step_klass_name = "AsyncFailureJob"
            expected_error_class = StandardError
            @mock_step_record.klass = failing_step_klass_name
            @mock_step_record.state = :running
            @mock_step_record.arguments = { data: 5 }.transform_keys(&:to_s)
            @async_job_instance.executions = 0 # First attempt (0-based)

            # Expectations
            @mock_orchestrator.expects(:step_starting).with(@step_id).returns(true)
            # Expect first find_step (before perform)
            @mock_repository.expects(:find_step).with(@step_id).returns(@mock_step_record).once
            # Expect second find_step (inside rescue block before RetryHandler)
            @mock_repository.expects(:find_step).with(@step_id).returns(@mock_step_record).once

            # Expect RetryHandler calls (via repo)
            @mock_repository.expects(:increment_step_retries).with(@step_id).once
            @mock_repository.expects(:record_step_error).with(@step_id, instance_of(expected_error_class)).once

            # DO NOT expect step_failed or step_succeeded from orchestrator
            @mock_orchestrator.expects(:step_failed).never
            @mock_orchestrator.expects(:step_succeeded).never

            # Act & Assert: Expect the intended error to be re-raised by RetryHandler
            error = assert_raises(expected_error_class) do
              @async_job_instance.perform(@step_id, @workflow_id, failing_step_klass_name)
            end
            assert_match(/Job failed intentionally/, error.message)
          end


          def test_perform_user_step_failure_path_reaches_max_attempts
            # Arrange: Simulate final failure (attempt 4 >= max 4)
            failing_step_klass_name = "AsyncFailureJob"
            expected_error_class = StandardError
            @mock_step_record.klass = failing_step_klass_name
            @mock_step_record.state = :running
            @mock_step_record.arguments = { data: 5 }.transform_keys(&:to_s)
            # Set executions to indicate the final attempt (AJ is 0-based, RetryHandler expects 1-based)
            # If max_attempts is 4, the last execution number is 3 (0, 1, 2, 3)
            @async_job_instance.executions = 4

            # Expectations
            @mock_orchestrator.expects(:step_starting).with(@step_id).returns(true)
            # Expect first find_step (before perform)
            @mock_repository.expects(:find_step).with(@step_id).returns(@mock_step_record).once
            # Expect second find_step (inside rescue block before RetryHandler)
            @mock_repository.expects(:find_step).with(@step_id).returns(@mock_step_record).once

            # Expect RetryHandler to call orchestrator.step_failed
            # Match error details loosely
            error_details_matcher = has_entries(
              class: expected_error_class.name,
              message: "Job failed intentionally"
            )
            @mock_orchestrator.expects(:step_failed)
                              .with(@step_id, error_details_matcher)
                              .once

            # DO NOT expect step_succeeded
            @mock_orchestrator.expects(:step_succeeded).never

            # Act
            # Perform should NOT raise an error here, as RetryHandler returns :failed internally
            @async_job_instance.perform(@step_id, @workflow_id, failing_step_klass_name)

            # Assert: Verification happens in teardown
          end


          def test_perform_does_nothing_if_step_starting_returns_false
             # Arrange
             @mock_orchestrator.expects(:step_starting).with(@step_id).returns(false)
             # DO NOT expect find_step or anything else from repo/orchestrator
             @mock_repository.expects(:find_step).never
             @mock_orchestrator.expects(:step_succeeded).never
             @mock_orchestrator.expects(:step_failed).never

             # Act
             @async_job_instance.perform(@step_id, @workflow_id, @step_klass_name)
             # Assert: Verification happens in teardown
          end

          def test_perform_raises_persistence_error_if_step_not_found_after_starting
             # Arrange
             @mock_orchestrator.expects(:step_starting).with(@step_id).returns(true)
             # Expect first find_step to return nil
             @mock_repository.expects(:find_step).with(@step_id).returns(nil)
             # Do NOT expect step_succeeded or step_failed
             @mock_orchestrator.expects(:step_succeeded).never
             @mock_orchestrator.expects(:step_failed).never

             # Act & Assert
             error = assert_raises(Yantra::Errors::StepNotFound) do
               @async_job_instance.perform(@step_id, @workflow_id, @step_klass_name)
             end
             assert_match(/Step record #{@step_id} not found after starting/, error.message)
          end

          def test_perform_raises_step_definition_error_if_klass_invalid
            invalid_klass_name = "NonExistentStepClass"
            @mock_step_record.klass = invalid_klass_name

            # Expectations
            @mock_orchestrator.expects(:step_starting).with(@step_id).returns(true)
            # Expect first find_step
            @mock_repository.expects(:find_step).with(@step_id).returns(@mock_step_record)

            # Expect StepJob to catch LoadError/NameError, call orchestrator.step_failed, and re-raise
            error_details_matcher = has_entries(
              class: "Yantra::Errors::StepDefinitionError", # Expecting our wrapped error
              message: regexp_matches(/Class #{invalid_klass_name} could not be loaded/)
            )
            @mock_orchestrator.expects(:step_failed)
                              .with(@step_id, error_details_matcher)
                              .once

            # Act & Assert: Expect StepDefinitionError
            error = assert_raises(Yantra::Errors::StepDefinitionError) do
              @async_job_instance.perform(@step_id, @workflow_id, invalid_klass_name)
            end
            assert_match(/Class #{invalid_klass_name} could not be loaded/, error.message)
          end

        end
      end # if defined?
    end
  end
end

