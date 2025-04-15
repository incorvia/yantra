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
    # Simulate successful work
    "Success output with #{data * multiplier}"
  end
end

class AsyncFailureJob < Yantra::Job
  # NOTE: This perform only expects :data
  def perform(data:)
    raise StandardError, "User job failed processing #{data}!"
  end
end

module Yantra
  module Worker
    module ActiveJob
      # Run tests only if ActiveJob components could be loaded/defined
      if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Worker::ActiveJob::AsyncJob)

        class AsyncJobTest < YantraActiveRecordTestCase # Inherit for potential DB interaction via mocks if needed

          def setup
            super # Handle DB cleaning etc. from base class
            @mock_repo = Minitest::Mock.new
            @mock_orchestrator_instance = Minitest::Mock.new # Mock the instance

            @job_id = SecureRandom.uuid
            @workflow_id = SecureRandom.uuid
            @job_klass_name = "AsyncSuccessJob"
            # Arguments stored for the job - note :multiplier is included
            @arguments = { data: 10, multiplier: 2 }

            # Mock object returned by repo.find_job
            # Using OpenStruct for simplicity, could also be a Minitest::Mock
            @mock_job_record = OpenStruct.new(
              id: @job_id,
              workflow_id: @workflow_id,
              klass: @job_klass_name,
              state: :enqueued, # Start as enqueued by default
              arguments: @arguments.transform_keys(&:to_s), # Simulate string keys from JSON
              queue_name: 'default'
            )

            # Instantiate the actual job we are testing
            @async_job_instance = AsyncJob.new
            # Mock ActiveJob's executions counter for retry tests
            # Default to 1 unless overridden in test
            @async_job_instance.executions = 1
          end

          def teardown
            # Simplify teardown - Minitest::Mock#verify handles cases with no expectations
            @mock_repo.verify
            @mock_orchestrator_instance.verify
            super
          end

          # Helper to run perform within the stubbed context for repo/orchestrator access
          def run_perform_with_stubs(&block)
             Yantra.stub(:repository, @mock_repo) do
               Yantra::Core::Orchestrator.stub(:new, @mock_orchestrator_instance) do
                  yield # Run the test expectations and action
               end
             end
          end


          # --- Test Perform Method ---

          def test_perform_success_path
            # Arrange
            expected_output = "Success output with 20"
            @mock_job_record.state = :enqueued # Ensure starting state

            run_perform_with_stubs do
              # Expectations on Repository
              @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
              @mock_repo.expect(:update_job_attributes, true) do |jid, attrs, opts|
                 jid == @job_id && attrs[:state] == Yantra::Core::StateMachine::RUNNING.to_s && attrs[:started_at].is_a?(Time) && opts == { expected_old_state: :enqueued }
              end
              @mock_repo.expect(:update_job_attributes, true) do |jid, attrs, opts|
                 jid == @job_id &&
                 attrs[:state] == Yantra::Core::StateMachine::SUCCEEDED.to_s &&
                 attrs[:finished_at].is_a?(Time) &&
                 attrs[:output] == expected_output &&
                 opts == { expected_old_state: :running }
              end

              # Expectations on Orchestrator
              @mock_orchestrator_instance.expect(:job_finished, nil, [@job_id])

              # Act
              @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
            end # Stubs are removed here
          end

          def test_perform_user_job_failure_path_allows_retry
            # Arrange: Simulate first failure (attempt 1 < max 3)
            failing_job_klass_name = "AsyncFailureJob"
            @mock_job_record.klass = failing_job_klass_name
            @mock_job_record.state = :enqueued
            @async_job_instance.executions = 1 # Set attempt number

            # Mock the user job class method for max attempts (example)
            # Need a way to access user_job_klass within stub block, maybe pass it?
            # Or mock Yantra.configuration.default_max_job_attempts = 3

            run_perform_with_stubs do
              # Expectations on Repository
              @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
              # Expect update to running state
              @mock_repo.expect(:update_job_attributes, true) do |jid, attrs, opts|
                 jid == @job_id && attrs[:state] == Yantra::Core::StateMachine::RUNNING.to_s && attrs[:started_at].is_a?(Time) && opts == { expected_old_state: :enqueued }
              end
              # Expect Yantra's internal retry counter to be incremented
              @mock_repo.expect(:increment_job_retries, true, [@job_id]) # <<< ADDED EXPECTATION

              # --> Expect NO update to :failed state
              # --> Expect NO call to record_job_error
              # --> Expect NO call to set_workflow_has_failures_flag
              # --> Expect NO call to orchestrator.job_finished

              # Act & Assert: Expect the original error to be re-raised for AJ backend
              assert_raises(ArgumentError, /unknown keyword: :multiplier/) do
                @async_job_instance.perform(@job_id, @workflow_id, failing_job_klass_name)
              end
            end # Stubs are removed here
          end

          def test_perform_user_job_failure_path_reaches_max_attempts
            # Arrange: Simulate final failure (attempt 3 >= max 3)
            failing_job_klass_name = "AsyncFailureJob"
            @mock_job_record.klass = failing_job_klass_name
            @mock_job_record.state = :running # Assume it was running from previous attempt
            @async_job_instance.executions = 3 # Set attempt number to max (using default of 3)

            run_perform_with_stubs do
              # Expectations on Repository
              @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
              # Expect NO update to running state (already running)
              # Expect update to failed state (AFTER ArgumentError is caught and max attempts check)
              @mock_repo.expect(:update_job_attributes, true) do |jid, attrs, opts|
                 jid == @job_id && attrs[:state] == Yantra::Core::StateMachine::FAILED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running }
              end
              # Expect error details to be recorded
              @mock_repo.expect(:record_job_error, true) do |jid, error|
                 jid == @job_id && error.is_a?(ArgumentError) && error.message.include?("unknown keyword: :multiplier")
              end
              # Expect workflow failure flag to be set
              @mock_repo.expect(:set_workflow_has_failures_flag, true, [@workflow_id])

              # Expectations on Orchestrator
              @mock_orchestrator_instance.expect(:job_finished, nil, [@job_id])

              # Act
              @async_job_instance.perform(@job_id, @workflow_id, failing_job_klass_name)
            end # Stubs are removed here
          end


          # --- Renamed and Fixed Test ---
          def test_perform_does_nothing_if_job_is_terminal
             run_perform_with_stubs do
               # Arrange: find_job returns job in a terminal state
               @mock_job_record.state = :succeeded # Set terminal state
               @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
               # --> Expect NO other repo/orchestrator calls

               # Act
               @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
             end
             # Assertions handled by mock verification
          end

          def test_perform_does_nothing_if_job_not_found
             run_perform_with_stubs do
               # Arrange: find_job returns nil
               @mock_repo.expect(:find_job, nil, [@job_id])
               # Act
               @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
             end
          end

          def test_perform_does_nothing_if_update_to_running_fails
             run_perform_with_stubs do
               # Arrange: find_job returns enqueued job, but update fails
               @mock_job_record.state = :enqueued
               @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
               @mock_repo.expect(:update_job_attributes, false) do |jid, attrs, opts|
                 jid == @job_id && attrs[:state] == Yantra::Core::StateMachine::RUNNING.to_s && opts == { expected_old_state: :enqueued }
               end
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

