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
            @mock_job_record = OpenStruct.new(
              id: @job_id,
              workflow_id: @workflow_id,
              klass: @job_klass_name,
              state: :enqueued, # Start as enqueued
              arguments: @arguments.transform_keys(&:to_s), # Simulate string keys from JSON
              queue_name: 'default'
            )

            # Instantiate the actual job we are testing
            @async_job_instance = AsyncJob.new
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
                  yield
               end
             end
          end


          # --- Test Perform Method ---

          def test_perform_success_path
            # Arrange
            expected_output = "Success output with 20" # 10 * 2
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

          def test_perform_user_job_failure_path
            # Arrange
            failing_job_klass_name = "AsyncFailureJob"
            @mock_job_record.klass = failing_job_klass_name
            @mock_job_record.state = :enqueued
            # Note: @arguments still includes :multiplier which AsyncFailureJob doesn't accept

            run_perform_with_stubs do
              # Expectations on Repository
              @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
              # Expect update to running state
              @mock_repo.expect(:update_job_attributes, true) do |jid, attrs, opts|
                 jid == @job_id && attrs[:state] == Yantra::Core::StateMachine::RUNNING.to_s && attrs[:started_at].is_a?(Time) && opts == { expected_old_state: :enqueued }
              end

              # --- Execution of user perform will raise ArgumentError ---

              # Expect update to failed state (AFTER ArgumentError is caught)
              @mock_repo.expect(:update_job_attributes, true) do |jid, attrs, opts|
                 jid == @job_id && attrs[:state] == Yantra::Core::StateMachine::FAILED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running }
              end
              # Expect error details to be recorded - check for ArgumentError now
              @mock_repo.expect(:record_job_error, true) do |jid, error| # <<< UPDATED EXPECTATION
                 jid == @job_id &&
                 error.is_a?(ArgumentError) && # <<< Check for ArgumentError
                 error.message.include?("unknown keyword: :multiplier") # <<< Check specific message
              end
              # Expect workflow failure flag to be set
              @mock_repo.expect(:set_workflow_has_failures_flag, true, [@workflow_id])

              # Expectations on Orchestrator
              @mock_orchestrator_instance.expect(:job_finished, nil, [@job_id])

              # Act
              @async_job_instance.perform(@job_id, @workflow_id, failing_job_klass_name)
            end # Stubs are removed here
          end

          def test_perform_does_nothing_if_job_not_found
             run_perform_with_stubs do
               # Arrange: find_job returns nil
               @mock_repo.expect(:find_job, nil, [@job_id])
               # Act
               @async_job_instance.perform(@job_id, @workflow_id, @job_klass_name)
             end
          end

          def test_perform_does_nothing_if_job_not_enqueued
             run_perform_with_stubs do
               # Arrange: find_job returns job in wrong state
               @mock_job_record.state = :running
               @mock_repo.expect(:find_job, @mock_job_record, [@job_id])
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

          # TODO: Add test for NameError when user job class cannot be found
          # TODO: Add test for ArgumentError when user perform has wrong signature (covered slightly by failure test now)
          # TODO: Add tests for retry logic once implemented

        end

      end # if defined?
    end
  end
end

