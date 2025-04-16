# --- test/worker/active_job/async_job_test.rb ---

require "test_helper"
require "ostruct" # For creating simple mock objects

# Conditionally load ActiveJob related files
if AR_LOADED # Using AR_LOADED as proxy for full dev env
  require "yantra/worker/active_job/step_job"
  require "yantra/core/orchestrator"
  require "yantra/core/state_machine"
  require "yantra/errors"
  require "yantra/step" # Need base Yantra::Step
  require "minitest/mock"
  require "yantra/worker/retry_handler"
end

# --- Dummy User Job Classes ---
class AsyncSuccessJob < Yantra::Step
  def perform(data:, multiplier: 1); "Success output with #{data * multiplier}"; end
end
class AsyncFailureJob < Yantra::Step
  # Accept args to avoid ArgumentError, then raise intended error
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
            # Ensure max attempts default is known for tests using correct config key
            # *** FIX HERE: Use default_step_options hash ***
            Yantra.configure { |c| c.default_step_options[:retries] = 3 }

            @mock_repo = Minitest::Mock.new
            @mock_orchestrator_instance = Minitest::Mock.new

            @step_id = SecureRandom.uuid
            @workflow_id = SecureRandom.uuid
            @step_klass_name = "AsyncSuccessJob"
            @arguments = { data: 10, multiplier: 2 }

            @mock_step_record = OpenStruct.new(
              id: @step_id, workflow_id: @workflow_id, klass: @step_klass_name,
              state: :running,
              arguments: @arguments.transform_keys(&:to_s),
              retries: 0,
              queue: 'default_queue'
            )

            @async_job_instance = StepJob.new
          end

          def teardown
            @mock_repo.verify
            @mock_orchestrator_instance.verify
            Yantra::Configuration.reset! if defined?(Yantra::Configuration) && Yantra::Configuration.respond_to?(:reset!)
            super
          end

          # Helper to run perform within the stubbed context
          def run_perform_with_stubs(&block)
             # Stub the repository method directly on the Yantra module
             # Ensure Yantra module itself is available
             Yantra.stub(:repository, @mock_repo) do
               # Stub the Orchestrator class's new method
               Yantra::Core::Orchestrator.stub(:new, @mock_orchestrator_instance) do
                  yield
               end
             end
          end

          # --- Test Perform Method ---

          def test_perform_success_path
            # Arrange
            expected_output = "Success output with 20"
            @mock_step_record.state = :running
            @async_job_instance.executions = 1

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expect(:step_starting, true, [@step_id])
              @mock_repo.expect(:find_step, @mock_step_record, [@step_id])
              @mock_repo.expect(:get_step_dependencies, [], [@step_id])
              @mock_orchestrator_instance.expect(:step_succeeded, nil, [@step_id, expected_output])

              # Act
              @async_job_instance.perform(@step_id, @workflow_id, @step_klass_name)
            end
          end

          def test_perform_user_step_failure_path_allows_retry
            # Arrange: Simulate first failure (attempt 1 < max 3)
            failing_step_klass_name = "AsyncFailureJob"
            expected_error_class = StandardError
            @mock_step_record.klass = failing_step_klass_name
            @mock_step_record.state = :running
            @mock_step_record.arguments = { data: 5 }.transform_keys(&:to_s) # Match args for AsyncFailureJob
            @async_job_instance.executions = 1

            run_perform_with_stubs do
                # Expectations
                @mock_orchestrator_instance.expect(:step_starting, true, [@step_id])
                @mock_repo.expect(:find_step, @mock_step_record, [@step_id])
                @mock_repo.expect(:get_step_dependencies, [], [@step_id])

                # Expect RetryHandler calls
                @mock_repo.expect(:increment_step_retries, true, [@step_id])
                @mock_repo.expect(:record_step_error, true) do |jid, err_object|
                   jid == @step_id && err_object.is_a?(expected_error_class)
                end

                # Act & Assert: Expect the intended error
                error = assert_raises(expected_error_class) do
                  @async_job_instance.perform(@step_id, @workflow_id, failing_step_klass_name)
                end
                assert_match(/Job failed intentionally/, error.message)
            end
          end


          def test_perform_user_step_failure_path_reaches_max_attempts
            # Arrange: Simulate final failure (attempt 3 >= max 3)
            failing_step_klass_name = "AsyncFailureJob"
            expected_error_class = StandardError
            @mock_step_record.klass = failing_step_klass_name
            @mock_step_record.state = :running
            @mock_step_record.arguments = { data: 5 }.transform_keys(&:to_s) # Match args
            @async_job_instance.executions = 3 # Set to max attempts (using default_step_options[:retries]=3 means max attempts is 3)

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expect(:step_starting, true, [@step_id])
              @mock_repo.expect(:find_step, @mock_step_record, [@step_id])
              @mock_repo.expect(:get_step_dependencies, [], [@step_id])

              # Expect RetryHandler calls for permanent failure
              @mock_repo.expect(:update_step_attributes, true) do |jid, attrs, opts|
                 jid == @step_id &&
                 attrs[:state] == Yantra::Core::StateMachine::FAILED.to_s &&
                 attrs[:finished_at].is_a?(Time) &&
                 opts == { expected_old_state: :running }
              end
              @mock_repo.expect(:record_step_error, true) do |jid, err_object|
                 jid == @step_id && err_object.is_a?(expected_error_class)
              end
              @mock_repo.expect(:set_workflow_has_failures_flag, true, [@workflow_id])

              # Expect StepJob to call orchestrator.step_finished
              @mock_repo.expect(:find_step, OpenStruct.new(state: 'failed'), [@step_id])
              @mock_orchestrator_instance.expect(:step_finished, nil, [@step_id])

              # Act
              @async_job_instance.perform(@step_id, @workflow_id, failing_step_klass_name)
            end
          end


          def test_perform_does_nothing_if_step_starting_returns_false
             run_perform_with_stubs do
               # Arrange
               @mock_orchestrator_instance.expect(:step_starting, false, [@step_id])

               # Act
               @async_job_instance.perform(@step_id, @workflow_id, @step_klass_name)
             end
          end

          def test_perform_raises_persistence_error_if_step_not_found_after_starting
             run_perform_with_stubs do
               # Arrange
               @mock_orchestrator_instance.expect(:step_starting, true, [@step_id])
               @mock_repo.expect(:find_step, nil, [@step_id])

               # Act & Assert
               error = assert_raises(Yantra::Errors::StepNotFound) do
                  @async_job_instance.perform(@step_id, @workflow_id, @step_klass_name)
               end
               assert_match(/Job record #{@step_id} not found after starting/, error.message)
             end
          end

          def test_perform_raises_step_definition_error_if_klass_invalid
            invalid_klass_name = "NonExistentStepClass"
            @mock_step_record.klass = invalid_klass_name

            run_perform_with_stubs do
               # Expectations
               @mock_orchestrator_instance.expect(:step_starting, true, [@step_id])
               @mock_repo.expect(:find_step, @mock_step_record, [@step_id])
               # No get_step_dependencies expected here

               # Act & Assert: Expect StepDefinitionError
               error = assert_raises(Yantra::Errors::StepDefinitionError) do
                  @async_job_instance.perform(@step_id, @workflow_id, invalid_klass_name)
               end
               assert_match(/Class #{invalid_klass_name} could not be loaded/, error.message)
            end
          end

        end
      end # if defined?
    end
  end
end

