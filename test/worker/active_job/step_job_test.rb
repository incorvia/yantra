# --- test/worker/active_job/step_job_test.rb ---

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
  require "mocha/minitest" # <<< Ensure Mocha is required for sequences
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
          # --- UPDATED: Include Mocha::API ---
          include Mocha::API # Required for Mocha::Sequence

          def setup
            super
            Yantra.configure { |c| c.default_step_options[:retries] = 3 } # Default 4 attempts

            @mock_repo = Minitest::Mock.new
            # Use Mocha mock for Orchestrator as we need sequences now
            @mock_orchestrator_instance = mock('orchestrator')

            @step_id = SecureRandom.uuid
            @workflow_id = SecureRandom.uuid
            @step_klass_name = "AsyncSuccessJob"
            @arguments = { data: 10, multiplier: 2 }

            @mock_step_record = OpenStruct.new(
              id: @step_id, workflow_id: @workflow_id, klass: @step_klass_name,
              state: :running, # Start as running for simplicity in some tests
              arguments: @arguments.transform_keys(&:to_s),
              retries: 0,
              queue: 'default_queue'
            )

            @async_job_instance = StepJob.new
          end

          def teardown
            @mock_repo.verify
            # No verify needed for Mocha mocks, happens automatically
            # @mock_orchestrator_instance.verify
            Yantra::Configuration.reset! if defined?(Yantra::Configuration) && Yantra::Configuration.respond_to?(:reset!)
            super
          end

          # Helper to run perform within the stubbed context
          def run_perform_with_stubs(&block)
             Yantra.stub(:repository, @mock_repo) do
               # --- UPDATED: Stub new with Mocha mock ---
               Yantra::Core::Orchestrator.stub(:new, @mock_orchestrator_instance) do
                  yield
               end
             end
          end

          # --- Test Perform Method ---

          def test_perform_success_path
            # Arrange
            expected_output = "Success output with 20"
            @mock_step_record.state = :running # Assume starting succeeded
            @async_job_instance.executions = 1

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expects(:step_starting).with(@step_id).returns(true)
              @mock_repo.expect(:find_step, @mock_step_record, [@step_id])
              @mock_repo.expect(:get_step_dependencies, [], [@step_id])
              @mock_orchestrator_instance.expects(:step_succeeded).with(@step_id, expected_output) # Use expects for Mocha mock

              # Act
              @async_job_instance.perform(@step_id, @workflow_id, @step_klass_name)
            end
          end

          def test_perform_user_step_failure_path_allows_retry
            # Arrange: Simulate first failure (attempt 1 < max 4)
            failing_step_klass_name = "AsyncFailureJob"
            expected_error_class = StandardError
            @mock_step_record.klass = failing_step_klass_name
            @mock_step_record.state = :running
            @mock_step_record.arguments = { data: 5 }.transform_keys(&:to_s)
            @async_job_instance.executions = 1 # First attempt

            # --- Use Mocha mock for Orchestrator ---
            run_perform_with_stubs do
                # Expectations
                @mock_orchestrator_instance.expects(:step_starting).with(@step_id).returns(true)
                @mock_repo.expect(:find_step, @mock_step_record, [@step_id])
                @mock_repo.expect(:get_step_dependencies, [], [@step_id])

                # Expect RetryHandler calls (via repo)
                @mock_repo.expect(:increment_step_retries, true, [@step_id])
                @mock_repo.expect(:record_step_error, true) do |jid, err_object|
                   jid == @step_id && err_object.is_a?(expected_error_class)
                end
                # DO NOT expect step_finished from orchestrator

                # Act & Assert: Expect the intended error to be re-raised
                error = assert_raises(expected_error_class) do
                  @async_job_instance.perform(@step_id, @workflow_id, failing_step_klass_name)
                end
                assert_match(/Job failed intentionally/, error.message)
            end
          end


          # --- UPDATED: Use Mocha Sequence ---
          def test_perform_user_step_failure_path_reaches_max_attempts
            # Arrange: Simulate final failure (attempt 4 >= max 4)
            failing_step_klass_name = "AsyncFailureJob"
            expected_error_class = StandardError
            @mock_step_record.klass = failing_step_klass_name
            @mock_step_record.state = :running
            @mock_step_record.arguments = { data: 5 }.transform_keys(&:to_s)
            @async_job_instance.executions = 4 # Set to max attempts (default retries=3)

            # Define mock record returned AFTER failure update
            mock_step_record_failed = @mock_step_record.dup
            mock_step_record_failed.state = 'failed' # Use string state

            # Use Mocha mock for Orchestrator and Sequence
            sequence = Mocha::Sequence.new('step_job_permanent_fail')

            run_perform_with_stubs do
              # Expectations
              @mock_orchestrator_instance.expects(:step_starting).with(@step_id).returns(true).in_sequence(sequence)
              # Expect first find_step (returns running record)
              @mock_repo.expect(:find_step, @mock_step_record, [@step_id]) # No sequence needed for Minitest Mock
              @mock_repo.expect(:get_step_dependencies, [], [@step_id]) # No sequence needed

              # Expect RetryHandler calls (via repo) for permanent failure
              @mock_repo.expect(:update_step_attributes, true) do |jid, attrs, opts|
                 jid == @step_id &&
                 attrs[:state] == Yantra::Core::StateMachine::FAILED.to_s &&
                 attrs[:finished_at].is_a?(Time) &&
                 opts == { expected_old_state: :running }
              end # No sequence needed
              @mock_repo.expect(:record_step_error, true) do |jid, err_object|
                 jid == @step_id && err_object.is_a?(expected_error_class)
              end # No sequence needed
              @mock_repo.expect(:set_workflow_has_failures_flag, true, [@workflow_id]) # No sequence needed

              # Expect StepJob to call orchestrator.step_finished AFTER handler runs
              # This find_step happens *after* the update_step_attributes above
              @mock_repo.expect(:find_step, mock_step_record_failed, [@step_id]) # <<< Expectation for the second find_step call

              # Expect orchestrator call *last*
              @mock_orchestrator_instance.expects(:step_finished).with(@step_id).in_sequence(sequence)

              # Act
              # Perform should NOT raise an error here, as RetryHandler returns :failed
              @async_job_instance.perform(@step_id, @workflow_id, failing_step_klass_name)

              # Assert: Verification happens in teardown
            end
          end
          # --- END UPDATED TEST ---


          def test_perform_does_nothing_if_step_starting_returns_false
             run_perform_with_stubs do
               # Arrange
               @mock_orchestrator_instance.expects(:step_starting).with(@step_id).returns(false)
               # DO NOT expect find_step or anything else

               # Act
               @async_job_instance.perform(@step_id, @workflow_id, @step_klass_name)
               # Assert: Verification happens in teardown
             end
          end

          def test_perform_raises_persistence_error_if_step_not_found_after_starting
             run_perform_with_stubs do
               # Arrange
               @mock_orchestrator_instance.expects(:step_starting).with(@step_id).returns(true)
               @mock_repo.expect(:find_step, nil, [@step_id]) # Simulate step disappearing

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
               @mock_orchestrator_instance.expects(:step_starting).with(@step_id).returns(true)
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

