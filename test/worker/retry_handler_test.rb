# test/worker/retry_handler_test.rb

require "test_helper"
require "ostruct" # For mock objects

# Explicitly require files under test and dependencies
require "yantra/worker/retry_handler"
require "yantra/core/state_machine"
require "yantra/errors"
require "yantra/step" # Need base class for yantra_max_attempts
require "yantra/events/notifier_interface"
require "yantra/persistence/repository_interface"

# Dummy Step class for testing max_attempts override
class RetryStepWithOverride < Yantra::Step
  def self.yantra_max_attempts; 2; end
  def perform; end
end

class RetryStepWithoutOverride < Yantra::Step
  def perform; end
end


module Yantra
  module Worker
    class RetryHandlerTest < Minitest::Test

      # Setup common instance variables before each test
      def setup
        # Mock repository and notifier
        @mock_repo = Minitest::Mock.new
        @mock_notifier = Minitest::Mock.new

        # Mock step record data
        @step_id = SecureRandom.uuid
        @workflow_id = SecureRandom.uuid
        @mock_step_record = OpenStruct.new(
          id: @step_id,
          workflow_id: @workflow_id,
          klass: "RetryStepWithoutOverride",
          state: :running, # Assume it was running before failure
          retries: 3 # Example: already retried 3 times (so next attempt is 4th)
        )

        # Mock error
        @mock_error = StandardError.new("Something went wrong")
        @mock_error.set_backtrace(["line 1", "line 2"])
      end

      def teardown
        # Verify mocks after each test
        @mock_repo.verify
        @mock_notifier.verify
        # Reset global config if needed
        Yantra::Configuration.reset! if defined?(Yantra::Configuration) && Yantra::Configuration.respond_to?(:reset!)
      end

      # --- Test handle_error! when max attempts are reached ---
      def test_handle_error_fails_permanently_and_publishes_event
        # Stub repository access for this test
        Yantra.stub(:repository, @mock_repo) do
          # Arrange
          max_attempts = 4 # Calculated based on default retries = 3
          executions = 4   # Simulate reaching max attempts
          finished_at_time = Time.now # Capture time for expectation
          formatted_error = { class: "StandardError", message: "Something went wrong", backtrace: ["line 1", "line 2"] }

          handler = RetryHandler.new(
            repository: @mock_repo,
            step_record: @mock_step_record,
            error: @mock_error,
            executions: executions,
            user_step_klass: RetryStepWithoutOverride,
            notifier: @mock_notifier
          )

          Time.stub :current, finished_at_time do # Freeze time

            # --- Expectations ---
            # --- UPDATED: Correct expectation for update_step_attributes ---
            @mock_repo.expect(
              :update_step_attributes,
              true, # Assume update succeeds
              [ # Positional arguments array
                @step_id, # arg 1
                { state: Yantra::Core::StateMachine::FAILED.to_s, finished_at: finished_at_time } # arg 2 (attrs hash)
              ],
              # Keyword arguments hash (placed after positional args array)
              expected_old_state: :running
            )
            # --- END UPDATE ---

            @mock_repo.expect(:record_step_error, formatted_error, [@step_id, @mock_error])
            @mock_repo.expect(:set_workflow_has_failures_flag, true, [@workflow_id])
            @mock_notifier.expect(:publish, nil) do |event_name, payload|
               event_name == 'yantra.step.failed' && payload[:step_id] == @step_id &&
               payload[:workflow_id] == @workflow_id &&
               payload[:klass] == @mock_step_record.klass &&
               payload[:state] == Yantra::Core::StateMachine::FAILED &&
               payload[:finished_at] == finished_at_time &&
               payload[:error] == formatted_error &&
               payload[:retries] == @mock_step_record.retries
            end
            # --- End Expectations ---

            # Act
            result = handler.handle_error!

            # Assert
            assert_equal :failed, result

          end # End Time.stub
        end # --- END Yantra.stub block ---
      end


      # --- Test handle_error! when retries are remaining ---
      def test_handle_error_prepares_for_retry_and_raises
        Yantra.stub(:repository, @mock_repo) do
          # Arrange
          max_attempts = 4 # Assuming default retries = 3
          executions = 1   # First failure, retries remaining
          handler = RetryHandler.new(
            repository: @mock_repo,
            step_record: @mock_step_record,
            error: @mock_error,
            executions: executions,
            user_step_klass: RetryStepWithoutOverride,
            notifier: @mock_notifier
          )

          # --- Expectations ---
          @mock_repo.expect(:increment_step_retries, true, [@step_id])
          @mock_repo.expect(:record_step_error, true, [@step_id, @mock_error])
          # Notifier should NOT be called
          # --- End Expectations ---

          # Act & Assert
          raised_error = assert_raises(StandardError) do
            handler.handle_error!
          end
          assert_same @mock_error, raised_error
        end # --- END Yantra.stub block ---
      end


      # --- Test max attempts calculation ---
      def test_get_max_attempts_uses_step_override
         Yantra.stub(:repository, @mock_repo) do
           @mock_step_record.retries = 1
           handler = RetryHandler.new(repository: @mock_repo, step_record: @mock_step_record, error: @mock_error, executions: 1, user_step_klass: RetryStepWithOverride, notifier: @mock_notifier)
           assert_equal 2, handler.send(:get_max_attempts)
         end
      end

      def test_get_max_attempts_uses_global_config
         Yantra.stub(:repository, @mock_repo) do
           Yantra.configure { |c| c.default_step_options[:retries] = 4 } # 4 retries = 5 attempts
           handler = RetryHandler.new(repository: @mock_repo, step_record: @mock_step_record, error: @mock_error, executions: 1, user_step_klass: RetryStepWithoutOverride, notifier: @mock_notifier)
           assert_equal 5, handler.send(:get_max_attempts)
         end
      end

       def test_get_max_attempts_uses_default_if_no_config
         Yantra.stub(:repository, @mock_repo) do
           Yantra.configure { |c| c.default_step_options.delete(:retries) }
           handler = RetryHandler.new(repository: @mock_repo, step_record: @mock_step_record, error: @mock_error, executions: 1, user_step_klass: RetryStepWithoutOverride, notifier: @mock_notifier)
           assert_equal 1, handler.send(:get_max_attempts)
         end
       end

    end # class RetryHandlerTest
  end # module Worker
end # module Yantra

