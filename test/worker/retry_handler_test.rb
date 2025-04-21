# test/worker/retry_handler_test.rb

require "test_helper"
require "ostruct" # For mock objects
require "securerandom" # For UUIDs

require "yantra/worker/retry_handler"
require "yantra/core/state_machine"
require "yantra/errors"
require "yantra/step" # Need base class for yantra_max_attempts
require "yantra/persistence/repository_interface"
require "yantra/core/orchestrator"

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
        @mock_repo = Minitest::Mock.new
        @mock_orchestrator = Minitest::Mock.new

        # Mock step record data using OpenStruct
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

        # Stub global configuration if needed (using Mocha for this example)
        # If you have a different way of managing config in tests, adjust accordingly
        Yantra.stubs(:configuration).returns(
          stub(
            default_step_options: { retries: 3 }, # Default: 3 retries = 4 attempts
            default_max_step_attempts: nil
          )
        )
      end

      def teardown
        # Verify mocks after each test
        @mock_repo.verify
        # --- ADDED: Verify orchestrator mock ---
        @mock_orchestrator.verify

        # Unstub global config if stubbed with Mocha
        Yantra.unstub(:configuration)
        # Reset global config if needed (using your pattern)
        # Yantra::Configuration.reset! if defined?(Yantra::Configuration) && Yantra::Configuration.respond_to?(:reset!)
      end

      # --- Test handle_error! when max attempts are reached ---
      def test_handle_error_fails_permanently_and_calls_orchestrator
        # Stub repository access for this test (using Mocha style here, adjust if needed)
        Yantra.stub(:repository, @mock_repo) do
          # Arrange
          executions = 4   # Simulate reaching max attempts
          formatted_error = { class: "StandardError", message: "Something went wrong", backtrace: ["line 1", "line 2"] }

          # --- UPDATED: Pass orchestrator mock ---
          handler = RetryHandler.new(
            repository: @mock_repo,
            step_record: @mock_step_record,
            error: @mock_error,
            executions: executions,
            user_step_klass: RetryStepWithoutOverride,
            orchestrator: @mock_orchestrator # Pass the mock
          )

          # --- Expectations ---
          # Expect orchestrator#step_failed to be called
          # Use a block to validate the arguments passed
          @mock_orchestrator.expect(:step_failed, nil) do |arg_step_id, arg_error_info|
              arg_step_id == @step_id &&
              arg_error_info[:class] == formatted_error[:class] &&
              arg_error_info[:message] == formatted_error[:message] &&
              arg_error_info[:backtrace] == formatted_error[:backtrace]
          end

          # Act
          result = handler.handle_error!

          # Assert
          assert_equal :failed, result

          # end # End Time.stub (if used)
        end # --- END Yantra.stub block ---
      end


      # --- Test handle_error! when retries are remaining ---
      def test_handle_error_prepares_for_retry_and_raises
        Yantra.stub(:repository, @mock_repo) do
          # Arrange
          executions = 1   # First failure, retries remaining

          # --- UPDATED: Pass orchestrator mock ---
          handler = RetryHandler.new(
            repository: @mock_repo,
            step_record: @mock_step_record,
            error: @mock_error,
            executions: executions,
            user_step_klass: RetryStepWithoutOverride,
            orchestrator: @mock_orchestrator # Pass the mock
          )

          # --- Expectations ---
          @mock_repo.expect(:increment_step_retries, true, [@step_id])
          @mock_repo.expect(:record_step_error, true, [@step_id, @mock_error])
          # No expectation set for @mock_orchestrator.step_failed,
          # so verify() will fail if it *is* called.
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
            # --- UPDATED: Pass orchestrator mock ---
            handler = RetryHandler.new(
                repository: @mock_repo,
                step_record: @mock_step_record,
                error: @mock_error, executions: 1,
                user_step_klass: RetryStepWithOverride,
                orchestrator: @mock_orchestrator
            )
            assert_equal 2, handler.send(:get_max_attempts)
          end
      end

      def test_get_max_attempts_uses_global_config
         Yantra.stub(:repository, @mock_repo) do
            # Use Mocha stubbing for global config as before, adjust if needed
            Yantra.stubs(:configuration).returns(
              stub(
                default_step_options: { retries: 4 }, # 4 retries = 5 attempts
                default_max_step_attempts: nil
              )
            )
            # --- UPDATED: Pass orchestrator mock ---
            handler = RetryHandler.new(
                repository: @mock_repo, step_record: @mock_step_record,
                error: @mock_error, executions: 1,
                user_step_klass: RetryStepWithoutOverride,
                orchestrator: @mock_orchestrator
            )
            assert_equal 5, handler.send(:get_max_attempts)
          end
      end

       def test_get_max_attempts_uses_default_if_no_config
         Yantra.stub(:repository, @mock_repo) do
            Yantra.stubs(:configuration).returns(
              stub(
                default_step_options: {}, # No retries defined
                default_max_step_attempts: nil
              )
            )
            # --- UPDATED: Pass orchestrator mock ---
            handler = RetryHandler.new(
                repository: @mock_repo, step_record: @mock_step_record,
                error: @mock_error, executions: 1,
                user_step_klass: RetryStepWithoutOverride,
                orchestrator: @mock_orchestrator
            )
            assert_equal 1, handler.send(:get_max_attempts) # Default attempts = 1
          end
       end

    end # class RetryHandlerTest
  end # module Worker
end # module Yantra

