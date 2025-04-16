# test/core/state_machine_test.rb

require "test_helper" # Basic Minitest setup
require "yantra/core/state_machine"
require "yantra/errors" # Required for InvalidStateTransition error

module Yantra
  module Core
    class StateMachineTest < Minitest::Test

      # Include the module to easily access constants like PENDING, RUNNING etc.
      include StateMachine

      def test_defines_correct_state_constants
        assert_equal :pending, PENDING
        assert_equal :enqueued, ENQUEUED
        assert_equal :running, RUNNING
        assert_equal :succeeded, SUCCEEDED
        assert_equal :failed, FAILED
        assert_equal :cancelled, CANCELLED
      end

      def test_all_states_set_is_correct
        expected_states = Set[
          PENDING, ENQUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED
        ]
        assert_equal expected_states, StateMachine.states
      end

      def test_terminal_states_set_is_correct
        # Update expected set: :failed is no longer strictly terminal
        # to allow for retries (failed -> enqueued).
        expected_terminal = Set[SUCCEEDED, CANCELLED]
        assert_equal expected_terminal, StateMachine.terminal_states
      end

      def test_terminal_check_method
        assert StateMachine.terminal?(:succeeded), ":succeeded should be terminal"
        assert StateMachine.terminal?(:cancelled), ":cancelled should be terminal"
        assert StateMachine.terminal?("succeeded"), "String 'succeeded' should be terminal"

        refute StateMachine.terminal?(:pending), ":pending should not be terminal"
        refute StateMachine.terminal?(:enqueued), ":enqueued should not be terminal"
        refute StateMachine.terminal?(:running), ":running should not be terminal"
        refute StateMachine.terminal?("running"), "String 'running' should not be terminal"
        # Update assertion: :failed is not strictly terminal because it can be retried
        refute StateMachine.terminal?(:failed), ":failed should NOT be strictly terminal (can be retried)"
        refute StateMachine.terminal?(:unknown_state), "Unknown state should not be terminal"
        refute StateMachine.terminal?(nil), "Nil should not be terminal"
      end

      def test_valid_transitions_allowed
        assert StateMachine.valid_transition?(:pending, :enqueued), "pending -> enqueued should be valid"
        assert StateMachine.valid_transition?(:pending, :cancelled), "pending -> cancelled should be valid"
        assert StateMachine.valid_transition?(:pending, :running), "pending -> running should be valid" # Based on current definition

        assert StateMachine.valid_transition?(:enqueued, :running), "enqueued -> running should be valid"
        assert StateMachine.valid_transition?(:enqueued, :cancelled), "enqueued -> cancelled should be valid"

        assert StateMachine.valid_transition?(:running, :succeeded), "running -> succeeded should be valid"
        assert StateMachine.valid_transition?(:running, :failed), "running -> failed should be valid"
        assert StateMachine.valid_transition?(:running, :cancelled), "running -> cancelled should be valid"

        # This assertion should now pass because :failed is not terminal anymore
        assert StateMachine.valid_transition?(:failed, :enqueued), "failed -> enqueued (retry) should be valid"
        assert StateMachine.valid_transition?(:failed, :cancelled), "failed -> cancelled should be valid"

        # Test with strings
        assert StateMachine.valid_transition?("running", "succeeded"), "String transition should be valid"
      end

      def test_invalid_transitions_disallowed
        # From terminal states
        refute StateMachine.valid_transition?(:succeeded, :running), "succeeded -> running should be invalid"
        refute StateMachine.valid_transition?(:failed, :running), "failed -> running should be invalid"
        refute StateMachine.valid_transition?(:cancelled, :pending), "cancelled -> pending should be invalid"

        # Skipping states
        refute StateMachine.valid_transition?(:pending, :succeeded), "pending -> succeeded should be invalid"
        refute StateMachine.valid_transition?(:enqueued, :failed), "enqueued -> failed should be invalid"

        # Invalid target state for source state
        refute StateMachine.valid_transition?(:pending, :pending), "pending -> pending should be invalid"
        refute StateMachine.valid_transition?(:running, :enqueued), "running -> enqueued should be invalid"
        refute StateMachine.valid_transition?(:failed, :succeeded), "failed -> succeeded should be invalid" # Cannot go directly from fail to success

        # Unknown states
        refute StateMachine.valid_transition?(:pending, :unknown), "pending -> unknown should be invalid"
        refute StateMachine.valid_transition?(:unknown, :pending), "unknown -> pending should be invalid"
        refute StateMachine.valid_transition?(nil, :pending), "nil -> pending should be invalid"
        refute StateMachine.valid_transition?(:pending, nil), "pending -> nil should be invalid"
      end

      def test_validate_transition_bang_passes_for_valid
        # Should return true and not raise error for valid transitions
        assert StateMachine.validate_transition!(:pending, :enqueued)
        assert StateMachine.validate_transition!(:running, :failed)
        assert StateMachine.validate_transition!(:failed, :cancelled)
        assert StateMachine.validate_transition!(:failed, :enqueued) # Add check for the previously failing case
      end

      def test_validate_transition_bang_raises_for_invalid
        # Should raise InvalidStateTransition for invalid transitions
        error = assert_raises(Yantra::Errors::InvalidStateTransition) do
          StateMachine.validate_transition!(:succeeded, :running)
        end
        # Wrap regex in parentheses to resolve parser ambiguity warning
        assert_match(/Cannot transition from state :succeeded to :running/, error.message) # <-- UPDATED

        error = assert_raises(Yantra::Errors::InvalidStateTransition) do
          StateMachine.validate_transition!(:pending, :failed)
        end
        assert_match(/Cannot transition from state :pending to :failed/, error.message) # <-- UPDATED

        # Check invalid state symbol
         error = assert_raises(Yantra::Errors::InvalidStateTransition) do
           StateMachine.validate_transition!(:pending, :invalid_state_symbol)
         end
         assert_match(/Cannot transition from state :pending to :invalid_state_symbol/, error.message) # <-- UPDATED

         # Check transition from terminal
         error = assert_raises(Yantra::Errors::InvalidStateTransition) do
           StateMachine.validate_transition!(:cancelled, :pending)
         end
         assert_match(/Cannot transition from state :cancelled to :pending/, error.message) # <-- UPDATED
      end

      # Need to also update the assert_match in StepDependencyRecordTest if that file exists
      # (Assuming the user has applied the previous fix there manually)

    end
  end
end

