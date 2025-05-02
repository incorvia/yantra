# test/core/state_machine_test.rb
# frozen_string_literal: true

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
        # --- ADDED: Test for SCHEDULING ---
        assert_equal :scheduling, SCHEDULING
        # --- END ADDED ---
        assert_equal :enqueued, ENQUEUED
        assert_equal :running, RUNNING
        assert_equal :succeeded, SUCCEEDED
        assert_equal :failed, FAILED
        assert_equal :cancelled, CANCELLED
      end

      def test_all_states_set_is_correct
        expected_states = Set[
          PENDING, SCHEDULING, ENQUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED
        ]
        assert_equal expected_states.sort, StateMachine.states.sort # Compare sorted arrays
      end

      def test_releasable_from_states_is_correct
        expected_states = Set[PENDING]
        assert_equal expected_states, StateMachine::RELEASABLE_FROM_STATES
      end

      def test_prerequisite_met_states_is_correct
        expected_states = Set[SUCCEEDED]
        assert_equal expected_states, StateMachine::PREREQUISITE_MET_STATES
      end

      def test_cancellable_states_is_correct
        expected_states = Set[PENDING, SCHEDULING, ENQUEUED]
        assert_equal expected_states, StateMachine::CANCELLABLE_STATES
      end

      def test_terminal_states_set_is_correct
        # --- UPDATED: Removed FAILED ---
        expected_terminal = Set[SUCCEEDED, CANCELLED]
        # --- END UPDATED ---
        assert_equal expected_terminal, StateMachine.terminal_states
      end

      def test_terminal_check_method
        assert StateMachine.terminal?(:succeeded), ":succeeded should be terminal"
        assert StateMachine.terminal?(:cancelled), ":cancelled should be terminal"
        assert StateMachine.terminal?("succeeded"), "String 'succeeded' should be terminal"

        refute StateMachine.terminal?(:pending), ":pending should not be terminal"
        refute StateMachine.terminal?(:scheduling), ":scheduling should not be terminal" # Added check
        refute StateMachine.terminal?(:enqueued), ":enqueued should not be terminal"
        refute StateMachine.terminal?(:running), ":running should not be terminal"
        refute StateMachine.terminal?("running"), "String 'running' should not be terminal"
        # --- UPDATED: FAILED is not terminal ---
        refute StateMachine.terminal?(:failed), ":failed should NOT be strictly terminal (can be retried)"
        # --- END UPDATED ---
        refute StateMachine.terminal?(:unknown_state), "Unknown state should not be terminal"
        refute StateMachine.terminal?(nil), "Nil should not be terminal"
      end

      def test_triggers_downstream_processing_method
        assert StateMachine.triggers_downstream_processing?(:succeeded), ":succeeded should trigger"
        assert StateMachine.triggers_downstream_processing?(:failed), ":failed should trigger"
        assert StateMachine.triggers_downstream_processing?(:cancelled), ":cancelled should trigger"

        refute StateMachine.triggers_downstream_processing?(:pending), ":pending should not trigger"
        refute StateMachine.triggers_downstream_processing?(:scheduling), ":scheduling should not trigger"
        refute StateMachine.triggers_downstream_processing?(:enqueued), ":enqueued should not trigger"
        refute StateMachine.triggers_downstream_processing?(:running), ":running should not trigger"
        refute StateMachine.triggers_downstream_processing?(nil), "nil should not trigger"
      end

      def test_cancellable_method
        assert StateMachine.cancellable?(:pending), ":pending should be cancellable"
        assert StateMachine.cancellable?(:scheduling), ":scheduling should be cancellable"
        assert StateMachine.cancellable?(:enqueued), ":enqueued should be cancellable"

        refute StateMachine.cancellable?(:running), ":running should not be cancellable"
        refute StateMachine.cancellable?(:succeeded), ":succeeded should not be cancellable"
        refute StateMachine.cancellable?(:failed), ":failed should not be cancellable"
        refute StateMachine.cancellable?(:cancelled), ":cancelled should not be cancellable"
        refute StateMachine.cancellable?(nil), "nil should not be cancellable"
      end


      def test_valid_transitions_allowed
        # --- UPDATED: Transitions involving SCHEDULING ---
        assert StateMachine.valid_transition?(:pending, :scheduling), "pending -> scheduling should be valid"
        assert StateMachine.valid_transition?(:pending, :cancelled), "pending -> cancelled should be valid"

        assert StateMachine.valid_transition?(:scheduling, :enqueued), "scheduling -> enqueued should be valid"
        assert StateMachine.valid_transition?(:scheduling, :cancelled), "scheduling -> cancelled should be valid"

        assert StateMachine.valid_transition?(:enqueued, :running), "enqueued -> running should be valid"
        assert StateMachine.valid_transition?(:enqueued, :cancelled), "enqueued -> cancelled should be valid"

        assert StateMachine.valid_transition?(:running, :succeeded), "running -> succeeded should be valid"
        assert StateMachine.valid_transition?(:running, :failed), "running -> failed should be valid"
        assert StateMachine.valid_transition?(:running, :cancelled), "running -> cancelled should be valid"

        # FAILED can transition for retry or cancellation
        assert StateMachine.valid_transition?(:failed, :enqueued), "failed -> enqueued (retry) should be valid"
        assert StateMachine.valid_transition?(:failed, :cancelled), "failed -> cancelled should be valid"
        # --- END UPDATED ---

        # Test with strings
        assert StateMachine.valid_transition?("running", "succeeded"), "String transition should be valid"
      end

      def test_invalid_transitions_disallowed
        # From terminal states (SUCCEEDED, CANCELLED)
        refute StateMachine.valid_transition?(:succeeded, :running), "succeeded -> running should be invalid"
        refute StateMachine.valid_transition?(:cancelled, :pending), "cancelled -> pending should be invalid"

        # --- UPDATED: Invalid transitions based on new rules ---
        # Cannot transition from FAILED back to RUNNING directly
        refute StateMachine.valid_transition?(:failed, :running), "failed -> running should be invalid"
        # Cannot transition directly from PENDING to ENQUEUED/RUNNING anymore
        refute StateMachine.valid_transition?(:pending, :enqueued), "pending -> enqueued should be invalid"
        refute StateMachine.valid_transition?(:pending, :running), "pending -> running should be invalid"
        # Cannot transition from SCHEDULING to RUNNING directly
        refute StateMachine.valid_transition?(:scheduling, :running), "scheduling -> running should be invalid"
        # Cannot transition from ENQUEUED back to SCHEDULING
        refute StateMachine.valid_transition?(:enqueued, :scheduling), "enqueued -> scheduling should be invalid"
        # --- END UPDATED ---

        # Skipping states
        refute StateMachine.valid_transition?(:pending, :succeeded), "pending -> succeeded should be invalid"
        refute StateMachine.valid_transition?(:enqueued, :failed), "enqueued -> failed should be invalid"

        # Invalid target state for source state
        refute StateMachine.valid_transition?(:pending, :pending), "pending -> pending should be invalid"
        refute StateMachine.valid_transition?(:running, :enqueued), "running -> enqueued should be invalid"
        refute StateMachine.valid_transition?(:failed, :succeeded), "failed -> succeeded should be invalid"

        # Unknown states
        refute StateMachine.valid_transition?(:pending, :unknown), "pending -> unknown should be invalid"
        refute StateMachine.valid_transition?(:unknown, :pending), "unknown -> pending should be invalid"
        refute StateMachine.valid_transition?(nil, :pending), "nil -> pending should be invalid"
        refute StateMachine.valid_transition?(:pending, nil), "pending -> nil should be invalid"
      end

      def test_validate_transition_bang_passes_for_valid
        # Should return true and not raise error for valid transitions
        assert StateMachine.validate_transition!(:pending, :scheduling)
        assert StateMachine.validate_transition!(:scheduling, :enqueued)
        assert StateMachine.validate_transition!(:enqueued, :running)
        assert StateMachine.validate_transition!(:running, :failed)
        assert StateMachine.validate_transition!(:failed, :cancelled)
        assert StateMachine.validate_transition!(:failed, :enqueued)
      end

      def test_validate_transition_bang_raises_for_invalid
        # Should raise InvalidStateTransition for invalid transitions
        error = assert_raises(Yantra::Errors::InvalidStateTransition) do
          StateMachine.validate_transition!(:succeeded, :running)
        end
        assert_match(/Cannot transition from state :succeeded to :running/, error.message)

        error = assert_raises(Yantra::Errors::InvalidStateTransition) do
          # --- UPDATED: Test invalid transition based on new rules ---
          StateMachine.validate_transition!(:pending, :enqueued)
          # --- END UPDATED ---
        end
        assert_match(/Cannot transition from state :pending to :enqueued/, error.message)

        # Check invalid state symbol
        error = assert_raises(Yantra::Errors::InvalidStateTransition) do
          StateMachine.validate_transition!(:pending, :invalid_state_symbol)
        end
        assert_match(/Cannot transition from state :pending to :invalid_state_symbol/, error.message)

        # Check transition from terminal
        error = assert_raises(Yantra::Errors::InvalidStateTransition) do
          StateMachine.validate_transition!(:cancelled, :pending)
        end
        assert_match(/Cannot transition from state :cancelled to :pending/, error.message)
      end

    end
  end
end

