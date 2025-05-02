# test/core/state_machine_test.rb
# frozen_string_literal: true

require "test_helper" # Basic Minitest setup
require "yantra/core/state_machine"
require "yantra/errors" # Required for InvalidStateTransition error
require 'set'

module Yantra
  module Core
    class StateMachineTest < Minitest::Test

      # Include the module to easily access constants
      include StateMachine

      def test_defines_correct_state_constants
        assert_equal :pending, PENDING
        assert_equal :scheduling, SCHEDULING
        # ENQUEUED removed
        assert_equal :running, RUNNING
        assert_equal :post_processing, POST_PROCESSING
        assert_equal :succeeded, SUCCEEDED
        assert_equal :failed, FAILED
        assert_equal :cancelled, CANCELLED
      end

      def test_all_states_set_is_correct
        expected_states = Set[
          PENDING, SCHEDULING, RUNNING, POST_PROCESSING, SUCCEEDED, FAILED, CANCELLED
        ]
        assert_equal expected_states.sort, StateMachine.states.sort # Compare sorted arrays
      end

      def test_releasable_from_states_is_correct
        expected_states = Set[PENDING]
        assert_equal expected_states, StateMachine::RELEASABLE_FROM_STATES
      end

      def test_prerequisite_met_states_is_correct
        # --- UPDATED: Added POST_PROCESSING as potentially met ---
        # Depending on if downstream steps should run after post-processing starts
        # or only after full success. Let's assume SUCCEEDED is the only true met state for now.
        # If post-processing should also unlock, add it here.
        expected_states = Set[POST_PROCESSING, SUCCEEDED]
        # expected_states = Set[SUCCEEDED, POST_PROCESSING] # Alternative
        # --- END UPDATED ---
        assert_equal expected_states, StateMachine::PREREQUISITE_MET_STATES
      end

      # Note: CANCELLABLE_STATES constant removed in favor of is_cancellable_state? helper

      def test_startable_states_is_correct
        # RUNNING included for idempotency
        # SCHEDULING included because worker picks up from this state
        # PENDING included because worker might pick up before state update? (Safer)
        expected_states = Set[PENDING, SCHEDULING, RUNNING]
        assert_equal expected_states, StateMachine::STARTABLE_STATES
      end


      def test_terminal_states_set_is_correct
        expected_terminal = Set[SUCCEEDED, CANCELLED] # FAILED is not terminal
        assert_equal expected_terminal, StateMachine.terminal_states
      end

      def test_non_terminal_states_set_is_correct
        expected_non_terminal = Set[FAILED, PENDING, SCHEDULING, RUNNING, POST_PROCESSING, FAILED]
        assert_equal expected_non_terminal.sort, StateMachine::NON_TERMINAL_STATES.sort
      end

      def test_terminal_check_method
        assert StateMachine.terminal?(:succeeded)
        assert StateMachine.terminal?(:cancelled)
        refute StateMachine.terminal?(:pending)
        refute StateMachine.terminal?(:scheduling)
        refute StateMachine.terminal?(:running)
        refute StateMachine.terminal?(:post_processing)
        refute StateMachine.terminal?(:failed) # Correctly not terminal
        refute StateMachine.terminal?(nil)
      end

      def test_triggers_downstream_processing_method
        assert StateMachine.triggers_downstream_processing?(:post_processing) # Success path trigger
        assert StateMachine.triggers_downstream_processing?(:failed)
        assert StateMachine.triggers_downstream_processing?(:cancelled)

        refute StateMachine.triggers_downstream_processing?(:pending)
        refute StateMachine.triggers_downstream_processing?(:scheduling)
        # refute StateMachine.triggers_downstream_processing?(:enqueued) # Removed state
        refute StateMachine.triggers_downstream_processing?(:running)
        refute StateMachine.triggers_downstream_processing?(:succeeded) # Downstream already processed via POST_PROCESSING
        refute StateMachine.triggers_downstream_processing?(nil)
      end

      def test_is_cancellable_state_helper
        now = Time.now
        assert StateMachine.is_cancellable_state?(:pending, nil), "Pending should be cancellable"
        assert StateMachine.is_cancellable_state?(:pending, now), "Pending should be cancellable even with timestamp"
        assert StateMachine.is_cancellable_state?(:scheduling, nil), "Scheduling without timestamp should be cancellable"

        refute StateMachine.is_cancellable_state?(:scheduling, now), "Scheduling *with* timestamp should NOT be cancellable"
        # refute StateMachine.is_cancellable_state?(:enqueued, now), "Enqueued removed"
        refute StateMachine.is_cancellable_state?(:running, nil), "Running should not be cancellable"
        refute StateMachine.is_cancellable_state?(:succeeded, nil), "Succeeded should not be cancellable"
        refute StateMachine.is_cancellable_state?(:failed, nil), "Failed should not be cancellable"
        refute StateMachine.is_cancellable_state?(:cancelled, nil), "Cancelled should not be cancellable"
      end

      def test_is_enqueue_candidate_state_helper
        now = Time.now
        assert StateMachine.is_enqueue_candidate_state?(:pending, nil), "Pending should be candidate"
        assert StateMachine.is_enqueue_candidate_state?(:pending, now), "Pending should be candidate even with timestamp"
        assert StateMachine.is_enqueue_candidate_state?(:scheduling, nil), "Scheduling without timestamp should be candidate"

        refute StateMachine.is_enqueue_candidate_state?(:scheduling, now), "Scheduling *with* timestamp should NOT be candidate"
        # refute StateMachine.is_enqueue_candidate_state?(:enqueued, now), "Enqueued removed"
        refute StateMachine.is_enqueue_candidate_state?(:running, nil), "Running should not be candidate"
        # ... other non-candidate states ...
      end

      def test_valid_transitions_allowed
        assert StateMachine.valid_transition?(:pending, :scheduling)
        assert StateMachine.valid_transition?(:pending, :cancelled)

        assert StateMachine.valid_transition?(:scheduling, :running) # Worker picks up
        assert StateMachine.valid_transition?(:scheduling, :cancelled)
        assert StateMachine.valid_transition?(:scheduling, :failed) # Enqueue critical failure

        # assert StateMachine.valid_transition?(:enqueued, :running) # Removed state
        # assert StateMachine.valid_transition?(:enqueued, :cancelled) # Removed state

        assert StateMachine.valid_transition?(:running, :post_processing) # Success path
        assert StateMachine.valid_transition?(:running, :failed)
        assert StateMachine.valid_transition?(:running, :cancelled)

        assert StateMachine.valid_transition?(:post_processing, :succeeded)
        assert StateMachine.valid_transition?(:post_processing, :failed)

        assert StateMachine.valid_transition?(:failed, :pending) # Retry resets to PENDING
        assert StateMachine.valid_transition?(:failed, :cancelled)

        # Test with strings
        assert StateMachine.valid_transition?("running", "post_processing")
      end

      def test_invalid_transitions_disallowed
        # From terminal states (SUCCEEDED, CANCELLED)
        refute StateMachine.valid_transition?(:succeeded, :running)
        refute StateMachine.valid_transition?(:cancelled, :pending)

        # Invalid transitions based on new rules
        refute StateMachine.valid_transition?(:failed, :running) # Cannot go back to running directly
        refute StateMachine.valid_transition?(:pending, :running) # Must go via scheduling
        refute StateMachine.valid_transition?(:pending, :enqueued) # State removed
        refute StateMachine.valid_transition?(:scheduling, :pending) # Cannot go back
        refute StateMachine.valid_transition?(:running, :scheduling) # Cannot go back
        refute StateMachine.valid_transition?(:post_processing, :running) # Cannot go back

        # Skipping states
        refute StateMachine.valid_transition?(:pending, :succeeded)
        refute StateMachine.valid_transition?(:scheduling, :succeeded)

        # Invalid target state for source state
        refute StateMachine.valid_transition?(:pending, :pending)
        refute StateMachine.valid_transition?(:running, :enqueued) # State removed
        refute StateMachine.valid_transition?(:failed, :succeeded)

        # Unknown states
        refute StateMachine.valid_transition?(:pending, :unknown)
        refute StateMachine.valid_transition?(:unknown, :pending)
        refute StateMachine.valid_transition?(nil, :pending)
        refute StateMachine.valid_transition?(:pending, nil)
      end

      def test_validate_transition_bang_passes_for_valid
        assert StateMachine.validate_transition!(:pending, :scheduling)
        assert StateMachine.validate_transition!(:scheduling, :running)
        assert StateMachine.validate_transition!(:running, :post_processing)
        assert StateMachine.validate_transition!(:post_processing, :succeeded)
        assert StateMachine.validate_transition!(:running, :failed)
        assert StateMachine.validate_transition!(:failed, :cancelled)
        assert StateMachine.validate_transition!(:failed, :pending) # Check retry reset
      end

      def test_validate_transition_bang_raises_for_invalid
        error = assert_raises(Yantra::Errors::InvalidStateTransition) do
          StateMachine.validate_transition!(:succeeded, :running)
        end
        assert_match(/Cannot transition from state :succeeded to :running/, error.message)

        error = assert_raises(Yantra::Errors::InvalidStateTransition) do
          StateMachine.validate_transition!(:pending, :running) # Invalid direct transition
        end
        assert_match(/Cannot transition from state :pending to :running/, error.message)

        error = assert_raises(Yantra::Errors::InvalidStateTransition) do
          StateMachine.validate_transition!(:pending, :invalid_state_symbol)
        end
        assert_match(/Cannot transition from state :pending to :invalid_state_symbol/, error.message)

        error = assert_raises(Yantra::Errors::InvalidStateTransition) do
          StateMachine.validate_transition!(:cancelled, :pending)
        end
        assert_match(/Cannot transition from state :cancelled to :pending/, error.message)
      end

    end
  end
end

