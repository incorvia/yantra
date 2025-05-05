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
        assert_equal :scheduling, SCHEDULING # Added
        assert_equal :enqueued, ENQUEUED     # Added
        # AWAITING_EXECUTION removed
        assert_equal :running, RUNNING
        assert_equal :post_processing, POST_PROCESSING
        assert_equal :succeeded, SUCCEEDED
        assert_equal :failed, FAILED
        assert_equal :cancelled, CANCELLED
      end

      def test_all_states_set_is_correct
        expected_states = Set[
          PENDING, SCHEDULING, ENQUEUED, RUNNING, POST_PROCESSING, SUCCEEDED, FAILED, CANCELLED
        ]
        # Use .to_a.sort because Set order isn't guaranteed for comparison
        assert_equal expected_states.to_a.sort, StateMachine.states.sort, "ALL_STATES set is incorrect"
      end

      def test_releasable_from_states_is_correct
        expected_states = Set[PENDING]
        assert_equal expected_states, StateMachine::RELEASABLE_FROM_STATES
      end

      def test_prerequisite_met_states_is_correct
        expected_states = Set[POST_PROCESSING, SUCCEEDED]
        assert_equal expected_states, StateMachine::PREREQUISITE_MET_STATES
      end

      # Note: CANCELLABLE_STATES constant replaced by is_cancellable_state? helper

      def test_startable_states_is_correct
        # States from which a worker can pick up a job to run
        expected_states = Set[SCHEDULING, ENQUEUED, RUNNING] # RUNNING included for idempotency
        assert_equal expected_states, StateMachine::STARTABLE_STATES
      end

      def test_terminal_states_set_is_correct
        expected_terminal = Set[SUCCEEDED, CANCELLED] # FAILED is not terminal
        assert_equal expected_terminal, StateMachine.terminal_states
      end

      def test_non_terminal_states_set_is_correct
        # All states except the strictly terminal ones
        expected_non_terminal = Set[PENDING, SCHEDULING, ENQUEUED, RUNNING, POST_PROCESSING, FAILED]
        assert_equal expected_non_terminal.to_a.sort, StateMachine::NON_TERMINAL_STATES.to_a.sort
      end

      def test_work_in_progress_states_set_is_correct
        # States indicating work is not finished (used for workflow completion check)
        expected_wip = Set[PENDING, SCHEDULING, ENQUEUED, RUNNING, POST_PROCESSING]
        assert_equal expected_wip, StateMachine::WORK_IN_PROGRESS_STATES
      end

      def test_terminal_check_method
        assert StateMachine.terminal?(:succeeded)
        assert StateMachine.terminal?(:cancelled)
        refute StateMachine.terminal?(:pending)
        refute StateMachine.terminal?(:scheduling) # New
        refute StateMachine.terminal?(:enqueued)   # New
        refute StateMachine.terminal?(:running)
        refute StateMachine.terminal?(:post_processing)
        refute StateMachine.terminal?(:failed) # Correctly not terminal
        refute StateMachine.terminal?(nil)
        refute StateMachine.terminal?(:invalid_state)
      end

      def test_triggers_downstream_processing_method
        assert StateMachine.triggers_downstream_processing?(:post_processing) # Success path trigger
        assert StateMachine.triggers_downstream_processing?(:failed)
        assert StateMachine.triggers_downstream_processing?(:cancelled)

        refute StateMachine.triggers_downstream_processing?(:pending)
        refute StateMachine.triggers_downstream_processing?(:scheduling) # New
        refute StateMachine.triggers_downstream_processing?(:enqueued)   # New
        refute StateMachine.triggers_downstream_processing?(:running)
        refute StateMachine.triggers_downstream_processing?(:succeeded) # Downstream already processed via POST_PROCESSING
        refute StateMachine.triggers_downstream_processing?(nil)
        refute StateMachine.triggers_downstream_processing?(:invalid_state)
      end

      def test_is_cancellable_state_helper
        # Now only depends on the state symbol
        assert StateMachine.is_cancellable_state?(:pending), "Pending should be cancellable"
        assert StateMachine.is_cancellable_state?(:scheduling), "Scheduling should be cancellable"

        refute StateMachine.is_cancellable_state?(:enqueued), "Enqueued should NOT be cancellable"
        refute StateMachine.is_cancellable_state?(:running), "Running should not be cancellable"
        refute StateMachine.is_cancellable_state?(:post_processing), "PostProcessing should not be cancellable"
        refute StateMachine.is_cancellable_state?(:succeeded), "Succeeded should not be cancellable"
        refute StateMachine.is_cancellable_state?(:failed), "Failed should not be cancellable"
        refute StateMachine.is_cancellable_state?(:cancelled), "Cancelled should not be cancellable"
        refute StateMachine.is_cancellable_state?(nil), "Nil should not be cancellable"
        refute StateMachine.is_cancellable_state?(:invalid_state), "Invalid state should not be cancellable"
      end

      def test_is_enqueue_candidate_state_helper
        # Checks if state is PENDING or SCHEDULING (for retries)
        assert StateMachine.is_enqueue_candidate_state?(:pending), "Pending should be candidate"
        assert StateMachine.is_enqueue_candidate_state?(:scheduling), "Scheduling should be candidate (for retry)"

        refute StateMachine.is_enqueue_candidate_state?(:enqueued), "Enqueued should NOT be candidate"
        refute StateMachine.is_enqueue_candidate_state?(:running), "Running should not be candidate"
        refute StateMachine.is_enqueue_candidate_state?(:post_processing), "PostProcessing should not be candidate"
        refute StateMachine.is_enqueue_candidate_state?(:succeeded), "Succeeded should not be candidate"
        refute StateMachine.is_enqueue_candidate_state?(:failed), "Failed should not be candidate"
        refute StateMachine.is_enqueue_candidate_state?(:cancelled), "Cancelled should not be candidate"
        refute StateMachine.is_enqueue_candidate_state?(nil), "Nil should not be candidate"
        refute StateMachine.is_enqueue_candidate_state?(:invalid_state), "Invalid state should not be candidate"
      end

      def test_valid_transitions_allowed
        # Test key allowed transitions based on the updated VALID_TRANSITIONS hash
        assert StateMachine.valid_transition?(:pending, :scheduling)
        assert StateMachine.valid_transition?(:pending, :cancelled)

        assert StateMachine.valid_transition?(:scheduling, :enqueued) # Normal path
        assert StateMachine.valid_transition?(:scheduling, :running)  # Immediate pickup
        assert StateMachine.valid_transition?(:scheduling, :failed)   # Enqueue error
        assert StateMachine.valid_transition?(:scheduling, :cancelled)

        assert StateMachine.valid_transition?(:enqueued, :running)
        assert StateMachine.valid_transition?(:enqueued, :failed) # Worker immediate fail

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
        refute StateMachine.valid_transition?(:pending, :running) # Must go via scheduling/enqueued
        refute StateMachine.valid_transition?(:pending, :enqueued) # Must go via scheduling
        refute StateMachine.valid_transition?(:scheduling, :pending) # Cannot go back
        refute StateMachine.valid_transition?(:enqueued, :scheduling) # Cannot go back
        refute StateMachine.valid_transition?(:running, :scheduling) # Cannot go back
        refute StateMachine.valid_transition?(:running, :enqueued)   # Cannot go back
        refute StateMachine.valid_transition?(:post_processing, :running) # Cannot go back

        # Skipping states
        refute StateMachine.valid_transition?(:pending, :succeeded)
        refute StateMachine.valid_transition?(:scheduling, :succeeded)

        # Invalid target state for source state
        refute StateMachine.valid_transition?(:pending, :pending)
        refute StateMachine.valid_transition?(:failed, :succeeded)

        # Unknown states
        refute StateMachine.valid_transition?(:pending, :unknown)
        refute StateMachine.valid_transition?(:unknown, :pending)
        refute StateMachine.valid_transition?(nil, :pending)
        refute StateMachine.valid_transition?(:pending, nil)
      end

      def test_validate_transition_bang_passes_for_valid
        assert StateMachine.validate_transition!(:pending, :scheduling)
        assert StateMachine.validate_transition!(:scheduling, :enqueued)
        assert StateMachine.validate_transition!(:scheduling, :running)
        assert StateMachine.validate_transition!(:enqueued, :running)
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

        error = assert_raises(Yantra::Errors::InvalidStateTransition) do
          StateMachine.validate_transition!(:enqueued, :scheduling) # Cannot go back
        end
        assert_match(/Cannot transition from state :enqueued to :scheduling/, error.message)
      end

    end # class StateMachineTest
  end # module Core
end # module Yantra

