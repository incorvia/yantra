# lib/yantra/core/state_machine.rb
# frozen_string_literal: true

require 'set'

module Yantra
  module Core
    # Defines valid states and transitions for workflows and steps.
    module StateMachine
      # --- Canonical States ---
      PENDING         = :pending
      AWAITING_EXECUTION      = :awaiting_execution # Step is being processed for handoff to job system.
                                   # enqueued_at timestamp indicates successful handoff.
      RUNNING         = :running
      POST_PROCESSING = :post_processing
      SUCCEEDED       = :succeeded
      FAILED          = :failed
      CANCELLED       = :cancelled

      # All valid states
      ALL_STATES = Set[
        PENDING, AWAITING_EXECUTION, RUNNING, POST_PROCESSING, SUCCEEDED, FAILED, CANCELLED
      ].freeze

      # States a step can be in to be considered for starting the enqueue process
      RELEASABLE_FROM_STATES = Set[PENDING].freeze # Only PENDING steps are initially releasable

      # States a prerequisite must be in to be considered 'met'
      PREREQUISITE_MET_STATES = Set[POST_PROCESSING, SUCCEEDED].freeze

      # States a step can be in to be eligible for cancellation (before running)
      CANCELLABLE_STATES_LOGIC = ->(state_symbol, enqueued_at_value) {
        state = state_symbol&.to_sym
        (state == PENDING) || (state == AWAITING_EXECUTION && enqueued_at_value.nil?)
      }

      # States from which a step can transition to RUNNING
      STARTABLE_STATES = Set[PENDING, AWAITING_EXECUTION, RUNNING].freeze

      # Terminal states (cannot transition *from* these naturally in standard flow)
      TERMINAL_STATES = Set[
        SUCCEEDED, CANCELLED
      ].freeze # FAILED is not strictly terminal due to retry

      # States indicating work is still potentially in progress or waiting
      NON_TERMINAL_STATES = ALL_STATES - TERMINAL_STATES
      # => Set[:pending, :awaiting_execution, :running, :post_processing]
      
      WORK_IN_PROGRESS_STATES = Set[
        PENDING, AWAITING_EXECUTION, RUNNING, POST_PROCESSING
      ].freeze

      # Allowed transitions between states during normal operation
      VALID_TRANSITIONS = {
        PENDING         => Set[AWAITING_EXECUTION, CANCELLED].freeze,
        # AWAITING_EXECUTION can transition to RUNNING (if worker picks up),
        # FAILED (if critical enqueue error), or CANCELLED.
        # The state isn't explicitly set back to PENDING on recoverable enqueue error.
        AWAITING_EXECUTION      => Set[RUNNING, CANCELLED, FAILED].freeze,
        RUNNING         => Set[POST_PROCESSING, FAILED, CANCELLED].freeze,
        POST_PROCESSING => Set[SUCCEEDED, FAILED].freeze,
        FAILED          => Set[PENDING, CANCELLED].freeze, # Retry resets to PENDING
        SUCCEEDED       => Set[].freeze,
        CANCELLED       => Set[].freeze
      }.freeze

      # --- Helper Methods ---

      # Can this state be considered for starting the enqueue process?
      def self.can_enqueue?(state_symbol)
        RELEASABLE_FROM_STATES.include?(state_symbol&.to_sym)
      end

      # Does this state satisfy a prerequisite dependency?
      def self.prerequisite_met?(state_symbol)
        PREREQUISITE_MET_STATES.include?(state_symbol&.to_sym)
      end

      # Can a step in this state transition to RUNNING?
      def self.eligible_for_perform?(state_symbol)
        STARTABLE_STATES.include?(state_symbol&.to_sym)
      end

      # Returns true if the given state is strictly terminal
      def self.terminal?(state)
        TERMINAL_STATES.include?(state&.to_sym)
      end

      # Returns true if the state indicates downstream processing should occur
      def self.triggers_downstream_processing?(state_symbol)
        [POST_PROCESSING, FAILED, CANCELLED].include?(state_symbol&.to_sym)
      end

      # Checks if a step is in a state where it can be safely cancelled
      def self.is_cancellable_state?(state_symbol, enqueued_at_value)
        state = state_symbol&.to_sym
        is_pending = (state == PENDING)
        is_stuck_awaiting_execution = (state == AWAITING_EXECUTION && enqueued_at_value.nil?)
        is_pending || is_stuck_awaiting_execution
      end

      # Checks if a step is in a state where it's a candidate for an enqueue attempt
      def self.is_enqueue_candidate_state?(state_symbol, enqueued_at_value)
        state = state_symbol&.to_sym
        is_pending = (state == PENDING)
        is_stuck_awaiting_execution = (state == AWAITING_EXECUTION && enqueued_at_value.nil?)
        is_pending || is_stuck_awaiting_execution
      end

      # Returns all defined states
      def self.states
        ALL_STATES
      end

      # Returns states considered strictly terminal for normal execution flow
      def self.terminal_states
        TERMINAL_STATES
      end

      # Returns true if transition from one state to another is valid
      def self.valid_transition?(from_state, to_state)
        from = from_state&.to_sym
        to   = to_state&.to_sym
        return false unless ALL_STATES.include?(from) && ALL_STATES.include?(to)
        return false if terminal?(from) # Use the helper method
        VALID_TRANSITIONS[from]&.include?(to) || false
      end

      # Raises an error if the given transition is not valid
      def self.validate_transition!(from_state, to_state)
        unless valid_transition?(from_state, to_state)
          raise Yantra::Errors::InvalidStateTransition,
                "Cannot transition from state :#{from_state} to :#{to_state}"
        end
        true
      end

    end
  end
end
