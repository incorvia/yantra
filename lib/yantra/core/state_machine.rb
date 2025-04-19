# lib/yantra/core/state_machine.rb

require 'set'

module Yantra
  module Core
    # Defines valid states and transitions for workflows and steps.
    module StateMachine
      # Canonical states
      PENDING   = :pending
      ENQUEUED  = :enqueued
      RUNNING   = :running
      SUCCEEDED = :succeeded
      FAILED    = :failed
      CANCELLED = :cancelled

      # All valid states
      ALL_STATES = Set[
        PENDING, ENQUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED
      ].freeze

      # Terminal states (cannot transition from these)
      TERMINAL_STATES = Set[
        SUCCEEDED, CANCELLED
      ].freeze

      # Allowed transitions between states
      VALID_TRANSITIONS = {
        PENDING   => Set[ENQUEUED, CANCELLED, RUNNING].freeze,
        ENQUEUED  => Set[RUNNING, CANCELLED].freeze,
        RUNNING   => Set[SUCCEEDED, FAILED, CANCELLED].freeze,
        FAILED    => Set[ENQUEUED, CANCELLED].freeze,
        SUCCEEDED => Set[].freeze,
        CANCELLED => Set[].freeze
      }.freeze

      # Returns true if transition from one state to another is valid
      def self.valid_transition?(from_state, to_state)
        from = from_state&.to_sym
        to   = to_state&.to_sym

        return false unless ALL_STATES.include?(from) && ALL_STATES.include?(to)
        return false if TERMINAL_STATES.include?(from)

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

      # Returns all defined states
      def self.states
        ALL_STATES
      end

      # Returns terminal states
      def self.terminal_states
        TERMINAL_STATES
      end

      # Returns true if the given state is terminal
      def self.terminal?(state)
        TERMINAL_STATES.include?(state&.to_sym)
      end
    end
  end
end

