# lib/yantra/core/state_machine.rb

require 'set' # Use Set for efficient lookups of allowed states

module Yantra
  module Core
    # Defines the canonical states and valid transitions for Yantra Workflows and Jobs.
    # This is a simple, dependency-free implementation used internally to enforce
    # the lifecycle rules. It does not manage state itself, only validates transitions.
    module StateMachine
      # --- Canonical States ---
      # Define states as symbols for consistency.

      PENDING   = :pending    # Initial state, not yet queued or running.
      ENQUEUED  = :enqueued   # Submitted to the background job queue (Jobs only).
      RUNNING   = :running    # Worker processing job / Workflow processing jobs.
      SUCCEEDED = :succeeded  # Completed successfully. Terminal state.
      FAILED    = :failed     # Failed execution, possibly after retries. Potentially retryable.
      CANCELLED = :cancelled  # Explicitly cancelled by user or system. Terminal state.

      # Set of all valid state symbols for quick validation.
      ALL_STATES = Set[
        PENDING, ENQUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED
      ].freeze

      # Set of truly terminal states (cannot transition out of these).
      # Note: FAILED is removed as it can transition to ENQUEUED (retry) or CANCELLED.
      TERMINAL_STATES = Set[
        SUCCEEDED, CANCELLED
      ].freeze

      # Defines valid transitions FROM a state TO other states.
      # This primarily models the *Job* lifecycle, which is slightly more complex.
      # Workflow lifecycle is simpler (e.g., PENDING -> RUNNING -> SUCCEEDED/FAILED/CANCELLED).
      # The validation methods can be used for both, assuming workflow states are a subset.
      VALID_TRANSITIONS = {
        PENDING   => Set[ENQUEUED, CANCELLED, RUNNING].freeze, # Job defined -> Queued or Cancelled or Run immediately?
        ENQUEUED  => Set[RUNNING, CANCELLED].freeze,           # Queued Job -> Picked up by worker or Cancelled
        RUNNING   => Set[SUCCEEDED, FAILED, CANCELLED].freeze, # Executing Job -> Finished or Cancelled
        FAILED    => Set[ENQUEUED, CANCELLED].freeze,          # Failed Job -> Retried (re-enqueued) or Cancelled
        # Terminal states have no valid outgoing transitions defined here
        SUCCEEDED => Set[].freeze,
        CANCELLED => Set[].freeze
      }.freeze # Freeze the main hash for immutability

      # Validates if transitioning from one state to another is allowed.
      #
      # @param from_state [Symbol, String] The current state.
      # @param to_state [Symbol, String] The desired next state.
      # @return [Boolean] true if the transition is valid, false otherwise.
      def self.valid_transition?(from_state, to_state)
        # Normalize inputs to symbols for consistent lookup
        current_sym = from_state&.to_sym
        next_sym = to_state&.to_sym

        # Ensure both states are recognized Yantra states
        unless ALL_STATES.include?(current_sym) && ALL_STATES.include?(next_sym)
          return false
        end

        # Check if 'from' state is strictly terminal (Succeeded, Cancelled)
        # Note: We allow transitions *from* FAILED (e.g., retry)
        return false if TERMINAL_STATES.include?(current_sym)

        # Check if the transition is defined in our hash
        allowed_next_states = VALID_TRANSITIONS[current_sym]
        allowed_next_states&.include?(next_sym) || false
      end

      # Validates a transition and raises an error if invalid.
      # Useful for enforcing state changes before attempting persistence.
      #
      # @param from_state [Symbol, String] The current state.
      # @param to_state [Symbol, String] The desired next state.
      # @raise [Yantra::Errors::InvalidStateTransition] if the transition is not allowed.
      # @return [true] if the transition is valid.
      def self.validate_transition!(from_state, to_state)
        unless valid_transition?(from_state, to_state)
          # Ensure Yantra::Errors::InvalidStateTransition is defined in errors.rb
          raise Yantra::Errors::InvalidStateTransition,
                "Cannot transition from state :#{from_state} to :#{to_state}"
        end
        true
      end

      # Returns the set of all defined states.
      # @return [Set<Symbol>]
      def self.states
        ALL_STATES
      end

       # Returns the set of strictly terminal states (no outgoing transitions allowed).
      # @return [Set<Symbol>]
      def self.terminal_states
        TERMINAL_STATES
      end

      # Checks if a given state is considered strictly terminal (no further transitions possible).
      # Note: FAILED is not considered terminal by this definition if retries are possible.
      # @param state [Symbol, String] The state to check.
      # @return [Boolean]
      def self.terminal?(state)
        TERMINAL_STATES.include?(state&.to_sym)
      end

    end
  end
end

