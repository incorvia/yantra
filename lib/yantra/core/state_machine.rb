# lib/yantra/core/state_machine.rb
# frozen_string_literal: true

require 'set'
require_relative '../errors' # Required for InvalidStateTransition

module Yantra
  module Core
    # Defines valid states and transitions for workflows and steps.
    module StateMachine
      # --- Canonical States ---
      PENDING         = :pending           # Initial state, waiting for dependencies or start.
      SCHEDULING      = :scheduling        # Prerequisites met, claimed for enqueueing attempt.
      ENQUEUED        = :enqueued          # Successfully handed off to the background job system.
      RUNNING         = :running           # Worker has picked up the job and started execution.
      POST_PROCESSING = :post_processing   # Step's perform method succeeded, handling dependents.
      SUCCEEDED       = :succeeded         # Step completed successfully, including post-processing.
      FAILED          = :failed            # Step failed permanently (after retries) or critically.
      CANCELLED       = :cancelled         # Step was cancelled before execution started or completed.

      # All valid states
      ALL_STATES = Set[
        PENDING, SCHEDULING, ENQUEUED, RUNNING, POST_PROCESSING, SUCCEEDED, FAILED, CANCELLED
      ].freeze

      # States a step can be in to be considered for starting the transition to scheduling
      RELEASABLE_FROM_STATES = Set[PENDING].freeze

      # States a prerequisite must be in to be considered 'met'
      # POST_PROCESSING is included because dependents can start processing once the parent's core work is done.
      PREREQUISITE_MET_STATES = Set[POST_PROCESSING, SUCCEEDED].freeze

      # States a step can be in to be eligible for cancellation
      # Cancellation targets steps before they are successfully running.
      # PENDING: Definitely cancellable.
      # SCHEDULING: Cancellable (enqueue hasn't succeeded/been confirmed yet).
      # ENQUEUED: Not typically cancellable via Yantra (already in the job queue).
      CANCELLABLE_STATES = Set[PENDING, SCHEDULING].freeze

      # States from which a step can transition to RUNNING (i.e., worker can pick it up)
      # Includes RUNNING for idempotency.
      STARTABLE_STATES = Set[SCHEDULING, ENQUEUED, RUNNING].freeze

      # Terminal states (cannot transition *from* these naturally in standard flow)
      TERMINAL_STATES = Set[
        SUCCEEDED, CANCELLED
      ].freeze # FAILED is not strictly terminal due to retry

      # States indicating work is still potentially in progress or waiting
      NON_TERMINAL_STATES = ALL_STATES - TERMINAL_STATES
      # => Set[:pending, :scheduling, :enqueued, :running, :post_processing, :failed]

      # States representing active work or waiting for active work (used for workflow completion check)
      WORK_IN_PROGRESS_STATES = Set[
        PENDING, SCHEDULING, ENQUEUED, RUNNING, POST_PROCESSING
      ].freeze

      # Allowed transitions between states during normal operation
      VALID_TRANSITIONS = {
        PENDING         => Set[SCHEDULING, CANCELLED].freeze,
        SCHEDULING      => Set[ENQUEUED, RUNNING, FAILED, CANCELLED].freeze, # -> Enqueued (success), Running (immediate pickup), Failed (enqueue error), Cancelled
        ENQUEUED        => Set[RUNNING, FAILED].freeze,                       # -> Running (worker pickup), Failed (worker immediate failure)
        RUNNING         => Set[POST_PROCESSING, FAILED, CANCELLED].freeze,     # -> PostProcessing (success), Failed (runtime error), Cancelled (external)
        POST_PROCESSING => Set[SUCCEEDED, FAILED].freeze,                     # -> Succeeded (final), Failed (post-processing error)
        FAILED          => Set[PENDING, CANCELLED].freeze,                     # -> Pending (retry), Cancelled (external)
        SUCCEEDED       => Set[].freeze,                                       # Terminal
        CANCELLED       => Set[].freeze                                        # Terminal
      }.freeze

      # --- Helper Methods ---

      # Can this state be considered for starting the transition to scheduling?
      def self.can_begin_scheduling?(state_symbol)
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
        # POST_PROCESSING for success path, FAILED/CANCELLED for failure path
        [POST_PROCESSING, FAILED, CANCELLED].include?(state_symbol&.to_sym)
      end

      # Checks if a step is in a state where it can be safely cancelled by Yantra
      # (Replaces the old lambda logic, simplifies based on new states)
      def self.is_cancellable_state?(state_symbol)
        CANCELLABLE_STATES.include?(state_symbol&.to_sym)
      end

      # Checks if a step is in a state where it's a candidate for an enqueue attempt
      # (Includes retries finding steps stuck in SCHEDULING)
      def self.is_enqueue_candidate_state?(state_symbol)
        state = state_symbol&.to_sym
        state == PENDING || state == SCHEDULING
      end

      # Returns all defined states
      def self.states
        ALL_STATES.to_a # Return as array for easier iteration if needed elsewhere
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

    end # module StateMachine
  end # module Core
end # module Yantra
