# frozen_string_literal: true

require 'set'
require_relative '../errors' # Required for InvalidStateTransition

module Yantra
  module Core
    # Defines valid states and transitions for workflows and steps.
    module StateMachine
      # --- Canonical States ---
      PENDING         = :pending
      SCHEDULING      = :scheduling
      ENQUEUED        = :enqueued
      RUNNING         = :running
      POST_PROCESSING = :post_processing
      SUCCEEDED       = :succeeded
      FAILED          = :failed
      CANCELLED       = :cancelled

      ALL_STATES = Set[
        PENDING, SCHEDULING, ENQUEUED, RUNNING,
        POST_PROCESSING, SUCCEEDED, FAILED, CANCELLED
      ].freeze

      RELEASABLE_FROM_STATES = Set[PENDING].freeze

      PREREQUISITE_MET_STATES = Set[
        POST_PROCESSING, SUCCEEDED
      ].freeze

      CANCELLABLE_STATES = Set[
        PENDING, SCHEDULING
      ].freeze

      STARTABLE_STATES = Set[
        SCHEDULING, ENQUEUED, RUNNING
      ].freeze

      TERMINAL_STATES = Set[
        SUCCEEDED, CANCELLED
      ].freeze

      NON_TERMINAL_STATES = ALL_STATES - TERMINAL_STATES

      WORK_IN_PROGRESS_STATES = Set[
        PENDING, SCHEDULING, ENQUEUED, RUNNING, POST_PROCESSING
      ].freeze

      VALID_TRANSITIONS = {
        PENDING         => Set[SCHEDULING, CANCELLED].freeze,
        SCHEDULING      => Set[ENQUEUED, RUNNING, FAILED, CANCELLED].freeze,
        ENQUEUED        => Set[RUNNING, FAILED].freeze,
        RUNNING         => Set[POST_PROCESSING, FAILED, CANCELLED].freeze,
        POST_PROCESSING => Set[SUCCEEDED, FAILED].freeze,
        FAILED          => Set[PENDING, CANCELLED].freeze,
        SUCCEEDED       => Set[].freeze,
        CANCELLED       => Set[].freeze
      }.freeze

      # --- Helper Methods ---

      def self.can_begin_scheduling?(state_symbol)
        RELEASABLE_FROM_STATES.include?(state_symbol&.to_sym)
      end

      def self.prerequisite_met?(state_symbol)
        PREREQUISITE_MET_STATES.include?(state_symbol&.to_sym)
      end

      def self.eligible_for_perform?(state_symbol)
        STARTABLE_STATES.include?(state_symbol&.to_sym)
      end

      def self.terminal?(state_symbol)
        TERMINAL_STATES.include?(state_symbol&.to_sym)
      end

      def self.triggers_downstream_processing?(state_symbol)
        [POST_PROCESSING, FAILED, CANCELLED].include?(state_symbol&.to_sym)
      end

      def self.is_cancellable_state?(state_symbol)
        CANCELLABLE_STATES.include?(state_symbol&.to_sym)
      end

      def self.is_enqueue_candidate_state?(state_symbol)
        %i[pending scheduling].include?(state_symbol&.to_sym)
      end

      def self.states
        ALL_STATES.to_a
      end

      def self.terminal_states
        TERMINAL_STATES
      end

      def self.valid_transition?(from_state, to_state)
        from = from_state&.to_sym
        to   = to_state&.to_sym

        return false unless ALL_STATES.include?(from) && ALL_STATES.include?(to)
        return false if terminal?(from)

        VALID_TRANSITIONS[from]&.include?(to) || false
      end

      def self.validate_transition!(from_state, to_state)
        return true if valid_transition?(from_state, to_state)

        raise Yantra::Errors::InvalidStateTransition,
              "Cannot transition from state :#{from_state} to :#{to_state}"
      end
    end
  end
end

