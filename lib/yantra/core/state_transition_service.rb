# lib/yantra/core/state_transition_service.rb (New File)
# lib/yantra/core/state_transition_service.rb
# frozen_string_literal: true

require_relative 'state_machine'
require_relative '../errors'
require_relative '../persistence/repository_interface'

module Yantra
  module Core
    class StateTransitionService
      attr_reader :repository, :logger

      def initialize(repository: Yantra.repository, logger: Yantra.logger)
        @repository = repository
        @logger = logger || Logger.new(IO::NULL)
      end

      # Safely transitions a step's state with validation and optimistic locking.
      def transition_step(step_id, new_state_sym, expected_old_state: nil, extra_attrs: {})
        current_step = repository.find_step(step_id)

        unless current_step
          log_error "Step #{step_id} not found for state transition to #{new_state_sym}."
          return false
        end

        current_state = current_step.state.to_sym

        unless StateMachine.valid_transition?(current_state, new_state_sym)
          log_error "Invalid state transition for step #{step_id}: #{current_state} -> #{new_state_sym}."
          return false
        end

        optimistic_lock = expected_old_state || current_state
        update_attrs = { state: new_state_sym.to_s }.merge(extra_attrs)

        updated = repository.update_step_attributes(
          step_id,
          update_attrs,
          expected_old_state: optimistic_lock
        )

        unless updated
          actual_state = repository.find_step(step_id)&.state || 'unknown'
          log_warn "Failed to update step #{step_id} to #{new_state_sym}. Expected: #{optimistic_lock}, Found: #{actual_state}."
        end

        updated
      rescue Yantra::Errors::PersistenceError => e
        log_error "PersistenceError for step #{step_id} -> #{new_state_sym}: #{e.message}"
        false
      rescue => e
        log_error "Unexpected error for step #{step_id} -> #{new_state_sym}: #{e.class} - #{e.message}"
        raise
      end

      private

      def log_info(msg)
        logger&.info { "[StateTransitionService] #{msg}" }
      end

      def log_warn(msg)
        logger&.warn { "[StateTransitionService] #{msg}" }
      end

      def log_error(msg)
        logger&.error { "[StateTransitionService] #{msg}" }
      end

      def log_debug(msg)
        logger&.debug { "[StateTransitionService] #{msg}" }
      end
    end
  end
end

