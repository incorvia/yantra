# lib/yantra/core/state_transition_service.rb (New File)
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
        # Add validation for repository if needed
      end

      # Safely transitions a step's state.
      # Fetches current state, validates transition, performs update.
      # Returns true on successful update, false otherwise (e.g., validation fail, optimistic lock fail).
      def transition_step(step_id, new_state_sym, expected_old_state: nil, extra_attrs: {})
        current_step = repository.find_step(step_id)
        unless current_step
          log_error "Step #{step_id} not found for state transition to #{new_state_sym}."
          return false # Or raise StepNotFound? Returning false might be safer in Orchestrator context
        end
        current_state_sym = current_step.state.to_sym

        # 1. Validate Transition using StateMachine
        unless StateMachine.valid_transition?(current_state_sym, new_state_sym)
          log_error "Invalid state transition attempted for step #{step_id}: #{current_state_sym} -> #{new_state_sym}."
          return false
        end

        # 2. Perform Update using Repository
        # Use the explicitly passed expected_old_state for optimistic locking if provided,
        # otherwise use the fetched current_state_sym.
        optimistic_lock_state = expected_old_state || current_state_sym
        update_attrs = { state: new_state_sym.to_s }.merge(extra_attrs)

        updated = repository.update_step_attributes(
          step_id,
          update_attrs,
          expected_old_state: optimistic_lock_state
        )

        unless updated
          # Log the optimistic lock failure or other update issue
          actual_state = repository.find_step(step_id)&.state || 'unknown' # Re-fetch for accurate logging
          log_warn "Failed to update step #{step_id} to #{new_state_sym}. Expected old state: #{optimistic_lock_state}, actual found: #{actual_state}."
        end

        updated # Return true/false based on update success
      rescue Yantra::Errors::PersistenceError => e
         log_error "PersistenceError during step state transition for #{step_id} to #{new_state_sym}: #{e.message}"
         false # Treat persistence errors during update as failure
      rescue StandardError => e
         log_error "Unexpected error during step state transition for #{step_id} to #{new_state_sym}: #{e.class} - #{e.message}"
         raise e # Re-raise unexpected errors
      end

      private

      def log_info(msg);  @logger&.info  { "[StateTransitionService] #{msg}" } end
      def log_warn(msg);  @logger&.warn  { "[StateTransitionService] #{msg}" } end
      def log_error(msg); @logger&.error { "[StateTransitionService] #{msg}" } end
      def log_debug(msg); @logger&.debug { "[StateTransitionService] #{msg}" } end

    end
  end
end
