# test/core/state_transition_service_test.rb
# frozen_string_literal: true

require 'test_helper'
require 'mocha/minitest'

# --- Yantra Requires ---
require 'yantra/core/state_transition_service'
require 'yantra/core/state_machine'
require 'yantra/errors'

# --- Mocks ---
# Using Structs for simple mocks
MockStepTS = Struct.new(:id, :state, keyword_init: true) do
  def initialize(state: 'pending', **kwargs)
    super(state: state.to_sym, **kwargs)
  end
  def state
    self[:state].to_s
  end
  # Helper to get state as symbol for internal test logic
  def state_sym
    self[:state]
  end
end

MockWorkflowTS = Struct.new(:id, :state, keyword_init: true) do
  def initialize(state: 'pending', **kwargs)
    super(state: state.to_sym, **kwargs)
  end
  def state
    self[:state].to_s
  end
  def state_sym
    self[:state]
  end
end

module Yantra
  module Core
    class StateTransitionServiceTest < Minitest::Test
      include StateMachine # Make constants available

      def setup
        @repo = mock('Repository')
        @logger = mock('Logger')
        @logger.stubs(:debug)
        @logger.stubs(:info)
        @logger.stubs(:warn)
        @logger.stubs(:error)

        @service = StateTransitionService.new(repository: @repo, logger: @logger)

        @step_id = "step-#{SecureRandom.uuid}"
        @workflow_id = "wf-#{SecureRandom.uuid}"
        @now = Time.current
      end

      def teardown
        Mocha::Mockery.instance.teardown
      end

      # ==================================
      # transition_step Tests
      # ==================================

      def test_transition_step_success_no_expected_state
        current_step = MockStepTS.new(id: @step_id, state: :scheduling) # Start from SCHEDULING
        new_state = :running
        extra_attrs = { started_at: @now }
        expected_update_attrs = { state: new_state.to_s }.merge(extra_attrs)

        sequence = Mocha::Sequence.new('transition_success')
        @repo.expects(:find_step).with(@step_id).returns(current_step).in_sequence(sequence)
        @repo.expects(:update_step_attributes)
            .with(@step_id, expected_update_attrs, expected_old_state: :scheduling) # Expect current state
            .returns(true).in_sequence(sequence)

        result = @service.transition_step(@step_id, new_state, extra_attrs: extra_attrs)
        assert result, "Should return true on successful transition"
      end

      def test_transition_step_success_with_expected_state
        current_step = MockStepTS.new(id: @step_id, state: :running)
        new_state = :post_processing
        expected_old = :running
        extra_attrs = { performed_at: @now }
        expected_update_attrs = { state: new_state.to_s }.merge(extra_attrs)

        sequence = Mocha::Sequence.new('transition_success_expected')
        @repo.expects(:find_step).with(@step_id).returns(current_step).in_sequence(sequence)
        @repo.expects(:update_step_attributes)
            .with(@step_id, expected_update_attrs, expected_old_state: expected_old) # Uses provided expected state
            .returns(true).in_sequence(sequence)

        result = @service.transition_step(@step_id, new_state, expected_old_state: expected_old, extra_attrs: extra_attrs)
        assert result, "Should return true on successful transition with correct expected state"
      end

      def test_transition_step_fails_on_invalid_transition
        current_step = MockStepTS.new(id: @step_id, state: :pending)
        new_state = :running # Invalid transition from pending

        @repo.expects(:find_step).with(@step_id).returns(current_step)
        @repo.expects(:update_step_attributes).never # Update should not be called
        @logger.expects(:error) # Expect error log

        result = @service.transition_step(@step_id, new_state)
        refute result, "Should return false for invalid transition"
      end

      def test_transition_step_fails_on_expected_state_mismatch
        current_step = MockStepTS.new(id: @step_id, state: :scheduling) # Actual state is SCHEDULING
        new_state = :running
        expected_old = :pending # Mismatched expectation
        expected_update_attrs = { state: new_state.to_s } # Attributes that would be passed

        sequence = Mocha::Sequence.new('transition_fail_mismatch')
        @repo.expects(:find_step).with(@step_id).returns(current_step).in_sequence(sequence)
        # Expect update call with the incorrect expected state and specific attrs
        @repo.expects(:update_step_attributes)
            .with(@step_id, expected_update_attrs, expected_old_state: expected_old) # Use the incorrect expected state
            .returns(false).in_sequence(sequence) # Simulate optimistic lock failure

        # Expect re-fetch for logging
        @repo.expects(:find_step).with(@step_id).returns(current_step).in_sequence(sequence)
        @logger.expects(:warn) # Expect warn log

        result = @service.transition_step(@step_id, new_state, expected_old_state: expected_old)
        refute result, "Should return false on expected state mismatch"
      end

      def test_transition_step_fails_if_step_not_found
        @repo.expects(:find_step).with(@step_id).returns(nil)
        @repo.expects(:update_step_attributes).never
        @logger.expects(:error) # Expect error log

        result = @service.transition_step(@step_id, :running)
        refute result, "Should return false if step not found"
      end

      def test_transition_step_fails_on_persistence_error
        current_step = MockStepTS.new(id: @step_id, state: :enqueued) # Start from ENQUEUED
        new_state = :running
        error = Yantra::Errors::PersistenceError.new("DB write error")
        expected_update_attrs = { state: new_state.to_s } # Attributes that would be passed

        @repo.expects(:find_step).with(@step_id).returns(current_step)
        # Expect update_step_attributes to be called with specific attrs and raise error
        @repo.expects(:update_step_attributes)
            .with(@step_id, expected_update_attrs, expected_old_state: :enqueued) # Expect correct args
            .raises(error)
        @logger.expects(:error) # Expect error log

        result = @service.transition_step(@step_id, new_state)
        refute result, "Should return false on PersistenceError during update"
      end

      def test_transition_step_reraises_unexpected_error
        current_step = MockStepTS.new(id: @step_id, state: :scheduling) # Start from SCHEDULING
        new_state = :running
        error = StandardError.new("Unexpected boom")
        expected_update_attrs = { state: new_state.to_s } # Attributes that would be passed

        @repo.expects(:find_step).with(@step_id).returns(current_step)
        # Expect update_step_attributes to be called with specific attrs and raise error
        @repo.expects(:update_step_attributes)
            .with(@step_id, expected_update_attrs, expected_old_state: :scheduling) # Expect correct args
            .raises(error)
        @logger.expects(:error) # Expect error log

        assert_raises(StandardError, "Unexpected boom") do
          @service.transition_step(@step_id, new_state)
        end
      end

    end # class StateTransitionServiceTest
  end # module Core
end # module Yantra

