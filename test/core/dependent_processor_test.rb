# test/core/dependent_processor_test.rb
# frozen_string_literal: true

require 'test_helper'
require 'set'

# --- Yantra Requires ---
# Assuming test_helper loads necessary Yantra files
require 'yantra/core/dependent_processor'
require 'yantra/core/state_machine'
require 'yantra/errors'

# --- Mocks ---
# Simple mock for step records used in tests
MockStep = Struct.new(:id, :workflow_id, :klass, :state, keyword_init: true)

module Yantra
  module Core
    class DependentProcessorTest < Minitest::Test

      def setup
        # Use Mocha mocks for ALL collaborators
        @repository = mock('Repository')
        @step_enqueuer = mock('StepEnqueuer')
        @logger = mock('Logger')

        # Stub the :call method on step_enqueuer so initializer check passes
        @step_enqueuer.stubs(:call)

        # Stub logger methods - individual tests can add 'expects' if needed
        @logger.stubs(:debug)
        @logger.stubs(:info)
        @logger.stubs(:warn)
        @logger.stubs(:error)

        @processor = DependentProcessor.new(
          repository: @repository,
          step_enqueuer: @step_enqueuer,
          logger: @logger
        )

        # Common IDs
        @workflow_id = "wf-#{SecureRandom.uuid}"
        @finished_step_id = "step-finished-#{SecureRandom.uuid}"
        @dependent1_id = "step-dep1-#{SecureRandom.uuid}"
        @dependent2_id = "step-dep2-#{SecureRandom.uuid}"
        @prereq1_id = "step-prereq1-#{SecureRandom.uuid}"
        @prereq2_id = "step-prereq2-#{SecureRandom.uuid}"
        @grandchild1_id = "step-gc1-#{SecureRandom.uuid}"
      end

      def teardown
        # Mocha verifies expectations automatically if 'expects' was used
      end

      # --- Test Cases ---

      # === Success Path ===

      def test_call_success_no_dependents
        # Arrange
        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns([])

        # Act
        result = @processor.call(
          finished_step_id: @finished_step_id,
          finished_state: StateMachine::SUCCEEDED,
          workflow_id: @workflow_id
        )

        # Assert
        assert_nil result
      end

      def test_call_success_one_dependent_ready
        # Arrange
        dependents = [@dependent1_id]
        parents_map = { @dependent1_id => [@finished_step_id] }
        # --- CORRECTED: Include finished_step_id in expected state fetch ---
        all_involved_ids = [@dependent1_id, @finished_step_id].uniq
        states_map = {
          @dependent1_id.to_s => StateMachine::PENDING.to_s,
          @finished_step_id.to_s => StateMachine::SUCCEEDED.to_s
        }

        # Stub respond_to? to force bulk path
        @repository.stubs(:respond_to?).with(:get_step_states).returns(true)

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        # --- CORRECTED: Expect the correct list of IDs ---
        @repository.expects(:get_step_states).with(all_involved_ids).returns(states_map)

        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: [@dependent1_id])

        # Act
        result = @processor.call(
          finished_step_id: @finished_step_id,
          finished_state: StateMachine::SUCCEEDED,
          workflow_id: @workflow_id
        )

        # Assert
        assert_nil result
      end

      def test_call_success_multiple_dependents_one_ready
        # Arrange
        dependents = [@dependent1_id, @dependent2_id]
        parents_map = {
          @dependent1_id => [@finished_step_id],
          @dependent2_id => [@finished_step_id, @prereq1_id]
        }
        # --- CORRECTED: Include finished_step_id in expected state fetch ---
        all_involved_ids = [@dependent1_id, @dependent2_id, @finished_step_id, @prereq1_id].uniq
        states_map = {
          @dependent1_id.to_s => StateMachine::PENDING.to_s,
          @dependent2_id.to_s => StateMachine::PENDING.to_s,
          @prereq1_id.to_s => StateMachine::PENDING.to_s,
          @finished_step_id.to_s => StateMachine::SUCCEEDED.to_s
        }

        # Stub respond_to? to force bulk path
        @repository.stubs(:respond_to?).with(:get_step_states).returns(true)

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        # --- CORRECTED: Expect the correct list of IDs ---
        @repository.expects(:get_step_states).with(all_involved_ids).returns(states_map)

        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: [@dependent1_id])

        # Act
        result = @processor.call(
          finished_step_id: @finished_step_id,
          finished_state: StateMachine::SUCCEEDED,
          workflow_id: @workflow_id
        )

        # Assert
        assert_nil result
      end

       def test_call_success_dependent_not_pending
        # Arrange
        dependents = [@dependent1_id]
        parents_map = { @dependent1_id => [@finished_step_id] }
         # --- CORRECTED: Include finished_step_id in expected state fetch ---
        all_involved_ids = [@dependent1_id, @finished_step_id].uniq
        states_map = {
          @dependent1_id.to_s => StateMachine::RUNNING.to_s,
          @finished_step_id.to_s => StateMachine::SUCCEEDED.to_s
        }

        # Stub respond_to? to force bulk path
        @repository.stubs(:respond_to?).with(:get_step_states).returns(true)

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        # --- CORRECTED: Expect the correct list of IDs ---
        @repository.expects(:get_step_states).with(all_involved_ids).returns(states_map)

        @step_enqueuer.expects(:call).never

        # Act
        result = @processor.call(
          finished_step_id: @finished_step_id,
          finished_state: StateMachine::SUCCEEDED,
          workflow_id: @workflow_id
        )

        # Assert
        assert_nil result
      end

      # === Failure/Cancellation Path ===

      def test_call_failure_cancels_pending_dependents
        # Arrange
        dependents = [@dependent1_id, @dependent2_id]
        # Stub respond_to? to force non-bulk path
        @repository.stubs(:respond_to?).with(:get_step_states).returns(false)
        @repository.stubs(:respond_to?).with(:get_dependent_ids_bulk).returns(false)

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        # Expectations within find_all_pending_descendants loop (non-bulk)
        @repository.expects(:find_step).with(@dependent1_id).returns(MockStep.new(id: @dependent1_id, state: 'pending'))
        @repository.expects(:get_dependent_ids).with(@dependent1_id).returns([])
        @repository.expects(:find_step).with(@dependent2_id).returns(MockStep.new(id: @dependent2_id, state: 'running'))
        # Expect bulk cancellation ONLY for dependent1
        @repository.expects(:cancel_steps_bulk).with([@dependent1_id]).returns(1)

        # Act
        cancelled_ids = @processor.call(
          finished_step_id: @finished_step_id,
          finished_state: StateMachine::FAILED, # Or CANCELLED
          workflow_id: @workflow_id
        )

        # Assert
        assert_equal [@dependent1_id], cancelled_ids
      end

      def test_call_failure_cancels_pending_descendants_recursively
        # Arrange: finished -> dep1 (pending) -> grandchild1 (pending)
        #          finished -> dep2 (running)
        dependents = [@dependent1_id, @dependent2_id]
        expected_cancelled_ids = [@dependent1_id, @grandchild1_id]

        # Stub respond_to? to force non-bulk path
        @repository.stubs(:respond_to?).with(:get_step_states).returns(false)
        @repository.stubs(:respond_to?).with(:get_dependent_ids_bulk).returns(false)

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        # Expectations within find_all_pending_descendants loop (non-bulk)
        @repository.expects(:find_step).with(@dependent1_id).returns(MockStep.new(id: @dependent1_id, state: 'pending'))
        @repository.expects(:get_dependent_ids).with(@dependent1_id).returns([@grandchild1_id])
        @repository.expects(:find_step).with(@dependent2_id).returns(MockStep.new(id: @dependent2_id, state: 'running'))
        @repository.expects(:find_step).with(@grandchild1_id).returns(MockStep.new(id: @grandchild1_id, state: 'pending'))
        @repository.expects(:get_dependent_ids).with(@grandchild1_id).returns([])

        # --- CORRECTED: Expect the exact array or use a more robust matcher ---
        # Option A: Expect exact array (if order is predictable)
        # @repository.expects(:cancel_steps_bulk).with(expected_cancelled_ids).returns(2)
        # Option B: Use a block matcher for unordered comparison (Safer)
        @repository.expects(:cancel_steps_bulk).with do |actual_ids|
          actual_ids.is_a?(Array) && actual_ids.sort == expected_cancelled_ids.sort
        end.returns(2)
        # --- END CORRECTION ---


         # Act
        cancelled_ids = @processor.call(
          finished_step_id: @finished_step_id,
          finished_state: StateMachine::FAILED,
          workflow_id: @workflow_id
        )

        # Assert
        assert_equal expected_cancelled_ids.sort, cancelled_ids.sort
      end

      # === Error Handling ===

      def test_call_handles_repository_error_gracefully
         # Arrange
        @repository.expects(:get_dependent_ids).with(@finished_step_id).raises(StandardError, "DB Connection Error")
        # Expect logger to be called (simplify expectation)
        @logger.expects(:error) # Expect it to be called at least once

        # Act & Assert
        assert_raises(StandardError, "DB Connection Error") do
           @processor.call(
            finished_step_id: @finished_step_id,
            finished_state: StateMachine::SUCCEEDED,
            workflow_id: @workflow_id
          )
        end
        # Verification handled by Mocha expects
      end

      # Helper to match array contents regardless of order (Keep as is)
      # def match_array_including_only(expected_array)
      #   ->(actual_array) { actual_array.is_a?(Array) && actual_array.sort == expected_array.sort }
      # end

    end # class DependentProcessorTest
  end # module Core
end # module Yantra
