# test/core/dependent_processor_test.rb
# frozen_string_literal: true

require 'test_helper'
require 'set'
require 'active_support/core_ext/numeric/time' # For .seconds

# --- Yantra Requires ---
# Assuming test_helper loads necessary Yantra files
require 'yantra/core/dependent_processor'
require 'yantra/core/state_machine'
require 'yantra/errors'

# --- Mocks ---
# Simple mock for step records used in tests
# --- CORRECTED: Added :enqueued_at ---
MockStepRecord = Struct.new(
  :id, :workflow_id, :klass, :state, :queue, :delay_seconds, :enqueued_at,
  :max_attempts, :retries, :created_at,
  keyword_init: true
)
# --- END CORRECTED ---

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

        # Common test data
        @workflow_id = "wf-#{SecureRandom.uuid}"
        @finished_step_id = "step-finished-#{SecureRandom.uuid}"
        @dependent1_id = "step-dep1-#{SecureRandom.uuid}"
        @dependent2_id = "step-dep2-#{SecureRandom.uuid}"
        @prereq1_id = "step-prereq1-#{SecureRandom.uuid}"
        @prereq2_id = "step-prereq2-#{SecureRandom.uuid}"
        @grandchild1_id = "step-gc1-#{SecureRandom.uuid}"
        @now = Time.current
      end

      def teardown
        Mocha::Mockery.instance.teardown
      end

      # --- Test Cases ---

      # === Success Path (process_successors) ===

      def test_process_successors_no_dependents
        # Arrange
        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns([])

        # Act
        # Call the specific method for the success path
        @processor.process_successors(
          finished_step_id: @finished_step_id,
          workflow_id: @workflow_id
        )

        # Assert: No error raised, mocks verified by teardown
      end

      def test_process_successors_one_dependent_ready
        # Arrange
        dependents = [@dependent1_id]
        parents_map = { @dependent1_id => [@finished_step_id] }
        all_involved_ids = [@dependent1_id, @finished_step_id].uniq
        # Mock StepRecord objects for find_steps
        step1_pending = MockStepRecord.new(id: @dependent1_id, state: 'pending', enqueued_at: nil)
        finished_step = MockStepRecord.new(id: @finished_step_id, state: 'succeeded')

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        # Expect find_steps instead of get_step_states now
        @repository.expects(:find_steps).with(all_involved_ids).returns([step1_pending, finished_step])

        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: [@dependent1_id])

        # Act
        @processor.process_successors(
          finished_step_id: @finished_step_id,
          workflow_id: @workflow_id
        )

        # Assert: Handled by mock verification
      end

      def test_process_successors_multiple_dependents_one_ready
        # Arrange
        dependents = [@dependent1_id, @dependent2_id]
        parents_map = {
          @dependent1_id => [@finished_step_id],
          @dependent2_id => [@finished_step_id, @prereq1_id]
        }
        all_involved_ids = [@dependent1_id, @dependent2_id, @finished_step_id, @prereq1_id].uniq
        # Mock StepRecord objects
        step1_pending = MockStepRecord.new(id: @dependent1_id, state: 'pending', enqueued_at: nil)
        step2_pending = MockStepRecord.new(id: @dependent2_id, state: 'pending', enqueued_at: nil)
        prereq1_pending = MockStepRecord.new(id: @prereq1_id, state: 'pending') # Not succeeded
        finished_step = MockStepRecord.new(id: @finished_step_id, state: 'succeeded')

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        @repository.expects(:find_steps).with(all_involved_ids).returns([step1_pending, step2_pending, prereq1_pending, finished_step])

        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: [@dependent1_id])

        # Act
        @processor.process_successors(
          finished_step_id: @finished_step_id,
          workflow_id: @workflow_id
        )

        # Assert: Handled by mock verification
      end

      def test_process_successors_dependent_not_pending_or_stuck_awaiting_execution
        # Arrange
        dependents = [@dependent1_id]
        parents_map = { @dependent1_id => [@finished_step_id] }
        all_involved_ids = [@dependent1_id, @finished_step_id].uniq
        # Mock StepRecord objects
        step1_running = MockStepRecord.new(id: @dependent1_id, state: 'running') # Not pending or stuck awaiting_execution
        finished_step = MockStepRecord.new(id: @finished_step_id, state: 'succeeded')

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        @repository.expects(:find_steps).with(all_involved_ids).returns([step1_running, finished_step])

        @step_enqueuer.expects(:call).never

        # Act
        @processor.process_successors(
          finished_step_id: @finished_step_id,
          workflow_id: @workflow_id
        )

        # Assert: Handled by mock verification
      end

      def test_process_successors_handles_stuck_awaiting_execution_step
        # Arrange: Test the case where a dependent failed enqueue previously
        dependents = [@dependent1_id]
        parents_map = { @dependent1_id => [@finished_step_id] }
        all_involved_ids = [@dependent1_id, @finished_step_id].uniq
        # Mock StepRecord objects
        step1_stuck = MockStepRecord.new(id: @dependent1_id, state: 'awaiting_execution', enqueued_at: nil) # Stuck state
        finished_step = MockStepRecord.new(id: @finished_step_id, state: 'succeeded')

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        @repository.expects(:find_steps).with(all_involved_ids).returns([step1_stuck, finished_step])

        # Expect enqueuer to be called for the stuck step
        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: [@dependent1_id])

        # Act
        @processor.process_successors(
          finished_step_id: @finished_step_id,
          workflow_id: @workflow_id
        )

        # Assert: Handled by mock verification
      end


      # === Failure/Cancellation Path (process_failure_cascade) ===
      def test_process_failure_cascade_cancels_eligible_dependents
        dependents = [@dependent1_id, @dependent2_id]
        now = Time.current

        step1_pending = MockStepRecord.new(id: @dependent1_id, state: 'pending', enqueued_at: nil)
        step2_enqueued = MockStepRecord.new(id: @dependent2_id, state: 'awaiting_execution', enqueued_at: now)

        steps_map = {
          @dependent1_id => step1_pending,
          @dependent2_id => step2_enqueued
        }

        dependents_map = {
          @dependent1_id => [],
          @dependent2_id => []
        }

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:find_steps).with(dependents).returns(steps_map.values)
        @repository.expects(:get_dependent_ids_bulk).with(dependents).returns(dependents_map)

        @repository.expects(:bulk_update_steps).with(
          [@dependent1_id],
          has_entries(state: 'cancelled')
        ).returns(1)

        result = @processor.process_failure_cascade(finished_step_id: @finished_step_id, workflow_id: @workflow_id)
        assert_equal [@dependent1_id], result
      end

      def test_process_failure_cascade_cancels_eligible_descendants_recursively
        dep1 = @dependent1_id
        dep2 = @dependent2_id
        gc1 = @grandchild1_id

        dependents = [dep1, dep2]

        step_dep1 = MockStepRecord.new(id: dep1, state: 'pending', enqueued_at: nil)
        step_dep2 = MockStepRecord.new(id: dep2, state: 'running') # Not cancellable
        step_gc1  = MockStepRecord.new(id: gc1, state: 'awaiting_execution', enqueued_at: nil)

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:find_steps).with(dependents).returns([step_dep1, step_dep2])
        @repository.expects(:get_dependent_ids_bulk).with(dependents).returns({ dep1 => [gc1], dep2 => [] })
        @repository.expects(:find_steps).with([gc1]).returns([step_gc1])
        @repository.expects(:get_dependent_ids_bulk).with([gc1]).returns({ gc1 => [] })

        expected = [dep1, gc1]

        @repository.expects(:bulk_update_steps).with do |ids, attrs|
          ids.sort == expected.sort &&
            attrs[:state] == 'cancelled'
        end.returns(2)

        result = @processor.process_failure_cascade(finished_step_id: @finished_step_id, workflow_id: @workflow_id)
        assert_equal expected.sort, result.sort
      end

      # === Error Handling ===

      def test_process_successors_handles_repository_error
         # Arrange
        @repository.expects(:get_dependent_ids).with(@finished_step_id).raises(StandardError, "DB Connection Error")
        @logger.expects(:error) # Expect error log

        # Act & Assert
        assert_raises(StandardError, "DB Connection Error") do
           @processor.process_successors(
            finished_step_id: @finished_step_id,
            workflow_id: @workflow_id
          )
        end
      end

      def test_process_failure_cascade_handles_repository_error
         # Arrange
        @repository.expects(:get_dependent_ids).with(@finished_step_id).raises(StandardError, "DB Connection Error")
        @logger.expects(:error) # Expect error log

        # Act & Assert
        assert_raises(StandardError, "DB Connection Error") do
           @processor.process_failure_cascade(
            finished_step_id: @finished_step_id,
            workflow_id: @workflow_id
          )
        end
      end

      def test_process_successors_enqueues_stuck_awaiting_execution_step
        dependents = [@dependent1_id]
        parents_map = { @dependent1_id => [@finished_step_id] }
        all_involved_ids = [@dependent1_id, @finished_step_id]

        stuck_step = MockStepRecord.new(id: @dependent1_id, state: 'awaiting_execution', enqueued_at: nil)
        succeeded_parent = MockStepRecord.new(id: @finished_step_id, state: 'succeeded')

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        @repository.expects(:find_steps).with(all_involved_ids).returns([stuck_step, succeeded_parent])
        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: [@dependent1_id])

        @processor.process_successors(finished_step_id: @finished_step_id, workflow_id: @workflow_id)
      end

      def test_process_failure_cascade_skips_already_enqueued_awaiting_execution_step
        dependents = [@dependent1_id]
        now = Time.current

        step1_enqueued = MockStepRecord.new(id: @dependent1_id, state: 'awaiting_execution', enqueued_at: now)
        steps_map = { @dependent1_id => step1_enqueued }
        dependents_map = { @dependent1_id => [] }

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:find_steps).with(dependents).returns(steps_map.values)
        @repository.expects(:get_dependent_ids_bulk).with(dependents).returns(dependents_map)

        @repository.expects(:bulk_update_steps).never

        result = @processor.process_failure_cascade(finished_step_id: @finished_step_id, workflow_id: @workflow_id)
        assert_equal [], result
      end

      # Helper to match array contents regardless of order
      def match_array_including_only(expected_array)
        ->(actual_array) { actual_array.is_a?(Array) && actual_array.sort == expected_array.sort }
      end

    end # class DependentProcessorTest
  end # module Core
end # module Yantra

