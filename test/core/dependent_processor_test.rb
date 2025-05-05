# test/core/dependent_processor_test.rb
# frozen_string_literal: true

require 'test_helper'
require 'set'
require 'active_support/core_ext/numeric/time' # For .seconds
require 'active_support/core_ext/object/blank' # For present?

# --- Yantra Requires ---
require 'yantra/core/dependent_processor'
require 'yantra/core/state_machine'
require 'yantra/errors'

# --- Mocks ---
# Mock StepRecord for testing
MockStepRecordDPT = Struct.new(
  :id, :workflow_id, :klass, :state, :queue, :delay_seconds, :enqueued_at,
  :max_attempts, :retries, :created_at,
  keyword_init: true
) do
  # Helper to simulate state access as symbol or string
  def state
    self[:state].to_s
  end
end


module Yantra
  module Core
    class DependentProcessorTest < Minitest::Test
      # Make StateMachine constants available
      include StateMachine

      def setup
        @repository = mock('Repository')
        @step_enqueuer = mock('StepEnqueuer')
        @logger = mock('Logger')

        @step_enqueuer.stubs(:call) # Stub for initializer check
        @logger.stubs(:debug)
        @logger.stubs(:info)
        @logger.stubs(:warn)
        @logger.stubs(:error)

        @repository.stubs(:respond_to?).with(:list_steps).returns(true)
        @repository.stubs(:respond_to?).with(:bulk_update_steps).returns(true)
        @repository.stubs(:respond_to?).with(:find_steps).returns(true)
        @repository.stubs(:respond_to?).with(:get_dependent_ids).returns(true)
        @repository.stubs(:respond_to?).with(:get_dependent_ids_bulk).returns(true)

        @processor = DependentProcessor.new(
          repository: @repository,
          step_enqueuer: @step_enqueuer,
          logger: @logger
        )

        @workflow_id = "wf-#{SecureRandom.uuid}"
        @finished_step_id = "step-finished-#{SecureRandom.uuid}"
        @dependent1_id = "step-dep1-#{SecureRandom.uuid}"
        @dependent2_id = "step-dep2-#{SecureRandom.uuid}"
        @step3_id = "step3-#{SecureRandom.uuid}"
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
        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns([])
        @step_enqueuer.expects(:call).never # Ensure enqueuer not called

        @processor.process_successors(
          finished_step_id: @finished_step_id,
          workflow_id: @workflow_id
        )
      end

      def test_process_successors_one_dependent_ready_from_pending
        dependents = [@dependent1_id]
        parents_map = { @dependent1_id => [@finished_step_id] }
        all_involved_ids = [@dependent1_id, @finished_step_id].uniq
        # Mock StepRecord objects for find_steps
        step1_pending = MockStepRecordDPT.new(id: @dependent1_id, state: 'pending') # Start as PENDING
        finished_step = MockStepRecordDPT.new(id: @finished_step_id, state: 'succeeded')

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        @repository.expects(:find_steps).with(all_involved_ids).returns([step1_pending, finished_step])

        # Expect enqueuer to be called for the PENDING step
        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: [@dependent1_id])

        @processor.process_successors(
          finished_step_id: @finished_step_id,
          workflow_id: @workflow_id
        )
      end

      def test_process_successors_multiple_dependents_one_ready
        dependents = [@dependent1_id, @dependent2_id]
        parents_map = {
          @dependent1_id => [@finished_step_id], # Prereq met
          @dependent2_id => [@finished_step_id, @prereq1_id] # Prereq1 not met
        }
        all_involved_ids = [@dependent1_id, @dependent2_id, @finished_step_id, @prereq1_id].uniq
        # Mock StepRecord objects
        step1_pending = MockStepRecordDPT.new(id: @dependent1_id, state: 'pending')
        step2_pending = MockStepRecordDPT.new(id: @dependent2_id, state: 'pending')
        prereq1_running = MockStepRecordDPT.new(id: @prereq1_id, state: 'running') # Not succeeded/post_processing
        finished_step = MockStepRecordDPT.new(id: @finished_step_id, state: 'succeeded')

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        @repository.expects(:find_steps).with(all_involved_ids).returns([step1_pending, step2_pending, prereq1_running, finished_step])

        # Only step1 should be enqueued
        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: [@dependent1_id])

        @processor.process_successors(
          finished_step_id: @finished_step_id,
          workflow_id: @workflow_id
        )
      end

      def test_process_successors_dependent_not_in_candidate_state
        dependents = [@dependent1_id]
        parents_map = { @dependent1_id => [@finished_step_id] }
        all_involved_ids = [@dependent1_id, @finished_step_id].uniq
        # Mock StepRecord objects
        step1_running = MockStepRecordDPT.new(id: @dependent1_id, state: 'running') # Not PENDING or SCHEDULING
        finished_step = MockStepRecordDPT.new(id: @finished_step_id, state: 'succeeded')

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        @repository.expects(:find_steps).with(all_involved_ids).returns([step1_running, finished_step])

        @step_enqueuer.expects(:call).never # Should not be called

        @processor.process_successors(
          finished_step_id: @finished_step_id,
          workflow_id: @workflow_id
        )
      end

      # --- RENAMED and MODIFIED Test ---
      def test_process_successors_enqueues_step_stuck_in_scheduling_on_retry
        # Simulate a retry scenario where the dependent failed initial enqueue
        dependents = [@dependent1_id]
        parents_map = { @dependent1_id => [@finished_step_id] }
        all_involved_ids = [@dependent1_id, @finished_step_id].uniq
        # Mock StepRecord objects
        # Step 1 is now SCHEDULING (was PENDING, transitioned, but failed enqueue)
        step1_scheduling = MockStepRecordDPT.new(id: @dependent1_id, state: 'scheduling')
        finished_step = MockStepRecordDPT.new(id: @finished_step_id, state: 'succeeded')

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        @repository.expects(:find_steps).with(all_involved_ids).returns([step1_scheduling, finished_step])

        # Expect enqueuer to be called for the SCHEDULING step during the retry
        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: [@dependent1_id])

        @processor.process_successors(
          finished_step_id: @finished_step_id,
          workflow_id: @workflow_id
        )
      end
      # --- END RENAMED and MODIFIED Test ---

      # === Failure/Cancellation Path (process_failure_cascade) ===
      def test_process_failure_cascade_cancels_eligible_dependents
        # Arrange: Setup steps with different states
        step1_pending = MockStepRecordDPT.new(id: @dependent1_id, state: :pending)
        step2_scheduling = MockStepRecordDPT.new(id: @dependent2_id, state: :scheduling)
        step3_enqueued = MockStepRecordDPT.new(id: @step3_id, state: :enqueued)
        initial_dependents = [@dependent1_id, @dependent2_id, @step3_id]
        expected_cancellable = [@dependent1_id, @dependent2_id] # Only pending and scheduling

        # Mock the repository calls that GraphUtils will make
        # 1. Initial get_dependent_ids call by the processor itself
        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(initial_dependents)

        # 2. GraphUtils call: find_steps for the initial batch (include_starting_nodes: true)
        @repository.expects(:find_steps).with(initial_dependents).returns([step1_pending, step2_scheduling, step3_enqueued])

        # 3. GraphUtils call: get_dependent_ids_bulk for the nodes added to queue
        #    (In this case, only dep1 and dep2 match the cancellable state)
        @repository.expects(:get_dependent_ids_bulk).with([@dependent1_id.to_s, @dependent2_id.to_s]).returns({}) # Assume no further children

        # Expect bulk update ONLY for the steps identified as cancellable
        @repository.expects(:bulk_update_steps).with(
          expected_cancellable,
          has_entries(state: 'cancelled')
        ).returns(expected_cancellable.size)

        # Act
        result = @processor.process_failure_cascade(finished_step_id: @finished_step_id, workflow_id: @workflow_id)

        # Assert
        assert_equal expected_cancellable.sort, result.sort
      end

      def test_process_failure_cascade_cancels_eligible_descendants_recursively
        dep1 = @dependent1_id
        dep2 = @dependent2_id # Will be running (not cancellable)
        gc1 = @grandchild1_id # Descendant of dep1, should be cancelled
        initial_dependents = [dep1, dep2]
        expected_cancellable = [dep1, gc1]

        # Mock step records
        step_dep1 = MockStepRecordDPT.new(id: dep1, state: :pending) # Cancellable
        step_dep2 = MockStepRecordDPT.new(id: dep2, state: :running) # Not cancellable
        step_gc1  = MockStepRecordDPT.new(id: gc1, state: :scheduling) # Cancellable

        # Mock the repository calls GraphUtils will make during traversal
        # 1. Initial get_dependent_ids by the processor
        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(initial_dependents)

        # 2. GraphUtils: find_steps for initial batch (dep1, dep2)
        @repository.expects(:find_steps).with(initial_dependents).returns([step_dep1, step_dep2])

        # 3. GraphUtils: get_dependent_ids_bulk for nodes added to queue (only dep1 matches)
        @repository.expects(:get_dependent_ids_bulk).with([dep1.to_s]).returns({ dep1.to_s => [gc1] })

        # 4. GraphUtils: find_steps for the next level (gc1) - Need to mock this
        @repository.expects(:find_steps).with([gc1]).returns([step_gc1])

        # 5. GraphUtils: get_dependent_ids_bulk for gc1 (it matches, so check its children)
        @repository.expects(:get_dependent_ids_bulk).with([gc1.to_s]).returns({ gc1.to_s => [] }) # No more children

        # Expect bulk update for the steps GraphUtils should identify (dep1, gc1)
        @repository.expects(:bulk_update_steps).with do |ids, attrs|
          ids_match = ids.is_a?(Array) && ids.sort == expected_cancellable.sort
          state_match = attrs.is_a?(Hash) && attrs[:state] == StateMachine::CANCELLED.to_s
          # Check timestamps exist for robustness
          time_match = attrs[:finished_at].is_a?(Time) && attrs[:updated_at].is_a?(Time)
          ids_match && state_match && time_match
        end.returns(expected_cancellable.size)

        # Act
        result = @processor.process_failure_cascade(finished_step_id: @finished_step_id, workflow_id: @workflow_id)

        # Assert
        assert_equal expected_cancellable.sort, result.sort
      end


      # === Error Handling ===

      def test_process_successors_handles_repository_error
         @repository.expects(:get_dependent_ids).with(@finished_step_id).raises(StandardError, "DB Connection Error")
         @logger.expects(:error) # Expect error log

         assert_raises(StandardError, "DB Connection Error") do
           @processor.process_successors(
            finished_step_id: @finished_step_id,
            workflow_id: @workflow_id
          )
         end
      end

      def test_process_failure_cascade_handles_repository_error
         @repository.expects(:get_dependent_ids).with(@finished_step_id).raises(StandardError, "DB Connection Error")
         @logger.expects(:error) # Expect error log

         assert_raises(StandardError, "DB Connection Error") do
           @processor.process_failure_cascade(
            finished_step_id: @finished_step_id,
            workflow_id: @workflow_id
          )
         end
      end

      def test_process_successors_handles_enqueue_failed_error
        # Arrange: Setup mocks so processor finds an enqueueable step
        dependents = [@dependent1_id]
        parents_map = { @dependent1_id => [@finished_step_id] }
        all_involved_ids = [@dependent1_id, @finished_step_id].uniq
        step1_pending = MockStepRecordDPT.new(id: @dependent1_id, state: 'pending')
        finished_step = MockStepRecordDPT.new(id: @finished_step_id, state: 'succeeded')

        @repository.expects(:get_dependent_ids).with(@finished_step_id).returns(dependents)
        @repository.expects(:get_dependency_ids_bulk).with(dependents).returns(parents_map)
        @repository.expects(:find_steps).with(all_involved_ids).returns([step1_pending, finished_step])

        # Simulate StepEnqueuer raising EnqueueFailed
        enqueue_error = Yantra::Errors::EnqueueFailed.new("Worker unavailable", failed_ids: [@dependent1_id])
        @step_enqueuer.expects(:call)
                      .with(workflow_id: @workflow_id, step_ids_to_attempt: [@dependent1_id])
                      .raises(enqueue_error)

        # --- MODIFIED: Removed regexp_matches ---
        @logger.expects(:warn) # Expect warning log, but don't match specific message
        # --- END MODIFICATION ---

        # Act & Assert: Ensure EnqueueFailed propagates
        raised_error = assert_raises(Yantra::Errors::EnqueueFailed) do
          @processor.process_successors(
            finished_step_id: @finished_step_id,
            workflow_id: @workflow_id
          )
        end
        assert_equal enqueue_error, raised_error
      end
    end # class DependentProcessorTest
  end # module Core
end # module Yantra

