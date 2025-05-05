# test/core/step_enqueuer_test.rb
# frozen_string_literal: true

require 'test_helper'
require 'set'
require 'active_support/core_ext/numeric/time' # For .seconds

# --- Yantra Requires ---
require 'yantra/core/step_enqueuer'
require 'yantra/core/state_machine'
require 'yantra/errors'

# --- Mocks ---
# Simple mock for step records used in tests
MockStepRecordSET = Struct.new(
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
    class StepEnqueuerTest < Minitest::Test
      # Make StateMachine constants available
      include StateMachine

      def setup
        @repository = mock('Repository')
        @worker_adapter = mock('WorkerAdapter')
        @notifier = mock('Notifier')
        @logger = mock('Logger')

        # Stub methods that might be called
        @repository.stubs(:bulk_transition_steps)
        @repository.stubs(:find_steps) # General stub
        @repository.stubs(:bulk_update_steps)
        @worker_adapter.stubs(:enqueue)
        @worker_adapter.stubs(:enqueue_in)
        @notifier.stubs(:publish)
        @logger.stubs(:debug)
        @logger.stubs(:info)
        @logger.stubs(:warn)
        @logger.stubs(:error) # Stub error generally

        # Stub respond_to? checks needed by initializer
        @repository.stubs(:respond_to?).with(:bulk_transition_steps).returns(true)
        @repository.stubs(:respond_to?).with(:find_steps).returns(true)

        @enqueuer = StepEnqueuer.new(
          repository: @repository,
          worker_adapter: @worker_adapter,
          notifier: @notifier,
          logger: @logger
        )

        @workflow_id = "wf-#{SecureRandom.uuid}"
        @step1_id = "step1-#{SecureRandom.uuid}"
        @step2_id = "step2-#{SecureRandom.uuid}"
        @step3_id = "step3-#{SecureRandom.uuid}"
        @now = Time.current
      end

      def teardown
        Mocha::Mockery.instance.teardown
      end

      # --- Test Cases ---

      def test_call_returns_empty_if_no_ids_provided
        # No repo calls expected
        assert_equal [], @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: [])
        assert_equal [], @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: nil)
      end

      def test_call_returns_empty_if_transition_fails_or_returns_no_ids
        step_ids = [@step1_id]
        step1_pending = MockStepRecordSET.new(id: @step1_id, state: 'pending')

        @repository.expects(:find_steps).with(step_ids).returns([step1_pending])
        @repository.expects(:bulk_transition_steps)
                   .with([@step1_id], has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING)
                   .returns([]) # Simulate no steps transitioned

        # Ensure subsequent methods are not called
        # Note: Second find_steps *will* be called with empty array []
        @worker_adapter.expects(:enqueue).never
        @repository.expects(:bulk_update_steps).never
        @notifier.expects(:publish).never

        result = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        assert_equal [], result # Expect empty array return value now
      end

      def test_call_handles_transition_persistence_error
        step_ids = [@step1_id]
        step1_pending = MockStepRecordSET.new(id: @step1_id, state: 'pending')
        error = Yantra::Errors::PersistenceError.new("DB write failed during transition")

        @repository.expects(:find_steps).with(step_ids).returns([step1_pending])
        @repository.expects(:bulk_transition_steps)
                   .with([@step1_id], has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING)
                   .raises(error)
        @logger.expects(:error)

        @worker_adapter.expects(:enqueue).never
        @repository.expects(:bulk_update_steps).never
        @notifier.expects(:publish).never

        raised_error = assert_raises(Yantra::Errors::PersistenceError) do
          @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        end
        assert_equal error, raised_error
      end


      def test_call_enqueues_immediate_step_successfully
        step_ids = [@step1_id]
        step1_pending = MockStepRecordSET.new(id: @step1_id, state: 'pending', klass: 'Step1')
        step1_scheduling = MockStepRecordSET.new(id: @step1_id, state: 'scheduling', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: nil, queue: 'q1')

        sequence = Mocha::Sequence.new('enqueue_success')
        @repository.expects(:find_steps).with(step_ids).returns([step1_pending]).in_sequence(sequence)
        @repository.expects(:bulk_transition_steps)
                   .with([@step1_id], has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING)
                   .returns([@step1_id]).in_sequence(sequence)
        @repository.expects(:find_steps)
                   .with([@step1_id])
                   .returns([step1_scheduling]).in_sequence(sequence)
        @worker_adapter.expects(:enqueue)
                       .with(step1_scheduling.id, @workflow_id, step1_scheduling.klass, step1_scheduling.queue)
                       .returns(true).in_sequence(sequence)
        expected_attrs_phase3 = { state: ENQUEUED.to_s, enqueued_at: @now, updated_at: @now }
        @repository.expects(:bulk_update_steps)
                   .with([@step1_id], expected_attrs_phase3)
                   .returns(1).in_sequence(sequence)
        @notifier.expects(:publish)
                 .with('yantra.step.bulk_enqueued', has_entries(enqueued_ids: [@step1_id], enqueued_at: @now))
                 .in_sequence(sequence)

        Time.stub :current, @now do
          processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
          assert_equal [@step1_id], processed_ids
        end
      end

      def test_call_schedules_delayed_step_successfully
        step_ids = [@step1_id]
        delay = 300
        step1_pending = MockStepRecordSET.new(id: @step1_id, state: 'pending', klass: 'Step1')
        step1_scheduling = MockStepRecordSET.new(id: @step1_id, state: 'scheduling', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: delay, queue: 'q1')

        sequence = Mocha::Sequence.new('enqueue_delayed_success')
        @repository.expects(:find_steps).with(step_ids).returns([step1_pending]).in_sequence(sequence)
        @repository.expects(:bulk_transition_steps).with([@step1_id], any_parameters).returns([@step1_id]).in_sequence(sequence)
        @repository.expects(:find_steps).with([@step1_id]).returns([step1_scheduling]).in_sequence(sequence)
        @worker_adapter.expects(:enqueue_in)
                       .with(delay, step1_scheduling.id, @workflow_id, step1_scheduling.klass, step1_scheduling.queue)
                       .returns(true).in_sequence(sequence)
        expected_attrs_phase3 = { state: ENQUEUED.to_s, enqueued_at: @now, updated_at: @now }
        @repository.expects(:bulk_update_steps)
                   .with([@step1_id], expected_attrs_phase3)
                   .returns(1).in_sequence(sequence)
        @notifier.expects(:publish)
                 .with('yantra.step.bulk_enqueued', has_entries(enqueued_ids: [@step1_id], enqueued_at: @now))
                 .in_sequence(sequence)

        Time.stub :current, @now do
          processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
          assert_equal [@step1_id], processed_ids
        end
      end

      def test_call_handles_mix_of_immediate_and_delayed
        step_ids = [@step1_id, @step2_id, @step3_id]
        delay = 60
        step1_p = MockStepRecordSET.new(id: @step1_id, state: 'pending', klass: 'Step1')
        step2_p = MockStepRecordSET.new(id: @step2_id, state: 'pending', klass: 'Step2')
        step3_p = MockStepRecordSET.new(id: @step3_id, state: 'pending', klass: 'Step3')
        initial_steps = [step1_p, step2_p, step3_p]
        step1_s = MockStepRecordSET.new(id: @step1_id, state: 'scheduling', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: nil, queue: 'q1')
        step2_s = MockStepRecordSET.new(id: @step2_id, state: 'scheduling', klass: 'Step2', workflow_id: @workflow_id, delay_seconds: delay, queue: 'q2')
        step3_s = MockStepRecordSET.new(id: @step3_id, state: 'scheduling', klass: 'Step3', workflow_id: @workflow_id, delay_seconds: 0, queue: 'q3')
        all_scheduling_steps = [step1_s, step2_s, step3_s]
        all_ids = all_scheduling_steps.map(&:id)

        sequence = Mocha::Sequence.new('enqueue_mixed_success')
        @repository.expects(:find_steps).with(step_ids).returns(initial_steps).in_sequence(sequence)
        @repository.expects(:bulk_transition_steps).with(all_ids, any_parameters).returns(all_ids).in_sequence(sequence)
        @repository.expects(:find_steps).with(all_ids).returns(all_scheduling_steps).in_sequence(sequence)

        @worker_adapter.expects(:enqueue).with(step1_s.id, @workflow_id, step1_s.klass, step1_s.queue).returns(true)
        @worker_adapter.expects(:enqueue).with(step3_s.id, @workflow_id, step3_s.klass, step3_s.queue).returns(true)
        @worker_adapter.expects(:enqueue_in).with(delay, step2_s.id, @workflow_id, step2_s.klass, step2_s.queue).returns(true)

        expected_attrs_phase3 = { state: ENQUEUED.to_s, enqueued_at: @now, updated_at: @now }
        @repository.expects(:bulk_update_steps)
                   .with { |ids, attrs| ids.is_a?(Array) && ids.sort == all_ids.sort && attrs == expected_attrs_phase3 }
                   .returns(3).in_sequence(sequence)
        @notifier.expects(:publish)
                 .with { |name, payload| name == 'yantra.step.bulk_enqueued' && payload[:enqueued_ids].sort == all_ids.sort && payload[:enqueued_at] == @now }
                 .in_sequence(sequence)

        Time.stub :current, @now do
          processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
          assert_equal all_ids.sort, processed_ids.sort
        end
      end

      # --- CORRECTED: test_call_raises_enqueue_failed_if_adapter_returns_false ---
      def test_call_raises_enqueue_failed_if_adapter_returns_false
        step_ids = [@step1_id, @step2_id]
        step1_p = MockStepRecordSET.new(id: @step1_id, state: 'pending', klass: 'Step1')
        step2_p = MockStepRecordSET.new(id: @step2_id, state: 'pending', klass: 'Step2')
        step1_s = MockStepRecordSET.new(id: @step1_id, state: 'scheduling', klass: 'Step1', queue: 'q1')
        step2_s = MockStepRecordSET.new(id: @step2_id, state: 'scheduling', klass: 'Step2', queue: 'q2')
        all_ids = [@step1_id, @step2_id]

        @repository.expects(:find_steps).with(step_ids).returns([step1_p, step2_p])
        @repository.expects(:bulk_transition_steps).with(all_ids, any_parameters).returns(all_ids)
        @repository.expects(:find_steps).with(all_ids).returns([step1_s, step2_s])

        # Simulate failure for step2
        @worker_adapter.expects(:enqueue).with(step1_s.id, any_parameters).returns(true)
        @worker_adapter.expects(:enqueue).with(step2_s.id, any_parameters).returns(false)

        @repository.expects(:bulk_update_steps).never
        @notifier.expects(:publish).never

        error = assert_raises(Yantra::Errors::EnqueueFailed) do
          Time.stub :current, @now do
            @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
          end
        end
        # Assert that ONLY step2 is in the failed list
        assert_equal [@step2_id], error.failed_ids, "Failed IDs should only include step 2"
      end
      # --- END CORRECTION ---

      # --- CORRECTED: test_call_raises_enqueue_failed_if_adapter_raises_error ---
       def test_call_raises_enqueue_failed_if_adapter_returns_false
        step_ids = [@step1_id, @step2_id]
        # Initial state mocks
        step1_p = MockStepRecordSET.new(id: @step1_id, state: :pending, klass: 'Step1') # Use symbol
        step2_p = MockStepRecordSET.new(id: @step2_id, state: :pending, klass: 'Step2') # Use symbol
        # Mocks for state AFTER transition but BEFORE enqueue attempt
        step1_s = MockStepRecordSET.new(id: @step1_id, state: :scheduling, klass: 'Step1', workflow_id: @workflow_id, queue: 'q1') # Use symbol
        step2_s = MockStepRecordSET.new(id: @step2_id, state: :scheduling, klass: 'Step2', workflow_id: @workflow_id, queue: 'q2') # Use symbol
        all_ids = [@step1_id, @step2_id]

        # Define the sequence of mock calls
        sequence = Mocha::Sequence.new('enqueue_fail_sequence')
        # 1. Initial find_steps to determine states
        @repository.expects(:find_steps).with(step_ids).returns([step1_p, step2_p]).in_sequence(sequence)
        # 2. bulk_transition_steps for pending steps
        @repository.expects(:bulk_transition_steps).with(all_ids, any_parameters).returns(all_ids).in_sequence(sequence)
        # 3. find_steps again to get details for enqueueing (should return steps in 'scheduling' state)
        @repository.expects(:find_steps).with(all_ids).returns([step1_s, step2_s]).in_sequence(sequence)

        # 4. Simulate enqueue attempts
        @worker_adapter.expects(:enqueue).with(step1_s.id, @workflow_id, step1_s.klass, step1_s.queue).returns(true).in_sequence(sequence)
        @worker_adapter.expects(:enqueue).with(step2_s.id, @workflow_id, step2_s.klass, step2_s.queue).returns(false).in_sequence(sequence) # Step 2 fails

        # Ensure Phase 3 is not reached
        @repository.expects(:bulk_update_steps).never
        @notifier.expects(:publish).never

        # Act and Assert
        error = assert_raises(Yantra::Errors::EnqueueFailed) do
          Time.stub :current, @now do
            @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
          end
        end
        # Assert that ONLY step2 is in the failed list
        assert_equal [@step2_id], error.failed_ids, "Failed IDs should only include step 2"
      end



      # --- END CORRECTION ---

      def test_call_handles_phase3_update_failure_gracefully
        step_ids = [@step1_id]
        step1_p = MockStepRecordSET.new(id: @step1_id, state: 'pending', klass: 'Step1')
        step1_s = MockStepRecordSET.new(id: @step1_id, state: 'scheduling', klass: 'Step1', queue: 'q1')
        update_error = Yantra::Errors::PersistenceError.new("DB write failed during final update")

        sequence = Mocha::Sequence.new('phase3_fail')
        @repository.expects(:find_steps).with(step_ids).returns([step1_p]).in_sequence(sequence)
        @repository.expects(:bulk_transition_steps).with([@step1_id], any_parameters).returns([@step1_id]).in_sequence(sequence)
        @repository.expects(:find_steps).with([@step1_id]).returns([step1_s]).in_sequence(sequence)
        @worker_adapter.expects(:enqueue).with(step1_s.id, any_parameters).returns(true).in_sequence(sequence)
        expected_attrs_phase3 = { state: ENQUEUED.to_s, enqueued_at: @now, updated_at: @now }
        @repository.expects(:bulk_update_steps)
                   .with([@step1_id], expected_attrs_phase3)
                   .raises(update_error).in_sequence(sequence)
        @notifier.expects(:publish)
                 .with('yantra.step.bulk_enqueued', has_entries(enqueued_ids: [@step1_id], enqueued_at: @now))
                 .in_sequence(sequence)
        @logger.expects(:error)

        processed_ids = nil
        Time.stub :current, @now do
          processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        end

        assert_equal [@step1_id], processed_ids
      end

      def match_array_in_any_order(expected_array)
        ->(actual_array) { actual_array.is_a?(Array) && actual_array.sort == expected_array.sort }
      end

    end # class StepEnqueuerTest
  end # module Core
end # module Yantra

