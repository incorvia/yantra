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
  # Helper to get state as symbol for internal test logic
  def state_sym
    self[:state]
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
        # @repository.stubs(:bulk_update_steps) # No longer expected in main flow
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

      def test_call_returns_empty_if_transition_to_scheduling_fails_or_returns_no_ids
        step_ids = [@step1_id]
        step1_pending = MockStepRecordSET.new(id: @step1_id, state: :pending) # Use symbol

        # Expect initial find_steps
        @repository.expects(:find_steps).with(step_ids).returns([step1_pending])
        # Expect bulk_transition_steps to be called and return empty
        @repository.expects(:bulk_transition_steps)
                   .with([@step1_id], has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING)
                   .returns([]) # Simulate no steps transitioned

        # Ensure subsequent methods are not called
        # Note: Second find_steps *will* be called with empty array []
        @worker_adapter.expects(:enqueue).never
        # No longer expect bulk_update_steps here
        @notifier.expects(:publish).never

        result = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        assert_equal [], result # Expect empty array return value now
      end

      def test_call_handles_transition_to_scheduling_persistence_error
        step_ids = [@step1_id]
        step1_pending = MockStepRecordSET.new(id: @step1_id, state: :pending) # Use symbol
        error = Yantra::Errors::PersistenceError.new("DB write failed during transition")

        @repository.expects(:find_steps).with(step_ids).returns([step1_pending])
        @repository.expects(:bulk_transition_steps)
                   .with([@step1_id], has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING)
                   .raises(error)
        @logger.expects(:error)

        @worker_adapter.expects(:enqueue).never
        # No longer expect bulk_update_steps here
        @notifier.expects(:publish).never

        raised_error = assert_raises(Yantra::Errors::PersistenceError) do
          @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        end
        assert_equal error, raised_error
      end


      def test_call_enqueues_immediate_step_successfully
        step_ids = [@step1_id]
        step1_pending = MockStepRecordSET.new(id: @step1_id, state: :pending, klass: 'Step1') # Use symbol
        step1_scheduling = MockStepRecordSET.new(id: @step1_id, state: :scheduling, klass: 'Step1', workflow_id: @workflow_id, delay_seconds: nil, queue: 'q1') # Use symbol

        sequence = Mocha::Sequence.new('enqueue_success')

        # Phase 1: Transition PENDING -> SCHEDULING
        @repository.expects(:find_steps).with(step_ids).returns([step1_pending]).in_sequence(sequence)
        @repository.expects(:bulk_transition_steps)
                   .with([@step1_id], has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING)
                   .returns([@step1_id]).in_sequence(sequence) # Return transitioned ID

        # Phase 2: Enqueue with worker
        @repository.expects(:find_steps)
                   .with([@step1_id])
                   .returns([step1_scheduling]).in_sequence(sequence) # Find the step (now scheduling)
        @worker_adapter.expects(:enqueue)
                       .with(step1_scheduling.id, @workflow_id, step1_scheduling.klass, step1_scheduling.queue)
                       .returns(true).in_sequence(sequence) # Simulate successful enqueue

        # Phase 3: Transition SCHEDULING -> ENQUEUED (using bulk_transition_steps)
        expected_attrs_phase3 = { state: ENQUEUED.to_s, enqueued_at: @now, updated_at: @now }
        @repository.expects(:bulk_transition_steps) # <<< CHANGED: Use bulk_transition_steps
                   .with([@step1_id], expected_attrs_phase3, expected_old_state: SCHEDULING) # <<< CHANGED: Added expected_old_state
                   .returns([@step1_id]).in_sequence(sequence) # Return ID transitioned to ENQUEUED

        # Phase 4: Publish event
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
        step1_pending = MockStepRecordSET.new(id: @step1_id, state: :pending, klass: 'Step1') # Use symbol
        step1_scheduling = MockStepRecordSET.new(id: @step1_id, state: :scheduling, klass: 'Step1', workflow_id: @workflow_id, delay_seconds: delay, queue: 'q1') # Use symbol

        sequence = Mocha::Sequence.new('enqueue_delayed_success')

        # Phase 1: Transition PENDING -> SCHEDULING
        @repository.expects(:find_steps).with(step_ids).returns([step1_pending]).in_sequence(sequence)
        @repository.expects(:bulk_transition_steps).with([@step1_id], has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING).returns([@step1_id]).in_sequence(sequence)

        # Phase 2: Enqueue with worker (delayed)
        @repository.expects(:find_steps).with([@step1_id]).returns([step1_scheduling]).in_sequence(sequence)
        @worker_adapter.expects(:enqueue_in)
                       .with(delay, step1_scheduling.id, @workflow_id, step1_scheduling.klass, step1_scheduling.queue)
                       .returns(true).in_sequence(sequence)

        # Phase 3: Transition SCHEDULING -> ENQUEUED
        expected_attrs_phase3 = { state: ENQUEUED.to_s, enqueued_at: @now, updated_at: @now }
        @repository.expects(:bulk_transition_steps) # <<< CHANGED
                   .with([@step1_id], expected_attrs_phase3, expected_old_state: SCHEDULING) # <<< CHANGED
                   .returns([@step1_id]).in_sequence(sequence)

        # Phase 4: Publish event
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
        step1_p = MockStepRecordSET.new(id: @step1_id, state: :pending, klass: 'Step1') # Use symbol
        step2_p = MockStepRecordSET.new(id: @step2_id, state: :pending, klass: 'Step2') # Use symbol
        step3_p = MockStepRecordSET.new(id: @step3_id, state: :pending, klass: 'Step3') # Use symbol
        initial_steps = [step1_p, step2_p, step3_p]
        step1_s = MockStepRecordSET.new(id: @step1_id, state: :scheduling, klass: 'Step1', workflow_id: @workflow_id, delay_seconds: nil, queue: 'q1') # Use symbol
        step2_s = MockStepRecordSET.new(id: @step2_id, state: :scheduling, klass: 'Step2', workflow_id: @workflow_id, delay_seconds: delay, queue: 'q2') # Use symbol
        step3_s = MockStepRecordSET.new(id: @step3_id, state: :scheduling, klass: 'Step3', workflow_id: @workflow_id, delay_seconds: 0, queue: 'q3') # Use symbol
        all_scheduling_steps = [step1_s, step2_s, step3_s]
        all_ids = all_scheduling_steps.map(&:id)

        sequence = Mocha::Sequence.new('enqueue_mixed_success')

        # Phase 1: Transition PENDING -> SCHEDULING
        @repository.expects(:find_steps).with(step_ids).returns(initial_steps).in_sequence(sequence)
        @repository.expects(:bulk_transition_steps).with(all_ids, has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING).returns(all_ids).in_sequence(sequence)

        # Phase 2: Enqueue with worker
        @repository.expects(:find_steps).with(all_ids).returns(all_scheduling_steps).in_sequence(sequence)
        @worker_adapter.expects(:enqueue).with(step1_s.id, @workflow_id, step1_s.klass, step1_s.queue).returns(true)
        @worker_adapter.expects(:enqueue).with(step3_s.id, @workflow_id, step3_s.klass, step3_s.queue).returns(true)
        @worker_adapter.expects(:enqueue_in).with(delay, step2_s.id, @workflow_id, step2_s.klass, step2_s.queue).returns(true)

        # Phase 3: Transition SCHEDULING -> ENQUEUED
        expected_attrs_phase3 = { state: ENQUEUED.to_s, enqueued_at: @now, updated_at: @now }
        # Use a block matcher for flexibility with ID order
        @repository.expects(:bulk_transition_steps) # <<< CHANGED
                   .with(
                     all_ids,
                     expected_attrs_phase3,
                     expected_old_state: SCHEDULING # <<< CHANGED
                   )
                   .returns(all_ids).in_sequence(sequence) # <<< FIX: Ensure returns array

        # Phase 4: Publish event
        @notifier.expects(:publish)
                 .with { |name, payload| name == 'yantra.step.bulk_enqueued' && payload[:enqueued_ids].sort == all_ids.sort && payload[:enqueued_at] == @now }
                 .in_sequence(sequence)

        Time.stub :current, @now do
          processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
          assert_equal all_ids.sort, processed_ids.sort
        end
      end

      def test_call_raises_enqueue_failed_if_adapter_returns_false
        step_ids = [@step1_id, @step2_id]
        step1_p = MockStepRecordSET.new(id: @step1_id, state: :pending, klass: 'Step1') # Use symbol
        step2_p = MockStepRecordSET.new(id: @step2_id, state: :pending, klass: 'Step2') # Use symbol
        step1_s = MockStepRecordSET.new(id: @step1_id, state: :scheduling, klass: 'Step1', workflow_id: @workflow_id, queue: 'q1') # Use symbol
        step2_s = MockStepRecordSET.new(id: @step2_id, state: :scheduling, klass: 'Step2', workflow_id: @workflow_id, queue: 'q2') # Use symbol
        all_ids = [@step1_id, @step2_id]

        sequence = Mocha::Sequence.new('enqueue_fail_sequence')

        # Phase 1: Transition PENDING -> SCHEDULING
        @repository.expects(:find_steps).with(step_ids).returns([step1_p, step2_p]).in_sequence(sequence)
        @repository.expects(:bulk_transition_steps).with(all_ids, has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING).returns(all_ids).in_sequence(sequence)

        # Phase 2: Enqueue with worker (step 2 fails)
        @repository.expects(:find_steps).with(all_ids).returns([step1_s, step2_s]).in_sequence(sequence)
        @worker_adapter.expects(:enqueue).with(step1_s.id, @workflow_id, step1_s.klass, step1_s.queue).returns(true).in_sequence(sequence)
        @worker_adapter.expects(:enqueue).with(step2_s.id, @workflow_id, step2_s.klass, step2_s.queue).returns(false).in_sequence(sequence) # Step 2 fails

        # Phase 3: Transition SCHEDULING -> ENQUEUED (only for step 1)
        expected_attrs_phase3 = { state: ENQUEUED.to_s, enqueued_at: @now, updated_at: @now }
        @repository.expects(:bulk_transition_steps) # <<< CHANGED
                   .with([@step1_id], expected_attrs_phase3, expected_old_state: SCHEDULING) # <<< CHANGED: Only step 1
                   .returns([@step1_id]).in_sequence(sequence)

        # Phase 4: Publish event (only for step 1)
        @notifier.expects(:publish)
                 .with('yantra.step.bulk_enqueued', has_entries(enqueued_ids: [@step1_id], enqueued_at: @now)) # <<< CHANGED: Only step 1
                 .in_sequence(sequence)

        # Act & Assert: Expect EnqueueFailed error
        error = assert_raises(Yantra::Errors::EnqueueFailed) do
          Time.stub :current, @now do
            @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
          end
        end
        # Assert that ONLY step2 is in the failed list
        assert_equal [@step2_id], error.failed_ids, "Failed IDs should only include step 2"
      end

      def test_call_raises_enqueue_failed_if_adapter_raises_error
        step_ids = [@step1_id]
        step1_p = MockStepRecordSET.new(id: @step1_id, state: :pending, klass: 'Step1') # Use symbol
        step1_s = MockStepRecordSET.new(id: @step1_id, state: :scheduling, klass: 'Step1', workflow_id: @workflow_id, queue: 'q1') # Use symbol
        enqueue_error = StandardError.new("Redis connection lost")

        sequence = Mocha::Sequence.new('enqueue_raise_fail')

        # Phase 1: Transition PENDING -> SCHEDULING (Should happen)
        @repository.expects(:find_steps).with(step_ids).returns([step1_p]).in_sequence(sequence)
        @repository.expects(:bulk_transition_steps)
                   .with([@step1_id], has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING)
                   .returns([@step1_id]).in_sequence(sequence)

        # Phase 2: Enqueue with worker (raises error)
        @repository.expects(:find_steps).with([@step1_id]).returns([step1_s]).in_sequence(sequence)
        @worker_adapter.expects(:enqueue)
                       .with(step1_s.id, @workflow_id, step1_s.klass, step1_s.queue)
                       .raises(enqueue_error).in_sequence(sequence)

        # Phase 3: Transition SCHEDULING -> ENQUEUED (Should NOT happen)
        @repository.expects(:bulk_transition_steps)
                   .with(any_parameters, any_parameters, has_entry(expected_old_state: SCHEDULING)) # Match the specific call signature
                   .never # <<< FIX: Expect Phase 3 transition to never happen

        # Phase 4: Publish event (Should NOT happen)
        @notifier.expects(:publish).never

        # Act & Assert: Expect EnqueueFailed error
        error = assert_raises(Yantra::Errors::EnqueueFailed) do
           Time.stub :current, @now do
            @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
           end
        end
        assert_includes error.failed_ids, @step1_id
      end

      # <<< FIX: Updated test name and assertions >>>
      def test_call_returns_enqueued_id_when_phase3_transition_fails
        step_ids = [@step1_id]
        step1_p = MockStepRecordSET.new(id: @step1_id, state: :pending, klass: 'Step1') # Use symbol
        step1_s = MockStepRecordSET.new(id: @step1_id, state: :scheduling, klass: 'Step1', workflow_id: @workflow_id, queue: 'q1') # Use symbol
        transition_error = Yantra::Errors::PersistenceError.new("DB write failed during final transition")

        sequence = Mocha::Sequence.new('phase3_fail')

        # Phase 1: Transition PENDING -> SCHEDULING
        @repository.expects(:find_steps).with(step_ids).returns([step1_p]).in_sequence(sequence)
        @repository.expects(:bulk_transition_steps).with([@step1_id], has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING).returns([@step1_id]).in_sequence(sequence)

        # Phase 2: Enqueue with worker (succeeds)
        @repository.expects(:find_steps).with([@step1_id]).returns([step1_s]).in_sequence(sequence)
        @worker_adapter.expects(:enqueue).with(step1_s.id, @workflow_id, step1_s.klass, step1_s.queue).returns(true).in_sequence(sequence)

        # Phase 3: Transition SCHEDULING -> ENQUEUED (raises error)
        expected_attrs_phase3 = { state: ENQUEUED.to_s, enqueued_at: @now, updated_at: @now }
        @repository.expects(:bulk_transition_steps) # <<< CHANGED
                   .with([@step1_id], expected_attrs_phase3, expected_old_state: SCHEDULING) # <<< CHANGED
                   .raises(transition_error).in_sequence(sequence)

        # Phase 4: Publish event (SHOULD STILL BE CALLED)
        @notifier.expects(:publish)
                 .with('yantra.step.bulk_enqueued', has_entries(enqueued_ids: [@step1_id], enqueued_at: @now))
                 .in_sequence(sequence)

        # Expect error log for the failed transition
        @logger.expects(:error)

        # Act: Call the enqueuer
        processed_ids = nil
        Time.stub :current, @now do
          # The call should NOT raise the PersistenceError from Phase 3, but log it.
          # It should still return the ID that was successfully enqueued in Phase 2.
          processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        end

        # Assert: The ID successfully handed off to the worker should be returned
        assert_equal [@step1_id], processed_ids, "Should return ID enqueued in Phase 2 despite Phase 3 DB error"
      end
      # <<< END FIX >>>

    end # class StepEnqueuerTest
  end # module Core
end # module Yantra

