# test/core/step_enqueuer_test.rb
# frozen_string_literal: true

require 'test_helper'
require 'set'
require 'active_support/core_ext/numeric/time' # For .seconds

require 'yantra/core/step_enqueuer'
require 'yantra/core/state_machine'
require 'yantra/errors'

# Simple mock for step records used in tests
MockStepRecordSET = Struct.new(
  :id, :workflow_id, :klass, :state, :queue, :delay_seconds, :enqueued_at,
  :max_attempts, :retries, :created_at,
  keyword_init: true
) do
  def state
    self[:state].to_s
  end
end

module Yantra
  module Core
    class StepEnqueuerTest < Minitest::Test
      include StateMachine

      def setup
        @repository = mock('Repository')
        @worker_adapter = mock('WorkerAdapter')
        @notifier = mock('Notifier')
        @logger = mock('Logger')

        @repository.stubs(:bulk_transition_steps)
        @repository.stubs(:find_steps)
        @repository.stubs(:bulk_update_steps)
        @worker_adapter.stubs(:enqueue)
        @worker_adapter.stubs(:enqueue_in)
        @notifier.stubs(:publish)
        @logger.stubs(:debug)
        @logger.stubs(:info)
        @logger.stubs(:warn)
        @logger.stubs(:error)

        @repository.stubs(:respond_to?).with(:bulk_transition_steps).returns(true)

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

      def test_call_returns_empty_if_no_ids_provided
        assert_equal [], @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: [])
        assert_equal [], @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: nil)
      end

      def test_call_returns_empty_if_transition_fails_or_returns_no_ids
        step_ids = [@step1_id]
        @repository.expects(:bulk_transition_steps)
                   .with(step_ids, has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING)
                   .returns([])

        @repository.expects(:find_steps).never
        @worker_adapter.expects(:enqueue).never
        @repository.expects(:bulk_update_steps).never
        @notifier.expects(:publish).never

        result = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        assert_equal [], result
      end

      def test_call_handles_transition_persistence_error
        step_ids = [@step1_id]
        error = Yantra::Errors::PersistenceError.new("DB write failed during transition")

        @repository.expects(:bulk_transition_steps)
                   .with(step_ids, has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING)
                   .raises(error)
        @logger.expects(:error)

        @repository.expects(:find_steps).never
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
        step1_scheduling = MockStepRecordSET.new(id: @step1_id, state: 'scheduling', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: nil, queue: 'q1')

        sequence = Mocha::Sequence.new('enqueue_success')

        @repository.expects(:bulk_transition_steps)
                   .with(step_ids, has_entry(state: SCHEDULING.to_s), expected_old_state: PENDING)
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
        step1_scheduling = MockStepRecordSET.new(id: @step1_id, state: 'scheduling', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: delay, queue: 'q1')

        sequence = Mocha::Sequence.new('enqueue_delayed_success')

        @repository.expects(:bulk_transition_steps).with(step_ids, any_parameters).returns([@step1_id]).in_sequence(sequence)
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
        step1 = MockStepRecordSET.new(id: @step1_id, state: 'scheduling', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: nil, queue: 'q1')
        step2 = MockStepRecordSET.new(id: @step2_id, state: 'scheduling', klass: 'Step2', workflow_id: @workflow_id, delay_seconds: delay, queue: 'q2')
        step3 = MockStepRecordSET.new(id: @step3_id, state: 'scheduling', klass: 'Step3', workflow_id: @workflow_id, delay_seconds: 0, queue: 'q3')
        all_scheduling_steps = [step1, step2, step3]
        all_ids = all_scheduling_steps.map(&:id)

        sequence = Mocha::Sequence.new('enqueue_mixed_success')
        @repository.expects(:bulk_transition_steps).with(step_ids, any_parameters).returns(all_ids).in_sequence(sequence)
        @repository.expects(:find_steps).with(all_ids).returns(all_scheduling_steps).in_sequence(sequence)

        @worker_adapter.expects(:enqueue).with(step1.id, @workflow_id, step1.klass, step1.queue).returns(true)
        @worker_adapter.expects(:enqueue).with(step3.id, @workflow_id, step3.klass, step3.queue).returns(true)
        @worker_adapter.expects(:enqueue_in).with(delay, step2.id, @workflow_id, step2.klass, step2.queue).returns(true)

        expected_attrs_phase3 = { state: ENQUEUED.to_s, enqueued_at: @now, updated_at: @now }
        @repository.expects(:bulk_update_steps)
                   .with { |ids, attrs| ids.is_a?(Array) && ids.sort == all_ids.sort && attrs == expected_attrs_phase3 }
                   .returns(3).in_sequence(sequence)

        @notifier.expects(:publish)
                 .with do |event_name, payload|
                    event_name == 'yantra.step.bulk_enqueued' &&
                    payload[:workflow_id] == @workflow_id &&
                    payload[:enqueued_ids].is_a?(Array) &&
                    payload[:enqueued_ids].sort == all_ids.sort &&
                    payload[:enqueued_at] == @now
                 end
                 .in_sequence(sequence)

        Time.stub :current, @now do
          processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
          assert_equal all_ids.sort, processed_ids.sort
        end
      end

      def test_call_raises_enqueue_failed_if_adapter_returns_false
        step_ids = [@step1_id, @step2_id]
        step1 = MockStepRecordSET.new(id: @step1_id, state: 'scheduling', klass: 'Step1', queue: 'q1')
        step2 = MockStepRecordSET.new(id: @step2_id, state: 'scheduling', klass: 'Step2', queue: 'q2')
        all_scheduling_steps = [step1, step2]
        all_ids = all_scheduling_steps.map(&:id)

        @repository.expects(:bulk_transition_steps).with(step_ids, any_parameters).returns(all_ids)
        @repository.expects(:find_steps).with(all_ids).returns(all_scheduling_steps)

        @worker_adapter.expects(:enqueue).with(step1.id, any_parameters).returns(true)
        @worker_adapter.expects(:enqueue).with(step2.id, any_parameters).returns(false)

        @repository.expects(:bulk_update_steps).never
        @notifier.expects(:publish).never

        error = assert_raises(Yantra::Errors::EnqueueFailed) do
          Time.stub :current, @now do
            @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
          end
        end
        assert_includes error.failed_ids, @step2_id
        refute_includes error.failed_ids, @step1_id
      end

      def test_call_raises_enqueue_failed_if_adapter_raises_error
        step_ids = [@step1_id]
        step1 = MockStepRecordSET.new(id: @step1_id, state: 'scheduling', klass: 'Step1', queue: 'q1')

        @repository.expects(:bulk_transition_steps).with(step_ids, any_parameters).returns([@step1_id])
        @repository.expects(:find_steps).with([@step1_id]).returns([step1])
        enqueue_error = StandardError.new("Redis connection lost")
        @worker_adapter.expects(:enqueue).with(step1.id, any_parameters).raises(enqueue_error)

        @repository.expects(:bulk_update_steps).never
        @notifier.expects(:publish).never

        error = assert_raises(Yantra::Errors::EnqueueFailed) do
           Time.stub :current, @now do
            @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
           end
        end
        assert_includes error.failed_ids, @step1_id
      end

      def test_call_handles_phase3_update_failure_gracefully
        step_ids = [@step1_id]
        step1_scheduling = MockStepRecordSET.new(id: @step1_id, state: 'scheduling', klass: 'Step1', queue: 'q1')
        update_error = Yantra::Errors::PersistenceError.new("DB write failed during final update")

        sequence = Mocha::Sequence.new('phase3_fail')
        @repository.expects(:bulk_transition_steps).with(step_ids, any_parameters).returns([@step1_id]).in_sequence(sequence)
        @repository.expects(:find_steps).with([@step1_id]).returns([step1_scheduling]).in_sequence(sequence)
        @worker_adapter.expects(:enqueue).with(step1_scheduling.id, any_parameters).returns(true).in_sequence(sequence)
        expected_attrs_phase3 = { state: ENQUEUED.to_s, enqueued_at: @now, updated_at: @now }
        @repository.expects(:bulk_update_steps)
                   .with([@step1_id], expected_attrs_phase3)
                   .raises(update_error).in_sequence(sequence)
        @notifier.expects(:publish)
                 .with('yantra.step.bulk_enqueued', has_entries(enqueued_ids: [@step1_id], enqueued_at: @now))
                 .in_sequence(sequence)
        @logger.expects(:error) # Removed regexp_matches

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

