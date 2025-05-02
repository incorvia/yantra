# test/core/step_enqueuer_test.rb
# frozen_string_literal: true

require 'test_helper'
require 'set'
require 'active_support/core_ext/numeric/time' # For .seconds

# --- Yantra Requires ---
# Assuming test_helper loads necessary Yantra files
require 'yantra/core/step_enqueuer'
require 'yantra/core/state_machine'
require 'yantra/errors'

# --- Mocks ---
# Simple mock for step records used in tests
MockStepRecord = Struct.new(
  :id, :workflow_id, :klass, :state, :queue, :delay_seconds, :enqueued_at,
  :max_attempts, :retries, :created_at,
  keyword_init: true
)

module Yantra
  module Core
    class StepEnqueuerTest < Minitest::Test

      def setup
        # Use Mocha mocks for ALL collaborators
        @repository = mock('Repository')
        @worker_adapter = mock('WorkerAdapter')
        @notifier = mock('Notifier')
        @logger = mock('Logger')

        # Stub worker adapter methods needed by StepEnqueuer
        @worker_adapter.stubs(:enqueue)
        @worker_adapter.stubs(:enqueue_in)

        # Stub logger methods - individual tests can add 'expects' if needed
        @logger.stubs(:debug)
        @logger.stubs(:info)
        @logger.stubs(:warn)
        @logger.stubs(:error)

        @enqueuer = StepEnqueuer.new(
          repository: @repository,
          worker_adapter: @worker_adapter,
          notifier: @notifier,
          logger: @logger
        )

        # Common test data
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
        assert_equal [], @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: [])
        assert_equal [], @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: nil)
      end

      def test_call_returns_empty_if_repository_find_fails
        step_ids = [@step1_id]
        @repository.expects(:find_steps).with(step_ids).raises(Yantra::Errors::PersistenceError, "DB error")
        @logger.expects(:error) # Just expect error was called

        assert_equal [], @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
      end

      def test_call_skips_steps_not_found_or_not_pending
        step_ids = [@step1_id, @step2_id, @step3_id]
        step1_pending = MockStepRecord.new(id: @step1_id, state: 'pending', klass: 'Step1', workflow_id: @workflow_id, max_attempts: 1, retries: 0, created_at: @now)
        step2_running = MockStepRecord.new(id: @step2_id, state: 'running', klass: 'Step2', workflow_id: @workflow_id, max_attempts: 1, retries: 0, created_at: @now)
        # Step 3 is not found by find_steps

        @repository.expects(:find_steps).with(step_ids).returns([step1_pending, step2_running])
        @worker_adapter.expects(:enqueue).with(step1_pending.id, @workflow_id, step1_pending.klass, step1_pending.queue).returns(true)
        # Expect bulk upsert for Phase 1 (state -> scheduling)
        @repository.expects(:bulk_upsert_steps).with do |updates|
          updates.size == 1 && updates[0][:id] == @step1_id && updates[0][:state] == StateMachine::SCHEDULING.to_s
        end.returns(1)
        # Expect bulk update for Phase 3 (state -> enqueued, timestamps)
        @repository.expects(:bulk_update_steps).with([@step1_id], has_key(:enqueued_at)).returns(1)


        # Expect event ONLY for step 1 - simplified matcher
        @notifier.expects(:publish).with('yantra.step.bulk_enqueued', has_entry(enqueued_ids: [@step1_id]))

        # Expect warnings for skipped steps
        @logger.expects(:warn) # Simplified

        processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)

        assert_equal [@step1_id], processed_ids, "Should only report step 1 as processed"
      end

      def test_call_enqueues_immediate_step
        step_ids = [@step1_id]
        step1 = MockStepRecord.new(id: @step1_id, state: 'pending', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: nil, queue: 'q1', max_attempts: 1, retries: 0, created_at: @now)

        @repository.expects(:find_steps).with(step_ids).returns([step1])
        # Phase 1: Update state to scheduling
        @repository.expects(:bulk_upsert_steps).with do |updates|
          updates.size == 1 && updates[0][:id] == @step1_id && updates[0][:state] == StateMachine::SCHEDULING.to_s && updates[0][:delayed_until].nil?
        end.returns(1)
        # Phase 2: Enqueue
        @worker_adapter.expects(:enqueue).with(step1.id, @workflow_id, step1.klass, step1.queue).returns(true)
        # Phase 3: Update state to enqueued and timestamps
        @repository.expects(:bulk_update_steps).with([@step1_id], has_key(:enqueued_at)).returns(1)

        # Simplified matcher
        @notifier.expects(:publish).with('yantra.step.bulk_enqueued', has_entry(enqueued_ids: [@step1_id]))

        processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        assert_equal [@step1_id], processed_ids
      end

      def test_call_schedules_delayed_step
        step_ids = [@step1_id]
        delay = 300
        step1 = MockStepRecord.new(id: @step1_id, state: 'pending', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: delay, queue: 'q1', max_attempts: 1, retries: 0, created_at: @now)

        @repository.expects(:find_steps).with(step_ids).returns([step1])
        # Phase 1: Update state to scheduling & delayed_until
        @repository.expects(:bulk_upsert_steps).with do |updates|
          updates.size == 1 &&
            updates[0][:id] == @step1_id &&
            updates[0][:state] == StateMachine::SCHEDULING.to_s &&
            updates[0][:delayed_until].is_a?(Time) &&
            updates[0][:delayed_until] > Time.current
        end.returns(1)
        # Phase 2: Enqueue In
        @worker_adapter.expects(:enqueue_in).with(delay, step1.id, @workflow_id, step1.klass, step1.queue).returns(true)
        # Phase 3: Update state to enqueued and timestamps
        @repository.expects(:bulk_update_steps).with([@step1_id], has_key(:enqueued_at)).returns(1)

        # Simplified matcher
        @notifier.expects(:publish).with('yantra.step.bulk_enqueued', has_entry(enqueued_ids: [@step1_id]))

        processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        assert_equal [@step1_id], processed_ids
      end

      def test_call_handles_mix_of_immediate_and_delayed
        step_ids = [@step1_id, @step2_id, @step3_id]
        delay = 60
        step1 = MockStepRecord.new(id: @step1_id, state: 'pending', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: nil, queue: 'q1', max_attempts: 1, retries: 0, created_at: @now)
        step2 = MockStepRecord.new(id: @step2_id, state: 'pending', klass: 'Step2', workflow_id: @workflow_id, delay_seconds: delay, queue: 'q2', max_attempts: 1, retries: 0, created_at: @now)
        step3 = MockStepRecord.new(id: @step3_id, state: 'pending', klass: 'Step3', workflow_id: @workflow_id, delay_seconds: 0, queue: 'q3', max_attempts: 1, retries: 0, created_at: @now)
        expected_enqueued_ids = [@step1_id, @step2_id, @step3_id]

        @repository.expects(:find_steps).with(step_ids).returns([step1, step2, step3])
        # Phase 1: Update state to scheduling & delayed_until
        @repository.expects(:bulk_upsert_steps).with do |updates|
          updates.size == 3 &&
            updates.find { |h| h[:id] == @step1_id && h[:delayed_until].nil? && h[:state] == StateMachine::SCHEDULING.to_s } &&
            updates.find { |h| h[:id] == @step2_id && h[:delayed_until].is_a?(Time) && h[:state] == StateMachine::SCHEDULING.to_s } &&
            updates.find { |h| h[:id] == @step3_id && h[:delayed_until].nil? && h[:state] == StateMachine::SCHEDULING.to_s }
        end.returns(3)
        # Phase 2: Enqueue/Enqueue In
        @worker_adapter.expects(:enqueue).with(step1.id, @workflow_id, step1.klass, step1.queue).returns(true)
        @worker_adapter.expects(:enqueue).with(step3.id, @workflow_id, step3.klass, step3.queue).returns(true)
        @worker_adapter.expects(:enqueue_in).with(delay, step2.id, @workflow_id, step2.klass, step2.queue).returns(true)
        # Phase 3: Update state to enqueued and timestamps
        @repository.expects(:bulk_update_steps).with(expected_enqueued_ids, has_key(:enqueued_at)).returns(3)

        # Expect one event with all three IDs
        @notifier.expects(:publish).with(
          'yantra.step.bulk_enqueued',
          has_entries(
            workflow_id: @workflow_id,
            enqueued_ids: match_array_including_only(expected_enqueued_ids),
            enqueued_at: kind_of(Time)
          )
        )

        processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        assert_equal expected_enqueued_ids.sort, processed_ids.sort
      end

      def test_call_handles_adapter_enqueue_failure
        step_ids = [@step1_id]
        step1 = MockStepRecord.new(id: @step1_id, state: 'pending', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: nil, queue: 'q1', max_attempts: 1, retries: 0, created_at: @now)

        @repository.expects(:find_steps).with(step_ids).returns([step1])
        # Phase 1: Update state to scheduling
        @repository.expects(:bulk_upsert_steps).with(any_parameters).returns(1)
        # Phase 2: Simulate adapter failure
        @worker_adapter.expects(:enqueue).with(step1.id, @workflow_id, step1.klass, step1.queue).returns(false)
        # Phase 3: Expect NO timestamp update
        @repository.expects(:bulk_update_steps).never
        @notifier.expects(:publish).never
        # Expect warning log
        @logger.expects(:warn)

        processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        assert_equal [], processed_ids, "Should return empty array if enqueue fails"
        # Assert step state remains 'scheduling' (optional)
        # assert_equal 'scheduling', repository.find_step(@step1_id).state
      end

      def test_call_handles_adapter_enqueue_in_failure
        step_ids = [@step1_id]
        delay = 60
        step1 = MockStepRecord.new(id: @step1_id, state: 'pending', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: delay, queue: 'q1', max_attempts: 1, retries: 0, created_at: @now)

        @repository.expects(:find_steps).with(step_ids).returns([step1])
        # Phase 1: Update state to scheduling
        @repository.expects(:bulk_upsert_steps).with(any_parameters).returns(1)
        # Phase 2: Simulate adapter failure
        @worker_adapter.expects(:enqueue_in).with(delay, step1.id, @workflow_id, step1.klass, step1.queue).returns(false)
        # Phase 3: Expect NO timestamp update
        @repository.expects(:bulk_update_steps).never
        @notifier.expects(:publish).never
        # Expect warning log
        @logger.expects(:warn)

        processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        assert_equal [], processed_ids, "Should return empty array if enqueue_in fails"
        # Assert step state remains 'scheduling' (optional)
      end

      def test_call_handles_bulk_upsert_failure_phase1
        step_ids = [@step1_id]
        step1 = MockStepRecord.new(id: @step1_id, state: 'pending', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: nil, queue: 'q1', max_attempts: 1, retries: 0, created_at: @now)

        @repository.expects(:find_steps).with(step_ids).returns([step1])
        # Simulate Phase 1 failure
        @repository.expects(:bulk_upsert_steps).with(any_parameters).raises(Yantra::Errors::PersistenceError, "Phase 1 DB write failed")
        # Expect NO adapter calls, NO phase 3 update, NO event
        @worker_adapter.expects(:enqueue).never
        @worker_adapter.expects(:enqueue_in).never
        @repository.expects(:bulk_update_steps).never # Check this specific method isn't called
        @notifier.expects(:publish).never
        # --- MODIFIED: Simplify logger expectation ---
        @logger.expects(:error) # Just expect error was called
        # --- END MODIFIED ---

        processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        assert_equal [], processed_ids, "Should return empty array if Phase 1 fails"
      end

      def test_call_handles_bulk_update_failure_phase3
        step_ids = [@step1_id]
        step1 = MockStepRecord.new(id: @step1_id, state: 'pending', klass: 'Step1', workflow_id: @workflow_id, delay_seconds: nil, queue: 'q1', max_attempts: 1, retries: 0, created_at: @now)

        @repository.expects(:find_steps).with(step_ids).returns([step1])
        # Phase 1: Success
        @repository.expects(:bulk_upsert_steps).with(any_parameters).returns(1)
        # Phase 2: Success
        @worker_adapter.expects(:enqueue).with(step1.id, @workflow_id, step1.klass, step1.queue).returns(true)
        # Phase 3: Simulate failure
        @repository.expects(:bulk_update_steps).with([@step1_id], any_parameters).raises(Yantra::Errors::PersistenceError, "Phase 3 DB write failed")
        # Expect NO event publish
        @notifier.expects(:publish).never
        # --- MODIFIED: Simplify logger expectation ---
        @logger.expects(:error) # Just expect error was called
        # --- END MODIFIED ---

        processed_ids = @enqueuer.call(workflow_id: @workflow_id, step_ids_to_attempt: step_ids)
        # Note: Step was already sent to adapter, so we still return its ID
        assert_equal [@step1_id], processed_ids, "Should return ID even if Phase 3 update fails"
        # Assert step state remains 'scheduling' (optional)
      end

      # Helper to match array contents regardless of order
      def match_array_including_only(expected_array)
        ->(actual_array) { actual_array.is_a?(Array) && actual_array.sort == expected_array.sort }
      end

    end # class StepEnqueuerTest
  end # module Core
end # module Yantra

