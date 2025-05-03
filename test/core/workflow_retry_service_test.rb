# test/core/workflow_retry_service_test.rb
# frozen_string_literal: true

require 'test_helper'
require 'mocha/minitest'

# --- Yantra Requires ---
require 'yantra/core/workflow_retry_service'
require 'yantra/core/state_machine'
require 'yantra/core/step_enqueuer'
require 'yantra/errors'

# --- Mocks ---
MockStepRecordWRST = Struct.new(
  :id, :workflow_id, :klass, :state, :queue, :delay_seconds, :enqueued_at,
  :max_attempts, :retries, :created_at,
  keyword_init: true
) do
  def state; self[:state].to_s; end
end

module Yantra
  module Core
    class WorkflowRetryServiceTest < Minitest::Test
      include StateMachine

      def setup
        @repo = mock('Repository')
        @worker = mock('WorkerAdapter')
        @notifier = mock('Notifier')
        @logger = mock('Logger')
        @step_enqueuer = mock('StepEnqueuer')

        @logger.stubs(:debug)
        @logger.stubs(:info)
        @logger.stubs(:warn)
        @logger.stubs(:error)
        Yantra.stubs(:logger).returns(@logger)

        StepEnqueuer.stubs(:new)
                    .with(repository: @repo, worker_adapter: @worker, notifier: @notifier)
                    .returns(@step_enqueuer)

        @workflow_id = "wf-#{SecureRandom.uuid}"
        @step1_id = "step1-#{SecureRandom.uuid}"
        @step2_id = "step2-#{SecureRandom.uuid}"
        @step3_id = "step3-#{SecureRandom.uuid}"

        @repo.stubs(:respond_to?).with(:bulk_update_steps).returns(true)
        @repo.stubs(:respond_to?).with(:list_steps).returns(true)

        @service = WorkflowRetryService.new(
          workflow_id: @workflow_id,
          repository: @repo,
          worker_adapter: @worker,
          notifier: @notifier
        )
      end

      def teardown
        Mocha::Mockery.instance.teardown
      end

      def test_call_returns_empty_array_if_no_retryable_steps_found
        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns([])
        @repo.expects(:bulk_update_steps).never
        @step_enqueuer.expects(:call).never
        result = @service.call
        assert_equal 0, result
      end

      def test_call_resets_and_enqueues_failed_and_stuck_steps
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: 'failed')
        stuck_step = MockStepRecordWRST.new(id: @step2_id, state: 'awaiting_execution', enqueued_at: nil)
        retryable_steps = [failed_step, stuck_step]
        retryable_ids = [@step1_id, @step2_id]

        sequence = Mocha::Sequence.new('retry_flow')
        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns(retryable_steps).in_sequence(sequence)
        @repo.expects(:bulk_update_steps).with(retryable_ids, has_entries(state: StateMachine::PENDING)).returns(true).in_sequence(sequence)
        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: retryable_ids).returns(2).in_sequence(sequence)

        result = @service.call
        assert_equal 2, result
      end

      def test_call_returns_only_successfully_enqueued_ids_count
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: 'failed')
        stuck_step = MockStepRecordWRST.new(id: @step2_id, state: 'awaiting_execution', enqueued_at: nil)
        retryable_steps = [failed_step, stuck_step]
        retryable_ids = [@step1_id, @step2_id]
        successfully_enqueued_count = 1

        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns(retryable_steps)
        @repo.expects(:bulk_update_steps).with(retryable_ids, has_entries(state: StateMachine::PENDING)).returns(true)
        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: retryable_ids).returns(successfully_enqueued_count)

        result = @service.call
        assert_equal successfully_enqueued_count, result
      end

      def test_call_raises_error_if_reset_fails
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: 'failed')
        retryable_ids = [@step1_id]

        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns([failed_step])
        @repo.expects(:bulk_update_steps).with(retryable_ids, has_entries(state: StateMachine::PENDING)).raises(Yantra::Errors::PersistenceError.new("Failed to reset"))
        @step_enqueuer.expects(:call).never

        assert_equal 0, @service.call
      end

      def test_call_raises_error_if_enqueuer_fails
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: 'failed')
        retryable_ids = [@step1_id]

        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns([failed_step])
        @repo.expects(:bulk_update_steps).with(retryable_ids, has_entries(state: StateMachine::PENDING)).returns(true)
        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: retryable_ids).raises(Yantra::Errors::EnqueueFailed.new("Enqueue failed"))

        assert_equal 0, @service.call
      end

      def test_call_handles_find_retryable_steps_error
        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).raises(StandardError.new("DB Find Error"))
        @logger.expects(:error).yields("[WorkflowRetryService] Error fetching failed steps for #{@workflow_id}: DB Find Error")
        @repo.expects(:bulk_update_steps).never
        @step_enqueuer.expects(:call).never

        result = @service.call
        assert_equal 0, result
      end

    end
  end
end

