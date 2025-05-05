# test/core/workflow_retry_service_test.rb
# frozen_string_literal: true

require 'test_helper'
require 'mocha/minitest'

require 'yantra/core/workflow_retry_service'
require 'yantra/core/state_machine'
require 'yantra/core/step_enqueuer'
require 'yantra/errors'
require 'yantra/core/graph_utils'

MockStepRecordWRST = Struct.new(
  :id, :workflow_id, :klass, :state, :queue, :delay_seconds, :enqueued_at,
  :max_attempts, :retries, :created_at,
  keyword_init: true
) do
  def state; self[:state].to_s; end
  def state_sym; self[:state]; end
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
                    .with(repository: @repo, worker_adapter: @worker, notifier: @notifier, logger: @logger)
                    .returns(@step_enqueuer)

        @workflow_id = "wf-#{SecureRandom.uuid}"
        @step1_id = "step1-#{SecureRandom.uuid}"
        @step2_id = "step2-#{SecureRandom.uuid}"
        @step3_id = "step3-#{SecureRandom.uuid}"

        @repo.stubs(:respond_to?).returns(true)

        @service = WorkflowRetryService.new(
          workflow_id: @workflow_id,
          repository: @repo,
          worker_adapter: @worker,
          notifier: @notifier
        )
      end

      def teardown
        Mocha::Mockery.instance.teardown
        StepEnqueuer.unstub(:new)
      end

      def test_call_returns_empty_array_if_no_retryable_steps_found
        @repo.expects(:list_steps).returns([])
        @repo.expects(:get_dependent_ids_bulk).never
        @repo.expects(:bulk_update_steps).never
        @step_enqueuer.expects(:call).never
        assert_equal 0, @service.call
      end

      def test_call_resets_and_enqueues_failed_steps
        failed_step1 = MockStepRecordWRST.new(id: @step1_id, state: :failed)
        cancelled_descendant = MockStepRecordWRST.new(id: @step2_id, state: :cancelled)

        failed_step_ids = [@step1_id]
        ids_to_reset = [@step1_id, @step2_id]
        captured_ids = nil

        seq = sequence('retry_flow')

        @repo.expects(:list_steps).returns([failed_step1]).in_sequence(seq)
        @repo.expects(:get_dependent_ids_bulk).with([@step1_id.to_s]).returns({ @step1_id.to_s => [@step2_id] }).in_sequence(seq)
        @repo.expects(:find_steps).with([@step2_id]).returns([cancelled_descendant]).in_sequence(seq)
        @repo.expects(:get_dependent_ids_bulk).with([@step2_id.to_s]).returns({ @step2_id.to_s => [] }).in_sequence(seq)

        @repo.expects(:bulk_update_steps).with { |ids, _attrs|
          captured_ids = ids
          true
        }.returns(ids_to_reset.size).in_sequence(seq)

        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: failed_step_ids).returns(failed_step_ids).in_sequence(seq)

        result = @service.call
        assert_equal 1, result
        assert_equal ids_to_reset.sort, captured_ids.sort
      end

      def test_call_returns_only_successfully_enqueued_ids_count
        failed_step1 = MockStepRecordWRST.new(id: @step1_id, state: :failed)
        failed_step3 = MockStepRecordWRST.new(id: @step3_id, state: :failed)
        cancelled_descendant = MockStepRecordWRST.new(id: @step2_id, state: :cancelled)

        failed_step_ids = [@step1_id, @step3_id]
        ids_to_reset = [@step1_id, @step3_id, @step2_id]
        successfully_enqueued_ids = [@step1_id]
        captured_ids = nil

        seq = sequence('partial_retry_flow')

        @repo.expects(:list_steps).returns([failed_step1, failed_step3]).in_sequence(seq)
        @repo.expects(:get_dependent_ids_bulk).with(failed_step_ids.map(&:to_s)).returns({
          @step1_id.to_s => [@step2_id],
          @step3_id.to_s => []
        }).in_sequence(seq)
        @repo.expects(:find_steps).with([@step2_id]).returns([cancelled_descendant]).in_sequence(seq)
        @repo.expects(:get_dependent_ids_bulk).with([@step2_id.to_s]).returns({ @step2_id.to_s => [] }).in_sequence(seq)

        @repo.expects(:bulk_update_steps).with { |ids, _attrs|
          captured_ids = ids
          true
        }.returns(ids_to_reset.size).in_sequence(seq)

        @step_enqueuer.expects(:call).with(workflow_id: @workflow_id, step_ids_to_attempt: failed_step_ids).returns(successfully_enqueued_ids).in_sequence(seq)

        result = @service.call
        assert_equal 1, result
        assert_equal ids_to_reset.sort, captured_ids.sort
      end

      def test_call_returns_zero_if_reset_fails
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: :failed)
        failed_step_ids = [@step1_id]

        @repo.expects(:list_steps).returns([failed_step])
        @repo.expects(:get_dependent_ids_bulk).returns({})
        @repo.expects(:bulk_update_steps).with { |ids, _|
          assert_equal failed_step_ids.sort, ids.sort
          true
        }.returns(0)
        @logger.expects(:warn)
        @step_enqueuer.expects(:call).never

        assert_equal 0, @service.call
      end

      def test_call_returns_zero_if_reset_raises_persistence_error
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: :failed)
        failed_step_ids = [@step1_id]
        error = Yantra::Errors::PersistenceError.new("fail")

        @repo.expects(:list_steps).returns([failed_step])
        @repo.expects(:get_dependent_ids_bulk).returns({})
        @repo.expects(:bulk_update_steps).raises(error)
        @logger.expects(:error)
        @step_enqueuer.expects(:call).never

        assert_equal 0, @service.call
      end

      def test_call_returns_zero_if_enqueuer_fails_with_enqueue_failed
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: :failed)
        failed_step_ids = [@step1_id]
        error = Yantra::Errors::EnqueueFailed.new("fail", failed_ids: failed_step_ids)

        @repo.expects(:list_steps).returns([failed_step])
        @repo.expects(:get_dependent_ids_bulk).returns({})
        @repo.expects(:bulk_update_steps).returns(1)
        @step_enqueuer.expects(:call).raises(error)
        @logger.expects(:error)

        assert_equal 0, @service.call
      end

      def test_call_returns_zero_if_enqueuer_fails_with_standard_error
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: :failed)
        failed_step_ids = [@step1_id]

        @repo.expects(:list_steps).returns([failed_step])
        @repo.expects(:get_dependent_ids_bulk).returns({})
        @repo.expects(:bulk_update_steps).returns(1)
        @step_enqueuer.expects(:call).raises(StandardError.new("oops"))
        @logger.expects(:error)

        assert_equal 0, @service.call
      end

      def test_call_handles_find_retryable_steps_error
        @repo.expects(:list_steps).raises(StandardError.new("broken"))
        @logger.expects(:error)
        @repo.expects(:get_dependent_ids_bulk).never
        @repo.expects(:bulk_update_steps).never
        @step_enqueuer.expects(:call).never

        assert_equal 0, @service.call
      end
    end
  end
end

