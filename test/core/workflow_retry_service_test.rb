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
        @worker = mock('WorkerAdapter') # Needed for StepEnqueuer init
        @notifier = mock('Notifier')   # Needed for StepEnqueuer init
        @logger = mock('Logger')
        @step_enqueuer = mock('StepEnqueuer') # Mock the enqueuer instance

        @logger.stubs(:debug)
        @logger.stubs(:info)
        @logger.stubs(:warn)
        @logger.stubs(:error)
        Yantra.stubs(:logger).returns(@logger) # Stub global logger

        # Stub StepEnqueuer.new to return OUR mock instance
        StepEnqueuer.stubs(:new)
                    .with(repository: @repo, worker_adapter: @worker, notifier: @notifier, logger: @logger)
                    .returns(@step_enqueuer)

        @workflow_id = "wf-#{SecureRandom.uuid}"
        @step1_id = "step1-#{SecureRandom.uuid}"
        @step2_id = "step2-#{SecureRandom.uuid}"
        @step3_id = "step3-#{SecureRandom.uuid}"

        # Stub repo checks needed by service initializer
        @repo.stubs(:respond_to?).with(:list_steps).returns(true)
        @repo.stubs(:respond_to?).with(:bulk_update_steps).returns(true)

        # Initialize the service AFTER stubbing StepEnqueuer.new
        @service = WorkflowRetryService.new(
          workflow_id: @workflow_id,
          repository: @repo,
          worker_adapter: @worker,
          notifier: @notifier
        )
      end

      def teardown
        Mocha::Mockery.instance.teardown
        StepEnqueuer.unstub(:new) # Unstub the class method
      end

      def test_call_returns_empty_array_if_no_retryable_steps_found
        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns([])
        @repo.expects(:bulk_update_steps).never
        @step_enqueuer.expects(:call).never
        result = @service.call
        assert_equal 0, result
      end

      def test_call_resets_and_enqueues_failed_steps
        failed_step1 = MockStepRecordWRST.new(id: @step1_id, state: 'failed')
        failed_step2 = MockStepRecordWRST.new(id: @step2_id, state: 'failed')
        retryable_steps = [failed_step1, failed_step2]
        retryable_ids = [@step1_id, @step2_id]
        expected_enqueued_count = 2
        # --- MODIFIED: Mock returns array ---
        mock_enqueuer_return_array = retryable_ids # Array with 2 IDs
        # --- END MODIFICATION ---

        sequence = Mocha::Sequence.new('retry_flow')
        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns(retryable_steps).in_sequence(sequence)
        @repo.expects(:bulk_update_steps).with(retryable_ids, has_entries(state: StateMachine::PENDING.to_s)).returns(retryable_ids.size).in_sequence(sequence)
        # --- MODIFIED: Mock returns array ---
        @step_enqueuer.expects(:call)
                      .with(workflow_id: @workflow_id, step_ids_to_attempt: retryable_ids)
                      .returns(mock_enqueuer_return_array) # Mock enqueuer returns array
                      .in_sequence(sequence)
        # --- END MODIFICATION ---

        result = @service.call
        assert_equal expected_enqueued_count, result # Service should return the size
      end


      def test_call_returns_only_successfully_enqueued_ids_count
        failed_step1 = MockStepRecordWRST.new(id: @step1_id, state: 'failed')
        failed_step2 = MockStepRecordWRST.new(id: @step2_id, state: 'failed')
        retryable_steps = [failed_step1, failed_step2]
        retryable_ids = [@step1_id, @step2_id]
        successfully_enqueued_count = 1
        # --- MODIFIED: Mock returns array ---
        # Simulate enqueuer returning only one ID
        mock_enqueuer_return_array = [retryable_ids.first] # Array with 1 ID
        # --- END MODIFICATION ---

        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns(retryable_steps)
        @repo.expects(:bulk_update_steps).with(retryable_ids, has_entries(state: StateMachine::PENDING.to_s)).returns(retryable_ids.size)
        # --- MODIFIED: Mock returns array ---
        @step_enqueuer.expects(:call)
                      .with(workflow_id: @workflow_id, step_ids_to_attempt: retryable_ids)
                      .returns(mock_enqueuer_return_array) # Mock enqueuer returns array
        # --- END MODIFICATION ---

        result = @service.call
        assert_equal successfully_enqueued_count, result # Service should return the size
      end


      def test_call_returns_zero_if_reset_fails
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: 'failed')
        retryable_ids = [@step1_id]
        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns([failed_step])
        @repo.expects(:bulk_update_steps).with(retryable_ids, has_entries(state: StateMachine::PENDING.to_s)).returns(0)
        @logger.expects(:warn)
        @step_enqueuer.expects(:call).never
        assert_equal 0, @service.call
      end

      def test_call_returns_zero_if_reset_raises_persistence_error
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: 'failed')
        retryable_ids = [@step1_id]
        error = Yantra::Errors::PersistenceError.new("Failed to reset")
        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns([failed_step])
        @repo.expects(:bulk_update_steps).with(retryable_ids, has_entries(state: StateMachine::PENDING.to_s)).raises(error)
        @logger.expects(:error)
        @step_enqueuer.expects(:call).never
        assert_equal 0, @service.call
      end

      def test_call_returns_zero_if_enqueuer_fails_with_enqueue_failed
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: 'failed')
        retryable_ids = [@step1_id]
        enqueue_error = Yantra::Errors::EnqueueFailed.new("Enqueue failed", failed_ids: [@step1_id])
        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns([failed_step])
        @repo.expects(:bulk_update_steps).with(retryable_ids, has_entries(state: StateMachine::PENDING.to_s)).returns(1)
        @step_enqueuer.expects(:call)
                      .with(workflow_id: @workflow_id, step_ids_to_attempt: retryable_ids)
                      .raises(enqueue_error)
        @logger.expects(:error)
        assert_equal 0, @service.call
      end

       def test_call_returns_zero_if_enqueuer_fails_with_standard_error
        failed_step = MockStepRecordWRST.new(id: @step1_id, state: 'failed')
        retryable_ids = [@step1_id]
        other_error = StandardError.new("Something else broke")
        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).returns([failed_step])
        @repo.expects(:bulk_update_steps).with(retryable_ids, has_entries(state: StateMachine::PENDING.to_s)).returns(1)
        @step_enqueuer.expects(:call)
                      .with(workflow_id: @workflow_id, step_ids_to_attempt: retryable_ids)
                      .raises(other_error)
        @logger.expects(:error)
        assert_equal 0, @service.call
      end

      def test_call_handles_find_retryable_steps_error
        @repo.expects(:list_steps).with(workflow_id: @workflow_id, status: :failed).raises(StandardError.new("DB Find Error"))
        @logger.expects(:error)
        @repo.expects(:bulk_update_steps).never
        @step_enqueuer.expects(:call).never
        result = @service.call
        assert_equal 0, result
      end

    end # class WorkflowRetryServiceTest
  end # module Core
end # module Yantra

