# frozen_string_literal: true

require 'test_helper'
require 'ostruct'
require 'mocha/minitest'

# --- Yantra Requires ---
require 'yantra/core/step_executor'
require 'yantra/core/orchestrator'
require 'yantra/worker/retry_handler'
require 'yantra/errors'

module Yantra
  module Core
    class StepExecutorTest < Minitest::Test
      def setup
        @mock_repository = mock('repository')
        @mock_orchestrator = mock('orchestrator')
        @mock_notifier = mock('notifier')
        @retry_handler_klass = Yantra::Worker::RetryHandler

        # Allow initializer sanity checks to pass
        @mock_repository.stubs(:respond_to?).returns(true)
        @mock_orchestrator.stubs(:respond_to?).returns(true)

        @executor = StepExecutor.new(
          repository: @mock_repository,
          orchestrator: @mock_orchestrator,
          notifier: @mock_notifier,
          retry_handler_class: @retry_handler_klass,
          logger: Logger.new(IO::NULL) # suppress log output during tests
        )

        @step_id = SecureRandom.uuid
        @workflow_id = SecureRandom.uuid
        @step_klass_name = "MyTestStep"
        @mock_step_record = OpenStruct.new(id: @step_id, performed_at: nil)
      end

      def teardown
        super
      end

      def test_execute_increments_total_executions_counter_when_start_is_allowed
        @executor.stubs(:load_step_record!).returns(@mock_step_record)
        @executor.stubs(:orchestrator_allows_start?).with(@step_id, @mock_step_record).returns(true)
        @executor.stubs(:run_user_step!)

        @mock_repository.expects(:increment_step_executions).with(@step_id).once

        @executor.execute(
          step_id: @step_id,
          workflow_id: @workflow_id,
          step_klass_name: @step_klass_name
        )
      end

      def test_execute_does_not_increment_total_executions_if_start_is_not_allowed
        @executor.stubs(:load_step_record!).returns(@mock_step_record)
        @executor.stubs(:orchestrator_allows_start?).with(@step_id, @mock_step_record).returns(false)

        @mock_repository.expects(:increment_step_executions).never

        @executor.execute(
          step_id: @step_id,
          workflow_id: @workflow_id,
          step_klass_name: @step_klass_name
        )
      end

      def test_execute_increments_total_executions_even_if_step_fails
        @executor.stubs(:load_step_record!).returns(@mock_step_record)
        @executor.stubs(:orchestrator_allows_start?).returns(true)
        @executor.stubs(:run_user_step!).raises(StandardError, "User code failed!")
        @executor.stubs(:handle_failure)

        @mock_repository.expects(:increment_step_executions).with(@step_id).once

        @executor.execute(
          step_id: @step_id,
          workflow_id: @workflow_id,
          step_klass_name: @step_klass_name
        )
      end
    end
  end
end

