# test/core/orchestrator_test.rb

require "test_helper"
require "yantra/core/orchestrator"
require "yantra/core/state_machine"
require "yantra/errors"
require "yantra/persistence/repository_interface"
require "yantra/worker/enqueuing_interface"
require "yantra/events/notifier_interface"
require "minitest/mock"
require "ostruct"
require 'time' # <-- Add require for Time

# Define simple Structs - still useful for defining return values
MockStep = Struct.new(:id, :workflow_id, :klass, :state, :queue, :output, :error, :retries, :created_at, :enqueued_at, :started_at, :finished_at, :dependencies) do
  def initialize(*args)
    super(*args)
    self.dependencies ||= []
  end
end
MockWorkflow = Struct.new(:id, :state, :klass, :started_at, :finished_at, :has_failures)

module Yantra
  module Core
    class OrchestratorTest < Minitest::Test

      def setup
        @mock_repo = Minitest::Mock.new
        @mock_worker = Minitest::Mock.new
        @mock_notifier = Minitest::Mock.new

        # Expectations for initialize validation
        @mock_repo.expect(:is_a?, true, [Yantra::Persistence::RepositoryInterface])
        @mock_worker.expect(:is_a?, true, [Yantra::Worker::EnqueuingInterface])
        @mock_notifier.expect(:is_a?, true, [Yantra::Events::NotifierInterface])

        @orchestrator = Orchestrator.new(
          repository: @mock_repo,
          worker_adapter: @mock_worker,
          notifier: @mock_notifier
        )

        # Use fixed IDs for easier debugging if needed
        @workflow_id = "wf-#{SecureRandom.uuid}"
        @step_a_id = "step-a-#{SecureRandom.uuid}"
        @step_b_id = "step-b-#{SecureRandom.uuid}"
        @step_c_id = "step-c-#{SecureRandom.uuid}"

        # Freeze time for consistent timestamp checks if needed
        @frozen_time = Time.parse("2025-04-16 15:30:00 -0500") # Example time
        Time.stub :current, @frozen_time do
          # Ensure setup actions that use Time.current happen within the stub
        end
      end

      def teardown
        # No need to unstub Time here if using block form of stub
        @mock_repo.verify
        @mock_worker.verify
        @mock_notifier.verify
      end

      # --- Test start_workflow ---
      # (No changes needed here)
      def test_start_workflow_enqueues_initial_jobs
        Time.stub :current, @frozen_time do # Ensure consistency if Time.current is used
          workflow = MockWorkflow.new(@workflow_id, :pending, "TestWorkflow")
          initial_job = MockStep.new(@step_a_id, @workflow_id, "StepA", :pending, "default")
          @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
              wf_id == @workflow_id && attrs[:state] == StateMachine::RUNNING.to_s && attrs[:started_at] == @frozen_time && opts == { expected_old_state: :pending }
          end
          @mock_repo.expect(:find_ready_jobs, [@step_a_id], [@workflow_id])
          @mock_repo.expect(:find_step, initial_job, [@step_a_id])
          @mock_repo.expect(:update_step_attributes, true) do |step_id, attrs, opts|
              step_id == @step_a_id && attrs[:state] == StateMachine::ENQUEUED.to_s && attrs[:enqueued_at] == @frozen_time && opts == { expected_old_state: :pending }
          end
          @mock_worker.expect(:enqueue, nil, [@step_a_id, @workflow_id, "StepA", "default"])
          result = @orchestrator.start_workflow(@workflow_id)
          assert result
        end
      end

      def test_start_workflow_enqueues_multiple_initial_jobs
         Time.stub :current, @frozen_time do
           workflow = MockWorkflow.new(@workflow_id, :pending, "TestWorkflow")
           initial_step_a = MockStep.new(@step_a_id, @workflow_id, "StepA", :pending, "q1")
           initial_step_b = MockStep.new(@step_b_id, @workflow_id, "StepB", :pending, "q2")
           @mock_repo.expect(:update_workflow_attributes, true) { |*args| args[0] == @workflow_id } # Simplified check
           @mock_repo.expect(:find_ready_jobs, [@step_a_id, @step_b_id], [@workflow_id])
           # Step A
           @mock_repo.expect(:find_step, initial_step_a, [@step_a_id])
           @mock_repo.expect(:update_step_attributes, true) { |*args| args[0] == @step_a_id } # Simplified check
           @mock_worker.expect(:enqueue, nil, [@step_a_id, @workflow_id, "StepA", "q1"])
           # Step B
           @mock_repo.expect(:find_step, initial_step_b, [@step_b_id])
           @mock_repo.expect(:update_step_attributes, true) { |*args| args[0] == @step_b_id } # Simplified check
           @mock_worker.expect(:enqueue, nil, [@step_b_id, @workflow_id, "StepB", "q2"])
           result = @orchestrator.start_workflow(@workflow_id)
           assert result
         end
      end

      def test_start_workflow_does_nothing_if_not_pending
        Time.stub :current, @frozen_time do
          workflow = MockWorkflow.new(@workflow_id, :running)
          @mock_repo.expect(:update_workflow_attributes, false) do |wf_id, attrs, opts|
             wf_id == @workflow_id && attrs[:state] == StateMachine::RUNNING.to_s && opts == { expected_old_state: :pending }
          end
          @mock_repo.expect(:find_workflow, workflow, [@workflow_id]) # Needed to check state if update fails
          result = @orchestrator.start_workflow(@workflow_id)
          refute result
        end
      end


      # --- Test step_succeeded ---
      # (No changes needed here)
      def test_step_succeeded_updates_state_and_calls_step_finished_and_publishes_event
        Time.stub :current, @frozen_time do
          result_output = { message: "Done" }
          # Simulate the state *after* the update for the find_step used by event publishing
          step_a_updated = MockStep.new(@step_a_id, @workflow_id, "StepA", :succeeded, "default", result_output, nil, 0, @frozen_time - 10, nil, @frozen_time - 5, @frozen_time)
          # Expect the update call
          @mock_repo.expect(:update_step_attributes, true) do |step_id, attrs, opts|
            step_id == @step_a_id &&
            attrs[:state] == StateMachine::SUCCEEDED.to_s &&
            attrs[:output] == result_output &&
            attrs[:finished_at] == @frozen_time && # Check timestamp
            opts == { expected_old_state: :running }
          end
          # Expect find_step for event payload
          @mock_repo.expect(:find_step, step_a_updated, [@step_a_id])
          # Expect notification publish
          @mock_notifier.expect(:publish, nil) do |event_name, payload|
              event_name == 'yantra.step.succeeded' &&
              payload[:step_id] == @step_a_id &&
              payload[:output] == result_output &&
              payload[:finished_at] == @frozen_time
          end
          # Mock step_finished call
          mock_step_finished = Minitest::Mock.new
          mock_step_finished.expect(:call, nil, [@step_a_id])
          @orchestrator.stub(:step_finished, mock_step_finished) do
             @orchestrator.step_succeeded(@step_a_id, result_output)
          end
          mock_step_finished.verify
        end
      end

      def test_step_succeeded_handles_update_failure
        Time.stub :current, @frozen_time do
          result_output = { message: "Done" }
          @mock_repo.expect(:update_step_attributes, false) do |step_id, attrs, opts|
             step_id == @step_a_id && opts == { expected_old_state: :running }
          end
          # Even if update fails, step_finished should still be called
          mock_step_finished = Minitest::Mock.new
          mock_step_finished.expect(:call, nil, [@step_a_id])
          @orchestrator.stub(:step_finished, mock_step_finished) do
             @orchestrator.step_succeeded(@step_a_id, result_output)
          end
          mock_step_finished.verify
        end
      end

      # --- Test step_failed ---
      # (No changes needed here)
      def test_step_failed_calls_step_finished
         mock_step_finished = Minitest::Mock.new
         mock_step_finished.expect(:call, nil, [@step_a_id])
         @orchestrator.stub(:step_finished, mock_step_finished) do
            @orchestrator.step_failed(@step_a_id)
         end
         mock_step_finished.verify
      end

      # --- Test step_finished (Success Path) ---

      # REFINED FIX for Error 3: test_step_finished_success_enqueues_ready_dependent
      def test_step_finished_success_enqueues_ready_dependent
        Time.stub :current, @frozen_time do
          # Arrange
          step_a = MockStep.new(@step_a_id, @workflow_id, "StepA", :succeeded, "default")
          step_b = MockStep.new(@step_b_id, @workflow_id, "StepB", :pending, "default")

          # Expectations for full flow triggered by step_finished(A)
          # 1. Find finished step A (inside step_finished)
          @mock_repo.expect(:find_step, step_a, [@step_a_id])
          # 2. Find dependents of A (B) (inside process_dependents)
          @mock_repo.expect(:get_step_dependents, [@step_b_id], [@step_a_id])
          # 3. Find dependent B (inside process_dependents, before checking deps)
          @mock_repo.expect(:find_step, step_b, [@step_b_id])
          # 4. Check B's dependencies (A) (inside dependencies_met?)
          @mock_repo.expect(:get_step_dependencies, [@step_a_id], [@step_b_id])
          # 5. Check state of dependency A (inside dependencies_met?)
          @mock_repo.expect(:find_step, step_a, [@step_a_id]) # -> deps met!
          # 6. Find step B again for enqueue_job (inside enqueue_job)
          @mock_repo.expect(:find_step, step_b, [@step_b_id])
          # 7. Update B to enqueued (inside enqueue_job)
          @mock_repo.expect(:update_step_attributes, true) do |id, attrs, opts|
              id == @step_b_id && attrs[:state] == StateMachine::ENQUEUED.to_s && attrs[:enqueued_at] == @frozen_time && opts == { expected_old_state: :pending }
          end
          # 8. Enqueue B (inside enqueue_job)
          @mock_worker.expect(:enqueue, nil, [@step_b_id, @workflow_id, "StepB", "default"])
          # Expectations for check_workflow_completion logic
          # 9. Check running count
          @mock_repo.expect(:running_step_count, 0, [@workflow_id])
          # 10. Check enqueued count (B is now enqueued)
          @mock_repo.expect(:enqueued_step_count, 1, [@workflow_id])
          # --> Workflow does NOT complete here because B is enqueued

          # Act
          @orchestrator.step_finished(@step_a_id)
        end
      end

      # REFINED FIX for Error 4: test_step_finished_success_does_not_enqueue_if_deps_not_met
      def test_step_finished_success_does_not_enqueue_if_deps_not_met
        Time.stub :current, @frozen_time do
          # Arrange
          step_a = MockStep.new(@step_a_id, @workflow_id, "StepA", :succeeded, "default")
          step_b = MockStep.new(@step_b_id, @workflow_id, "StepB", :pending, "default") # Other parent still pending
          step_c = MockStep.new(@step_c_id, @workflow_id, "StepC", :pending, "default") # Dependent job C
          workflow = MockWorkflow.new(@workflow_id, :running, "TestWorkflow")
          wf_updated = MockWorkflow.new(@workflow_id, :succeeded, "TestWorkflow", nil, @frozen_time, false) # Expect finished_at

          # Expectations
          # 1. Find finished A (inside step_finished)
          @mock_repo.expect(:find_step, step_a, [@step_a_id])
          # 2. Find dependent C (inside process_dependents)
          @mock_repo.expect(:get_step_dependents, [@step_c_id], [@step_a_id])
          # 3. Find C (inside process_dependents, before checking deps)
          @mock_repo.expect(:find_step, step_c, [@step_c_id])
          # 4. Get C's dependencies (A, B) (inside dependencies_met?)
          @mock_repo.expect(:get_step_dependencies, [@step_a_id, @step_b_id], [@step_c_id])
          # 5. Check A's state (inside dependencies_met?)
          @mock_repo.expect(:find_step, step_a, [@step_a_id]) # (succeeded)
          # 6. Check B's state (inside dependencies_met?)
          @mock_repo.expect(:find_step, step_b, [@step_b_id]) # (pending) -> deps not met!
          # 7. NO enqueue call for C expected
          # Now check workflow completion
          # 8. Check running count
          @mock_repo.expect(:running_step_count, 0, [@workflow_id])
          # 9. Check enqueued count (B not enqueued yet, C not enqueued)
          @mock_repo.expect(:enqueued_step_count, 0, [@workflow_id])
          # 10. Find workflow *before* update (inside check_workflow_completion)
          @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
          # 11. No failures check (inside check_workflow_completion)
          @mock_repo.expect(:workflow_has_failures?, false, [@workflow_id])
          # 12. Workflow finishes (inside check_workflow_completion)
          @mock_repo.expect(:update_workflow_attributes, true) do |id, attrs, opts|
              id == @workflow_id && attrs[:state] == StateMachine::SUCCEEDED.to_s && attrs[:finished_at] == @frozen_time && opts == { expected_old_state: :running }
          end
          # 13. Fetch updated workflow for event payload (inside check_workflow_completion)
          @mock_repo.expect(:find_workflow, wf_updated, [@workflow_id])
          # 14. Publish event (inside check_workflow_completion)
          @mock_notifier.expect(:publish, nil) { |name, payload| name == 'yantra.workflow.succeeded' && payload[:workflow_id] == @workflow_id }

          # Act
          @orchestrator.step_finished(@step_a_id)
        end
      end

      # REFINED FIX for Error 1: test_step_finished_success_completes_workflow_if_last_job
      def test_step_finished_success_completes_workflow_if_last_job
        Time.stub :current, @frozen_time do
          # Arrange
          step_a = MockStep.new(@step_a_id, @workflow_id, "StepA", :succeeded, "default")
          workflow_running = MockWorkflow.new(@workflow_id, :running, "TestWorkflow")
          workflow_succeeded = MockWorkflow.new(@workflow_id, :succeeded, "TestWorkflow", @frozen_time - 10, @frozen_time, false) # Expect finished_at

          # Expectations
          # 1. Find finished A (inside step_finished)
          @mock_repo.expect(:find_step, step_a, [@step_a_id])
          # 2. No dependents found (inside process_dependents)
          @mock_repo.expect(:get_step_dependents, [], [@step_a_id])
          # Check workflow completion
          # 3. Check running count
          @mock_repo.expect(:running_step_count, 0, [@workflow_id])
          # 4. Check enqueued count
          @mock_repo.expect(:enqueued_step_count, 0, [@workflow_id])
          # 5. Find workflow *before* update (inside check_workflow_completion)
          @mock_repo.expect(:find_workflow, workflow_running, [@workflow_id])
          # 6. No failures check (inside check_workflow_completion)
          @mock_repo.expect(:workflow_has_failures?, false, [@workflow_id])
          # 7. Update workflow (inside check_workflow_completion)
          @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
              wf_id == @workflow_id && attrs[:state] == StateMachine::SUCCEEDED.to_s && attrs[:finished_at] == @frozen_time && opts == { expected_old_state: :running }
          end
          # 8. Expect find_workflow *after* update to return the *updated* record for the event
          @mock_repo.expect(:find_workflow, workflow_succeeded, [@workflow_id])
          # 9. Publish event (inside check_workflow_completion)
          @mock_notifier.expect(:publish, nil) do |event_name, payload|
              event_name == 'yantra.workflow.succeeded' && payload[:workflow_id] == @workflow_id && payload[:state] == 'succeeded'
          end

          # Act
          @orchestrator.step_finished(@step_a_id)
        end
      end

      # REFINED FIX for Error 5: test_step_finished_failure_completes_workflow_if_last_job
      def test_step_finished_failure_completes_workflow_if_last_job
        Time.stub :current, @frozen_time do
          # Arrange
          step_a = MockStep.new(@step_a_id, @workflow_id, "StepA", :failed, "default")
          workflow_running = MockWorkflow.new(@workflow_id, :running, "TestWorkflow", @frozen_time - 10, nil, true)
          workflow_failed = MockWorkflow.new(@workflow_id, :failed, "TestWorkflow", @frozen_time - 10, @frozen_time, true) # Expect finished_at

          # Expectations
          # 1. Find finished A (inside step_finished)
          @mock_repo.expect(:find_step, step_a, [@step_a_id])
          # 2. No dependents found (inside process_dependents)
          @mock_repo.expect(:get_step_dependents, [], [@step_a_id])
          # Check workflow completion
          # 3. Check running count
          @mock_repo.expect(:running_step_count, 0, [@workflow_id])
          # 4. Check enqueued count
          @mock_repo.expect(:enqueued_step_count, 0, [@workflow_id])
          # 5. Find workflow *before* update (inside check_workflow_completion)
          @mock_repo.expect(:find_workflow, workflow_running, [@workflow_id])
          # 6. Has failures check (inside check_workflow_completion)
          @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id])
          # 7. Update workflow (inside check_workflow_completion)
          @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
              wf_id == @workflow_id && attrs[:state] == StateMachine::FAILED.to_s && attrs[:finished_at] == @frozen_time && opts == { expected_old_state: :running }
          end
          # 8. Expect find_workflow *after* update to return the *updated* record for the event
          @mock_repo.expect(:find_workflow, workflow_failed, [@workflow_id])
          # 9. Publish event (inside check_workflow_completion)
          @mock_notifier.expect(:publish, nil) do |event_name, payload|
              event_name == 'yantra.workflow.failed' && payload[:workflow_id] == @workflow_id && payload[:state] == 'failed'
          end

          # Act
          @orchestrator.step_finished(@step_a_id)
        end
      end

      # --- Test step_finished (Failure Path) ---

      # REFINED FIX for Error 2: test_step_finished_failure_cancels_dependents_recursively
      def test_step_finished_failure_cancels_dependents_recursively
        Time.stub :current, @frozen_time do
          # Arrange
          step_a = MockStep.new(@step_a_id, @workflow_id, "StepA", :failed, "default")
          step_b = MockStep.new(@step_b_id, @workflow_id, "StepB", :pending, "default")
          step_c = MockStep.new(@step_c_id, @workflow_id, "StepC", :pending, "default")
          workflow_running = MockWorkflow.new(@workflow_id, :running, "TestWorkflow", @frozen_time - 10, nil, true)
          workflow_failed = MockWorkflow.new(@workflow_id, :failed, "TestWorkflow", @frozen_time - 10, @frozen_time, true)

          # Expectations - Follow the logic carefully
          # 1. Find finished A (inside step_finished)
          @mock_repo.expect(:find_step, step_a, [@step_a_id])
          # 2. Find dependents of A (B) (inside process_dependents)
          @mock_repo.expect(:get_step_dependents, [@step_b_id], [@step_a_id])
          # process_dependents -> cancel_downstream_pending([B])
          # 3. Expect find_step for B to check its state (inside cancel_downstream_pending)
          @mock_repo.expect(:find_step, step_b, [@step_b_id])
          # 4. Cancel B (inside cancel_downstream_pending)
          @mock_repo.expect(:cancel_jobs_bulk, 1, [[@step_b_id]])
          # 5. Find dependents of B (C) (inside cancel_downstream_pending)
          @mock_repo.expect(:get_step_dependents, [@step_c_id], [@step_b_id])
          # cancel_downstream_pending([C]) - Recursive call
          # 6. Expect find_step for C to check its state (inside recursive cancel_downstream_pending)
          @mock_repo.expect(:find_step, step_c, [@step_c_id])
          # 7. Cancel C (inside recursive cancel_downstream_pending)
          @mock_repo.expect(:cancel_jobs_bulk, 1, [[@step_c_id]])
          # 8. Find dependents of C (none) (inside recursive cancel_downstream_pending)
          @mock_repo.expect(:get_step_dependents, [], [@step_c_id])
          # cancel_downstream_pending([]) -> returns
          # check_workflow_completion
          # 9. Check running count
          @mock_repo.expect(:running_step_count, 0, [@workflow_id])
          # 10. Check enqueued count
          @mock_repo.expect(:enqueued_step_count, 0, [@workflow_id])
          # 11. Find workflow *before* update
          @mock_repo.expect(:find_workflow, workflow_running, [@workflow_id])
          # 12. Has failures check
          @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id])
          # 13. Update workflow to failed
          @mock_repo.expect(:update_workflow_attributes, true) do |id, attrs, opts|
              id == @workflow_id && attrs[:state] == StateMachine::FAILED.to_s && attrs[:finished_at] == @frozen_time && opts == { expected_old_state: :running }
          end
          # 14. Fetch updated workflow for event
          @mock_repo.expect(:find_workflow, workflow_failed, [@workflow_id])
          # 15. Publish event
          @mock_notifier.expect(:publish, nil) { |name, payload| name == 'yantra.workflow.failed' && payload[:workflow_id] == @workflow_id }

          # Act
          @orchestrator.step_finished(@step_a_id)
        end
      end


      # --- Error Handling / Edge Case Tests ---
      # (No changes needed here)
      def test_start_workflow_handles_workflow_update_failure
        Time.stub :current, @frozen_time do
          # Arrange
          workflow = MockWorkflow.new(@workflow_id, :pending)
          @mock_repo.expect(:update_workflow_attributes, false) do |wf_id, attrs, opts|
              wf_id == @workflow_id && attrs[:state] == StateMachine::RUNNING.to_s && opts == { expected_old_state: :pending }
          end
          @mock_repo.expect(:find_workflow, workflow, [@workflow_id]) # Need to find it to check state
          # Act
          result = @orchestrator.start_workflow(@workflow_id)
          # Assert
          refute result
        end
      end

      def test_step_finished_handles_find_step_error
        # Arrange
        error_repo = Object.new
        def error_repo.find_step(id); raise Yantra::Errors::PersistenceError, "DB down"; end

        # Stub the orchestrator's repository reader
        @orchestrator.stub(:repository, error_repo) do
          # Act & Assert
          error = assert_raises(Yantra::Errors::PersistenceError) do
             @orchestrator.step_finished(@step_a_id)
          end
          assert_match(/DB down/, error.message)
        end
        # No expectations on @mock_repo needed for this specific test
      end

      def test_enqueue_step_handles_update_failure
        Time.stub :current, @frozen_time do
          # Arrange
          workflow = MockWorkflow.new(@workflow_id, :pending, "TestWorkflow")
          initial_job = MockStep.new(@step_a_id, @workflow_id, "StepA", :pending, "default")
          @mock_repo.expect(:update_workflow_attributes, true) { |*args| args[0] == @workflow_id }
          @mock_repo.expect(:find_ready_jobs, [@step_a_id], [@workflow_id])
          @mock_repo.expect(:find_step, initial_job, [@step_a_id])
          @mock_repo.expect(:update_step_attributes, false) do |step_id, attrs, opts|
              step_id == @step_a_id && attrs[:state] == StateMachine::ENQUEUED.to_s && opts == { expected_old_state: :pending }
          end
          # Act
          @orchestrator.start_workflow(@workflow_id)
          # Assert: No error raised, but enqueue didn't happen (verified by mock teardown)
        end
      end

      def test_enqueue_step_handles_worker_error_and_reverts_state
        Time.stub :current, @frozen_time do
          # Arrange
          workflow = MockWorkflow.new(@workflow_id, :pending, "TestWorkflow")
          initial_job = MockStep.new(@step_a_id, @workflow_id, "StepA", :pending, "default")
          reverted = false
          flag_set = false

          @mock_repo.expect(:update_workflow_attributes, true) { |*args| true }
          @mock_repo.expect(:find_ready_jobs, [@step_a_id], [@workflow_id])
          @mock_repo.expect(:find_step, initial_job, [@step_a_id])
          # Expect step update to enqueued, return true
          @mock_repo.expect(:update_step_attributes, true) do |id, attrs, opts|
             id == @step_a_id && attrs[:state] == StateMachine::ENQUEUED.to_s && attrs[:enqueued_at] == @frozen_time
          end
          # Expect worker enqueue to raise error
          @mock_worker.expect(:enqueue, nil) { |*args| raise Yantra::Errors::WorkerError, "Queue unavailable" }
          # Expect step update back to pending
          @mock_repo.expect(:update_step_attributes, true) do |id, attrs, opts|
             reverted = (id == @step_a_id && attrs[:state] == StateMachine::PENDING.to_s && attrs[:enqueued_at].nil?) # Check state and cleared timestamp
             true
          end
          # Expect failure flag set
          @mock_repo.expect(:set_workflow_has_failures_flag, true) { |id| flag_set = (id == @workflow_id); true }

          # Act
          @orchestrator.start_workflow(@workflow_id)

          # Assert state was reverted and flag set via mock expectations
          assert reverted, "Step state should have been reverted to pending"
          assert flag_set, "Workflow failure flag should have been set"
          # Teardown verifies mocks
        end
      end

    end
  end
end

