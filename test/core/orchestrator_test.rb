# test/core/orchestrator_test.rb

require "test_helper"
require "yantra/core/orchestrator"
require "yantra/core/state_machine"
require "yantra/errors"
require "minitest/mock" # For mocking dependencies

# Define simple Structs to represent data returned by the mock repository
MockJob = Struct.new(:id, :workflow_id, :klass, :state, :queue)
MockWorkflow = Struct.new(:id, :state)

module Yantra
  module Core
    class OrchestratorTest < Minitest::Test

      def setup
        # Create mocks for dependencies
        @mock_repo = Minitest::Mock.new
        @mock_worker = Minitest::Mock.new

        # Instantiate the orchestrator with mocks
        @orchestrator = Orchestrator.new(repository: @mock_repo, worker_adapter: @mock_worker)

        # Common IDs used in tests
        @workflow_id = SecureRandom.uuid
        @step_a_id = SecureRandom.uuid
        @step_b_id = SecureRandom.uuid
        @step_c_id = SecureRandom.uuid
      end

      def teardown
        # Verify that all mock expectations were met
        @mock_repo.verify
        @mock_worker.verify
      end

      # --- Test start_workflow ---
      # ... (tests remain the same) ...
      def test_start_workflow_enqueues_initial_jobs
        # Arrange
        workflow = MockWorkflow.new(@workflow_id, :pending)
        initial_job = MockJob.new(@step_a_id, @workflow_id, "StepA", :pending, "default")
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |*args| true }
        @mock_repo.expect(:find_ready_jobs, [@step_a_id], [@workflow_id])
        @mock_repo.expect(:find_step, initial_job, [@step_a_id])
        @mock_repo.expect(:update_step_attributes, true) { |*args| true }
        @mock_worker.expect(:enqueue, nil, [@step_a_id, @workflow_id, "StepA", "default"])
        # Act
        result = @orchestrator.start_workflow(@workflow_id)
        # Assert
        assert result, "start_workflow should return true on success"
      end

      def test_start_workflow_enqueues_multiple_initial_jobs
         # Arrange
         workflow = MockWorkflow.new(@workflow_id, :pending)
         initial_step_a = MockJob.new(@step_a_id, @workflow_id, "StepA", :pending, "q1")
         initial_step_b = MockJob.new(@step_b_id, @workflow_id, "StepB", :pending, "q2")
         # Expectations
         @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
         @mock_repo.expect(:update_workflow_attributes, true) { |*args| true }
         @mock_repo.expect(:find_ready_jobs, [@step_a_id, @step_b_id], [@workflow_id])
         @mock_repo.expect(:find_step, initial_step_a, [@step_a_id])
         @mock_repo.expect(:update_step_attributes, true) { |*args| true }
         @mock_worker.expect(:enqueue, nil, [@step_a_id, @workflow_id, "StepA", "q1"])
         @mock_repo.expect(:find_step, initial_step_b, [@step_b_id])
         @mock_repo.expect(:update_step_attributes, true) { |*args| true }
         @mock_worker.expect(:enqueue, nil, [@step_b_id, @workflow_id, "StepB", "q2"])
         # Act
         result = @orchestrator.start_workflow(@workflow_id)
         # Assert
         assert result
      end

      def test_start_workflow_does_nothing_if_not_pending
        workflow = MockWorkflow.new(@workflow_id, :running)
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        result = @orchestrator.start_workflow(@workflow_id)
        refute result
      end


      # --- Test step_finished (Success Path) ---
      # ... (these tests remain the same) ...
      def test_step_finished_success_enqueues_ready_dependent
        # Arrange
        step_a = MockJob.new(@step_a_id, @workflow_id, "StepA", :succeeded, "default")
        step_b = MockJob.new(@step_b_id, @workflow_id, "StepB", :pending, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_step, step_a, [@step_a_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:get_step_dependents, [@step_b_id], [@step_a_id])
        @mock_repo.expect(:find_step, step_b, [@step_b_id])
        @mock_repo.expect(:get_step_dependencies, [@step_a_id], [@step_b_id])
        @mock_repo.expect(:find_step, step_a, [@step_a_id])
        @mock_repo.expect(:find_step, step_b, [@step_b_id])
        @mock_repo.expect(:update_step_attributes, true) { |*args| true }
        @mock_worker.expect(:enqueue, nil, [@step_b_id, @workflow_id, "StepB", "default"])
        @mock_repo.expect(:running_step_count, 0, [@workflow_id])
        @mock_repo.expect(:enqueued_step_count, 1, [@workflow_id])
        # Act
        @orchestrator.step_finished(@step_a_id)
      end

      def test_step_finished_success_does_not_enqueue_if_deps_not_met
        # Arrange
        step_a = MockJob.new(@step_a_id, @workflow_id, "StepA", :succeeded, "default")
        step_b = MockJob.new(@step_b_id, @workflow_id, "StepB", :pending, "default")
        step_c = MockJob.new(@step_c_id, @workflow_id, "StepC", :pending, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_step, step_a, [@step_a_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:get_step_dependents, [@step_c_id], [@step_a_id])
        @mock_repo.expect(:find_step, step_c, [@step_c_id])
        @mock_repo.expect(:get_step_dependencies, [@step_a_id, @step_b_id], [@step_c_id])
        @mock_repo.expect(:find_step, step_a, [@step_a_id])
        @mock_repo.expect(:find_step, step_b, [@step_b_id])
        @mock_repo.expect(:running_step_count, 0, [@workflow_id])
        @mock_repo.expect(:enqueued_step_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, false, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |*args| true }
        # Act
        @orchestrator.step_finished(@step_a_id)
      end

      def test_step_finished_success_completes_workflow_if_last_job
        # Arrange
        step_a = MockJob.new(@step_a_id, @workflow_id, "StepA", :succeeded, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_step, step_a, [@step_a_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:get_step_dependents, [], [@step_a_id])
        @mock_repo.expect(:running_step_count, 0, [@workflow_id])
        @mock_repo.expect(:enqueued_step_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, false, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::SUCCEEDED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running }
        end
        # Act
        @orchestrator.step_finished(@step_a_id)
      end

      def test_step_finished_failure_completes_workflow_if_last_job
        # Arrange
        step_a = MockJob.new(@step_a_id, @workflow_id, "StepA", :failed, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_step, step_a, [@step_a_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:get_step_dependents, [], [@step_a_id])
        @mock_repo.expect(:running_step_count, 0, [@workflow_id])
        @mock_repo.expect(:enqueued_step_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::FAILED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running }
        end
        # Act
        @orchestrator.step_finished(@step_a_id)
      end

      # --- Test step_finished (Failure Path) ---
      def test_step_finished_failure_cancels_dependents_recursively
        # Arrange
        step_a = MockJob.new(@step_a_id, @workflow_id, "StepA", :failed, "default")
        step_b = MockJob.new(@step_b_id, @workflow_id, "StepB", :pending, "default")
        step_c = MockJob.new(@step_c_id, @workflow_id, "StepC", :pending, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_step, step_a, [@step_a_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:get_step_dependents, [@step_b_id], [@step_a_id])
        @mock_repo.expect(:get_step_dependents, [@step_c_id], [@step_b_id])
        @mock_repo.expect(:get_step_dependents, [], [@step_c_id])
        @mock_repo.expect(:cancel_jobs_bulk, 2) { |step_ids| Set.new(step_ids) == Set[@step_b_id, @step_c_id] }
        @mock_repo.expect(:running_step_count, 0, [@workflow_id])
        @mock_repo.expect(:enqueued_step_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::FAILED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running }
        end
        # Act
        @orchestrator.step_finished(@step_a_id)
      end


      # --- Error Handling Tests ---

      # --- UPDATED: Corrected mock syntax ---
      def test_start_workflow_handles_workflow_update_failure
        # Arrange
        workflow = MockWorkflow.new(@workflow_id, :pending)
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        # Expect update call, return false, use block ONLY for validation
        @mock_repo.expect(:update_workflow_attributes, false) do |wf_id, attrs, opts|
            # Block validates arguments and must return true if they match
            match = (wf_id == @workflow_id && attrs[:state] == StateMachine::RUNNING.to_s && opts == { expected_old_state: :pending })
            match # Return true/false based on argument match
        end

        # Act
        result = @orchestrator.start_workflow(@workflow_id)

        # Assert
        refute result
      end

      def test_step_finished_handles_find_step_error
        # Arrange
        @mock_repo.expect(:find_step, nil) { |_id| raise Yantra::Errors::PersistenceError, "DB down" }
        # Act & Assert
        error = assert_raises(Yantra::Errors::PersistenceError, (/DB down/)) do
           @orchestrator.step_finished(@step_a_id)
        end
        assert_match(/DB down/, error.message)
      end

      # --- UPDATED: Corrected mock syntax ---
      def test_enqueue_step_handles_update_failure
        # Arrange
        workflow = MockWorkflow.new(@workflow_id, :pending)
        initial_job = MockJob.new(@step_a_id, @workflow_id, "StepA", :pending, "default")
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |*args| true } # Workflow update succeeds
        @mock_repo.expect(:find_ready_jobs, [@step_a_id], [@workflow_id])
        @mock_repo.expect(:find_step, initial_job, [@step_a_id])
        # Mock job update to fail
        # Expect update call, return false, use block ONLY for validation
        @mock_repo.expect(:update_step_attributes, false) do |step_id, attrs, opts|
            # Block validates arguments and must return true if they match
            match = (step_id == @step_a_id && attrs[:state] == StateMachine::ENQUEUED.to_s && opts == { expected_old_state: :pending })
            match # Return true/false based on argument match
        end
        # --> Expect NO call to worker.enqueue

        # Act: start_workflow calls find_and_enqueue -> enqueue_job internally
        @orchestrator.start_workflow(@workflow_id)
        # Assert: No error raised, but enqueue didn't happen (verified by mock teardown)
      end

      def test_enqueue_step_handles_worker_error
        # Arrange
        workflow = MockWorkflow.new(@workflow_id, :pending)
        initial_job = MockJob.new(@step_a_id, @workflow_id, "StepA", :pending, "default")
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |*args| true }
        @mock_repo.expect(:find_ready_jobs, [@step_a_id], [@workflow_id])
        @mock_repo.expect(:find_step, initial_job, [@step_a_id])
        @mock_repo.expect(:update_step_attributes, true) { |*args| true } # Job update succeeds
        @mock_worker.expect(:enqueue, nil) { |*args| raise StandardError, "Queue unavailable" }
        # Act & Assert
        error = assert_raises(StandardError, (/Queue unavailable/)) do
           @orchestrator.start_workflow(@workflow_id)
        end
        assert_match(/Queue unavailable/, error.message)
      end

    end
  end
end

