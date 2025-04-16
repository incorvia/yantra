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
        @job_a_id = SecureRandom.uuid
        @job_b_id = SecureRandom.uuid
        @job_c_id = SecureRandom.uuid
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
        initial_job = MockJob.new(@job_a_id, @workflow_id, "JobA", :pending, "default")
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |*args| true }
        @mock_repo.expect(:find_ready_jobs, [@job_a_id], [@workflow_id])
        @mock_repo.expect(:find_job, initial_job, [@job_a_id])
        @mock_repo.expect(:update_job_attributes, true) { |*args| true }
        @mock_worker.expect(:enqueue, nil, [@job_a_id, @workflow_id, "JobA", "default"])
        # Act
        result = @orchestrator.start_workflow(@workflow_id)
        # Assert
        assert result, "start_workflow should return true on success"
      end

      def test_start_workflow_enqueues_multiple_initial_jobs
         # Arrange
         workflow = MockWorkflow.new(@workflow_id, :pending)
         initial_job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :pending, "q1")
         initial_job_b = MockJob.new(@job_b_id, @workflow_id, "JobB", :pending, "q2")
         # Expectations
         @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
         @mock_repo.expect(:update_workflow_attributes, true) { |*args| true }
         @mock_repo.expect(:find_ready_jobs, [@job_a_id, @job_b_id], [@workflow_id])
         @mock_repo.expect(:find_job, initial_job_a, [@job_a_id])
         @mock_repo.expect(:update_job_attributes, true) { |*args| true }
         @mock_worker.expect(:enqueue, nil, [@job_a_id, @workflow_id, "JobA", "q1"])
         @mock_repo.expect(:find_job, initial_job_b, [@job_b_id])
         @mock_repo.expect(:update_job_attributes, true) { |*args| true }
         @mock_worker.expect(:enqueue, nil, [@job_b_id, @workflow_id, "JobB", "q2"])
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


      # --- Test job_finished (Success Path) ---
      # ... (these tests remain the same) ...
      def test_job_finished_success_enqueues_ready_dependent
        # Arrange
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :succeeded, "default")
        job_b = MockJob.new(@job_b_id, @workflow_id, "JobB", :pending, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:get_job_dependents, [@job_b_id], [@job_a_id])
        @mock_repo.expect(:find_job, job_b, [@job_b_id])
        @mock_repo.expect(:get_job_dependencies, [@job_a_id], [@job_b_id])
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:find_job, job_b, [@job_b_id])
        @mock_repo.expect(:update_job_attributes, true) { |*args| true }
        @mock_worker.expect(:enqueue, nil, [@job_b_id, @workflow_id, "JobB", "default"])
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        @mock_repo.expect(:enqueued_job_count, 1, [@workflow_id])
        # Act
        @orchestrator.job_finished(@job_a_id)
      end

      def test_job_finished_success_does_not_enqueue_if_deps_not_met
        # Arrange
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :succeeded, "default")
        job_b = MockJob.new(@job_b_id, @workflow_id, "JobB", :pending, "default")
        job_c = MockJob.new(@job_c_id, @workflow_id, "JobC", :pending, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:get_job_dependents, [@job_c_id], [@job_a_id])
        @mock_repo.expect(:find_job, job_c, [@job_c_id])
        @mock_repo.expect(:get_job_dependencies, [@job_a_id, @job_b_id], [@job_c_id])
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:find_job, job_b, [@job_b_id])
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        @mock_repo.expect(:enqueued_job_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, false, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |*args| true }
        # Act
        @orchestrator.job_finished(@job_a_id)
      end

      def test_job_finished_success_completes_workflow_if_last_job
        # Arrange
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :succeeded, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:get_job_dependents, [], [@job_a_id])
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        @mock_repo.expect(:enqueued_job_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, false, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::SUCCEEDED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running }
        end
        # Act
        @orchestrator.job_finished(@job_a_id)
      end

      def test_job_finished_failure_completes_workflow_if_last_job
        # Arrange
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :failed, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:get_job_dependents, [], [@job_a_id])
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        @mock_repo.expect(:enqueued_job_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::FAILED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running }
        end
        # Act
        @orchestrator.job_finished(@job_a_id)
      end

      # --- Test job_finished (Failure Path) ---
      def test_job_finished_failure_cancels_dependents_recursively
        # Arrange
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :failed, "default")
        job_b = MockJob.new(@job_b_id, @workflow_id, "JobB", :pending, "default")
        job_c = MockJob.new(@job_c_id, @workflow_id, "JobC", :pending, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:get_job_dependents, [@job_b_id], [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [@job_c_id], [@job_b_id])
        @mock_repo.expect(:get_job_dependents, [], [@job_c_id])
        @mock_repo.expect(:cancel_jobs_bulk, 2) { |job_ids| Set.new(job_ids) == Set[@job_b_id, @job_c_id] }
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        @mock_repo.expect(:enqueued_job_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::FAILED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running }
        end
        # Act
        @orchestrator.job_finished(@job_a_id)
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

      def test_job_finished_handles_find_job_error
        # Arrange
        @mock_repo.expect(:find_job, nil) { |_id| raise Yantra::Errors::PersistenceError, "DB down" }
        # Act & Assert
        error = assert_raises(Yantra::Errors::PersistenceError, (/DB down/)) do
           @orchestrator.job_finished(@job_a_id)
        end
        assert_match(/DB down/, error.message)
      end

      # --- UPDATED: Corrected mock syntax ---
      def test_enqueue_job_handles_update_failure
        # Arrange
        workflow = MockWorkflow.new(@workflow_id, :pending)
        initial_job = MockJob.new(@job_a_id, @workflow_id, "JobA", :pending, "default")
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |*args| true } # Workflow update succeeds
        @mock_repo.expect(:find_ready_jobs, [@job_a_id], [@workflow_id])
        @mock_repo.expect(:find_job, initial_job, [@job_a_id])
        # Mock job update to fail
        # Expect update call, return false, use block ONLY for validation
        @mock_repo.expect(:update_job_attributes, false) do |job_id, attrs, opts|
            # Block validates arguments and must return true if they match
            match = (job_id == @job_a_id && attrs[:state] == StateMachine::ENQUEUED.to_s && opts == { expected_old_state: :pending })
            match # Return true/false based on argument match
        end
        # --> Expect NO call to worker.enqueue

        # Act: start_workflow calls find_and_enqueue -> enqueue_job internally
        @orchestrator.start_workflow(@workflow_id)
        # Assert: No error raised, but enqueue didn't happen (verified by mock teardown)
      end

      def test_enqueue_job_handles_worker_error
        # Arrange
        workflow = MockWorkflow.new(@workflow_id, :pending)
        initial_job = MockJob.new(@job_a_id, @workflow_id, "JobA", :pending, "default")
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |*args| true }
        @mock_repo.expect(:find_ready_jobs, [@job_a_id], [@workflow_id])
        @mock_repo.expect(:find_job, initial_job, [@job_a_id])
        @mock_repo.expect(:update_job_attributes, true) { |*args| true } # Job update succeeds
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

