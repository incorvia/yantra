# test/core/orchestrator_test.rb

require "test_helper"
require "yantra/core/orchestrator"
require "yantra/core/state_machine"
require "yantra/errors"
require "minitest/mock" # For mocking dependencies

# Define simple Structs to represent data returned by the mock repository
MockJob = Struct.new(:id, :workflow_id, :klass, :state, :queue_name)
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

      def test_start_workflow_enqueues_initial_jobs
        # Arrange
        workflow = MockWorkflow.new(@workflow_id, :pending)
        initial_job = MockJob.new(@job_a_id, @workflow_id, "JobA", :pending, "default")

        # Expectations on mocks
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        # Expect workflow state update to running - Use block for validation
        # Ensure state comparison uses the SYMBOL as passed by the orchestrator
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id &&
            attrs[:state] == StateMachine::RUNNING && # <<< FIXED: Compare symbol
            attrs[:started_at].is_a?(Time) &&
            opts == { expected_old_state: :pending }
        end
        # Expect call to find ready jobs (assume Job A is ready)
        @mock_repo.expect(:find_ready_jobs, [@job_a_id], [@workflow_id])
        # Expect call to find the ready job before enqueuing
        @mock_repo.expect(:find_job, initial_job, [@job_a_id])
        # Expect state update for Job A to enqueued - Use block for validation
        # Orchestrator's enqueue_job passes state as string, so compare string here
        @mock_repo.expect(:update_job_attributes, true) do |job_id, attrs, opts|
            job_id == @job_a_id &&
            attrs[:state] == StateMachine::ENQUEUED.to_s && # <<< Compare string state (correct for enqueue_job)
            attrs[:enqueued_at].is_a?(Time) &&
            opts == { expected_old_state: :pending }
        end
        # Expect call to enqueue Job A via worker adapter
        @mock_worker.expect(:enqueue, nil, [@job_a_id, @workflow_id, "JobA", "default"])

        # Act
        result = @orchestrator.start_workflow(@workflow_id)

        # Assert
        assert result, "start_workflow should return true on success"
      end

      def test_start_workflow_does_nothing_if_not_pending
        # Arrange
        workflow = MockWorkflow.new(@workflow_id, :running) # Already running
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        # No other calls to repo or worker should happen

        # Act
        result = @orchestrator.start_workflow(@workflow_id)

        # Assert
        refute result, "start_workflow should return false if not starting"
      end

      # --- Test job_finished (Success Path) ---

      def test_job_finished_success_enqueues_ready_dependent
        # Arrange: Job A succeeded, Job B depends on A, Job B is pending
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :succeeded, "default")
        job_b = MockJob.new(@job_b_id, @workflow_id, "JobB", :pending, "default")

        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id]) # Find finished job A
        @mock_repo.expect(:get_job_dependents, [@job_b_id], [@job_a_id]) # Job B depends on A
        @mock_repo.expect(:find_job, job_b, [@job_b_id]) # Find dependent job B (it's pending)
        @mock_repo.expect(:get_job_dependencies, [@job_a_id], [@job_b_id]) # Job B's only dependency is A
        @mock_repo.expect(:find_job, job_a, [@job_a_id]) # Check dependency A's state (it's succeeded)
        # --> Now enqueue Job B
        @mock_repo.expect(:find_job, job_b, [@job_b_id]) # Find B again for enqueue_job
        # Expect state update for Job B to enqueued - Use block for validation
        @mock_repo.expect(:update_job_attributes, true) do |job_id, attrs, opts|
            job_id == @job_b_id &&
            attrs[:state] == StateMachine::ENQUEUED.to_s && # <<< Compare string state (correct for enqueue_job)
            attrs[:enqueued_at].is_a?(Time) &&
            opts == { expected_old_state: :pending }
        end
        @mock_worker.expect(:enqueue, nil, [@job_b_id, @workflow_id, "JobB", "default"]) # Enqueue B
        # --> Check workflow completion
        @mock_repo.expect(:running_job_count, 1, [@workflow_id]) # Assume other jobs still running

        # Act
        @orchestrator.job_finished(@job_a_id)
      end

      def test_job_finished_success_completes_workflow_if_last_job
        # Arrange: Job A succeeded, it was the last running job, no failures occurred
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :succeeded, "default")
        workflow = MockWorkflow.new(@workflow_id, :running) # Workflow was running

        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id]) # Find finished job A
        @mock_repo.expect(:get_job_dependents, [], [@job_a_id]) # No dependents
        # --> Check workflow completion
        @mock_repo.expect(:running_job_count, 0, [@workflow_id]) # No jobs left running
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id]) # Find workflow for state check
        @mock_repo.expect(:workflow_has_failures?, false, [@workflow_id]) # No failures occurred
        # --> Update workflow state to succeeded - Use block for validation
        # Orchestrator's check_workflow_completion passes state as string
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id &&
            attrs[:state] == StateMachine::SUCCEEDED.to_s && # <<< Compare string state
            attrs[:finished_at].is_a?(Time) &&
            opts == { expected_old_state: :running }
        end

        # Act
        @orchestrator.job_finished(@job_a_id)
      end

      def test_job_finished_failure_completes_workflow_if_last_job
        # Arrange: Job A failed, it was the last running job
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :failed, "default")
        workflow = MockWorkflow.new(@workflow_id, :running) # Workflow was running

        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id]) # Find finished job A
        # Expect call to cancel downstream (even if none, it gets called)
        @mock_repo.expect(:get_job_dependents, [], [@job_a_id]) # Find dependents (returns none)
        # Expect find_all_descendants helper to return empty set []
        # Expect cancel_jobs_bulk NOT to be called because descendant set is empty
        # --> Check workflow completion
        @mock_repo.expect(:running_job_count, 0, [@workflow_id]) # No jobs left running
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id]) # Find workflow for state check
        @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id]) # Failures DID occur (flag was set)
        # --> Update workflow state to failed - Use block for validation
        # Orchestrator's check_workflow_completion passes state as string
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id &&
            attrs[:state] == StateMachine::FAILED.to_s && # <<< Compare string state
            attrs[:finished_at].is_a?(Time) &&
            opts == { expected_old_state: :running }
        end

        # Act
        @orchestrator.job_finished(@job_a_id)
      end

      # --- Test job_finished (Failure Path) ---

      def test_job_finished_failure_cancels_dependents_recursively # Name reflects old logic, now bulk
        # Arrange: Job A fails. Job B depends on A. Job C depends on B.
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :failed, "default")
        # We don't need job B and C objects directly, just their IDs for mock returns
        workflow = MockWorkflow.new(@workflow_id, :running) # Workflow is running

        # Expectations Sequence:
        # 1. Find failed job A
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        # 2. Find all descendants of A (B and C) via traversal
        #    - First call for A's dependents
        @mock_repo.expect(:get_job_dependents, [@job_b_id], [@job_a_id])
        #    - Second call for B's dependents
        @mock_repo.expect(:get_job_dependents, [@job_c_id], [@job_b_id])
        #    - Third call for C's dependents
        @mock_repo.expect(:get_job_dependents, [], [@job_c_id])
        # 3. Bulk cancel descendants B and C
        @mock_repo.expect(:cancel_jobs_bulk, 2) do |job_ids| # Expect return value 2 (updated count)
             # Check that the correct IDs are passed, order doesn't matter for Set comparison
             Set.new(job_ids) == Set[@job_b_id, @job_c_id]
        end
        # 4. Check workflow completion (assume A was last running job)
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        # 5. Find workflow for state check
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        # 6. Check failures flag (it's true because A failed)
        @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id])
        # 7. Update workflow state to failed - Use block for validation
        # Orchestrator's check_workflow_completion passes state as string
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id &&
            attrs[:state] == StateMachine::FAILED.to_s && # <<< Compare string state
            attrs[:finished_at].is_a?(Time) &&
            opts == { expected_old_state: :running }
        end


        # Act
        @orchestrator.job_finished(@job_a_id)

        # Assertions handled by mock verification in teardown
      end


      # TODO: Add test for check_and_enqueue_dependents when NOT all deps are met
      # TODO: Add test for find_and_enqueue_ready_jobs finding multiple jobs
      # TODO: Add test for error handling (e.g., repo methods return false/raise errors)

    end
  end
end

