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
        @job_a_id = SecureRandom.uuid # Initial Job 1
        @job_b_id = SecureRandom.uuid # Initial Job 2
        @job_c_id = SecureRandom.uuid # Depends on A and B
      end

      def teardown
        # Verify that all mock expectations were met
        @mock_repo.verify
        @mock_worker.verify
      end

      # --- Test start_workflow ---

      def test_start_workflow_enqueues_initial_jobs
        # Arrange: Test with one initial job
        workflow = MockWorkflow.new(@workflow_id, :pending)
        initial_job = MockJob.new(@job_a_id, @workflow_id, "JobA", :pending, "default")
        # Expectations
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::RUNNING && attrs[:started_at].is_a?(Time) && opts == { expected_old_state: :pending }
        end
        @mock_repo.expect(:find_ready_jobs, [@job_a_id], [@workflow_id]) # Returns one ready job
        @mock_repo.expect(:find_job, initial_job, [@job_a_id])
        @mock_repo.expect(:update_job_attributes, true) do |job_id, attrs, opts|
            job_id == @job_a_id && attrs[:state] == StateMachine::ENQUEUED.to_s && attrs[:enqueued_at].is_a?(Time) && opts == { expected_old_state: :pending }
        end
        @mock_worker.expect(:enqueue, nil, [@job_a_id, @workflow_id, "JobA", "default"])
        # Act
        result = @orchestrator.start_workflow(@workflow_id)
        # Assert
        assert result, "start_workflow should return true on success"
      end

      # --- NEW TEST for Multiple Initial Jobs ---
      def test_start_workflow_enqueues_multiple_initial_jobs
        # Arrange: Workflow starts, Jobs A and B have no dependencies
        workflow = MockWorkflow.new(@workflow_id, :pending)
        initial_job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :pending, "q1")
        initial_job_b = MockJob.new(@job_b_id, @workflow_id, "JobB", :pending, "q2")

        # Expectations
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::RUNNING && attrs[:started_at].is_a?(Time) && opts == { expected_old_state: :pending }
        end
        # Expect find_ready_jobs to return MULTIPLE jobs
        @mock_repo.expect(:find_ready_jobs, [@job_a_id, @job_b_id], [@workflow_id])

        # --> Enqueue Job A
        @mock_repo.expect(:find_job, initial_job_a, [@job_a_id])
        @mock_repo.expect(:update_job_attributes, true) do |job_id, attrs, opts|
            job_id == @job_a_id && attrs[:state] == StateMachine::ENQUEUED.to_s && attrs[:enqueued_at].is_a?(Time) && opts == { expected_old_state: :pending }
        end
        @mock_worker.expect(:enqueue, nil, [@job_a_id, @workflow_id, "JobA", "q1"]) # Note queue q1

        # --> Enqueue Job B
        @mock_repo.expect(:find_job, initial_job_b, [@job_b_id])
        @mock_repo.expect(:update_job_attributes, true) do |job_id, attrs, opts|
            job_id == @job_b_id && attrs[:state] == StateMachine::ENQUEUED.to_s && attrs[:enqueued_at].is_a?(Time) && opts == { expected_old_state: :pending }
        end
        @mock_worker.expect(:enqueue, nil, [@job_b_id, @workflow_id, "JobB", "q2"]) # Note queue q2

        # Act
        result = @orchestrator.start_workflow(@workflow_id)

        # Assert
        assert result, "start_workflow should return true on success"
        # Verification of all enqueue calls happens in teardown
      end
      # --- END NEW TEST ---


      # (test_start_workflow_does_nothing_if_not_pending remains the same)
      def test_start_workflow_does_nothing_if_not_pending
        workflow = MockWorkflow.new(@workflow_id, :running)
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        result = @orchestrator.start_workflow(@workflow_id)
        refute result
      end


      # --- Test job_finished (Success Path) ---

      # (test_job_finished_success_enqueues_ready_dependent remains the same)
      def test_job_finished_success_enqueues_ready_dependent
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :succeeded, "default")
        job_b = MockJob.new(@job_b_id, @workflow_id, "JobB", :pending, "default")
        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [@job_b_id], [@job_a_id])
        @mock_repo.expect(:find_job, job_b, [@job_b_id])
        @mock_repo.expect(:get_job_dependencies, [@job_a_id], [@job_b_id])
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:find_job, job_b, [@job_b_id])
        @mock_repo.expect(:update_job_attributes, true) do |job_id, attrs, opts|
            job_id == @job_b_id && attrs[:state] == StateMachine::ENQUEUED.to_s && attrs[:enqueued_at].is_a?(Time) && opts == { expected_old_state: :pending }
        end
        @mock_worker.expect(:enqueue, nil, [@job_b_id, @workflow_id, "JobB", "default"])
        @mock_repo.expect(:running_job_count, 1, [@workflow_id])
        # Act
        @orchestrator.job_finished(@job_a_id)
      end

      # (test_job_finished_success_does_not_enqueue_if_deps_not_met remains the same)
      def test_job_finished_success_does_not_enqueue_if_deps_not_met
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :succeeded, "default")
        job_b = MockJob.new(@job_b_id, @workflow_id, "JobB", :pending, "default")
        job_c = MockJob.new(@job_c_id, @workflow_id, "JobC", :pending, "default")
        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [@job_c_id], [@job_a_id])
        @mock_repo.expect(:find_job, job_c, [@job_c_id])
        @mock_repo.expect(:get_job_dependencies, [@job_a_id, @job_b_id], [@job_c_id])
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:find_job, job_b, [@job_b_id])
        @mock_repo.expect(:running_job_count, 1, [@workflow_id])
        # Act
        @orchestrator.job_finished(@job_a_id)
      end

      # (test_job_finished_success_completes_workflow_if_last_job remains the same)
      def test_job_finished_success_completes_workflow_if_last_job
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :succeeded, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [], [@job_a_id])
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, false, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::SUCCEEDED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running }
        end
        # Act
        @orchestrator.job_finished(@job_a_id)
      end

      # (test_job_finished_failure_completes_workflow_if_last_job remains the same)
      def test_job_finished_failure_completes_workflow_if_last_job
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :failed, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [], [@job_a_id])
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::FAILED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running }
        end
        # Act
        @orchestrator.job_finished(@job_a_id)
      end

      # --- Test job_finished (Failure Path) ---

      # (test_job_finished_failure_cancels_dependents_recursively remains the same)
      def test_job_finished_failure_cancels_dependents_recursively
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :failed, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        # Expectations
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [@job_b_id], [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [@job_c_id], [@job_b_id])
        @mock_repo.expect(:get_job_dependents, [], [@job_c_id])
        @mock_repo.expect(:cancel_jobs_bulk, 2) { |job_ids| Set.new(job_ids) == Set[@job_b_id, @job_c_id] }
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::FAILED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running }
        end
        # Act
        @orchestrator.job_finished(@job_a_id)
      end


      # TODO: Add test for job cancellation cancelling dependents
      # TODO: Add test for error handling (e.g., repo methods return false/raise errors)

    end
  end
end

