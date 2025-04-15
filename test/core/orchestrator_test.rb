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
      # (test_start_workflow_enqueues_initial_jobs remains the same)
      def test_start_workflow_enqueues_initial_jobs
        workflow = MockWorkflow.new(@workflow_id, :pending)
        initial_job = MockJob.new(@job_a_id, @workflow_id, "JobA", :pending, "default")
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |wf_id, attrs, opts| wf_id == @workflow_id && attrs[:state] == StateMachine::RUNNING && attrs[:started_at].is_a?(Time) && opts == { expected_old_state: :pending } }
        @mock_repo.expect(:find_ready_jobs, [@job_a_id], [@workflow_id])
        @mock_repo.expect(:find_job, initial_job, [@job_a_id])
        @mock_repo.expect(:update_job_attributes, true) { |job_id, attrs, opts| job_id == @job_a_id && attrs[:state] == StateMachine::ENQUEUED.to_s && attrs[:enqueued_at].is_a?(Time) && opts == { expected_old_state: :pending } }
        @mock_worker.expect(:enqueue, nil, [@job_a_id, @workflow_id, "JobA", "default"])
        result = @orchestrator.start_workflow(@workflow_id)
        assert result
      end

      # (test_start_workflow_enqueues_multiple_initial_jobs remains the same)
      def test_start_workflow_enqueues_multiple_initial_jobs
         workflow = MockWorkflow.new(@workflow_id, :pending)
         initial_job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :pending, "q1")
         initial_job_b = MockJob.new(@job_b_id, @workflow_id, "JobB", :pending, "q2")
         @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
         @mock_repo.expect(:update_workflow_attributes, true) { |wf_id, attrs, opts| wf_id == @workflow_id && attrs[:state] == StateMachine::RUNNING && attrs[:started_at].is_a?(Time) && opts == { expected_old_state: :pending } }
         @mock_repo.expect(:find_ready_jobs, [@job_a_id, @job_b_id], [@workflow_id])
         @mock_repo.expect(:find_job, initial_job_a, [@job_a_id])
         @mock_repo.expect(:update_job_attributes, true) { |job_id, attrs, opts| job_id == @job_a_id && attrs[:state] == StateMachine::ENQUEUED.to_s && attrs[:enqueued_at].is_a?(Time) && opts == { expected_old_state: :pending } }
         @mock_worker.expect(:enqueue, nil, [@job_a_id, @workflow_id, "JobA", "q1"])
         @mock_repo.expect(:find_job, initial_job_b, [@job_b_id])
         @mock_repo.expect(:update_job_attributes, true) { |job_id, attrs, opts| job_id == @job_b_id && attrs[:state] == StateMachine::ENQUEUED.to_s && attrs[:enqueued_at].is_a?(Time) && opts == { expected_old_state: :pending } }
         @mock_worker.expect(:enqueue, nil, [@job_b_id, @workflow_id, "JobB", "q2"])
         result = @orchestrator.start_workflow(@workflow_id)
         assert result
      end

      # (test_start_workflow_does_nothing_if_not_pending remains the same)
      def test_start_workflow_does_nothing_if_not_pending
        workflow = MockWorkflow.new(@workflow_id, :running)
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        result = @orchestrator.start_workflow(@workflow_id)
        refute result
      end


      # --- Test job_finished (Success Path) ---
      # (Previous job_finished success tests remain the same)
      def test_job_finished_success_enqueues_ready_dependent
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :succeeded, "default")
        job_b = MockJob.new(@job_b_id, @workflow_id, "JobB", :pending, "default")
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [@job_b_id], [@job_a_id])
        @mock_repo.expect(:find_job, job_b, [@job_b_id])
        @mock_repo.expect(:get_job_dependencies, [@job_a_id], [@job_b_id])
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:find_job, job_b, [@job_b_id])
        @mock_repo.expect(:update_job_attributes, true) { |job_id, attrs, opts| job_id == @job_b_id && attrs[:state] == StateMachine::ENQUEUED.to_s && attrs[:enqueued_at].is_a?(Time) && opts == { expected_old_state: :pending } }
        @mock_worker.expect(:enqueue, nil, [@job_b_id, @workflow_id, "JobB", "default"])
        @mock_repo.expect(:running_job_count, 1, [@workflow_id])
        @orchestrator.job_finished(@job_a_id)
      end

      def test_job_finished_success_does_not_enqueue_if_deps_not_met
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :succeeded, "default")
        job_b = MockJob.new(@job_b_id, @workflow_id, "JobB", :pending, "default")
        job_c = MockJob.new(@job_c_id, @workflow_id, "JobC", :pending, "default")
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [@job_c_id], [@job_a_id])
        @mock_repo.expect(:find_job, job_c, [@job_c_id])
        @mock_repo.expect(:get_job_dependencies, [@job_a_id, @job_b_id], [@job_c_id])
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:find_job, job_b, [@job_b_id]) # Job B is pending, so C not ready
        @mock_repo.expect(:running_job_count, 1, [@workflow_id]) # Job B still pending/running
        @orchestrator.job_finished(@job_a_id)
      end

      def test_job_finished_success_completes_workflow_if_last_job
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :succeeded, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [], [@job_a_id])
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, false, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |wf_id, attrs, opts| wf_id == @workflow_id && attrs[:state] == StateMachine::SUCCEEDED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running } }
        @orchestrator.job_finished(@job_a_id)
      end

      def test_job_finished_failure_completes_workflow_if_last_job
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :failed, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [], [@job_a_id])
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |wf_id, attrs, opts| wf_id == @workflow_id && attrs[:state] == StateMachine::FAILED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running } }
        @orchestrator.job_finished(@job_a_id)
      end

      # --- Test job_finished (Failure Path) ---

      def test_job_finished_failure_cancels_dependents_recursively
        job_a = MockJob.new(@job_a_id, @workflow_id, "JobA", :failed, "default")
        workflow = MockWorkflow.new(@workflow_id, :running)
        @mock_repo.expect(:find_job, job_a, [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [@job_b_id], [@job_a_id])
        @mock_repo.expect(:get_job_dependents, [@job_c_id], [@job_b_id])
        @mock_repo.expect(:get_job_dependents, [], [@job_c_id])
        @mock_repo.expect(:cancel_jobs_bulk, 2) { |job_ids| Set.new(job_ids) == Set[@job_b_id, @job_c_id] }
        @mock_repo.expect(:running_job_count, 0, [@workflow_id])
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        @mock_repo.expect(:workflow_has_failures?, true, [@workflow_id])
        @mock_repo.expect(:update_workflow_attributes, true) { |wf_id, attrs, opts| wf_id == @workflow_id && attrs[:state] == StateMachine::FAILED.to_s && attrs[:finished_at].is_a?(Time) && opts == { expected_old_state: :running } }
        @orchestrator.job_finished(@job_a_id)
      end


      # --- Error Handling Tests ---

      def test_start_workflow_handles_workflow_update_failure
        # Arrange
        workflow = MockWorkflow.new(@workflow_id, :pending)
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        # Expect update call, make it return false, check args with block
        @mock_repo.expect(:update_workflow_attributes, false) do |wf_id, attrs, opts|
            wf_id == @workflow_id && attrs[:state] == StateMachine::RUNNING && opts == { expected_old_state: :pending }
        end
        # --> Expect NO calls to find_ready_jobs or enqueue

        # Act
        result = @orchestrator.start_workflow(@workflow_id)

        # Assert
        refute result
      end

      def test_job_finished_handles_find_job_error
        # Arrange: Expect find_job to be called with @job_a_id
        # Use block form only, remove args array. Block raises error.
        @mock_repo.expect(
          :find_job,
          nil # Return value is irrelevant as block raises
          # No args array here
        ) do |job_id| # Block receives arguments
            assert_equal @job_a_id, job_id # Verify correct ID passed
            raise Yantra::Errors::PersistenceError, "DB down"
        end

        # Act & Assert: Expect the error to propagate (Orchestrator doesn't rescue yet)
        assert_raises(Yantra::Errors::PersistenceError, /DB down/) do
           @orchestrator.job_finished(@job_a_id)
        end
        # No other expectations needed as execution stops early
      end

      def test_enqueue_job_handles_update_failure
        # Arrange
        workflow = MockWorkflow.new(@workflow_id, :pending)
        initial_job = MockJob.new(@job_a_id, @workflow_id, "JobA", :pending, "default")
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        # Expect workflow update to succeed, check args with block
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
           wf_id == @workflow_id && attrs[:state] == StateMachine::RUNNING && opts == { expected_old_state: :pending }
        end
        @mock_repo.expect(:find_ready_jobs, [@job_a_id], [@workflow_id])
        @mock_repo.expect(:find_job, initial_job, [@job_a_id])
        # Expect job update call, make it return false, check args with block
        @mock_repo.expect(:update_job_attributes, false) do |job_id, attrs, opts|
            job_id == @job_a_id && attrs[:state] == StateMachine::ENQUEUED.to_s && opts == { expected_old_state: :pending }
        end
        # --> Expect NO call to worker_adapter.enqueue

        # Act
        @orchestrator.start_workflow(@workflow_id)
      end

      def test_enqueue_job_handles_worker_error
        # Arrange
        workflow = MockWorkflow.new(@workflow_id, :pending)
        initial_job = MockJob.new(@job_a_id, @workflow_id, "JobA", :pending, "default")
        @mock_repo.expect(:find_workflow, workflow, [@workflow_id])
        # Expect workflow update to succeed, check args with block
        @mock_repo.expect(:update_workflow_attributes, true) do |wf_id, attrs, opts|
           wf_id == @workflow_id && attrs[:state] == StateMachine::RUNNING && opts == { expected_old_state: :pending }
        end
        @mock_repo.expect(:find_ready_jobs, [@job_a_id], [@workflow_id])
        @mock_repo.expect(:find_job, initial_job, [@job_a_id])
        # Expect job update to succeed, check args with block
        @mock_repo.expect(:update_job_attributes, true) do |job_id, attrs, opts|
            job_id == @job_a_id && attrs[:state] == StateMachine::ENQUEUED.to_s && opts == { expected_old_state: :pending }
        end
        # Expect enqueue call, make it raise error using block form
        # Remove the args array, use only the block
        @mock_worker.expect(
           :enqueue,
           nil # Return value irrelevant
           # No args array here
        ) do |jid, wfid, kl, qn| # Block receives args
            # Optional: Check args inside block
            assert_equal @job_a_id, jid
            assert_equal @workflow_id, wfid
            assert_equal "JobA", kl
            assert_equal "default", qn
            # Raise the error
            raise StandardError, "Queue unavailable"
        end

        # Act & Assert
        # Orchestrator#enqueue_job doesn't rescue this yet, so error should propagate
        assert_raises(StandardError, /Queue unavailable/) do
           @orchestrator.start_workflow(@workflow_id)
        end
      end

      # TODO: Add test for job cancellation cancelling dependents (If needed, covered by failure?)
      # TODO: Add test for error handling in job_finished (e.g., repo methods return false/raise errors)
      # TODO: Add test for error handling in cancel_downstream_jobs

    end
  end
end

