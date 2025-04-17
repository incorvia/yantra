# test/core/orchestrator_test.rb

require "test_helper"
# require 'mocha/minitest' # Ensure this is in test_helper.rb
require "yantra/core/orchestrator"
require "yantra/core/state_machine"
require "yantra/errors"
require "yantra/persistence/repository_interface"
require "yantra/worker/enqueuing_interface"
require "yantra/events/notifier_interface"
# require "minitest/mock" # No longer needed
require "ostruct"
require 'time' # <-- Add require for Time

class StepAJob; end

# Define simple Structs - still useful for defining return values
# *** FIX: Use STRING states in mocks to match DB/comparison logic ***
MockStep = Struct.new(:id, :workflow_id, :klass, :state, :queue, :output, :error, :retries, :created_at, :enqueued_at, :started_at, :finished_at, :dependencies) do
  def initialize(id: nil, workflow_id: nil, klass: nil, state: 'pending', queue: 'default', output: nil, error: nil, retries: nil, created_at: nil, enqueued_at: nil, started_at: nil, finished_at: nil, dependencies: [])
    # Ensure state is stored as a string
    super(id, workflow_id, klass, state.to_s, queue, output, error, retries, created_at, enqueued_at, started_at, finished_at, dependencies || [])
  end
end
MockWorkflow = Struct.new(:id, :state, :klass, :started_at, :finished_at, :has_failures) do
  def initialize(id: nil, state: 'pending', klass: nil, started_at: nil, finished_at: nil, has_failures: false)
    # Ensure state is stored as a string
    super(id, state.to_s, klass, started_at, finished_at, has_failures)
  end
end
# *** END FIX ***

module Yantra
  module Core
    class OrchestratorTest < Minitest::Test
      include Mocha::API # Ensure Mocha methods are available

      # --- Define setup variables used across tests ---
      def setup
        # Use fixed IDs for easier debugging if needed
        @workflow_id = "wf-#{SecureRandom.uuid}"
        @step_a_id = "step-a-#{SecureRandom.uuid}"
        @step_b_id = "step-b-#{SecureRandom.uuid}"
        @step_c_id = "step-c-#{SecureRandom.uuid}"
        # Freeze time for consistent timestamp checks if needed
        @frozen_time = Time.parse("2025-04-16 15:30:00 -0500") # Example time
      end

      # --- Helper for Mocha setup within tests ---
      def setup_mocha_mocks_and_orchestrator
        # Use self.mock, self.stubs, self.expects provided by Mocha integration
        repo = mock('repository')
        worker = mock('worker_adapter')
        notifier = mock('notifier')

        # Stubs for initializer checks
        repo.stubs(:is_a?).with(Yantra::Persistence::RepositoryInterface).returns(true)
        worker.stubs(:is_a?).with(Yantra::Worker::EnqueuingInterface).returns(true)
        # Use respond_to? check for notifier as per user's orchestrator.rb version
        notifier.stubs(:respond_to?).with(:publish).returns(true)
        # Also stub is_a? in case other parts rely on it
        notifier.stubs(:is_a?).with(Yantra::Events::NotifierInterface).returns(true)


        # Local orchestrator instance with Mocha mocks
        orchestrator = Orchestrator.new(repository: repo, worker_adapter: worker, notifier: notifier)

        # Return mocks and orchestrator for use in test
        { repo: repo, worker: worker, notifier: notifier, orchestrator: orchestrator }
      end

      def test_start_workflow_enqueues_multiple_initial_jobs
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; worker = mocks[:worker]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('start_workflow_enqueues_multiple_initial_jobs')

        Time.stub :current, @frozen_time do
          # Mock data needed for test setup and returns
          workflow_running = MockWorkflow.new(id: @workflow_id, klass: "TestWorkflow", state: 'running', started_at: @frozen_time)
          initial_step_a = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'pending', queue: "q1")
          initial_step_b = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'pending', queue: "q2")
          # Mock records after update for event payloads
          step_a_enqueued = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'enqueued', queue: "q1", enqueued_at: @frozen_time)
          step_b_enqueued = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'enqueued', queue: "q2", enqueued_at: @frozen_time)

          # --- Mocha Expectations (Corrected Sequence v32) ---
          # 1. Expect workflow update to running
          repo.expects(:update_workflow_attributes).with(
            @workflow_id,
            has_entries(state: StateMachine::RUNNING.to_s, started_at: @frozen_time),
            expected_old_state: StateMachine::PENDING
          ).returns(true).in_sequence(sequence)

          # 2. Expect find_workflow for the 'workflow started' event payload
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)
          # 3. Expect the 'workflow started' publish call
          notifier.expects(:publish).with(
            'yantra.workflow.started',
            has_entries(workflow_id: @workflow_id, state: StateMachine::RUNNING)
          ).returns(nil).in_sequence(sequence)

          # 4. Expect find_ready_jobs
          repo.expects(:find_ready_steps).with(@workflow_id).returns([@step_a_id, @step_b_id]).in_sequence(sequence)

          # 5. Process Step A
          repo.expects(:find_step).with(@step_a_id).returns(initial_step_a).in_sequence(sequence)
          repo.expects(:update_step_attributes).with(
            @step_a_id,
            has_entries(state: StateMachine::ENQUEUED.to_s, enqueued_at: @frozen_time),
            expected_old_state: StateMachine::PENDING
          ).returns(true).in_sequence(sequence) # Update A
          worker.expects(:enqueue).with(@step_a_id, @workflow_id, "StepA", "q1").returns(nil).in_sequence(sequence) # Enqueue A
          # --- FIX: Add expectations for step A enqueued event ---
          repo.expects(:find_step).with(@step_a_id).returns(step_a_enqueued).in_sequence(sequence)
          notifier.expects(:publish).with(
            'yantra.step.enqueued',
            has_entries(step_id: @step_a_id, queue: "q1", enqueued_at: @frozen_time)
          ).returns(nil).in_sequence(sequence) # Publish A enqueued
          # --- End FIX ---

          # 6. Process Step B
          repo.expects(:find_step).with(@step_b_id).returns(initial_step_b).in_sequence(sequence) # Find B (1st time)
          repo.expects(:update_step_attributes).with(
            @step_b_id,
            has_entries(state: StateMachine::ENQUEUED.to_s, enqueued_at: @frozen_time),
            expected_old_state: StateMachine::PENDING
          ).returns(true).in_sequence(sequence) # Update B
          worker.expects(:enqueue).with(@step_b_id, @workflow_id, "StepB", "q2").returns(nil).in_sequence(sequence) # Enqueue B
          # --- FIX: Add expectations for step B enqueued event ---
          repo.expects(:find_step).with(@step_b_id).returns(step_b_enqueued).in_sequence(sequence) # Find B (2nd time, for event)
          notifier.expects(:publish).with(
            'yantra.step.enqueued',
            has_entries(step_id: @step_b_id, queue: "q2", enqueued_at: @frozen_time)
          ).returns(nil).in_sequence(sequence) # Publish B enqueued
          # --- End FIX ---
          # --- End Mocha Expectations ---

          # Act
          result = orchestrator.start_workflow(@workflow_id)
          assert result
        end
      end





      # =========================================================================
      def test_start_workflow_does_nothing_if_not_pending
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('start_workflow_does_nothing_if_not_pending')

        Time.stub :current, @frozen_time do
          workflow = MockWorkflow.new(id: @workflow_id, state: 'running') # Data for return value

          # Mocha Expectations
          repo.expects(:update_workflow_attributes).with(
            @workflow_id,
            has_entries(state: StateMachine::RUNNING.to_s),
            { expected_old_state: StateMachine::PENDING }
          ).returns(false).in_sequence(sequence) # Simulate update failure due to state mismatch
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow).in_sequence(sequence) # Expect find on failure

          # Act
          result = orchestrator.start_workflow(@workflow_id)
          refute result
        end
      end


      # --- Test step_succeeded ---
      # (Converted to Mocha - Corrected v21: Match orchestrator event payload)
      def test_step_succeeded_updates_state_and_calls_step_finished_and_publishes_event
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('step_succeeded_updates_state')

        Time.stub :current, @frozen_time do
          result_output = { message: "Done" }
          step_a_updated = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'succeeded', output: result_output, finished_at: @frozen_time)

          # Mocha Expectations
          repo.expects(:update_step_attributes).with(
            @step_a_id,
            has_entries(state: StateMachine::SUCCEEDED.to_s, output: result_output, finished_at: @frozen_time),
            { expected_old_state: StateMachine::RUNNING }
          ).returns(true).in_sequence(sequence)
          repo.expects(:find_step).with(@step_a_id).returns(step_a_updated).in_sequence(sequence)
          notifier.expects(:publish).with(
            'yantra.step.succeeded',
            has_entries(
              step_id: @step_a_id,
              workflow_id: @workflow_id,
              klass: "StepA",
              finished_at: @frozen_time,
              output: result_output
            )
          ).returns(nil).in_sequence(sequence)
          orchestrator.expects(:step_finished).with(@step_a_id).returns(nil).in_sequence(sequence)

          # Act
          orchestrator.step_succeeded(@step_a_id, result_output)
        end
      end

      def test_enqueue_step_handles_update_failure
  mocks = setup_mocha_mocks_and_orchestrator
  repo = mocks[:repo]; orchestrator = mocks[:orchestrator]; notifier = mocks[:notifier]
  # No sequence needed, just verify the update fails

  Time.stub :current, @frozen_time do
    initial_job = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'pending') # Data only
    # Mock workflow record needed for the publish call in start_workflow
    workflow_running = MockWorkflow.new(id: @workflow_id, klass: "TestWorkflow", state: 'running', started_at: @frozen_time)


    # --- Mocha Expectations (Corrected v36) ---
    # 1. Stub workflow update to allow start_workflow to proceed
    repo.stubs(:update_workflow_attributes)
        .with(@workflow_id, has_key(:state), has_key(:expected_old_state))
        .returns(true)

    # 2. Expect find_workflow for the 'workflow started' event payload
    repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running)
    # 3. Expect the 'workflow started' publish call
    notifier.expects(:publish).with(
        'yantra.workflow.started',
        has_entries(workflow_id: @workflow_id, state: StateMachine::RUNNING)
        ).returns(nil)

    # --- FIX: Add missing expectations ---
    # 4. Expect find_ready_steps
    repo.expects(:find_ready_steps).with(@workflow_id).returns([@step_a_id])
    # 5. Expect find_step (for enqueue_job)
    repo.expects(:find_step).with(@step_a_id).returns(initial_job)
    # --- End FIX ---

    # 6. Expect step update to FAIL
    repo.expects(:update_step_attributes).with(
      @step_a_id,
      # FIX: Expect the attributes the code actually tries to set
      has_entries(state: StateMachine::ENQUEUED.to_s, enqueued_at: @frozen_time),
      expected_old_state: StateMachine::PENDING
    ).returns(false) # <<< Key expectation: Simulate failure
    # --- End Mocha Expectations ---

    # Act
    orchestrator.start_workflow(@workflow_id)

    # Assert: Verification of expects happens automatically.
    # We implicitly assert that no error was raised and worker.enqueue was not called.
  end
end





      # --- Test step_failed ---
      # (Converted to Mocha)
      def test_step_failed_calls_step_finished
        mocks = setup_mocha_mocks_and_orchestrator
        orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('step_failed_calls_step_finished')

        # Mocha Expectation
        orchestrator.expects(:step_finished).with(@step_a_id).returns(nil).in_sequence(sequence)

        # Act
        orchestrator.step_failed(@step_a_id)
      end

      # --- Test step_finished (Success Path) ---

      # =========================================================================
      # MOCHA TEST - Using Sequence (Corrected v23)
      # =========================================================================
      def test_step_finished_success_enqueues_ready_dependent
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; worker = mocks[:worker]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('step_finished_success_enqueues_ready_dependent')

        Time.stub :current, @frozen_time do
          step_a = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'succeeded')
          step_b = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'pending', queue: 'default')
          # Mock step B record after update for event payload
          step_b_enqueued = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'enqueued', queue: 'default', enqueued_at: @frozen_time)


          # --- Mocha Expectations (Corrected Sequence v27) ---
          repo.expects(:find_step).with(@step_a_id).returns(step_a).in_sequence(sequence) # In step_finished
          repo.expects(:get_step_dependents).with(@step_a_id).returns([@step_b_id]).in_sequence(sequence) # In process_dependents
          repo.expects(:find_step).with(@step_b_id).returns(step_b).in_sequence(sequence) # In process_dependents (find B)
          # Should check dependencies now
          repo.expects(:get_step_dependencies).with(@step_b_id).returns([@step_a_id]).in_sequence(sequence) # In dependencies_met?
          repo.expects(:find_step).with(@step_a_id).returns(step_a).in_sequence(sequence) # In dependencies_met? (Check A state)
          # Should enqueue B
          repo.expects(:find_step).with(@step_b_id).returns(step_b).in_sequence(sequence) # In enqueue_step (find B before update)
          repo.expects(:update_step_attributes).with(
            @step_b_id,
            has_entries(state: StateMachine::ENQUEUED.to_s, enqueued_at: @frozen_time),
            { expected_old_state: StateMachine::PENDING }
          ).returns(true).in_sequence(sequence) # In enqueue_step
          # Enqueue call happens next
          worker.expects(:enqueue).with(@step_b_id, @workflow_id, "StepB", "default").returns(nil).in_sequence(sequence) # In enqueue_step
          # --- FIX: Add expectations for event publishing inside enqueue_step ---
          repo.expects(:find_step).with(@step_b_id).returns(step_b_enqueued).in_sequence(sequence) # In enqueue_step (after enqueue for event)
          notifier.expects(:publish).with(
            'yantra.step.enqueued',
            has_entries(step_id: @step_b_id, enqueued_at: @frozen_time, queue: 'default')
          ).in_sequence(sequence) # In enqueue_step
          # --- End FIX ---
          # Should check workflow completion AFTER enqueue attempt block finishes
          repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence) # In check_workflow_completion
          repo.expects(:enqueued_step_count).with(@workflow_id).returns(1).in_sequence(sequence) # In check_workflow_completion (B is enqueued)
          # --- End Mocha Expectations ---

          orchestrator.step_finished(@step_a_id)
        end
      end


      # =========================================================================
      # MOCHA TEST - Using Sequence
      # =========================================================================
      def test_step_finished_success_does_not_enqueue_if_deps_not_met
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('step_finished_success_does_not_enqueue')

        Time.stub :current, @frozen_time do
          step_a = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'succeeded')
          step_b = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'pending')
          step_c = MockStep.new(id: @step_c_id, workflow_id: @workflow_id, klass: "StepC", state: 'pending')
          workflow_succeeded = MockWorkflow.new(id: @workflow_id, klass: "TestWorkflow", state: 'succeeded', finished_at: @frozen_time)

          # --- Mocha Expectations ---
          repo.expects(:find_step).with(@step_a_id).returns(step_a).in_sequence(sequence) # In step_finished
          repo.expects(:get_step_dependents).with(@step_a_id).returns([@step_c_id]).in_sequence(sequence) # In process_dependents
          repo.expects(:find_step).with(@step_c_id).returns(step_c).in_sequence(sequence) # In process_dependents (find C)
          repo.expects(:get_step_dependencies).with(@step_c_id).returns([@step_a_id, @step_b_id]).in_sequence(sequence) # In dependencies_met?
          repo.expects(:find_step).with(@step_a_id).returns(step_a).in_sequence(sequence) # In dependencies_met? (Check A state)
          repo.expects(:find_step).with(@step_b_id).returns(step_b).in_sequence(sequence) # In dependencies_met? (Check B state -> Pending!)
          # Workflow Completion Checks:
          repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence) # In check_workflow_completion
          repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence) # In check_workflow_completion
          # --- Corrected Sequence for check_workflow_completion ---
          repo.expects(:workflow_has_failures?).with(@workflow_id).returns(false).in_sequence(sequence)
          repo.expects(:update_workflow_attributes).with(
            @workflow_id,
            has_entries(state: StateMachine::SUCCEEDED.to_s, finished_at: @frozen_time),
            { expected_old_state: StateMachine::RUNNING }
          ).returns(true).in_sequence(sequence)
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_succeeded).in_sequence(sequence) # Find AFTER update for event
          # ------------------------------------------------------
          notifier.expects(:publish).with(
            'yantra.workflow.succeeded',
            # Match payload from orchestrator check_workflow_completion
            has_entries(workflow_id: @workflow_id, state: StateMachine::SUCCEEDED, finished_at: @frozen_time)
          ).returns(nil).in_sequence(sequence)
          # --- End Mocha Expectations ---

          orchestrator.step_finished(@step_a_id)
        end
      end
      # =========================================================================

      # =========================================================================
      # MOCHA TEST - Using Sequence (Corrected)
      # =========================================================================
      def test_step_finished_success_completes_workflow_if_last_job
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('step_finished_success_completes')

        Time.stub :current, @frozen_time do
          step_a = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'succeeded')
          workflow_succeeded = MockWorkflow.new(id: @workflow_id, klass: "TestWorkflow", state: 'succeeded', started_at: @frozen_time - 10, finished_at: @frozen_time, has_failures: false)

          # --- Mocha Expectations ---
          repo.expects(:find_step).with(@step_a_id).returns(step_a).in_sequence(sequence)
          repo.expects(:get_step_dependents).with(@step_a_id).returns([]).in_sequence(sequence)
          repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          # --- Corrected Sequence for check_workflow_completion ---
          repo.expects(:workflow_has_failures?).with(@workflow_id).returns(false).in_sequence(sequence)
          repo.expects(:update_workflow_attributes).with(
            @workflow_id,
            has_entries(state: StateMachine::SUCCEEDED.to_s, finished_at: @frozen_time),
            { expected_old_state: StateMachine::RUNNING }
          ).returns(true).in_sequence(sequence)
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_succeeded).in_sequence(sequence)
          # ------------------------------------------------------
          notifier.expects(:publish).with(
            'yantra.workflow.succeeded',
            # Match payload from orchestrator check_workflow_completion
            has_entries(
              workflow_id: @workflow_id,
              state: StateMachine::SUCCEEDED, # Use constant symbol
              finished_at: @frozen_time
            )
          ).returns(nil).in_sequence(sequence)
          # --- End Mocha Expectations ---

          orchestrator.step_finished(@step_a_id)
        end
      end
      # =========================================================================


      # =========================================================================
      # MOCHA TEST - Using Sequence (Corrected)
      # =========================================================================
      def test_step_finished_failure_completes_workflow_if_last_job
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('step_finished_failure_completes')

        Time.stub :current, @frozen_time do
          step_a = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'failed')
          workflow_failed = MockWorkflow.new(id: @workflow_id, klass: "TestWorkflow", state: 'failed', started_at: @frozen_time - 10, finished_at: @frozen_time, has_failures: true)

          # --- Mocha Expectations ---
          repo.expects(:find_step).with(@step_a_id).returns(step_a).in_sequence(sequence)
          repo.expects(:get_step_dependents).with(@step_a_id).returns([]).in_sequence(sequence)
          repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          # --- Corrected Sequence for check_workflow_completion ---
          repo.expects(:workflow_has_failures?).with(@workflow_id).returns(true).in_sequence(sequence)
          repo.expects(:update_workflow_attributes).with(
            @workflow_id,
            has_entries(state: StateMachine::FAILED.to_s, finished_at: @frozen_time),
            { expected_old_state: StateMachine::RUNNING }
          ).returns(true).in_sequence(sequence)
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_failed).in_sequence(sequence)
          # ------------------------------------------------------
          notifier.expects(:publish).with(
            'yantra.workflow.failed',
            # Match payload from orchestrator check_workflow_completion
            has_entries(
              workflow_id: @workflow_id,
              state: StateMachine::FAILED, # Use constant symbol
              finished_at: @frozen_time
            )
          ).returns(nil).in_sequence(sequence)
          # --- End Mocha Expectations ---

          orchestrator.step_finished(@step_a_id)
        end
      end
      # =========================================================================


      # --- Test step_finished (Failure Path) ---

      # =========================================================================
      # MOCHA TEST - Using Sequence
      # =========================================================================
      def test_step_finished_failure_cancels_dependents_recursively
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('step_finished_failure_cancels')

        Time.stub :current, @frozen_time do
          step_a = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'failed')
          step_b = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'pending')
          step_c = MockStep.new(id: @step_c_id, workflow_id: @workflow_id, klass: "StepC", state: 'pending')
          workflow_failed = MockWorkflow.new(id: @workflow_id, klass: "TestWorkflow", state: 'failed', finished_at: @frozen_time, has_failures: true)

          # --- Mocha Expectations ---
          repo.expects(:find_step).with(@step_a_id).returns(step_a).in_sequence(sequence) # In step_finished
          repo.expects(:get_step_dependents).with(@step_a_id).returns([@step_b_id]).in_sequence(sequence) # In process_dependents
          repo.expects(:find_step).with(@step_b_id).returns(step_b).in_sequence(sequence) # In cancel_downstream_pending (check B state)
          repo.expects(:cancel_steps_bulk).with([@step_b_id]).returns(1).in_sequence(sequence) # In cancel_downstream_pending (cancel B)
          notifier.expects(:publish).with('yantra.step.cancelled', has_entries(step_id: @step_b_id)).in_sequence(sequence) # In cancel_downstream_pending (publish B cancelled)
          repo.expects(:get_step_dependents).with(@step_b_id).returns([@step_c_id]).in_sequence(sequence) # In cancel_downstream_pending (find C)
          repo.expects(:find_step).with(@step_c_id).returns(step_c).in_sequence(sequence) # In recursive cancel_downstream_pending (check C state)
          repo.expects(:cancel_steps_bulk).with([@step_c_id]).returns(1).in_sequence(sequence) # In recursive cancel_downstream_pending (cancel C)
          notifier.expects(:publish).with('yantra.step.cancelled', has_entries(step_id: @step_c_id)).in_sequence(sequence) # In recursive cancel_downstream_pending (publish C cancelled)
          repo.expects(:get_step_dependents).with(@step_c_id).returns([]).in_sequence(sequence) # In recursive cancel_downstream_pending (find C dependents)
          # Workflow Completion Checks:
          repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          # --- Corrected Sequence for check_workflow_completion ---
          repo.expects(:workflow_has_failures?).with(@workflow_id).returns(true).in_sequence(sequence)
          repo.expects(:update_workflow_attributes).with(
            @workflow_id,
            has_entries(state: StateMachine::FAILED.to_s, finished_at: @frozen_time),
            { expected_old_state: StateMachine::RUNNING }
          ).returns(true).in_sequence(sequence)
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_failed).in_sequence(sequence)
          # ---------------------------------------------
          notifier.expects(:publish).with(
            'yantra.workflow.failed',
            has_entries(workflow_id: @workflow_id, state: StateMachine::FAILED) # Use constant symbol
          ).returns(nil).in_sequence(sequence)
          # --- End Mocha Expectations ---

          orchestrator.step_finished(@step_a_id)
        end
      end
      # =========================================================================


      # --- Error Handling / Edge Case Tests ---
      # (Converted to Mocha - Corrected v20: Removed eventing expectations)
      def test_start_workflow_handles_workflow_update_failure
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; orchestrator = mocks[:orchestrator]
        # sequence = Mocha::Sequence.new('start_workflow_handles_update_failure') # Removed sequence

        Time.stub :current, @frozen_time do
          workflow = MockWorkflow.new(id: @workflow_id, state: 'pending') # Data only

          # Mocha Expectations
          repo.expects(:update_workflow_attributes).with(
            @workflow_id,
            has_entries(state: StateMachine::RUNNING.to_s),
            { expected_old_state: StateMachine::PENDING }
          ).returns(false) # Simulate failure
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow) # Called after failure

          # Act
          result = orchestrator.start_workflow(@workflow_id)
          refute result
        end
      end

      # Fixed this test in v9 - uses extend, not mocks - KEEP AS IS
      def test_step_finished_handles_find_step_error
        # Arrange
        error_repo = Object.new
        error_repo.extend(Yantra::Persistence::RepositoryInterface)
        def error_repo.find_step(id); raise Yantra::Errors::PersistenceError, "DB down"; end

        dummy_notifier = Object.new.extend(Yantra::Events::NotifierInterface)
        dummy_worker = Object.new.extend(Yantra::Worker::EnqueuingInterface)

        orchestrator_with_error = Orchestrator.new(repository: error_repo, worker_adapter: dummy_worker, notifier: dummy_notifier)

        # Act & Assert
        error = assert_raises(Yantra::Errors::PersistenceError) do
          orchestrator_with_error.step_finished(@step_a_id)
        end
        assert_match(/DB down/, error.message)
      end

      # (Converted to Mocha - Corrected v20: Removed eventing expectations)

      def test_enqueue_step_handles_worker_error_and_reverts_state
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; worker = mocks[:worker]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('enqueue_step_handles_worker_error') # Use sequence again

        Time.stub :current, @frozen_time do
          initial_job = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'pending') # Data only
          # Mock workflow record needed for the publish call in start_workflow
          workflow_running = MockWorkflow.new(id: @workflow_id, klass: "TestWorkflow", state: 'running', started_at: @frozen_time)

          # --- Mocha Expectations (Corrected Sequence) ---
          # 1. Expect workflow state update to running
          repo.expects(:update_workflow_attributes).with(
            @workflow_id,
            has_entries(state: StateMachine::RUNNING.to_s, started_at: @frozen_time),
            { expected_old_state: StateMachine::PENDING }
          ).returns(true).in_sequence(sequence)

          # 2. Expect find_workflow for the 'workflow started' event payload
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)

          # 3. Expect the 'workflow started' publish call
          notifier.expects(:publish).with(
            'yantra.workflow.started',
            has_entries(workflow_id: @workflow_id, state: StateMachine::RUNNING)
          ).returns(nil).in_sequence(sequence)

          # 4. Expect find_ready_steps
          repo.expects(:find_ready_steps).with(@workflow_id).returns([@step_a_id]).in_sequence(sequence)

          # 5. Expect find_step (for enqueue_step)
          repo.expects(:find_step).with(@step_a_id).returns(initial_job).in_sequence(sequence)

          # 6. Expect update_step_attributes (to enqueued)
          repo.expects(:update_step_attributes).with(
            @step_a_id,
            has_entries(state: StateMachine::ENQUEUED.to_s, enqueued_at: @frozen_time),
            { expected_old_state: StateMachine::PENDING }
          ).returns(true).in_sequence(sequence)

          # 7. Expect worker enqueue to raise the error
          worker.expects(:enqueue)
            .with(@step_a_id, @workflow_id, "StepA", "default")
            .raises(Yantra::Errors::WorkerError, "Queue unavailable").in_sequence(sequence)

          # 8. Expect the recovery actions in the rescue block of enqueue_step
          repo.expects(:update_step_attributes).with(
            @step_a_id,
            has_entries(state: StateMachine::PENDING.to_s, enqueued_at: nil) # Reverted state
          ).returns(true).in_sequence(sequence)
          repo.expects(:set_workflow_has_failures_flag).with(@workflow_id).returns(true).in_sequence(sequence)
          # --- End Mocha Expectations ---

          # Act
          # This calls start_workflow, which should trigger the sequence above
          orchestrator.start_workflow(@workflow_id)

          # Assert: Verification of expects happens automatically via Mocha teardown
        end
      end
    end
  end
end

