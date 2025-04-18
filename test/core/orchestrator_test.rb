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

        repo.stubs(:respond_to?).with(:get_step_dependencies_multi).returns(true)
        repo.stubs(:respond_to?).with(:fetch_step_states).returns(true)

        # Local orchestrator instance with Mocha mocks
        orchestrator = Orchestrator.new(repository: repo, worker_adapter: worker, notifier: notifier)

        # Return mocks and orchestrator for use in test
        { repo: repo, worker: worker, notifier: notifier, orchestrator: orchestrator }
      end

      def test_start_workflow_enqueues_multiple_initial_jobs
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; worker = mocks[:worker]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        ready_step_ids = [@step_a_id, @step_b_id]
        workflow_running = MockWorkflow.new(id: @workflow_id, klass: "TestWorkflow", state: 'running', started_at: @frozen_time)
        step_a_pending = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'pending', queue: 'q1')
        step_b_pending = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'pending', queue: 'q2')
        step_a_enqueued = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'enqueued', queue: 'q1', enqueued_at: @frozen_time)
        step_b_enqueued = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'enqueued', queue: 'q2', enqueued_at: @frozen_time)

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('start_workflow_enqueues_multiple_initial_jobs')

          # Expectations for start_workflow
          repo.expects(:update_workflow_attributes)
            .with(@workflow_id, { state: StateMachine::RUNNING.to_s, started_at: @frozen_time }, expected_old_state: StateMachine::PENDING)
            .returns(true).in_sequence(sequence)
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)
          # --- FIX: Correct payload expectation ---
          notifier.expects(:publish)
            .with('yantra.workflow.started', has_entries(workflow_id: @workflow_id, klass: "TestWorkflow", started_at: @frozen_time))
            .in_sequence(sequence)
          # --- END FIX ---
          repo.expects(:find_ready_steps).with(@workflow_id).returns(ready_step_ids).in_sequence(sequence)

          # Expectations for the loop calling enqueue_step(A) and enqueue_step(B)
          # Note: The actual order of A vs B might vary, sequences can be tricky here.
          # If this still fails on order, we might need to remove the sequence for the enqueue part.

          # enqueue_step(A) calls:
          repo.expects(:find_step).with(@step_a_id).returns(step_a_pending).in_sequence(sequence) # Start of enqueue_step
          # --- FIX: Use keyword argument ---
          repo.expects(:update_step_attributes)
            .with(@step_a_id, has_entries(state: "enqueued"), expected_old_state: :pending)
            .returns(true).in_sequence(sequence)
          # --- END FIX ---
          repo.expects(:find_step).with(@step_a_id).returns(step_a_enqueued).in_sequence(sequence) # For payload/worker
          notifier.expects(:publish).with('yantra.step.enqueued', has_entries(step_id: @step_a_id, queue: "q1")).in_sequence(sequence)
          worker.expects(:enqueue).with(@step_a_id, @workflow_id, "StepA", "q1").in_sequence(sequence)

          # enqueue_step(B) calls:
          repo.expects(:find_step).with(@step_b_id).returns(step_b_pending).in_sequence(sequence) # Start of enqueue_step
          # --- FIX: Use keyword argument ---
          repo.expects(:update_step_attributes)
            .with(@step_b_id, has_entries(state: "enqueued"), expected_old_state: :pending)
            .returns(true).in_sequence(sequence)
          # --- END FIX ---
          repo.expects(:find_step).with(@step_b_id).returns(step_b_enqueued).in_sequence(sequence) # For payload/worker
          notifier.expects(:publish).with('yantra.step.enqueued', has_entries(step_id: @step_b_id, queue: "q2")).in_sequence(sequence)
          worker.expects(:enqueue).with(@step_b_id, @workflow_id, "StepB", "q2").in_sequence(sequence)

          # Act
          orchestrator.start_workflow(@workflow_id)
          # Assertions handled by mock verification
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
      # test/core/orchestrator_test.rb (Specific Test - Fixed)
      def test_step_succeeded_updates_state_records_output_publishes_event_and_calls_step_finished
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        output = { result: "ok" }
        step_succeeded_record = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'succeeded', finished_at: @frozen_time, output: output)

        Time.stub :current, @frozen_time do
          # Expectations for step_succeeded itself
          # --- FIX: Use keyword argument for expected_old_state ---
          repo.expects(:update_step_attributes)
            .with(
              @step_a_id, # Positional arg 1
              { state: StateMachine::SUCCEEDED.to_s, finished_at: @frozen_time }, # Positional arg 2
              expected_old_state: StateMachine::RUNNING # Keyword arg
            )
              .returns(true)
            # --- END FIX ---

            repo.expects(:record_step_output).with(@step_a_id, output).returns(true)
            repo.expects(:find_step).with(@step_a_id).returns(step_succeeded_record) # For event payload
            notifier.expects(:publish).with('yantra.step.succeeded', has_entries(step_id: @step_a_id, output: output))

            # Expect step_finished to be called internally
            orchestrator.expects(:step_finished).with(@step_a_id)

            # Act
            orchestrator.step_succeeded(@step_a_id, output)
            # Assertions are handled by mock verification
        end
      end

      # test/core/orchestrator_test.rb (Specific Test - Corrected Count)
      def test_enqueue_step_handles_update_failure
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; worker = mocks[:worker]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        step_pending = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'pending')

        # Expectations: Simulate update_step_attributes returning false
        # Expect find_step to be called ONCE at the beginning of enqueue_step
        repo.expects(:find_step).with(@step_a_id).returns(step_pending).once # <<< FIX: Expect only once

        # Expect update_step_attributes to be called and fail
        repo.expects(:update_step_attributes)
          .with(
            @step_a_id,
            has_entries(state: StateMachine::ENQUEUED.to_s), # Time check removed earlier
            expected_old_state: StateMachine::PENDING
          )
            .returns(false)

          # DO NOT expect event publish or worker enqueue because the method returns early
          notifier.expects(:publish).never
          worker.expects(:enqueue).never

          # Act: Call the private method under test
          orchestrator.send(:enqueue_step, @step_a_id)
          # Assertions handled by mock verification
      end



      def test_step_succeeded_updates_state_records_output_publishes_event_and_calls_step_finished
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        output = { result: "ok" }
        step_succeeded_record = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'succeeded', finished_at: @frozen_time, output: output)

        Time.stub :current, @frozen_time do
          # Expectations for step_succeeded itself
          repo.expects(:update_step_attributes)
            .with(@step_a_id, { state: StateMachine::SUCCEEDED.to_s, finished_at: @frozen_time }, { expected_old_state: StateMachine::RUNNING })
            .returns(true)
          repo.expects(:record_step_output).with(@step_a_id, output).returns(true) # Assume output recording succeeds
          repo.expects(:find_step).with(@step_a_id).returns(step_succeeded_record) # For event payload
          notifier.expects(:publish).with('yantra.step.succeeded', has_entries(step_id: @step_a_id, output: output))

          # Expect step_finished to be called internally
          orchestrator.expects(:step_finished).with(@step_a_id)

          # Act
          orchestrator.step_succeeded(@step_a_id, output)
          # Assertions are handled by mock verification
        end
      end

      # --- Test step_finished ---

      # --- UPDATED: Reworked expectations for fetch_step_states ---
      # test/core/orchestrator_test.rb (Specific Test - Fixed Expectations for Optimized Path)
      def test_step_finished_success_enqueues_ready_dependent
  mocks = setup_mocha_mocks_and_orchestrator
  repo = mocks[:repo]; notifier = mocks[:notifier]; worker = mocks[:worker]; orchestrator = mocks[:orchestrator]
  # Setup: Step A succeeded, Step B depends on A and is pending
  step_a_succeeded = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: 'succeeded')
  step_b_pending = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'pending', queue: 'q_b')
  step_b_enqueued = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'enqueued', queue: 'q_b', enqueued_at: @frozen_time)

  Time.stub :current, @frozen_time do
    sequence = Mocha::Sequence.new('step_finished_success_enqueues_fixed') # Use a unique sequence name

    # Expectations for step_finished(A)
    repo.expects(:find_step).with(@step_a_id).returns(step_a_succeeded).in_sequence(sequence)
    repo.expects(:get_step_dependents).with(@step_a_id).returns([@step_b_id]).in_sequence(sequence) # B depends on A

    # --- Expectations for process_dependents (Optimized Success Path) ---
    # 1. Bulk fetch dependencies for dependents [B]
    repo.expects(:get_step_dependencies_multi).with([@step_b_id]).returns({ @step_b_id => [@step_a_id] }).in_sequence(sequence) # <<< USE MULTI
    # 2. Bulk fetch states for parents [A] AND dependent [B]
    ids_to_fetch = [@step_b_id, @step_a_id].uniq
    repo.expects(:fetch_step_states)
        .with() { |actual_ids| actual_ids.sort == ids_to_fetch.sort } # Match array content ignoring order
        .returns({ @step_a_id => 'succeeded', @step_b_id => 'pending' }) # <<< CORRECT RETURN HASH
        .in_sequence(sequence)
    # 3. is_ready_to_start?(B) uses the hash, no find_step(B) call expected here
    # --- End process_dependents expectations ---

    # --- Expectations for enqueue_step(B) because it's ready ---
    # 4. find_step(B) at start of enqueue_step
    repo.expects(:find_step).with(@step_b_id).returns(step_b_pending).in_sequence(sequence)
    # 5. Update B to enqueued (Use keyword arg)
    repo.expects(:update_step_attributes)
        .with(@step_b_id, has_entries(state: StateMachine::ENQUEUED.to_s), expected_old_state: StateMachine::PENDING)
        .returns(true).in_sequence(sequence)
    # 6. Find B again (for payload/enqueue)
    repo.expects(:find_step).with(@step_b_id).returns(step_b_enqueued).in_sequence(sequence)
    # 7. Publish enqueued event for B
    notifier.expects(:publish).with('yantra.step.enqueued', has_entries(step_id: @step_b_id)).in_sequence(sequence)
    # 8. Enqueue B job
    worker.expects(:enqueue).with(@step_b_id, @workflow_id, "StepB", "q_b").in_sequence(sequence)
    # --- End enqueue_step expectations ---

    # --- Expectations for check_workflow_completion ---
    # 9. Check counts
    repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
    repo.expects(:enqueued_step_count).with(@workflow_id).returns(1).in_sequence(sequence) # B is now enqueued
    # Note: check_workflow_completion exits here as enqueued_count > 0
    # --- End check_workflow_completion expectations ---

    # Act
    orchestrator.step_finished(@step_a_id)
    # Assertions handled by mock verification
  end
end


      # --- UPDATED: Reworked expectations ---
      # test/core/orchestrator_test.rb (Specific Test - Fixed Expectations)

      def test_step_finished_success_does_not_enqueue_if_deps_not_met
  mocks = setup_mocha_mocks_and_orchestrator
  repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
  # Setup: A succeeded, C depends on A & B, B is still pending
  step_a_succeeded = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: 'succeeded', klass: "StepA")
  step_b_pending = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, state: 'pending')
  step_c_pending = MockStep.new(id: @step_c_id, workflow_id: @workflow_id, state: 'pending', klass: "StepC")
  workflow_running = MockWorkflow.new(id: @workflow_id, klass: "MyWorkflow", state: 'running')
  workflow_succeeded = MockWorkflow.new(id: @workflow_id, klass: "MyWorkflow", state: 'succeeded', finished_at: @frozen_time)

  Time.stub :current, @frozen_time do
    sequence = Mocha::Sequence.new('step_finished_deps_not_met_completes_fixed')

    # Expectations for step_finished(A)
    repo.expects(:find_step).with(@step_a_id).returns(step_a_succeeded).in_sequence(sequence)
    repo.expects(:get_step_dependents).with(@step_a_id).returns([@step_c_id]).in_sequence(sequence) # C depends on A

    # --- Expectations for process_dependents (Optimized Success Path) ---
    # 1. Bulk fetch dependencies for dependents [C]
    repo.expects(:get_step_dependencies_multi).with([@step_c_id]).returns({ @step_c_id => [@step_a_id, @step_b_id] }).in_sequence(sequence) # <<< CORRECTED EXPECTATION
    # 2. Bulk fetch states for parents [A, B] AND dependent [C]
    ids_to_fetch = [@step_c_id, @step_a_id, @step_b_id].uniq
    repo.expects(:fetch_step_states)
        .with() { |actual_ids| actual_ids.sort == ids_to_fetch.sort } # Match array content ignoring order
        .returns({ @step_c_id => 'pending', @step_a_id => 'succeeded', @step_b_id => 'pending' }) # <<< CORRECTED ARGS/RETURN
        .in_sequence(sequence)
    # 3. is_ready_to_start?(C) uses the hash, no find_step(C) call expected here
    # repo.expects(:find_step).with(@step_c_id).returns(step_c_pending).in_sequence(sequence) # <<< REMOVED EXPECTATION
    # --- End process_dependents expectations ---

    # Note: enqueue_step(C) is NOT called because B is pending

    # --- Expectations for check_workflow_completion ---
    repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
    repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence) # Nothing got enqueued
    repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence) # Check if terminal
    repo.expects(:workflow_has_failures?).with(@workflow_id).returns(false).in_sequence(sequence) # No failures
    # Update workflow to succeeded (FIX: Use keyword arg)
    repo.expects(:update_workflow_attributes)
        .with(@workflow_id, { state: StateMachine::SUCCEEDED.to_s, finished_at: @frozen_time }, expected_old_state: StateMachine::RUNNING) # <<< CORRECTED EXPECTATION
        .returns(true).in_sequence(sequence)
    repo.expects(:find_workflow).with(@workflow_id).returns(workflow_succeeded).in_sequence(sequence) # For event payload
    notifier.expects(:publish).with('yantra.workflow.succeeded', has_entries(workflow_id: @workflow_id, state: :succeeded)).in_sequence(sequence)
    # --- End check_workflow_completion expectations ---

    # Act
    orchestrator.step_finished(@step_a_id)
    # Assertions handled by mock verification
  end
end






      # --- UPDATED: Reworked expectations ---
      def test_step_finished_failure_completes_workflow_if_last_job
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        # Setup: Step A failed, no dependents
        step_a_failed = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'failed', finished_at: @frozen_time)
        final_wf_record = MockWorkflow.new(id: @workflow_id, klass: "MyWorkflow", state: 'failed', finished_at: @frozen_time)

        Time.stub :current, @frozen_time do
          # Expectations for step_finished(A)
          repo.expects(:find_step).with(@step_a_id).returns(step_a_failed)
          repo.expects(:get_step_dependents).with(@step_a_id).returns([]) # No dependents

          # Expectations for check_workflow_completion
          repo.expects(:running_step_count).with(@workflow_id).returns(0)
          repo.expects(:enqueued_step_count).with(@workflow_id).returns(0)
          repo.expects(:find_workflow).with(@workflow_id).returns(MockWorkflow.new(id: @workflow_id, state: 'running')) # Assume running before check
          repo.expects(:workflow_has_failures?).with(@workflow_id).returns(true) # Assume flag was set
          repo.expects(:update_workflow_attributes)
            .with(@workflow_id, { state: StateMachine::FAILED.to_s, finished_at: @frozen_time }, { expected_old_state: StateMachine::RUNNING })
            .returns(true)
          repo.expects(:find_workflow).with(@workflow_id).returns(final_wf_record) # For event payload
          notifier.expects(:publish).with('yantra.workflow.failed', has_entries(workflow_id: @workflow_id, state: :failed))

          # Act
          orchestrator.step_finished(@step_a_id)
          # Assertions handled by mock verification
        end
      end


      # --- Test step failure/cancellation propagation ---

      # --- UPDATED: Test step_finished with failed state ---
      # test/core/orchestrator_test.rb (Specific Test - Fixed)

      def test_step_finished_failure_cancels_dependents_recursively
  mocks = setup_mocha_mocks_and_orchestrator
  repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
  # Setup: A failed, B depends on A (pending), C depends on B (pending)
  step_a_failed = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: 'failed')
  step_b_pending = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'pending')
  step_c_pending = MockStep.new(id: @step_c_id, workflow_id: @workflow_id, klass: "StepC", state: 'pending')
  step_b_cancelled = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'cancelled')
  step_c_cancelled = MockStep.new(id: @step_c_id, workflow_id: @workflow_id, klass: "StepC", state: 'cancelled')
  workflow_running = MockWorkflow.new(id: @workflow_id, klass: "MyWorkflow", state: 'running')
  workflow_failed = MockWorkflow.new(id: @workflow_id, klass: "MyWorkflow", state: 'failed', finished_at: @frozen_time)

  Time.stub :current, @frozen_time do
    sequence = Mocha::Sequence.new('failure_cascade')

    # Expectations for step_finished(A)
    repo.expects(:find_step).with(@step_a_id).returns(step_a_failed).in_sequence(sequence)
    repo.expects(:get_step_dependents).with(@step_a_id).returns([@step_b_id]).in_sequence(sequence) # B depends on A

    # Expectations for process_dependents (Optimized Failure Path)
    # 1. Bulk fetch dependencies for dependents [B]
    repo.expects(:get_step_dependencies_multi).with([@step_b_id]).returns({ @step_b_id => [@step_a_id] }).in_sequence(sequence)
    # 2. Bulk fetch states for parents [A] AND dependents [B] (Corrected Args/Return)
    repo.expects(:fetch_step_states)
        # --- FIX: Use block constraint to match array content ignoring order ---
        .with() { |actual_ids| actual_ids.sort == [@step_a_id, @step_b_id].sort }
        # --- END FIX ---
        .returns({@step_a_id => 'failed', @step_b_id => 'pending'}) # Need state of B too
        .in_sequence(sequence)

    # 3. Enter 'else' block because A failed, call cancel_downstream_pending(B)
    #    Note: find_step(B) is NOT called here anymore because state is pre-fetched

    # Expectations for cancel_downstream_pending(B)
    # It uses the pre-fetched state ('pending')
    repo.expects(:update_step_attributes)
        .with(@step_b_id, { state: StateMachine::CANCELLED.to_s, finished_at: @frozen_time }, expected_old_state: StateMachine::PENDING)
        .returns(true).in_sequence(sequence)
    repo.expects(:find_step).with(@step_b_id).returns(step_b_cancelled).in_sequence(sequence) # Inside cancel(B) for event payload
    notifier.expects(:publish).with('yantra.step.cancelled', has_entries(step_id: @step_b_id)).in_sequence(sequence)
    repo.expects(:get_step_dependents).with(@step_b_id).returns([@step_c_id]).in_sequence(sequence) # Inside cancel(B), find C

    # Expectations for recursive call cancel_downstream_pending(C)
    # Fetch state for C because it wasn't in the initial bulk fetch triggered by A's dependents
    repo.expects(:find_step).with(@step_c_id).returns(step_c_pending).in_sequence(sequence) # Inside cancel(C)
    repo.expects(:update_step_attributes)
        .with(@step_c_id, { state: StateMachine::CANCELLED.to_s, finished_at: @frozen_time }, expected_old_state: StateMachine::PENDING)
        .returns(true).in_sequence(sequence)
    repo.expects(:find_step).with(@step_c_id).returns(step_c_cancelled).in_sequence(sequence) # Inside cancel(C) for event payload
    notifier.expects(:publish).with('yantra.step.cancelled', has_entries(step_id: @step_c_id)).in_sequence(sequence)
    repo.expects(:get_step_dependents).with(@step_c_id).returns([]).in_sequence(sequence) # Inside cancel(C), find no dependents

    # Expectations for check_workflow_completion (called after A finishes)
    repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
    repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
    repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence) # Check if terminal
    repo.expects(:workflow_has_failures?).with(@workflow_id).returns(true).in_sequence(sequence) # Assume flag was set
    repo.expects(:update_workflow_attributes) # Update to failed
        .with(@workflow_id, has_entries(state: StateMachine::FAILED.to_s), expected_old_state: StateMachine::RUNNING)
        .returns(true).in_sequence(sequence)
    repo.expects(:find_workflow).with(@workflow_id).returns(workflow_failed).in_sequence(sequence) # For event payload
    notifier.expects(:publish).with('yantra.workflow.failed', has_entries(workflow_id: @workflow_id, state: :failed)).in_sequence(sequence)

    # Act
    orchestrator.step_finished(@step_a_id)
    # Assertions handled by mock verification
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

      def test_enqueue_step_handles_worker_error_and_reverts_state # Or ...and_marks_step_failed
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; worker = mocks[:worker]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('enqueue_step_handles_worker_error')

        Time.stub :current, @frozen_time do
          initial_job = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'pending', queue: 'q_a') # Added queue
          step_enqueued = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'enqueued', queue: 'q_a', enqueued_at: @frozen_time) # Added queue
          workflow_running = MockWorkflow.new(id: @workflow_id, klass: "TestWorkflow", state: 'running', started_at: @frozen_time)
          workflow_failed = MockWorkflow.new(id: @workflow_id, klass: "TestWorkflow", state: 'failed', finished_at: @frozen_time)
          enqueue_error = Yantra::Errors::WorkerError.new("Queue unavailable")

          # --- Mocha Expectations (Corrected Sequence) ---
          # 1. Expect workflow state update to running
          repo.expects(:update_workflow_attributes)
            .with(@workflow_id, { state: StateMachine::RUNNING.to_s, started_at: @frozen_time }, expected_old_state: StateMachine::PENDING)
            .returns(true).in_sequence(sequence)

          # 2. Expect find_workflow for the 'workflow started' event payload
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)

          # 3. Expect the 'workflow started' publish call (Corrected Payload Check)
          notifier.expects(:publish)
            .with('yantra.workflow.started', has_entries(workflow_id: @workflow_id, klass: "TestWorkflow", started_at: @frozen_time)) # Check klass/started_at
            .returns(nil).in_sequence(sequence)

          # 4. Expect find_ready_steps
          repo.expects(:find_ready_steps).with(@workflow_id).returns([@step_a_id]).in_sequence(sequence)

          # --- Expectations for internal call to enqueue_step ---
          # 5. Expect find_step (at start of enqueue_step)
          repo.expects(:find_step).with(@step_a_id).returns(initial_job).in_sequence(sequence)

          # 6. Expect update_step_attributes (to enqueued)
          repo.expects(:update_step_attributes)
            .with(@step_a_id, has_entries(state: StateMachine::ENQUEUED.to_s), expected_old_state: StateMachine::PENDING)
            .returns(true).in_sequence(sequence)

          # 7. Expect find_step (for event payload / worker call)
          repo.expects(:find_step).with(@step_a_id).returns(step_enqueued).in_sequence(sequence) # Expecting the enqueued version now

          # 8. Expect publish step enqueued event
          notifier.expects(:publish).with('yantra.step.enqueued', has_entries(step_id: @step_a_id)).in_sequence(sequence)

          # 9. Expect worker enqueue to raise the error
          worker.expects(:enqueue).with(@step_a_id, @workflow_id, "StepA", "q_a").raises(enqueue_error).in_sequence(sequence) # Use queue from mock

          # --- Expectations for recovery actions in the rescue block of enqueue_step ---
          # 10. Expect update step to FAILED (Corrected State)
          repo.expects(:update_step_attributes)
            .with(@step_a_id, has_entries(state: StateMachine::FAILED.to_s, error: has_key(:message))) # Expect FAILED state
            .returns(true).in_sequence(sequence)
          # 11. Expect set workflow failure flag
          repo.expects(:set_workflow_has_failures_flag).with(@workflow_id).returns(true).in_sequence(sequence)

          # --- Expectations for check_workflow_completion ---
          # 12. Check counts
          repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          # 13. Check current workflow state
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)
          # 14. Check failures flag
          repo.expects(:workflow_has_failures?).with(@workflow_id).returns(true).in_sequence(sequence)
          # 15. Update workflow to failed
          repo.expects(:update_workflow_attributes)
            .with(@workflow_id, has_entries(state: StateMachine::FAILED.to_s), expected_old_state: StateMachine::RUNNING)
            .returns(true).in_sequence(sequence)
          # 16. Find workflow for event payload
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_failed).in_sequence(sequence)
          # 17. Publish workflow failed event
          notifier.expects(:publish).with('yantra.workflow.failed', has_entries(workflow_id: @workflow_id, state: :failed)).in_sequence(sequence)
          # --- End Expectations ---

          # Act
          # This test calls start_workflow, which should trigger the sequence above
          orchestrator.start_workflow(@workflow_id)

          # Assert: Verification of expects happens automatically via Mocha teardown
        end
      end

      # --- Test step_starting ---
      def test_step_starting_publishes_event_on_success
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('step_starting_publishes_event')

        Time.stub :current, @frozen_time do
          # Mock step record found initially (in enqueued state)
          step_enqueued = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'enqueued')
          # Mock step record found *after* successful update (for event payload)
          step_running = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'running', started_at: @frozen_time)

          # --- Mocha Expectations ---
          # 1. Expect find_step (first check in step_starting)
          repo.expects(:find_step).with(@step_a_id).returns(step_enqueued).in_sequence(sequence)
          # 2. Expect update_step_attributes to running
          repo.expects(:update_step_attributes).with(
            @step_a_id,
            has_entries(state: StateMachine::RUNNING.to_s, started_at: @frozen_time),
            { expected_old_state: StateMachine::ENQUEUED }
          ).returns(true).in_sequence(sequence)
          # 3. Expect find_step again (to get payload for event)
          repo.expects(:find_step).with(@step_a_id).returns(step_running).in_sequence(sequence)
          # 4. *** Expect the notifier publish call ***
          notifier.expects(:publish).with(
            'yantra.step.started',
            has_entries(
              step_id: @step_a_id,
              workflow_id: @workflow_id,
              klass: "StepA",
              started_at: @frozen_time
            )
          ).returns(nil).in_sequence(sequence)
          # --- End Mocha Expectations ---

          # Act
          result = orchestrator.step_starting(@step_a_id)

          # Assert
          assert result, "step_starting should return true on success"
          # Mocha verifies the expectations automatically upon teardown
        end
      end

      def test_step_starting_does_not_publish_if_update_fails
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        sequence = Mocha::Sequence.new('step_starting_no_publish_on_fail')

        Time.stub :current, @frozen_time do
          step_enqueued = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'enqueued')

          # --- Mocha Expectations ---
          repo.expects(:find_step).with(@step_a_id).returns(step_enqueued).in_sequence(sequence)
          repo.expects(:update_step_attributes).with(
            @step_a_id,
            has_entries(state: StateMachine::RUNNING.to_s),
            { expected_old_state: StateMachine::ENQUEUED }
          ).returns(false).in_sequence(sequence) # Simulate update failure
          # *** Crucially, do NOT expect notifier.publish ***
          # --- End Mocha Expectations ---

          # Act
          result = orchestrator.step_starting(@step_a_id)

          # Assert
          refute result, "step_starting should return false if update fails"
        end
      end

      def test_step_starting_does_not_publish_if_already_running
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        # No sequence needed here as the order is simple and we're primarily testing absence of calls
        # sequence = Mocha::Sequence.new('step_starting_already_running_no_publish')

        Time.stub :current, @frozen_time do
          step_already_running = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'running', started_at: @frozen_time - 10)

          # --- Mocha Expectations ---
          # 1. Expect find_step the FIRST time (at the beginning of step_starting)
          repo.expects(:find_step).with(@step_a_id).returns(step_already_running) # No .in_sequence needed

          # Note: update_step_attributes should NOT be called.
          # Note: Second find_step should NOT be called (moved inside 'if').
          # Note: *** notifier.publish should NOT be called ***
          # Mocha verifies unexpected calls, so we don't need an explicit `expects(...).never`

          # --- End Mocha Expectations ---

          # Act
          result = orchestrator.step_starting(@step_a_id)

          # Assert
          assert result, "step_starting should still return true if already running"
          # Mocha verifies the *absence* of unexpected calls (like publish) during teardown
        end
      end
    end
  end
end

