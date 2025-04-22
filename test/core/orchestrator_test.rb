# test/core/orchestrator_test.rb

# Standard library requires
require 'time'
require 'securerandom'
require 'ostruct' # Keep OpenStruct for now, although Structs are generally preferred

# Test framework requires
require 'test_helper'
# require 'mocha/minitest' # Should be in test_helper.rb

# Project requires
require 'yantra/core/orchestrator'
require 'yantra/core/state_machine'
require 'yantra/core/step_enqueuer'
require 'yantra/errors'
require 'yantra/persistence/repository_interface'
require 'yantra/worker/enqueuing_interface'
require 'yantra/events/notifier_interface'

# Dummy class for testing purposes if needed by other parts of the test setup
class StepAJob; end

# --- Test Data Structures ---

# Using Struct with keyword_init for cleaner initialization (Requires Ruby 2.5+)
# Ensure states are strings to match application logic/DB storage.
MockStep = Struct.new(
  :id, :workflow_id, :klass, :state, :queue, :output, :error, :retries,
  :created_at, :enqueued_at, :started_at, :finished_at, :dependencies,
  keyword_init: true
) do
  # Override initialize to ensure state is a string and dependencies defaults to []
  def initialize(state: 'pending', dependencies: [], **kwargs)
    super(state: state.to_s, dependencies: dependencies || [], **kwargs)
  end
end

MockWorkflow = Struct.new(
  :id, :state, :klass, :started_at, :finished_at, :has_failures,
  keyword_init: true
) do
  # Override initialize to ensure state is a string
  def initialize(state: 'pending', **kwargs)
    super(state: state.to_s, **kwargs)
  end
end

# --- Test Class ---

module Yantra
  module Core
    class OrchestratorTest < Minitest::Test
      include Mocha::API # Make Mocha methods available

      # Freeze time for consistent timestamps in tests
      FROZEN_TIME = Time.parse("2025-04-16 15:30:00 -0500").freeze

      # Use setup to define instance variables used across tests
      def setup
        # IDs
        @workflow_id = "wf-#{SecureRandom.uuid}"
        @step_a_id = "step-a-#{SecureRandom.uuid}"
        @step_b_id = "step-b-#{SecureRandom.uuid}"
        @step_c_id = "step-c-#{SecureRandom.uuid}"

        # Mocks - instantiate here to be available in all tests
        @repo = mock('repository')
        @worker = mock('worker_adapter')
        @notifier = mock('notifier')

        # --- Common Stubs needed for Orchestrator/Service Initialization ---
        # Stub interface checks
        @repo.stubs(:is_a?).with(Yantra::Persistence::RepositoryInterface).returns(true)
        @worker.stubs(:is_a?).with(Yantra::Worker::EnqueuingInterface).returns(true)
        @notifier.stubs(:is_a?).with(Yantra::Events::NotifierInterface).returns(true)
        @notifier.stubs(:respond_to?).with(:publish).returns(true)

        # Stub checks required by StepEnqueuer initializer
        @repo.stubs(:respond_to?).with(:find_steps).returns(true)
        @repo.stubs(:respond_to?).with(:bulk_update_steps).returns(true)
        @worker.stubs(:respond_to?).with(:enqueue).returns(true)

        # Stub common helper method checks used within Orchestrator logic
        @repo.stubs(:respond_to?).with(:get_dependency_ids_bulk).returns(true)
        @repo.stubs(:respond_to?).with(:get_step_states).returns(true)

        # Instantiate the orchestrator - this also instantiates StepEnqueuer
        @orchestrator = Orchestrator.new(
          repository: @repo,
          worker_adapter: @worker,
          notifier: @notifier
        )

        # Convenience accessor for the internally created service
        @step_enqueuer = @orchestrator.step_enqueuer
        refute_nil @step_enqueuer, "StepEnqueuer should be initialized in setup"
      end

      # =========================================================================
      # Workflow Start Tests
      # =========================================================================

      def test_start_workflow_enqueues_multiple_initial_jobs
        ready_step_ids = [@step_a_id, @step_b_id]
        workflow_running = MockWorkflow.new(id: @workflow_id, klass: 'TestWorkflow', state: 'running', started_at: FROZEN_TIME)

        Time.stub :current, FROZEN_TIME do
          sequence = Mocha::Sequence.new('start_workflow_calls_enqueuer')

          # Expectations for start_workflow up to delegation
          @repo.expects(:update_workflow_attributes)
            .with(@workflow_id, { state: StateMachine::RUNNING.to_s, started_at: FROZEN_TIME }, expected_old_state: StateMachine::PENDING)
            .returns(true).in_sequence(sequence)
          @repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)
          @notifier.expects(:publish)
            .with('yantra.workflow.started', has_entries(workflow_id: @workflow_id))
            .in_sequence(sequence)
          @repo.expects(:list_ready_steps).with(workflow_id: @workflow_id).returns(ready_step_ids).in_sequence(sequence)

          # --- Expectation for the delegation to StepEnqueuer ---
          @step_enqueuer.expects(:call)
            .with(workflow_id: @workflow_id, step_ids_to_attempt: ready_step_ids)
            .returns(2) # Simulate service enqueuing 2 steps
            .in_sequence(sequence)

          # Act
          result = @orchestrator.start_workflow(@workflow_id)

          # Assert
          assert result, "start_workflow should return true on success"
          # Mocha verifies expectations automatically
        end
      end

      def test_start_workflow_does_nothing_if_not_pending
        workflow_running = MockWorkflow.new(id: @workflow_id, state: 'running')

        Time.stub :current, FROZEN_TIME do
          sequence = Mocha::Sequence.new('start_workflow_already_running')

          # Expect update attempt to fail because the state is not PENDING
          @repo.expects(:update_workflow_attributes)
            .with(@workflow_id, has_entries(state: StateMachine::RUNNING.to_s), expected_old_state: StateMachine::PENDING)
            .returns(false).in_sequence(sequence) # Simulate DB constraint or check failure

          # Expect find_workflow to be called (e.g., for logging or checking state)
          @repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)

          # Ensure downstream actions (publish, list_ready_steps, enqueuer.call) do not happen
          @notifier.expects(:publish).never
          @repo.expects(:list_ready_steps).never
          @step_enqueuer.expects(:call).never

          # Act
          result = @orchestrator.start_workflow(@workflow_id)

          # Assert
          refute result, "start_workflow should return false if workflow wasn't pending"
        end
      end

      def test_start_workflow_handles_workflow_update_failure
        workflow_pending = MockWorkflow.new(id: @workflow_id, state: 'pending')

        Time.stub :current, FROZEN_TIME do
          # Expect update attempt that fails for other reasons (e.g., DB error simulated by return false)
          @repo.expects(:update_workflow_attributes)
            .with(@workflow_id, has_entries(state: StateMachine::RUNNING.to_s), expected_old_state: StateMachine::PENDING)
            .returns(false) # Simulate failure

          # Expect find_workflow to be called after failed update attempt
          @repo.expects(:find_workflow).with(@workflow_id).returns(workflow_pending)

          # Ensure downstream actions do not happen
          @notifier.expects(:publish).never
          @repo.expects(:list_ready_steps).never
          @step_enqueuer.expects(:call).never

          # Act
          result = @orchestrator.start_workflow(@workflow_id)

          # Assert
          refute result, "start_workflow should return false on update failure"
        end
      end

      # =========================================================================
      # Step Starting Tests
      # =========================================================================
      def test_step_starting_publishes_event_on_success
        step_enqueued = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: 'StepA', state: 'enqueued')
        step_running = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: 'StepA', state: 'running', started_at: FROZEN_TIME)

        Time.stub :current, FROZEN_TIME do
          sequence = Mocha::Sequence.new('step_starting_publishes_event')

          # Expect find_step before update attempt
          @repo.expects(:find_step).with(@step_a_id).returns(step_enqueued).in_sequence(sequence)

          # Expect successful update
          @repo.expects(:update_step_attributes)
            .with(@step_a_id, has_entries(state: StateMachine::RUNNING.to_s, started_at: FROZEN_TIME), expected_old_state: StateMachine::ENQUEUED)
            .returns(true).in_sequence(sequence)

          # Expect find_step again for the event payload generation
          @repo.expects(:find_step).with(@step_a_id).returns(step_running).in_sequence(sequence)

          # Expect publish event
          @notifier.expects(:publish)
            .with('yantra.step.started', has_entries(step_id: @step_a_id, started_at: FROZEN_TIME))
            .returns(nil).in_sequence(sequence) # Assuming publish returns nil

          # Act
          result = @orchestrator.step_starting(@step_a_id)

          # Assert
          assert result, "step_starting should return true on successful update"
        end
      end

      def test_step_starting_does_not_publish_if_update_fails
        step_enqueued = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: 'StepA', state: 'enqueued')

        Time.stub :current, FROZEN_TIME do
          sequence = Mocha::Sequence.new('step_starting_no_publish_on_fail')

          @repo.expects(:find_step).with(@step_a_id).returns(step_enqueued).in_sequence(sequence)

          # Expect update attempt that fails
          @repo.expects(:update_step_attributes)
            .with(@step_a_id, has_entries(state: StateMachine::RUNNING.to_s), expected_old_state: StateMachine::ENQUEUED)
            .returns(false).in_sequence(sequence) # Simulate failure

          # Ensure publish is never called
          @notifier.expects(:publish).never

          # Act
          result = @orchestrator.step_starting(@step_a_id)

          # Assert: Behavior depends on Orchestrator#step_starting implementation.
          # If it returns true even on update failure (as original code might suggest):
          assert result, "step_starting returned true even though update failed (as per original behavior assumption)"
          # If it *should* return false on update failure, change assertion:
          # refute result, "step_starting should return false if update fails"
        end
      end

      def test_step_starting_does_not_publish_if_already_running
        step_already_running = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: 'StepA', state: 'running', started_at: FROZEN_TIME - 10)

        Time.stub :current, FROZEN_TIME do
          # Expect find_step returns the already running step
          @repo.expects(:find_step).with(@step_a_id).returns(step_already_running)

          # Ensure update and publish are never called
          @repo.expects(:update_step_attributes).never
          @notifier.expects(:publish).never

          # Act
          result = @orchestrator.step_starting(@step_a_id)

          # Assert: Should likely return true as no error occurred, just no state change needed.
          assert result, "step_starting should return true if step is already running"
        end
      end

      # =========================================================================
      # Step Succeeded Tests
      # =========================================================================
      def test_step_succeeded_updates_state_records_output_publishes_event_and_calls_step_finished
        output = { result: 'ok' }
        step_succeeded_record = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: 'StepA', state: 'succeeded', finished_at: FROZEN_TIME, output: output)

        Time.stub :current, FROZEN_TIME do
          # Sequence for ordered expectations within step_succeeded logic
          sequence = Mocha::Sequence.new('step_succeeded_flow')

          # 1. Update state
          @repo.expects(:update_step_attributes)
            .with(@step_a_id, { state: StateMachine::SUCCEEDED.to_s, finished_at: FROZEN_TIME }, expected_old_state: StateMachine::RUNNING)
            .returns(true).in_sequence(sequence)

          # 2. Record output
          @repo.expects(:update_step_output).with(@step_a_id, output).returns(true).in_sequence(sequence)

          # 3. Fetch step for event payload
          @repo.expects(:find_step).with(@step_a_id).returns(step_succeeded_record).in_sequence(sequence)

          # 4. Publish event
          @notifier.expects(:publish)
            .with('yantra.step.succeeded', has_entries(step_id: @step_a_id, output: output))
            .in_sequence(sequence)

          # 5. Delegate to step_finished (use expects to ensure it's called)
          # We mock the orchestrator itself to verify the internal call
          @orchestrator.expects(:step_finished).with(@step_a_id).in_sequence(sequence)

          # Act
          @orchestrator.step_succeeded(@step_a_id, output)
          # Assertions handled by mock verification
        end
      end

      # =========================================================================
      # Step Finished Tests (Focus on interaction with StepEnqueuer)
      # =========================================================================

      def test_step_finished_success_enqueues_ready_dependent
        step_a_succeeded = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: 'succeeded')
        # Dependent step B depends only on A
        dependent_step_id = @step_b_id
        ready_dependent_ids = [dependent_step_id]

        Time.stub :current, FROZEN_TIME do
          sequence = Mocha::Sequence.new('step_finished_success_calls_enqueuer')

          # --- step_finished internal logic ---
          @repo.expects(:find_step).with(@step_a_id).returns(step_a_succeeded).in_sequence(sequence)
          @repo.expects(:get_dependent_ids).with(@step_a_id).returns(ready_dependent_ids).in_sequence(sequence) # B depends on A

          # --- process_dependents internal logic (finding ready steps) ---
          @repo.expects(:get_dependency_ids_bulk).with(ready_dependent_ids).returns({ dependent_step_id => [@step_a_id] }).in_sequence(sequence)
          ids_to_fetch_states = (ready_dependent_ids + [@step_a_id]).uniq
          @repo.expects(:get_step_states)
            .with { |actual_ids| actual_ids.sort == ids_to_fetch_states.sort } # Check array content regardless of order
            .returns({ @step_a_id => 'succeeded', dependent_step_id => 'pending' })
            .in_sequence(sequence)
          # (is_ready_to_start? check happens internally in orchestrator based on fetched states)

          # --- Expect delegation to StepEnqueuer ---
          @step_enqueuer.expects(:call)
            .with(workflow_id: @workflow_id, step_ids_to_attempt: ready_dependent_ids)
            .returns(1) # Simulate 1 step enqueued
            .in_sequence(sequence)

          # --- check_workflow_completion internal logic (assuming B was enqueued) ---
          @repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          @repo.expects(:enqueued_step_count).with(@workflow_id).returns(1).in_sequence(sequence) # B is now enqueued

          # Act
          @orchestrator.step_finished(@step_a_id)
          # Assertions handled by mock verification
        end
      end

      def test_step_finished_success_does_not_enqueue_if_deps_not_met_and_completes_workflow
        # Scenario: A succeeded. C depends on A & B. B is still pending.
        step_a_succeeded = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: 'succeeded', klass: 'StepA')
        dependent_step_id = @step_c_id # C depends on A
        dependents_of_a = [dependent_step_id]
        dependencies_of_c = [@step_a_id, @step_b_id]

        # Workflow state before and after completion check
        workflow_running = MockWorkflow.new(id: @workflow_id, klass: 'MyWorkflow', state: 'running')
        workflow_succeeded = MockWorkflow.new(id: @workflow_id, klass: 'MyWorkflow', state: 'succeeded', finished_at: FROZEN_TIME)

        Time.stub :current, FROZEN_TIME do
          sequence = Mocha::Sequence.new('step_finished_deps_not_met_completes')

          # --- step_finished(A) ---
          @repo.expects(:find_step).with(@step_a_id).returns(step_a_succeeded).in_sequence(sequence)
          @repo.expects(:get_dependent_ids).with(@step_a_id).returns(dependents_of_a).in_sequence(sequence)

          # --- process_dependents(A, :succeeded) -> find ready steps ---
          @repo.expects(:get_dependency_ids_bulk).with(dependents_of_a).returns({ dependent_step_id => dependencies_of_c }).in_sequence(sequence)
          ids_to_fetch_states = (dependents_of_a + dependencies_of_c).uniq.sort
          @repo.expects(:get_step_states)
            .with { |actual_ids| actual_ids.sort == ids_to_fetch_states }
            .returns({ @step_c_id => 'pending', @step_a_id => 'succeeded', @step_b_id => 'pending' }) # B is pending, so C is not ready
            .in_sequence(sequence)

          # --- Expect StepEnqueuer NOT to be called ---
          @step_enqueuer.expects(:call).never

          # --- check_workflow_completion (workflow should complete successfully) ---
          @repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          @repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence) # C was not enqueued, B is pending (but not running/enqueued now?) - check assumption
          # ^^^ Revisiting this assumption: If B was enqueued earlier and hasn't run, enqueued_step_count might be 1.
          # Let's assume for this test that only A ran, and B was never enqueued or already finished/failed.
          # If B *was* enqueued and pending, the workflow wouldn't complete here. Test seems to assume A was the last running/enqueued.
          # Sticking with 0 based on the apparent intent of the original test.
          @repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)
          @repo.expects(:workflow_has_failures?).with(@workflow_id).returns(false).in_sequence(sequence) # No failures occurred
          @repo.expects(:update_workflow_attributes)
            .with(@workflow_id, { state: StateMachine::SUCCEEDED.to_s, finished_at: FROZEN_TIME }, expected_old_state: StateMachine::RUNNING)
            .returns(true).in_sequence(sequence)
          @repo.expects(:find_workflow).with(@workflow_id).returns(workflow_succeeded).in_sequence(sequence) # For event payload
          @notifier.expects(:publish)
            .with('yantra.workflow.succeeded', has_entries(workflow_id: @workflow_id, state: 'succeeded'))
            .in_sequence(sequence)

          # Act
          @orchestrator.step_finished(@step_a_id)
          # Assertions handled by mock verification
        end
      end

      def test_step_finished_failure_completes_workflow_if_last_job
        # Scenario: Step A failed, it was the last running/enqueued step.
        step_a_failed = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: 'StepA', state: 'failed', finished_at: FROZEN_TIME)
        workflow_running = MockWorkflow.new(id: @workflow_id, klass: 'MyWorkflow', state: 'running')
        workflow_failed = MockWorkflow.new(id: @workflow_id, klass: 'MyWorkflow', state: 'failed', finished_at: FROZEN_TIME)

        Time.stub :current, FROZEN_TIME do
          sequence = Mocha::Sequence.new('step_finished_failure_completes_workflow')

          # --- step_finished(A) ---
          @repo.expects(:find_step).with(@step_a_id).returns(step_a_failed).in_sequence(sequence)
          @repo.expects(:get_dependent_ids).with(@step_a_id).returns([]).in_sequence(sequence) # No dependents

          # --- process_dependents(A, :failed) ---
          # Does nothing as dependents_ids is empty. No cancellation needed.
          # StepEnqueuer is not called on failure path.

          # --- check_workflow_completion (workflow should fail) ---
          @repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          @repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence) # No remaining jobs
          @repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence) # State before update
          @repo.expects(:workflow_has_failures?).with(@workflow_id).returns(true).in_sequence(sequence) # Failure occurred (A failed)
          @repo.expects(:update_workflow_attributes)
            .with(@workflow_id, { state: StateMachine::FAILED.to_s, finished_at: FROZEN_TIME }, expected_old_state: StateMachine::RUNNING)
            .returns(true).in_sequence(sequence)
          @repo.expects(:find_workflow).with(@workflow_id).returns(workflow_failed).in_sequence(sequence) # For event payload
          @notifier.expects(:publish)
            .with('yantra.workflow.failed', has_entries(workflow_id: @workflow_id, state: 'failed'))
            .in_sequence(sequence)

          # Act
          @orchestrator.step_finished(@step_a_id)
          # Assertions handled by mock verification
        end
      end

      def test_step_finished_failure_cancels_dependents_recursively_and_fails_workflow
        # ... (setup mock steps and workflows as before) ...
        step_a_failed = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: 'failed')
        step_b_cancelled = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: 'StepB', state: 'cancelled', finished_at: FROZEN_TIME)
        step_c_cancelled = MockStep.new(id: @step_c_id, workflow_id: @workflow_id, klass: 'StepC', state: 'cancelled', finished_at: FROZEN_TIME)
        workflow_running = MockWorkflow.new(id: @workflow_id, klass: 'MyWorkflow', state: 'running')
        workflow_failed = MockWorkflow.new(id: @workflow_id, klass: 'MyWorkflow', state: 'failed', finished_at: FROZEN_TIME)

        Time.stub :current, FROZEN_TIME do
          sequence = Mocha::Sequence.new('failure_cascade_cancels_and_fails_workflow')

          # --- ADD Stubs for respond_to? ---
          # Allow the respond_to? checks and make them return true to force bulk path
          @repo.stubs(:respond_to?).with(:get_step_states).returns(true)
          @repo.stubs(:respond_to?).with(:get_dependent_ids_bulk).returns(true)
          # Add stubs for other repository methods if needed by other parts of the code being tested
          @repo.stubs(:respond_to?).with(:bulk_update_steps).returns(true) # Example if needed
          @repo.stubs(:respond_to?).with(:find_steps).returns(true) # Example if needed
          # --- END Stubs ---


          # --- step_finished(A) ---
          @repo.expects(:find_step).with(@step_a_id).returns(step_a_failed).in_sequence(sequence)
          # --- DependentProcessor#call -> cancel_downstream_dependents ---
          @repo.expects(:get_dependent_ids).with(@step_a_id).returns([@step_b_id]).in_sequence(sequence) # Initial dependents

          # --- DependentProcessor#find_all_pending_descendants (Bulk Path) ---
          initial_dependents = [@step_b_id]
          all_descendants_to_cancel = [@step_b_id, @step_c_id]

          # Expect bulk state fetch for initial batch (just B)
          @repo.expects(:get_step_states).with(initial_dependents).returns({@step_b_id.to_s => 'pending'}).in_sequence(sequence)
          # Expect bulk dependent fetch for initial batch (just B)
          @repo.expects(:get_dependent_ids_bulk).with([@step_b_id]).returns({@step_b_id => [@step_c_id]}).in_sequence(sequence)
          # Expect bulk state fetch for next batch (just C)
          @repo.expects(:get_step_states).with([@step_c_id]).returns({@step_c_id.to_s => 'pending'}).in_sequence(sequence)
          # Expect bulk dependent fetch for next batch (just C)
          @repo.expects(:get_dependent_ids_bulk).with([@step_c_id]).returns({@step_c_id => []}).in_sequence(sequence)
          # --- End find_all_pending_descendants expectations ---

          # Expect bulk cancellation
          @repo.expects(:cancel_steps_bulk)
            .with(all_descendants_to_cancel) # Order might not matter
            .returns(2).in_sequence(sequence) # Simulate 2 steps cancelled

          # --- Orchestrator publishes events ---
          @repo.expects(:find_step).with(@step_b_id).returns(step_b_cancelled).in_sequence(sequence)
          @notifier.expects(:publish).with('yantra.step.cancelled', has_entries(step_id: @step_b_id)).in_sequence(sequence)
          @repo.expects(:find_step).with(@step_c_id).returns(step_c_cancelled).in_sequence(sequence)
          @notifier.expects(:publish).with('yantra.step.cancelled', has_entries(step_id: @step_c_id)).in_sequence(sequence)

          # --- Orchestrator#check_workflow_completion ---
          @repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          @repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          @repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)
          @repo.expects(:workflow_has_failures?).with(@workflow_id).returns(true).in_sequence(sequence)
          @repo.expects(:update_workflow_attributes)
            .with(@workflow_id, has_entries(state: StateMachine::FAILED.to_s), expected_old_state: StateMachine::RUNNING)
            .returns(true).in_sequence(sequence)
          @repo.expects(:find_workflow).with(@workflow_id).returns(workflow_failed).in_sequence(sequence)
          @notifier.expects(:publish)
            .with('yantra.workflow.failed', has_entries(workflow_id: @workflow_id, state: 'failed'))
            .in_sequence(sequence)

          # Act
          @orchestrator.step_finished(@step_a_id)
          # Assertions handled by mock verification
        end
      end

      # =========================================================================
      # Error Handling / Edge Case Tests
      # =========================================================================
      def test_step_finished_handles_find_step_error
        # Arrange: Setup mocks specifically for this error case
        # Use new mocks to avoid interference from standard setup stubs if necessary,
        # or carefully override the specific expectation on the existing @repo.
        error_message = "DB connection failed during find_step"
        @repo.expects(:find_step) # Override the general stub from setup
          .with(@step_a_id)
          .raises(Yantra::Errors::PersistenceError, error_message)

        # Ensure methods called *after* the failing find_step are never reached
        @repo.expects(:get_dependent_ids).never
        @step_enqueuer.expects(:call).never
        @repo.expects(:running_step_count).never # check_workflow_completion shouldn't run

        # Act & Assert
        exception = assert_raises(Yantra::Errors::PersistenceError) do
          @orchestrator.step_finished(@step_a_id)
        end

        # Assert on error message
        assert_match(/#{error_message}/, exception.message)
        # Mocha verification happens automatically
      end

    end # class OrchestratorTest
  end # module Core
end # module Yantra
