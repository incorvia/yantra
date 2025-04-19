# test/core/orchestrator_test.rb

require "test_helper"
# require 'mocha/minitest' # Ensure this is in test_helper.rb
require "yantra/core/orchestrator"
require "yantra/core/state_machine"
require "yantra/core/step_enqueuing_service" # <<< Added require
require "yantra/errors"
require "yantra/persistence/repository_interface"
require "yantra/worker/enqueuing_interface"
require "yantra/events/notifier_interface"
# require "minitest/mock" # No longer needed
require "ostruct"
require 'time' # <-- Add require for Time
require 'securerandom' # Ensure SecureRandom is required

class StepAJob; end # Keep dummy class if needed elsewhere

# Define simple Structs - still useful for defining return values
# Use STRING states in mocks to match DB/comparison logic
MockStep = Struct.new(
  :id, :workflow_id, :klass, :state, :queue, :output, :error, :retries,
  :created_at, :enqueued_at, :started_at, :finished_at, :dependencies
) do
  def initialize(
    id: nil, workflow_id: nil, klass: nil, state: 'pending', queue: 'default',
    output: nil, error: nil, retries: nil, created_at: nil, enqueued_at: nil,
    started_at: nil, finished_at: nil, dependencies: []
  )
    # Ensure state is stored as a string
    super(
      id, workflow_id, klass, state.to_s, queue, output, error, retries,
      created_at, enqueued_at, started_at, finished_at, dependencies || []
    )
  end
end

MockWorkflow = Struct.new(
  :id, :state, :klass, :started_at, :finished_at, :has_failures
) do
  def initialize(
    id: nil, state: 'pending', klass: nil, started_at: nil, finished_at: nil,
    has_failures: false
  )
    # Ensure state is stored as a string
    super(id, state.to_s, klass, started_at, finished_at, has_failures)
  end
end


module Yantra
  module Core
    class OrchestratorTest < Minitest::Test
      include Mocha::API # Ensure Mocha methods are available

      # --- Define setup variables used across tests ---
      def setup
        @workflow_id = "wf-#{SecureRandom.uuid}"
        @step_a_id = "step-a-#{SecureRandom.uuid}"
        @step_b_id = "step-b-#{SecureRandom.uuid}"
        @step_c_id = "step-c-#{SecureRandom.uuid}"
        @frozen_time = Time.parse("2025-04-16 15:30:00 -0500")
      end

      # --- <<< CHANGED: Updated Helper for Mocha setup >>> ---
      def setup_mocha_mocks_and_orchestrator
        repo = mock('repository')
        worker = mock('worker_adapter')
        notifier = mock('notifier')

        # --- Stubs for Orchestrator initializer checks ---
        repo.stubs(:is_a?).with(Yantra::Persistence::RepositoryInterface).returns(true)
        worker.stubs(:is_a?).with(Yantra::Worker::EnqueuingInterface).returns(true)
        notifier.stubs(:respond_to?).with(:publish).returns(true)
        # Also stub is_a? for notifier in case it's used elsewhere
        notifier.stubs(:is_a?).with(Yantra::Events::NotifierInterface).returns(true)

        # --- Stubs for StepEnqueuingService initializer checks ---
        # These are called when Orchestrator calls StepEnqueuingService.new
        repo.stubs(:respond_to?).with(:find_steps).returns(true)
        repo.stubs(:respond_to?).with(:bulk_update_steps).returns(true)
        worker.stubs(:respond_to?).with(:enqueue).returns(true)
        # Notifier respond_to?(:publish) is already stubbed above

        # --- Stubs for methods commonly used by Orchestrator helpers ---
        # Stub these generally unless a specific test needs to expect them
        repo.stubs(:respond_to?).with(:get_dependencies_ids_bulk).returns(true)
        repo.stubs(:respond_to?).with(:fetch_step_states).returns(true)

        # Now instantiate the orchestrator (which will instantiate the service)
        # The initializer checks on the mocks should now pass because of the stubs above.
        orchestrator = Orchestrator.new(repository: repo, worker_adapter: worker, notifier: notifier)

        # Return mocks and orchestrator for use in test
        { repo: repo, worker: worker, notifier: notifier, orchestrator: orchestrator }
      end
      # --- <<< END CHANGED >>> ---


      # =========================================================================
      # Workflow Start Tests
      # =========================================================================

      # --- <<< CHANGED: Updated test_start_workflow to use StepEnqueuingService >>> ---
      def test_start_workflow_enqueues_multiple_initial_jobs
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; orchestrator = mocks[:orchestrator]
        # Get the enqueuer instance created within the orchestrator
        step_enqueuer = orchestrator.step_enqueuer
        refute_nil step_enqueuer, "StepEnqueuer should be initialized"

        ready_step_ids = [@step_a_id, @step_b_id]
        workflow_running = MockWorkflow.new(id: @workflow_id, klass: "TestWorkflow", state: 'running', started_at: @frozen_time)

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('start_workflow_calls_enqueuer')

          # Expectations for start_workflow up to delegation
          repo.expects(:update_workflow_attributes)
              .with(@workflow_id, { state: StateMachine::RUNNING.to_s, started_at: @frozen_time }, expected_old_state: StateMachine::PENDING)
              .returns(true).in_sequence(sequence)
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)
          # Mock the publish call directly on the orchestrator's notifier instance
          orchestrator.notifier.expects(:publish)
              .with('yantra.workflow.started', has_entries(workflow_id: @workflow_id))
              .in_sequence(sequence)
          repo.expects(:find_ready_steps).with(@workflow_id).returns(ready_step_ids).in_sequence(sequence)

          # --- Expectation for the call to StepEnqueuingService ---
          # Mock the call on the actual enqueuer instance held by the orchestrator
          step_enqueuer.expects(:call)
                       .with(workflow_id: @workflow_id, step_ids_to_attempt: ready_step_ids)
                       .returns(2) # Simulate 2 steps being successfully enqueued by the service
                       .in_sequence(sequence)
          # --- End Expectation for StepEnqueuingService ---

          # --- Removed expectations for individual enqueue_step calls ---

          # Act
          result = orchestrator.start_workflow(@workflow_id)

          # Assert
          assert result, "start_workflow should return true"
          # Mocha verifies the expectations
        end
      end
      # --- <<< END CHANGED >>> ---


      def test_start_workflow_does_nothing_if_not_pending
         # ... (This test likely remains the same, as it fails before calling the enqueuer) ...
         mocks = setup_mocha_mocks_and_orchestrator
         repo = mocks[:repo]; orchestrator = mocks[:orchestrator]
         sequence = Mocha::Sequence.new('start_workflow_does_nothing_if_not_pending')
         Time.stub :current, @frozen_time do
           workflow = MockWorkflow.new(id: @workflow_id, state: 'running')
           repo.expects(:update_workflow_attributes)
               .with(@workflow_id, has_entries(state: StateMachine::RUNNING.to_s), expected_old_state: StateMachine::PENDING)
               .returns(false).in_sequence(sequence)
           repo.expects(:find_workflow).with(@workflow_id).returns(workflow).in_sequence(sequence)
           result = orchestrator.start_workflow(@workflow_id)
           refute result
         end
      end

      def test_start_workflow_handles_workflow_update_failure
        # ... (This test likely remains the same) ...
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; orchestrator = mocks[:orchestrator]
        Time.stub :current, @frozen_time do
          workflow = MockWorkflow.new(id: @workflow_id, state: 'pending')
          repo.expects(:update_workflow_attributes)
              .with(@workflow_id, has_entries(state: StateMachine::RUNNING.to_s), expected_old_state: StateMachine::PENDING)
              .returns(false)
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow)
          result = orchestrator.start_workflow(@workflow_id)
          refute result
        end
      end

      # =========================================================================
      # Step Starting Tests (These tests don't involve the enqueuer, should be OK)
      # =========================================================================
       def test_step_starting_publishes_event_on_success
         mocks = setup_mocha_mocks_and_orchestrator
         repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
         sequence = Mocha::Sequence.new('step_starting_publishes_event')
         Time.stub :current, @frozen_time do
           step_enqueued = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'enqueued')
           step_running = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'running', started_at: @frozen_time)
           # Expect first find_step
           repo.expects(:find_step).with(@step_a_id).returns(step_enqueued).in_sequence(sequence)
           # Expect update
           repo.expects(:update_step_attributes)
               .with(@step_a_id, has_entries(state: StateMachine::RUNNING.to_s, started_at: @frozen_time), expected_old_state: StateMachine::ENQUEUED)
               .returns(true).in_sequence(sequence)
           # <<< FIX: Uncomment this expectation for the find_step inside publish_step_started_event >>>
           repo.expects(:find_step).with(@step_a_id).returns(step_running).in_sequence(sequence) # Needed for payload
           # Expect publish
           notifier.expects(:publish)
               .with('yantra.step.started', has_entries(step_id: @step_a_id, started_at: @frozen_time))
               .returns(nil).in_sequence(sequence)
           # Act
           result = orchestrator.step_starting(@step_a_id)
           # Assert
           assert result
         end
      end

      def test_step_starting_does_not_publish_if_update_fails
         # ... (Keep existing implementation) ...
         mocks = setup_mocha_mocks_and_orchestrator
         repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
         sequence = Mocha::Sequence.new('step_starting_no_publish_on_fail')
         Time.stub :current, @frozen_time do
           step_enqueued = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'enqueued')
           repo.expects(:find_step).with(@step_a_id).returns(step_enqueued).in_sequence(sequence)
           repo.expects(:update_step_attributes)
               .with(@step_a_id, has_entries(state: StateMachine::RUNNING.to_s), expected_old_state: StateMachine::ENQUEUED)
               .returns(false).in_sequence(sequence)
           notifier.expects(:publish).never
           result = orchestrator.step_starting(@step_a_id)
           # Update based on actual Orchestrator#step_starting return value if update fails
           # assert result # Original code returns true even if update fails
           # If you changed it to return false on update fail, use refute:
           # refute result, "step_starting should return false if update fails"
         end
      end

      def test_step_starting_does_not_publish_if_already_running
         # ... (Keep existing implementation) ...
         mocks = setup_mocha_mocks_and_orchestrator
         repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
         Time.stub :current, @frozen_time do
           step_already_running = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'running', started_at: @frozen_time - 10)
           repo.expects(:find_step).with(@step_a_id).returns(step_already_running)
           notifier.expects(:publish).never
           result = orchestrator.step_starting(@step_a_id)
           assert result
         end
      end

      # =========================================================================
      # Step Succeeded Tests (Should be OK, step_finished is stubbed/verified later)
      # =========================================================================
      def test_step_succeeded_updates_state_records_output_publishes_event_and_calls_step_finished
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        output = { result: "ok" }
        step_succeeded_record = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'succeeded', finished_at: @frozen_time, output: output)
        Time.stub :current, @frozen_time do
          # Expect update and record output
          repo.expects(:update_step_attributes)
            .with(@step_a_id, { state: StateMachine::SUCCEEDED.to_s, finished_at: @frozen_time }, expected_old_state: StateMachine::RUNNING)
            .returns(true)
          repo.expects(:record_step_output).with(@step_a_id, output).returns(true)

          # --- <<< FIX: Uncomment this expectation >>> ---
          # The publish_step_succeeded_event helper needs to find the step for the payload
          repo.expects(:find_step).with(@step_a_id).returns(step_succeeded_record)
          # --- <<< END FIX >>> ---

          # Expect publish and step_finished
          notifier.expects(:publish).with('yantra.step.succeeded', has_entries(step_id: @step_a_id, output: output))
          orchestrator.expects(:step_finished).with(@step_a_id) # Use expects if you need to verify it's called

          # Act
          orchestrator.step_succeeded(@step_a_id, output)
          # Assertions handled by mock verification
        end
      end


      # =========================================================================
      # Enqueue Step Tests (These tests are now INVALID as enqueue_step was removed)
      # =========================================================================

      # --- <<< REMOVED: Tests for private method enqueue_step >>> ---
      # def test_enqueue_step_handles_update_failure
      #   # ... (REMOVE THIS TEST) ...
      # end
      #
      # def test_enqueue_step_handles_worker_error_and_marks_step_failed
      #   # ... (REMOVE THIS TEST) ...
      # end
      # --- <<< END REMOVED >>> ---


      # =========================================================================
      # Step Finished Tests (These need significant changes)
      # =========================================================================

      # --- <<< CHANGED: Updated test_step_finished_success to use StepEnqueuingService >>> ---
      def test_step_finished_success_enqueues_ready_dependent
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; orchestrator = mocks[:orchestrator]
        step_enqueuer = orchestrator.step_enqueuer # Get the enqueuer instance
        refute_nil step_enqueuer

        step_a_succeeded = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: 'succeeded')
        # step_b_pending = MockStep.new(...) # Not needed for mock returns directly

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('step_finished_success_calls_enqueuer')

          # === Expectations for step_finished(A) up to delegation ===
          repo.expects(:find_step).with(@step_a_id).returns(step_a_succeeded).in_sequence(sequence)
          repo.expects(:get_dependent_ids).with(@step_a_id).returns([@step_b_id]).in_sequence(sequence) # B depends on A

          # Expectations for finding ready steps within process_dependents
          repo.expects(:get_dependencies_ids_bulk).with([@step_b_id]).returns({ @step_b_id => [@step_a_id] }).in_sequence(sequence)
          ids_to_fetch = [@step_b_id, @step_a_id].uniq
          repo.expects(:fetch_step_states)
              .with { |actual_ids| actual_ids.sort == ids_to_fetch.sort }
              .returns({ @step_a_id => 'succeeded', @step_b_id => 'pending' })
              .in_sequence(sequence)
          # (is_ready_to_start? check happens internally based on above)

          # --- Expectation for the call to StepEnqueuingService ---
          step_enqueuer.expects(:call)
                       .with(workflow_id: @workflow_id, step_ids_to_attempt: [@step_b_id]) # Expect call with ready step B
                       .returns(1) # Simulate 1 step enqueued
                       .in_sequence(sequence)
          # --- End Expectation for StepEnqueuingService ---

          # --- Removed expectations for find_steps, bulk_update_steps, worker.enqueue, notifier.publish ---
          # (These are now handled *inside* the StepEnqueuingService)

          # === Expectations for check_workflow_completion ===
          # (These should still run after process_dependents finishes)
          repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          # Assuming the enqueuer service successfully enqueued B, the count should be 1
          repo.expects(:enqueued_step_count).with(@workflow_id).returns(1).in_sequence(sequence)
          # --- End check_workflow_completion expectations ---

          # Act
          orchestrator.step_finished(@step_a_id)

          # Assertions handled by mock verification
        end
      end
      # --- <<< END CHANGED >>> ---


      # --- <<< CHANGED: Updated test_step_finished_success_deps_not_met >>> ---
      def test_step_finished_success_does_not_enqueue_if_deps_not_met_and_completes_workflow
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        step_enqueuer = orchestrator.step_enqueuer # Get the enqueuer instance
        refute_nil step_enqueuer

        # Setup: A succeeded, C depends on A & B, B is still pending
        step_a_succeeded = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: 'succeeded', klass: "StepA")
        workflow_running = MockWorkflow.new(id: @workflow_id, klass: "MyWorkflow", state: 'running')
        workflow_succeeded = MockWorkflow.new(id: @workflow_id, klass: "MyWorkflow", state: 'succeeded', finished_at: @frozen_time)

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('step_finished_deps_not_met_completes_refactored')

          # Expectations for step_finished(A) up to delegation check
          repo.expects(:find_step).with(@step_a_id).returns(step_a_succeeded).in_sequence(sequence)
          repo.expects(:get_dependent_ids).with(@step_a_id).returns([@step_c_id]).in_sequence(sequence) # C depends on A

          # Expectations for finding ready steps within process_dependents
          repo.expects(:get_dependencies_ids_bulk).with([@step_c_id]).returns({ @step_c_id => [@step_a_id, @step_b_id] }).in_sequence(sequence)
          ids_to_fetch = [@step_c_id, @step_a_id, @step_b_id].uniq
          repo.expects(:fetch_step_states)
              .with { |actual_ids| actual_ids.sort == ids_to_fetch.sort }
              .returns({ @step_c_id => 'pending', @step_a_id => 'succeeded', @step_b_id => 'pending' }) # B is pending
              .in_sequence(sequence)
          # (is_ready_to_start?(C) will return false internally)

          # --- Expect StepEnqueuingService NOT to be called ---
          step_enqueuer.expects(:call).never # Because no steps became ready
          # --- End Expectation ---

          # --- Expectations for check_workflow_completion (workflow should complete) ---
          repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence) # Nothing got enqueued
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)
          repo.expects(:workflow_has_failures?).with(@workflow_id).returns(false).in_sequence(sequence)
          repo.expects(:update_workflow_attributes)
              .with(@workflow_id, { state: StateMachine::SUCCEEDED.to_s, finished_at: @frozen_time }, expected_old_state: StateMachine::RUNNING)
              .returns(true).in_sequence(sequence)
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_succeeded).in_sequence(sequence)
          notifier.expects(:publish)
              .with('yantra.workflow.succeeded', has_entries(workflow_id: @workflow_id, state: 'succeeded'))
              .in_sequence(sequence)
          # --- End check_workflow_completion expectations ---

          # Act
          orchestrator.step_finished(@step_a_id)
          # Assertions handled by mock verification
        end
      end
      # --- <<< END CHANGED >>> ---


      # --- <<< NOTE: This test might need adjustment based on cancel_downstream_dependents implementation >>> ---
      def test_step_finished_failure_completes_workflow_if_last_job
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        step_enqueuer = orchestrator.step_enqueuer # Get instance, though not used here

        # Setup: Step A failed, no dependents
        step_a_failed = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: "StepA", state: 'failed', finished_at: @frozen_time)
        final_wf_record = MockWorkflow.new(id: @workflow_id, klass: "MyWorkflow", state: 'failed', finished_at: @frozen_time)

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('step_finished_failure_completes_workflow_refactored')

          # Expectations for step_finished(A)
          repo.expects(:find_step).with(@step_a_id).returns(step_a_failed).in_sequence(sequence)
          repo.expects(:get_dependent_ids).with(@step_a_id).returns([]).in_sequence(sequence) # No dependents

          # process_dependents does nothing as dependents_ids is empty

          # Expectations for check_workflow_completion
          repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          repo.expects(:find_workflow).with(@workflow_id).returns(MockWorkflow.new(id: @workflow_id, state: 'running')).in_sequence(sequence) # Assume running before check
          repo.expects(:workflow_has_failures?).with(@workflow_id).returns(true).in_sequence(sequence) # Assume flag was set
          repo.expects(:update_workflow_attributes)
              .with(@workflow_id, { state: StateMachine::FAILED.to_s, finished_at: @frozen_time }, expected_old_state: StateMachine::RUNNING)
              .returns(true).in_sequence(sequence)
          repo.expects(:find_workflow).with(@workflow_id).returns(final_wf_record).in_sequence(sequence) # For event payload
          notifier.expects(:publish)
              .with('yantra.workflow.failed', has_entries(workflow_id: @workflow_id, state: 'failed'))
              .in_sequence(sequence)

          # Act
          orchestrator.step_finished(@step_a_id)
          # Assertions handled by mock verification
        end
      end

      def test_step_finished_failure_cancels_dependents_recursively_and_fails_workflow
        mocks = setup_mocha_mocks_and_orchestrator
        repo = mocks[:repo]; notifier = mocks[:notifier]; orchestrator = mocks[:orchestrator]
        # step_enqueuer = orchestrator.step_enqueuer # Not used in failure path

        step_a_failed = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: 'failed')
        step_b_cancelled = MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: "StepB", state: 'cancelled', finished_at: @frozen_time)
        step_c_cancelled = MockStep.new(id: @step_c_id, workflow_id: @workflow_id, klass: "StepC", state: 'cancelled', finished_at: @frozen_time)
        workflow_running = MockWorkflow.new(id: @workflow_id, klass: "MyWorkflow", state: 'running')
        workflow_failed = MockWorkflow.new(id: @workflow_id, klass: "MyWorkflow", state: 'failed', finished_at: @frozen_time)

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('failure_cascade_refactored_v3') # New sequence name

          # --- Expectations for step_finished(A) -> process_dependents(A, :failed) ---
          repo.expects(:find_step).with(@step_a_id).returns(step_a_failed).in_sequence(sequence)
          repo.expects(:get_dependent_ids).with(@step_a_id).returns([@step_b_id]).in_sequence(sequence) # B depends on A

          # --- Expectations for cancel_downstream_dependents([B], A, :failed) ---
          # Expectations for find_all_pending_descendants([B])
          # Assuming bulk methods are available and stubbed in setup
          repo.expects(:fetch_step_states).with([@step_b_id]).returns({@step_b_id => 'pending'}).in_sequence(sequence)
          # <<< Expect bulk get_dependent_ids_bulk >>>
          repo.expects(:get_dependent_ids_bulk).with([@step_b_id]).returns({@step_b_id => [@step_c_id]}).in_sequence(sequence) # C depends on B
          # <<< Removed individual get_dependencies_ids call >>>
          repo.expects(:fetch_step_states).with([@step_c_id]).returns({@step_c_id => 'pending'}).in_sequence(sequence)
          # <<< Expect bulk get_dependent_ids_bulk >>>
          repo.expects(:get_dependent_ids_bulk).with([@step_c_id]).returns({@step_c_id => []}).in_sequence(sequence)
          # <<< Removed individual get_dependencies_ids call >>>
          # (find_all_pending_descendants returns [B, C])

          # Expect bulk cancellation
          repo.expects(:cancel_steps_bulk).with(includes(@step_b_id, @step_c_id)).returns(2).in_sequence(sequence)

          # Expect publishing cancellation events
          repo.expects(:find_step).with(@step_b_id).returns(step_b_cancelled).in_sequence(sequence)
          notifier.expects(:publish).with('yantra.step.cancelled', has_entries(step_id: @step_b_id)).in_sequence(sequence)
          repo.expects(:find_step).with(@step_c_id).returns(step_c_cancelled).in_sequence(sequence)
          notifier.expects(:publish).with('yantra.step.cancelled', has_entries(step_id: @step_c_id)).in_sequence(sequence)
          # --- End cancel_downstream_dependents expectations ---

          # --- Expectations for check_workflow_completion (called after A finishes) ---
          repo.expects(:running_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          repo.expects(:enqueued_step_count).with(@workflow_id).returns(0).in_sequence(sequence)
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_running).in_sequence(sequence)
          repo.expects(:workflow_has_failures?).with(@workflow_id).returns(true).in_sequence(sequence)
          repo.expects(:update_workflow_attributes)
            .with(@workflow_id, has_entries(state: StateMachine::FAILED.to_s), expected_old_state: StateMachine::RUNNING)
            .returns(true).in_sequence(sequence)
          repo.expects(:find_workflow).with(@workflow_id).returns(workflow_failed).in_sequence(sequence)
          notifier.expects(:publish)
            .with('yantra.workflow.failed', has_entries(workflow_id: @workflow_id, state: 'failed'))
            .in_sequence(sequence)
          # --- End check_workflow_completion expectations ---

          # Act
          orchestrator.step_finished(@step_a_id)
          # Assertions handled by mock verification
        end
      end

      # --- <<< END NOTE >>> ---


      # =========================================================================
      # Error Handling / Edge Case Tests (These likely don't involve the enqueuer)
      # =========================================================================
      def test_step_finished_handles_find_step_error
         # Arrange: Create standard mocks
         error_repo = mock('error_repository')
         dummy_notifier = mock('notifier')
         dummy_worker = mock('worker')

         # Stub methods needed for Orchestrator and StepEnqueuingService initializers
         error_repo.stubs(:is_a?).with(Yantra::Persistence::RepositoryInterface).returns(true)
         error_repo.stubs(:respond_to?).with(:find_steps).returns(true)
         error_repo.stubs(:respond_to?).with(:bulk_update_steps).returns(true)
         # Add stubs for any *other* methods called by find_all_pending_descendants etc.
         # if they might be hit before find_step raises the error. Check Orchestrator code flow.
         # Example:
         error_repo.stubs(:get_dependent_ids) # Stub methods called before find_step in step_finished path

         dummy_worker.stubs(:is_a?).with(Yantra::Worker::EnqueuingInterface).returns(true)
         dummy_worker.stubs(:respond_to?).with(:enqueue).returns(true)
         dummy_notifier.stubs(:respond_to?).with(:publish).returns(true)

         # *** Define the specific error behavior for find_step ***
         error_repo.expects(:find_step).with(@step_a_id).raises(Yantra::Errors::PersistenceError, "DB down")

         # Instantiate Orchestrator with these mocks
         orchestrator_with_error = Orchestrator.new(repository: error_repo, worker_adapter: dummy_worker, notifier: dummy_notifier)

         # Act & Assert
         error = assert_raises(Yantra::Errors::PersistenceError) do
           # Call the method under test
           orchestrator_with_error.step_finished(@step_a_id)
         end
         assert_match(/DB down/, error.message)
         # Mocha verification happens automatically on teardown
      end
    end # class OrchestratorTest
  end # module Core
end # module Yantra


