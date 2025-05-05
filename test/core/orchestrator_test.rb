# test/core/orchestrator_test.rb
# frozen_string_literal: true

require 'test_helper'
require 'mocha/minitest' # Ensure mocha integration
require 'securerandom'
require 'time'

# --- Yantra Requires ---
require 'yantra/core/orchestrator'
require 'yantra/core/state_machine'
require 'yantra/core/step_enqueuer'
require 'yantra/core/dependent_processor'
require 'yantra/core/state_transition_service'
require 'yantra/errors'
require 'yantra/persistence/repository_interface'
require 'yantra/worker/enqueuing_interface'
require 'yantra/events/notifier_interface'

# --- Mocks ---
# Using Struct for simple mocks
MockStep = Struct.new(
  :id, :workflow_id, :klass, :state, :queue, :output, :error, :retries, :enqueued_at,
  :created_at, :started_at, :finished_at, :dependencies, :max_attempts, :delay_seconds,
  :performed_at,
  keyword_init: true
) do
  def initialize(state: 'pending', dependencies: [], **kwargs)
    # Ensure state is stored as a symbol internally for mock consistency
    super(state: state.to_sym, dependencies: dependencies || [], **kwargs)
  end

  # Allow mock to respond to state.to_s for compatibility if needed by code under test
  def state
    self[:state].to_s
  end

  # Allow access as symbol for internal test logic
  def state_sym
    self[:state]
  end
end

MockWorkflow = Struct.new(
  :id, :state, :klass, :started_at, :finished_at, :has_failures,
  keyword_init: true
) do
  def initialize(state: 'pending', **kwargs)
    super(state: state.to_sym, **kwargs) # Store state as symbol internally for mock
  end

  # Allow mock to respond to state.to_s for compatibility if needed
  def state
    self[:state].to_s
  end

  # Allow access as symbol for internal test logic
  def state_sym
    self[:state]
  end
end

# --- Test Class ---

module Yantra
  module Core
    class OrchestratorTest < Minitest::Test
      include StateMachine # Make constants available

      def setup
        # Use mocks for dependencies
        @repo = mock('Repository')
        @worker = mock('WorkerAdapter')
        @notifier = mock('Notifier')
        @logger = mock('Logger')
        @transition_service = mock('StateTransitionService') # Mock the services
        @step_enqueuer = mock('StepEnqueuer')
        @dependent_processor = mock('DependentProcessor')

        # Stub logger methods
        @logger.stubs(:debug)
        @logger.stubs(:info)
        @logger.stubs(:warn)
        @logger.stubs(:error)

        # Stub global accessors to return mocks
        Yantra.stubs(:repository).returns(@repo)
        Yantra.stubs(:worker_adapter).returns(@worker)
        Yantra.stubs(:notifier).returns(@notifier)
        Yantra.stubs(:logger).returns(@logger)

        # Stub the .new methods for the services Orchestrator creates
        StateTransitionService.stubs(:new).with(repository: @repo, logger: @logger).returns(@transition_service)
        StepEnqueuer.stubs(:new).with(repository: @repo, worker_adapter: @worker, notifier: @notifier, logger: @logger).returns(@step_enqueuer)
        DependentProcessor.stubs(:new).with(repository: @repo, step_enqueuer: @step_enqueuer, logger: @logger).returns(@dependent_processor)

        # Instantiate Orchestrator - it will now get the mocked services
        @orchestrator = Orchestrator.new(repository: @repo, worker_adapter: @worker, notifier: @notifier)

        # Common IDs
        @workflow_id = "wf-#{SecureRandom.uuid}"
        @step_a_id = "step-a-#{SecureRandom.uuid}"
        @step_b_id = "step-b-#{SecureRandom.uuid}"
        @step_c_id = "step-c-#{SecureRandom.uuid}"

        # Define a consistent time for tests involving time
        @frozen_time = Time.parse("2025-01-15 10:30:00 UTC")
      end

      def teardown
        Mocha::Mockery.instance.teardown
        Time.unstub(:current) # Ensure time is unstubbed if stubbed in a test
      end

      # =========================================================================
      # Workflow Start Tests
      # =========================================================================

      def test_start_workflow_enqueues_initial_jobs
        ready_step_ids = [@step_a_id, @step_b_id]

        pending_steps = [
          MockStep.new(id: @step_a_id, dependencies: []),
          MockStep.new(id: @step_b_id, dependencies: [])
        ]

        workflow_running = MockWorkflow.new(id: @workflow_id, klass: 'TestWorkflow', state: :running, started_at: @frozen_time)

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('start_workflow_calls_enqueuer')

          # Expect update to running state
          @repo.expects(:update_workflow_attributes)
            .with(@workflow_id,
                  { state: StateMachine::RUNNING.to_s, started_at: @frozen_time },
                  expected_old_state: StateMachine::PENDING)
            .returns(true).in_sequence(sequence)

          # Workflow used for event payload
          @repo.expects(:find_workflow)
            .with(@workflow_id)
            .returns(workflow_running).in_sequence(sequence)

          @notifier.expects(:publish)
            .with('yantra.workflow.started', any_parameters)
            .in_sequence(sequence)

          # Expect listing of pending steps
          @repo.expects(:list_steps)
            .with(workflow_id: @workflow_id, status: :pending)
            .returns(pending_steps).in_sequence(sequence)

          # Expect call to get dependencies
          @repo.expects(:get_dependency_ids_bulk)
            .with(ready_step_ids)
            .returns({ @step_a_id => [], @step_b_id => [] }).in_sequence(sequence)

          # Expect step enqueuer call
          @step_enqueuer.expects(:call)
            .with(workflow_id: @workflow_id, step_ids_to_attempt: ready_step_ids)
            .returns(ready_step_ids.size) # StepEnqueuer now returns count
            .in_sequence(sequence)

          # Act
          result = @orchestrator.start_workflow(@workflow_id)

          # Assert
          assert result, "start_workflow should return true on success"
        end
      end

      def test_start_workflow_does_nothing_if_update_fails
        Time.stub :current, @frozen_time do
          # Expect update to fail
          @repo.expects(:update_workflow_attributes)
            .with(@workflow_id,
                  { state: StateMachine::RUNNING.to_s, started_at: @frozen_time },
                  expected_old_state: StateMachine::PENDING)
            .returns(false)

          # Ensure downstream actions do not happen
          @notifier.expects(:publish).never
          @repo.expects(:list_steps).never
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
      def test_step_starting_transitions_and_publishes_event_on_success
        # --- MODIFIED: Mock step as ENQUEUED (common state worker finds) ---
        step_enqueued = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: 'StepA', state: :enqueued)
        step_running = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: 'StepA', state: :running, started_at: @frozen_time)

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('step_starting_success')

          # Expect find_step to return the enqueued step
          @repo.expects(:find_step).with(@step_a_id).returns(step_enqueued).in_sequence(sequence)

          # Expect successful transition call via service FROM ENQUEUED
          @transition_service.expects(:transition_step)
            .with(@step_a_id, RUNNING, expected_old_state: [SCHEDULING, ENQUEUED], extra_attrs: { started_at: @frozen_time })
            .returns(true).in_sequence(sequence)

          # Expect find_step again for the event payload generation
          @repo.expects(:find_step).with(@step_a_id).returns(step_running).in_sequence(sequence)

          # Expect publish event
          @notifier.expects(:publish)
            .with('yantra.step.started', has_entries(step_id: @step_a_id, started_at: @frozen_time))
            .in_sequence(sequence)

          # Act
          result = @orchestrator.step_starting(@step_a_id)

          # Assert
          assert result, "step_starting should return true on successful update"
        end
      end

      def test_step_starting_does_not_publish_if_update_fails
        # --- MODIFIED: Mock step as SCHEDULING (another possible start state) ---
        step_scheduling = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: 'StepA', state: :scheduling)

        Time.stub :current, @frozen_time do
          # Expect initial find_step
          @repo.expects(:find_step).with(@step_a_id).returns(step_scheduling)

          # Expect the call to the transition service, mock it to return false
          # Expect transition FROM SCHEDULING
          @transition_service.expects(:transition_step)
            .with(@step_a_id, RUNNING, expected_old_state: [SCHEDULING, ENQUEUED], extra_attrs: has_key(:started_at))
            .returns(false) # Simulate update failure

          # When transition_service returns false, step_starting re-checks state
          @repo.expects(:find_step).with(@step_a_id).returns(step_scheduling) # Still scheduling

          # Ensure publish is never called
          @notifier.expects(:publish).never

          # Act
          result = @orchestrator.step_starting(@step_a_id)

          # Assert: step_starting should return false if update fails and state is not RUNNING
          refute result, "step_starting should return false if update fails"
        end
      end

      def test_step_starting_returns_true_if_already_running
        step_already_running = MockStep.new(id: @step_a_id, state: :running)

        Time.stub :current, @frozen_time do
          @repo.expects(:find_step).with(@step_a_id).returns(step_already_running)
          # Ensure transition service and notifier are never called
          @transition_service.expects(:transition_step).never
          @notifier.expects(:publish).never

          # Act
          result = @orchestrator.step_starting(@step_a_id)

          # Assert
          assert result, "step_starting should return true if step is already running"
        end
      end

      def test_step_starting_raises_error_if_invalid_start_state
        # Test with PENDING, which is no longer directly startable by the worker
        step_pending = MockStep.new(id: @step_a_id, state: :pending)

        @repo.expects(:find_step).with(@step_a_id).returns(step_pending)
        @transition_service.expects(:transition_step).never
        @notifier.expects(:publish).never

        # Act & Assert
        assert_raises(Yantra::Errors::OrchestrationError) do
          @orchestrator.step_starting(@step_a_id)
        end
      end

      # =========================================================================
      # Post Processing / Step Succeeded / Step Failed Tests
      # =========================================================================

      def test_handle_post_processing_success_calls_processor_and_finalizes
        step_post_processing = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: 'StepA', state: :post_processing)
        step_succeeded = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, klass: 'StepA', state: :succeeded, finished_at: @frozen_time, output: { result: 'from_executor' })

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('post_processing_success')

          # 1. Find step in post_processing
          @repo.expects(:find_step).with(@step_a_id).returns(step_post_processing).in_sequence(sequence)
          # 2. Call DependentProcessor for successors
          @dependent_processor.expects(:process_successors)
             .with(finished_step_id: @step_a_id, workflow_id: @workflow_id)
             .in_sequence(sequence)
          # 3. Call finalize_step_succeeded -> transition_service
          @transition_service.expects(:transition_step)
             .with(@step_a_id, SUCCEEDED, expected_old_state: POST_PROCESSING, extra_attrs: { finished_at: @frozen_time })
             .returns(true).in_sequence(sequence)
          # 4. Publish Succeeded Event
          @repo.expects(:find_step).with(@step_a_id).returns(step_succeeded).in_sequence(sequence) # For event payload
          @notifier.expects(:publish).with('yantra.step.succeeded', has_entries(step_id: @step_a_id)).in_sequence(sequence)
          # 5. Check Workflow Completion
          @orchestrator.expects(:check_workflow_completion).with(@workflow_id).in_sequence(sequence)

          # Act - Call handle_post_processing
          @orchestrator.handle_post_processing(@step_a_id)
        end
      end

      def test_handle_post_processing_handles_enqueue_failure
        step_post_processing = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: :post_processing)
        enqueue_error = Yantra::Errors::EnqueueFailed.new("Adapter failed", failed_ids: [@step_b_id]) # Example error

        @repo.expects(:find_step).with(@step_a_id).returns(step_post_processing)
        # Simulate DependentProcessor raising EnqueueFailed
        @dependent_processor.expects(:process_successors).raises(enqueue_error)
        # Expect finalize_step_succeeded NOT to be called
        @transition_service.expects(:transition_step).never
        @orchestrator.expects(:check_workflow_completion).never
        @logger.expects(:warn) # Expect warning log

        # Act & Assert
        exception = assert_raises(Yantra::Errors::EnqueueFailed) do
          @orchestrator.handle_post_processing(@step_a_id)
        end
        assert_equal enqueue_error, exception
      end

      def test_handle_post_processing_handles_unexpected_error
        step_post_processing = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: :post_processing)
        unexpected_error = StandardError.new("Something else broke")

        @repo.expects(:find_step).with(@step_a_id).returns(step_post_processing)
        # Simulate DependentProcessor raising unexpected error
        @dependent_processor.expects(:process_successors).raises(unexpected_error)
        # Expect handle_post_processing_failure to be called
        @orchestrator.expects(:handle_post_processing_failure).with(@step_a_id, unexpected_error)

        # Act - Call handle_post_processing
        @orchestrator.handle_post_processing(@step_a_id)
      end

      def test_step_failed_transitions_state_processes_failure_cascade
        error_info = { class: 'StandardError', message: 'It Broke' }
        step_running = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: :running)
        step_failed = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: :failed) # For post-failure find
        cancelled_ids = [@step_b_id]

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('step_failed_flow')

          # 1. Transition state to FAILED via service
          @transition_service.expects(:transition_step)
            .with(@step_a_id, FAILED, expected_old_state: RUNNING, extra_attrs: has_key(:error) & has_key(:finished_at))
            .returns(true).in_sequence(sequence)
          # 2. Set workflow failure flag
          @repo.expects(:find_step).with(@step_a_id).returns(step_running).in_sequence(sequence) # For getting workflow_id
          @repo.expects(:update_workflow_attributes).with(@workflow_id, { has_failures: true }).returns(true).in_sequence(sequence)
          # 3. Publish step failed event
          @repo.expects(:find_step).with(@step_a_id).returns(step_failed).in_sequence(sequence) # For event payload
          @notifier.expects(:publish).with('yantra.step.failed', has_key(:error)).in_sequence(sequence)
          # 4. Call process_failure_dependents_and_check_completion helper
          @orchestrator.expects(:process_failure_dependents_and_check_completion).with(@step_a_id).in_sequence(sequence)

          # Act
          @orchestrator.step_failed(@step_a_id, error_info)
        end
      end

      def test_process_failure_dependents_and_check_completion_calls_processor_and_check
        step_failed = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: :failed)
        cancelled_ids = [@step_b_id]
        sequence = Mocha::Sequence.new('failure_helper_flow')

        @repo.expects(:find_step).with(@step_a_id).returns(step_failed).in_sequence(sequence)
        # Expect call to dependent processor
        @dependent_processor.expects(:process_failure_cascade)
           .with(finished_step_id: @step_a_id, workflow_id: @workflow_id)
           .returns(cancelled_ids).in_sequence(sequence)
        # Expect event publishing for cancelled steps
        @repo.stubs(:find_step).with(@step_b_id).returns(MockStep.new(id: @step_b_id, workflow_id: @workflow_id, klass: 'StepB'))
        @notifier.expects(:publish).with('yantra.step.cancelled', has_entries(step_id: @step_b_id)).in_sequence(sequence)
        # Expect check_workflow_completion
        @orchestrator.expects(:check_workflow_completion).with(@workflow_id).in_sequence(sequence)

        # Act - Call the private helper method directly for testing
        @orchestrator.send(:process_failure_dependents_and_check_completion, @step_a_id)
      end


      # =========================================================================
      # check_workflow_completion Tests
      # =========================================================================
      def test_check_workflow_completion_marks_succeeded
        workflow_running = MockWorkflow.new(id: @workflow_id, klass: 'MyWorkflow', state: :running)
        workflow_succeeded = MockWorkflow.new(id: @workflow_id, klass: 'MyWorkflow', state: :succeeded, finished_at: @frozen_time)

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('workflow_completion_success')

          # Expect check for steps in progress (using updated WORK_IN_PROGRESS_STATES)
          @repo.expects(:has_steps_in_states?)
            .with(workflow_id: @workflow_id, states: StateMachine::WORK_IN_PROGRESS_STATES)
            .returns(false).in_sequence(sequence) # No steps in PENDING, SCHEDULING, ENQUEUED, RUNNING, POST_PROCESSING

          @repo.expects(:find_workflow)
            .with(@workflow_id)
            .returns(workflow_running).in_sequence(sequence)

          @repo.expects(:workflow_has_failures?)
            .with(@workflow_id)
            .returns(false).in_sequence(sequence)

          @repo.expects(:update_workflow_attributes)
            .with(@workflow_id,
                  { state: StateMachine::SUCCEEDED.to_s, finished_at: @frozen_time },
                  expected_old_state: StateMachine::RUNNING)
            .returns(true).in_sequence(sequence)

          @repo.expects(:find_workflow)
            .with(@workflow_id)
            .returns(workflow_succeeded).in_sequence(sequence)

          @notifier.expects(:publish)
            .with('yantra.workflow.succeeded', any_parameters)
            .in_sequence(sequence)

          # Act
          @orchestrator.send(:check_workflow_completion, @workflow_id)
        end
      end

      def test_check_workflow_completion_does_nothing_if_steps_in_progress
        # Expect check for steps in progress (returns true)
        @repo.expects(:has_steps_in_states?)
          .with(workflow_id: @workflow_id, states: StateMachine::WORK_IN_PROGRESS_STATES)
          .returns(true) # Simulate steps still running/scheduling/enqueued etc.

        # Ensure no other methods are called
        @repo.expects(:find_workflow).never
        @repo.expects(:workflow_has_failures?).never
        @repo.expects(:update_workflow_attributes).never
        @notifier.expects(:publish).never

        # Act
        @orchestrator.send(:check_workflow_completion, @workflow_id)
      end

      # =========================================================================
      # Error Handling / Edge Case Tests
      # =========================================================================
      def test_handle_post_processing_failure_transitions_and_processes_cascade
        step_post_processing = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: :post_processing)
        step_failed = MockStep.new(id: @step_a_id, workflow_id: @workflow_id, state: :failed) # For event payload
        cancelled_ids = [@step_b_id]
        error = StandardError.new("Post-processing boom")

        Time.stub :current, @frozen_time do
          sequence = Mocha::Sequence.new('post_processing_failure')

          @transition_service.expects(:transition_step)
            .with(@step_a_id, FAILED,
                  expected_old_state: POST_PROCESSING,
                  extra_attrs: has_entries(
                    error: has_entries(class: 'StandardError', message: 'Post-processing boom'),
                    finished_at: @frozen_time
                  )
                 ).returns(true).in_sequence(sequence)


          @repo.expects(:find_step).with(@step_a_id).returns(step_post_processing).in_sequence(sequence)
          @repo.expects(:update_workflow_attributes)
            .with(@workflow_id, { has_failures: true })
            .returns(true).in_sequence(sequence)

          @repo.expects(:find_step).with(@step_a_id).returns(step_failed).in_sequence(sequence)
          @notifier.expects(:publish).with('yantra.step.failed', has_key(:error)).in_sequence(sequence)

          @orchestrator.expects(:process_failure_dependents_and_check_completion)
            .with(@step_a_id).in_sequence(sequence)

          # Act
          @orchestrator.send(:handle_post_processing_failure, @step_a_id, error)
        end
      end

      # --- UPDATED: Test name for clarity ---
      def test_check_workflow_completion_does_not_finalize_if_scheduling_or_enqueued_steps_remain
        # Simulate steps remaining in SCHEDULING or ENQUEUED
        @repo.expects(:has_steps_in_states?)
          .with(workflow_id: @workflow_id, states: StateMachine::WORK_IN_PROGRESS_STATES)
          .returns(true) # WORK_IN_PROGRESS_STATES now includes SCHEDULING/ENQUEUED

        @repo.expects(:find_workflow).never
        @repo.expects(:update_workflow_attributes).never
        @notifier.expects(:publish).never

        @orchestrator.send(:check_workflow_completion, @workflow_id)
      end
      # --- END UPDATED ---


    end # class OrchestratorTest
  end # module Core
end # module Yantra

