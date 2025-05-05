# test/integration/active_job_integration_test.rb

require 'test_helper'
require 'json' # Keep, might be used implicitly

# --- Yantra Requires (Conditional) ---
# Keep original conditional loading logic
if AR_LOADED # Assumes test_helper defines AR_LOADED
  require 'yantra/client'
  require 'yantra/workflow'
  require 'yantra/step'
  require 'yantra/persistence/active_record/workflow_record'
  require 'yantra/persistence/active_record/step_record'
  require 'yantra/persistence/active_record/step_dependency_record'
  require 'yantra/worker/active_job/step_job'
  require 'yantra/worker/active_job/adapter'
  require 'yantra/core/step_enqueuer' # Needed for stubbing
  require 'yantra/core/step_executor' # Needed for retry test
  require 'active_job/test_helper' # Keep AJ helper require here
end

# --- Test Support Requires ---
require_relative '../support/test_notifier_adapter'


# --- Dummy Classes for Integration Tests ---
# (Dummy classes remain the same)
class IntegrationStepA < Yantra::Step
  def perform(msg: 'A'); puts "INTEGRATION_TEST: Job A running"; sleep 0.1; { output_a: msg.upcase }; end
end

class IntegrationStepB < Yantra::Step
  def perform(input_data:, msg: 'B'); puts "INTEGRATION_TEST: Job B running"; { output_b: "#{input_data[:a_out]}_#{msg.upcase}" }; end
end

class IntegrationStepC < Yantra::Step
  def perform(msg: 'C'); puts "INTEGRATION_TEST: Job C running"; { output_c: msg.downcase }; end
end

class IntegrationStepD < Yantra::Step
  def perform(input_b:, input_c:); puts "INTEGRATION_TEST: Job D running"; { output_d: "#{input_b[:output_b]}-#{input_c[:output_c]}" }; end
end

class IntegrationStepE < Yantra::Step
  def perform(msg: 'E')
    puts "INTEGRATION_TEST: Job E running"
    sleep 0.1
    { output_e: msg.upcase }
  end
end

class IntegrationStepFails < Yantra::Step
  def self.yantra_max_attempts; 1; end
  def perform(msg: 'F'); puts "INTEGRATION_TEST: Job Fails running - WILL FAIL"; raise StandardError, 'Integration job failed!'; end
end

class IntegrationStepDelayedFails < Yantra::Step
  def self.yantra_max_attempts; 1; end
  def perform(msg: 'F')
    puts "INTEGRATION_TEST: Delayed Job Fails running - WILL FAIL"
    raise StandardError, 'Delayed step intentionally failed!'
  end
end

class IntegrationJobRetry < Yantra::Step
  @@retry_test_attempts = Hash.new(0)
  def self.reset_attempts!; @@retry_test_attempts = Hash.new(0); end
  def self.yantra_max_attempts; 2; end

  def perform(msg: 'Retry')
    attempt_key = self.id || SecureRandom.uuid
    @@retry_test_attempts[attempt_key] = @@retry_test_attempts[attempt_key].to_i + 1
    current_attempt = @@retry_test_attempts[attempt_key]
    if current_attempt < 2
      raise StandardError, "Integration job failed on attempt #{current_attempt}!"
    else
      { output_retry: "Success on attempt #{current_attempt}" }
    end
  end
end

class PipeProducer < Yantra::Step
  def perform(value:)
    { produced_data: "PRODUCED_#{value.upcase}" }
  end
end

class PipeConsumer < Yantra::Step
  def perform()
    parent_data = parent_outputs
    producer_output_hash = parent_data.values.find { |output| output&.key?('produced_data') }
    unless producer_output_hash && producer_output_hash['produced_data']
      raise "Consumer failed: Did not receive expected data key 'produced_data' from producer. Got: #{parent_data.inspect}"
    end
    consumed_data = producer_output_hash['produced_data']
    { consumed: consumed_data, extra: 'CONSUMED' }
  end
end


# --- Dummy Workflow Classes ---
# (Workflow classes remain the same)
class LinearSuccessWorkflow < Yantra::Workflow
  def perform
    step_a_ref = run IntegrationStepA, name: :step_a, params: { msg: 'Hello' }
    run IntegrationStepB, name: :step_b, params: { input_data: { a_out: 'A_OUT' }, msg: 'World' }, after: step_a_ref
  end
end

class LinearFailureWorkflow < Yantra::Workflow
  def perform
    step_f_ref = run IntegrationStepFails, name: :step_f, params: { msg: 'Fail Me' }
    run IntegrationStepA, name: :step_a, params: { msg: 'Never runs' }, after: step_f_ref
  end
end

class ComplexGraphWorkflow < Yantra::Workflow
  def perform
    step_a_ref = run IntegrationStepA, name: :a, params: { msg: 'Start' }
    step_b_ref = run IntegrationStepB, name: :b, params: { input_data: { a_out: 'A_OUT' }, msg: 'B' }, after: step_a_ref
    step_c_ref = run IntegrationStepC, name: :c, params: { msg: 'C' }, after: step_a_ref
    run IntegrationStepD, name: :d, params: { input_b: { output_b: 'B_OUT' }, input_c: { output_c: 'c_out' } }, after: [step_b_ref, step_c_ref]
  end
end

class RetryWorkflow < Yantra::Workflow
  def perform
    step_r_ref = run IntegrationJobRetry, name: :step_r
    run IntegrationStepA, name: :step_a, after: step_r_ref
  end
end

class PipeliningWorkflow < Yantra::Workflow
  def perform
    producer_ref = run PipeProducer, name: :producer, params: { value: 'data123' }
    run PipeConsumer, name: :consumer, after: producer_ref
  end
end

class ParallelStartWorkflow < Yantra::Workflow
  def perform
    run IntegrationStepA, name: :step_a
    run IntegrationStepE, name: :step_e
    run IntegrationStepC, name: :step_c
  end
end

class MultiBranchWorkflow < Yantra::Workflow
  def perform
    step_a_ref = run IntegrationStepA, params: { msg: 'Start A' }
    run IntegrationStepB, params: { input_data: { a_out: 'A_OUTPUT' }, msg: 'Branch B' }, after: step_a_ref
    step_c_ref = run IntegrationStepC, params: { msg: 'Start C' }
    run IntegrationStepD, params: { input_b: {}, input_c: {} }, after: step_c_ref
  end
end

class DelayedStepWorkflow < Yantra::Workflow
  def perform
    step_a_ref = run IntegrationStepA, name: :start_step, params: { msg: 'Start Delayed' }
    run IntegrationStepE, name: :delayed_step, after: step_a_ref, delay: 5.minutes
  end
end

class DelayedFailureWorkflow < Yantra::Workflow
  def perform
    step_a = run IntegrationStepA, name: :start_step, params: { msg: 'Start' }
    run IntegrationStepDelayedFails, name: :fail_later, after: step_a, delay: 5.minutes
  end
end

module Yantra
  if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Client) && defined?(ActiveJob::TestHelper)

    class ActiveJobWorkflowExecutionTest < YantraActiveRecordTestCase
      include ActiveJob::TestHelper
      include ActiveSupport::Testing::TimeHelpers

      def setup
        super
        Yantra.configure do |config|
          config.persistence_adapter = :active_record
          config.worker_adapter = :active_job
          config.notification_adapter = TestNotifierAdapter
        end
        Yantra.instance_variable_set(:@repository, nil)
        Yantra.instance_variable_set(:@worker_adapter, nil)
        Yantra.instance_variable_set(:@notifier, nil)

        ActiveJob::Base.queue_adapter = :test
        clear_enqueued_jobs
        # --- ENSURE FALSE ---
        ActiveJob::Base.queue_adapter.perform_enqueued_jobs = false
        ActiveJob::Base.queue_adapter.perform_enqueued_at_jobs = false
        # --- END ENSURE ---

        IntegrationJobRetry.reset_attempts!
        @test_notifier = Yantra.notifier
        assert_instance_of TestNotifierAdapter, @test_notifier, 'TestNotifierAdapter should be configured'
        @test_notifier.clear!
      end

      def teardown
        @test_notifier.clear! if @test_notifier
        clear_enqueued_jobs
        Mocha::Mockery.instance.teardown
        Yantra::Configuration.reset! if defined?(Yantra::Configuration) && Yantra::Configuration.respond_to?(:reset!)
        super
      end

      def repository
        Yantra.repository
      end

      # --- Test Cases (Assertions might need further adjustment based on failures) ---

      def test_linear_workflow_success_end_to_end
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        step_b_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepB' }
        refute_nil step_a_record; refute_nil step_b_record

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start (Only workflow started and bulk enqueued expected now)
        assert_equal 2, @test_notifier.published_events.count, 'Expected 2 events after start'
        assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[1][:name]
        assert_equal [step_a_record.id], @test_notifier.published_events[1][:payload][:enqueued_ids]

        # Assert 1: Job A enqueued, state is 'enqueued'
        assert_equal 1, enqueued_jobs.size
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_a_record.id, workflow_id, 'IntegrationStepA'])
        assert_equal 'enqueued', step_a_record.reload.state

        # Act 2: Perform Job A
        @test_notifier.clear!
        perform_enqueued_jobs # Explicitly run Job A

        # Assert Events after Job A runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish step.started, step.succeeded, step.enqueued(B)'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[1][:name] # B is enqueued
        assert_equal [step_b_record.id], @test_notifier.published_events[1][:payload][:enqueued_ids]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[2][:name]

        # Assert 2: Job A succeeded, Job B enqueued
        step_a_record.reload
        assert_equal 'succeeded', step_a_record.state
        assert_equal({ 'output_a' => 'HELLO' }, step_a_record.output)
        assert_equal 1, enqueued_jobs.size # Only B is left
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_b_record.id, workflow_id, 'IntegrationStepB'])
        assert_equal 'enqueued', step_b_record.reload.state

        # Act 3: Perform Job B
        @test_notifier.clear!
        perform_enqueued_jobs # Explicitly run Job B

        # Assert Events after Job B runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish step.started, step.succeeded, workflow.succeeded'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]

        # Assert 3 & 4: Job B succeeded, Workflow succeeded
        step_b_record.reload
        assert_equal 'succeeded', step_b_record.state
        assert_equal({ 'output_b' => 'A_OUT_WORLD' }, step_b_record.output)
        assert_equal 0, enqueued_jobs.size # Queue empty
        wf_record = repository.find_workflow(workflow_id)
        assert_equal 'succeeded', wf_record.state
        refute wf_record.has_failures
        refute_nil wf_record.finished_at
      end

      def test_linear_workflow_failure_end_to_end
        workflow_id = Client.create_workflow(LinearFailureWorkflow)
        step_f_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepFails' }
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        refute_nil step_f_record; refute_nil step_a_record

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count
        assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[1][:name]
        assert_equal [step_f_record.id], @test_notifier.published_events[1][:payload][:enqueued_ids]

        # Assert 1: Job F enqueued
        assert_equal 1, enqueued_jobs.size
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_f_record.id, workflow_id, 'IntegrationStepFails'])
        assert_equal 'enqueued', step_f_record.reload.state

        # Act 2: Perform Job F (fails permanently)
        @test_notifier.clear!
        perform_enqueued_jobs # Explicitly run Job F

        # Assert Events after Job F fails
        assert_equal 4, @test_notifier.published_events.count, 'Should publish step.started, step.failed, step.cancelled, workflow.failed'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.failed', @test_notifier.published_events[1][:name]
        assert_equal 'yantra.step.cancelled', @test_notifier.published_events[2][:name] # Step A cancelled
        assert_equal 'yantra.workflow.failed', @test_notifier.published_events[3][:name]

        # Assert 2: Job F failed, Job A cancelled
        step_f_record.reload
        assert_equal 'failed', step_f_record.state
        refute_nil step_f_record.error
        step_a_record.reload
        assert_equal 'cancelled', step_a_record.state
        refute_nil step_a_record.finished_at

        # Assert 3: Workflow failed
        assert_equal 0, enqueued_jobs.size
        wf_record = repository.find_workflow(workflow_id)
        assert_equal 'failed', wf_record.state
        assert wf_record.has_failures
        refute_nil wf_record.finished_at
      end

      def test_complex_graph_success_end_to_end
        workflow_id = Client.create_workflow(ComplexGraphWorkflow)
        step_a = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        step_b = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepB' }
        step_c = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepC' }
        step_d = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepD' }
        [step_a, step_b, step_c, step_d].each { |s| refute_nil s }

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count, 'Should publish workflow.started, step.enqueued(A)'
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[1][:name]
        assert_equal [step_a.id], @test_notifier.published_events[1][:payload][:enqueued_ids]

        # Assert 1: Job A enqueued
        assert_equal 1, enqueued_jobs.size
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_a.id, workflow_id, 'IntegrationStepA'])
        assert_equal 'enqueued', step_a.reload.state

        # Act 2: Perform Job A
        @test_notifier.clear!
        perform_enqueued_jobs # Runs A

        # Assert Events after Job A runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish A.started, A.succeeded, B+C enqueued'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        enqueued_event = @test_notifier.published_events[1]
        assert_equal 'yantra.step.bulk_enqueued', enqueued_event[:name]
        assert_equal 2, enqueued_event[:payload][:enqueued_ids].length
        assert_includes enqueued_event[:payload][:enqueued_ids], step_b.id
        assert_includes enqueued_event[:payload][:enqueued_ids], step_c.id
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[2][:name]

        # Assert 2: A succeeded, B & C enqueued
        assert_equal 'succeeded', step_a.reload.state
        assert_equal 2, enqueued_jobs.size # B and C enqueued now
        assert_equal 'enqueued', step_b.reload.state
        assert_equal 'enqueued', step_c.reload.state

        # Act 3: Perform Job B and Job C
        @test_notifier.clear!
        perform_enqueued_jobs # Runs B and C

        # Assert Events after Job B & C run
        assert_equal 5, @test_notifier.published_events.count, 'Should publish B/C starts, B/C succeeds, D.enqueued'
        assert_equal 2, @test_notifier.find_events('yantra.step.started').count
        assert_equal 2, @test_notifier.find_events('yantra.step.succeeded').count
        d_enqueued_event = @test_notifier.find_event('yantra.step.bulk_enqueued') { |ev| ev[:payload][:enqueued_ids].include?(step_d.id) }
        assert_equal [step_d.id], d_enqueued_event[:payload][:enqueued_ids]

        # Assert 3: B succeeded, C succeeded, D enqueued
        assert_equal 'succeeded', step_b.reload.state
        assert_equal 'succeeded', step_c.reload.state
        assert_equal 1, enqueued_jobs.size # Only D left
        assert_equal 'enqueued', step_d.reload.state

        # Act 4: Perform Job D
        @test_notifier.clear!
        perform_enqueued_jobs # Run D

        # Assert Events after Job D runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish D.started, D.succeeded, workflow.succeeded'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]

        # Assert 4: D succeeded, Workflow succeeded
        assert_equal 'succeeded', step_d.reload.state
        assert_equal 0, enqueued_jobs.size
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
      end

      # --- MODIFIED: test_workflow_with_retries ---
      def test_workflow_with_retries
        workflow_id = Client.create_workflow(RetryWorkflow)
        step_r_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationJobRetry' }
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        refute_nil step_r_record; refute_nil step_a_record

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count, 'Should publish workflow.started, step_r.enqueued'
        assert_equal [step_r_record.id], @test_notifier.published_events[1][:payload][:enqueued_ids]

        # Assert 1: Job R enqueued
        assert_equal 1, enqueued_jobs.size
        assert_equal 'enqueued', step_r_record.reload.state

        # Act 2: Perform Job R (Attempt 1 - Fails)
        @test_notifier.clear!
        # Perform the job. ActiveJob's retry_on should catch the error internally.
        perform_enqueued_jobs(only: Worker::ActiveJob::StepJob)

        # Assert Events after Attempt 1 (Error caught internally by AJ)
        assert_equal 1, @test_notifier.published_events.count, 'Should publish only R.started'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]

        # Assert 2: State/Retry updates (Result of internal failure handling)
        step_r_record.reload
        assert_equal 'running', step_r_record.state # Yantra state remains running
        assert_equal 1, step_r_record.retries      # Retry count incremented
        refute_nil step_r_record.error             # Error recorded
        assert_match /Integration job failed on attempt 1/, step_r_record.error['message']

        # Assert 2.1: Job should be re-enqueued by ActiveJob's retry mechanism
        assert_equal 1, enqueued_jobs.size, 'Job should be re-enqueued for retry'
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_r_record.id, workflow_id, 'IntegrationJobRetry'])

        # Act 3: Perform Job R (Attempt 2 - Succeeds)
        @test_notifier.clear!
        perform_enqueued_jobs # Runs R again, should succeed this time

        # Assert Events after Attempt 2
        assert_equal 2, @test_notifier.published_events.count, 'Should publish R.succeeded, A.enqueued'
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[0][:name] # A enqueued
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name] # R succeeded

        # Assert 3: Job R succeeded, Job A enqueued
        step_r_record.reload
        assert_equal 'succeeded', step_r_record.state
        assert_equal 1, step_r_record.retries # Retries don't increment on success
        assert_equal({ 'output_retry' => 'Success on attempt 2' }, step_r_record.output)
        assert_equal 1, enqueued_jobs.size # Job A is enqueued
        assert_equal 'enqueued', step_a_record.reload.state

        # Act 4: Perform Job A
        @test_notifier.clear!
        perform_enqueued_jobs # Runs A

        # Assert Events after Job A runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish A.started, A.succeeded, workflow.succeeded'

        # Assert 4: Job A succeeded, Workflow succeeded
        assert_equal 'succeeded', step_a_record.reload.state
        assert_equal 0, enqueued_jobs.size
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
      end

      def test_enqueue_failure_is_transient_and_recovered_on_retry
        # Arrange: Create workflow
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        step_a = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        step_b = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepB' }
        refute_nil step_a; refute_nil step_b

        # Start workflow, Step A gets enqueued
        Client.start_workflow(workflow_id)
        assert_equal 1, enqueued_jobs.size
        assert_equal 'enqueued', step_a.reload.state

        # Arrange: Stub the worker adapter to fail enqueueing Step B the first time
        # Store original adapter if needed for teardown
        @original_worker_adapter = Yantra.worker_adapter
        worker_adapter_instance = Yantra.worker_adapter
        refute_nil worker_adapter_instance, "Worker adapter instance should not be nil"
        worker_adapter_instance.stubs(:enqueue)
          .with(step_b.id, any_parameters) # Match only Step B
          .returns(false).then.returns(true) # Fail first, succeed second

        # Act 1: Perform Step A's job.
        perform_enqueued_jobs(only: Worker::ActiveJob::StepJob)

        # Assert 1: Step A state, Step B state, and Job A re-enqueue
        step_a.reload
        step_b.reload
        assert_equal 'post_processing', step_a.state, "Step A should be post_processing"
        assert step_a.performed_at, "Step A should have performed_at set"
        assert_equal 'scheduling', step_b.state, "Step B should be stuck in scheduling"
        assert_nil step_b.enqueued_at, "Step B should not have enqueued_at due to enqueue failure"
        assert_equal 1, enqueued_jobs.size, "Job A should be re-enqueued after enqueue failure"
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_a.id, workflow_id, 'IntegrationStepA'])

        # Arrange 2: Clear notifier for next step
        @test_notifier.clear!
        # Unstub the specific method on the instance to allow normal behavior on retry
        worker_adapter_instance.unstub(:enqueue)

        # Act 2: Perform the retried Job A.
        # This runs the retried Job A (which was the only one in the queue)
        perform_enqueued_jobs

        # Assert 2: Step B should now be enqueued successfully
        step_a.reload # Reload Step A again after retry
        step_b.reload
        assert_equal 'succeeded', step_a.state, "Step A should be succeeded after retry completes post-processing"
        assert_equal 'enqueued', step_b.state, "Step B should now be enqueued after retry"
        refute_nil step_b.enqueued_at, "Step B should now have enqueued_at after retry"
        # --- CORRECTED ASSERTION ---
        # After retried Job A runs, Step B's job should be the only one in the queue
        assert_equal 1, enqueued_jobs.size, "Step B job should be in the queue"
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_b.id, workflow_id, 'IntegrationStepB'])
        # --- END CORRECTION ---

        # Assert 2: Events for Step B enqueue should be published on retry
        assert_equal 2, @test_notifier.published_events.count, "Should publish B.enqueued and A.succeeded on retry"
        assert @test_notifier.find_event('yantra.step.bulk_enqueued') { |ev| ev[:payload][:enqueued_ids] == [step_b.id] }
        assert @test_notifier.find_event('yantra.step.succeeded') { |ev| ev[:payload][:step_id] == step_a.id }

        # --- ADDED Act 3 and Assert 3 ---
        # Act 3: Perform the now-enqueued Job B
        @test_notifier.clear!
        clear_enqueued_jobs # Clear Job B before performing it
        Worker::ActiveJob::StepJob.perform_later(step_b.id, workflow_id, 'IntegrationStepB') # Re-enqueue for perform
        perform_enqueued_jobs

        # Assert 3: Final state check
        step_b.reload
        assert_equal 'succeeded', step_b.state
        assert_equal 0, enqueued_jobs.size
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
        # --- END ADDED ---
      end




      def test_pipelining_workflow
        workflow_id = Client.create_workflow(PipeliningWorkflow)
        producer_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'PipeProducer' }
        consumer_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'PipeConsumer' }
        refute_nil producer_record; refute_nil consumer_record

        producer_id = producer_record.id; consumer_id = consumer_record.id
        assert Yantra::Persistence::ActiveRecord::StepDependencyRecord.exists?(step_id: consumer_id, depends_on_step_id: producer_id)

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count, 'Should publish workflow.started, producer.enqueued'
        assert_equal [producer_record.id], @test_notifier.published_events[1][:payload][:enqueued_ids]

        # Assert 1: Producer enqueued
        assert_equal 1, enqueued_jobs.size
        assert_equal 'enqueued', producer_record.reload.state

        # Act 2: Perform Producer
        @test_notifier.clear!
        perform_enqueued_jobs

        # Assert Events after Producer runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish producer.started, producer.succeeded, consumer.enqueued'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal [consumer_record.id], @test_notifier.published_events[1][:payload][:enqueued_ids] # Consumer enqueued
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[2][:name]

        # Assert 2: Producer succeeded, Consumer enqueued
        producer_record.reload
        assert_equal 'succeeded', producer_record.state
        assert_equal({ 'produced_data' => 'PRODUCED_DATA123' }, producer_record.output)
        assert_equal 1, enqueued_jobs.size
        assert_equal 'enqueued', consumer_record.reload.state

        # Act 3: Perform Consumer
        @test_notifier.clear!
        perform_enqueued_jobs

        # Assert Events after Consumer runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish consumer.started, consumer.succeeded, workflow.succeeded'

        # Assert 3: Consumer succeeded, Workflow succeeded
        consumer_record.reload
        assert_equal 'succeeded', consumer_record.state
        assert_equal({ 'consumed' => 'PRODUCED_DATA123', 'extra' => 'CONSUMED' }, consumer_record.output)
        assert_equal 0, enqueued_jobs.size
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
      end

      def test_cancel_workflow_cancels_running_workflow
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        step_a_record = repository.list_steps(workflow_id: workflow_id).find { |s| s.klass == 'IntegrationStepA' }
        step_b_record = repository.list_steps(workflow_id: workflow_id).find { |s| s.klass == 'IntegrationStepB' }
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs # Run A, Step B becomes enqueued

        @test_notifier.clear!
        wf_record = repository.find_workflow(workflow_id)
        assert_equal 'running', wf_record.reload.state
        assert_equal 'succeeded', step_a_record.reload.state
        step_b_reloaded = step_b_record.reload
        assert_equal 'enqueued', step_b_reloaded.state
        refute_nil step_b_reloaded.enqueued_at

        # Act: Cancel the workflow
        cancel_result = Client.cancel_workflow(workflow_id)
        assert cancel_result

        # Assert Events: Only workflow cancelled event expected (step B is enqueued, not cancellable)
        assert_equal 1, @test_notifier.published_events.count, 'Expected only 1 event after cancel'
        assert_equal 'yantra.workflow.cancelled', @test_notifier.published_events[0][:name]
        assert_nil @test_notifier.find_event('yantra.step.cancelled')

        # Assert Final States
        wf_record.reload
        assert_equal 'cancelled', wf_record.state
        refute_nil wf_record.finished_at
        assert_equal 'succeeded', step_a_record.reload.state
        step_b_reloaded = step_b_record.reload
        assert_equal 'enqueued', step_b_reloaded.state, "Step B should remain enqueued"
        assert_nil step_b_reloaded.finished_at
      end

      def test_cancel_workflow_cancels_pending_workflow
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        step_b_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepB' }
        wf_record = repository.find_workflow(workflow_id)
        assert_equal 'pending', wf_record.reload.state
        assert_equal 'pending', step_a_record.reload.state
        assert_equal 'pending', step_b_record.reload.state
        @test_notifier.clear!

        # Act: Cancel
        cancel_result = Client.cancel_workflow(workflow_id)
        assert cancel_result

        # Assert Events: Expect 1 workflow + 2 step cancelled events
        assert_equal 3, @test_notifier.published_events.count, 'Expected 3 events after cancel (workflow + 2 steps)'
        assert_equal 'yantra.workflow.cancelled', @test_notifier.published_events[0][:name]
        step_cancelled_events = @test_notifier.find_events('yantra.step.cancelled')
        assert_equal 2, step_cancelled_events.count, 'Should be 2 step.cancelled events'
        cancelled_step_ids = step_cancelled_events.map { |ev| ev[:payload][:step_id] }
        assert_includes cancelled_step_ids, step_a_record.id
        assert_includes cancelled_step_ids, step_b_record.id

        # Assert Final States
        wf_record.reload
        assert_equal 'cancelled', wf_record.state
        refute_nil wf_record.finished_at
        assert_equal 'cancelled', step_a_record.reload.state
        assert_equal 'cancelled', step_b_record.reload.state
        refute_nil step_a_record.finished_at
        refute_nil step_b_record.finished_at
        refute Client.start_workflow(workflow_id)
        assert_equal 0, enqueued_jobs.size
      end

      def test_cancel_workflow_does_nothing_for_finished_workflow
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs # Run A
        perform_enqueued_jobs # Run B
        wf = repository.find_workflow(workflow_id)
        assert_equal 'succeeded', wf.state, "Workflow should be succeeded before cancel attempt"
        @test_notifier.clear!

        cancel_result = Client.cancel_workflow(workflow_id)

        refute cancel_result
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
        assert_equal 0, @test_notifier.published_events.count
      end

      def test_retry_failed_steps_restarts_failed_workflow
        workflow_id = Client.create_workflow(LinearFailureWorkflow)
        step_f_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepFails' }
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs # Runs F, fails permanently
        assert_equal 'failed', repository.find_workflow(workflow_id).state
        assert_equal 'failed', step_f_record.reload.state
        assert_equal 'cancelled', step_a_record.reload.state
        @test_notifier.clear!

        # Act: Retry
        reenqueued_count = Client.retry_failed_steps(workflow_id)

        # Assert: State reset, job re-enqueued, events published
        # --- CORRECTED: Assert count ---
        assert_equal 1, reenqueued_count, "Should report 1 step re-enqueued"
        # --- END CORRECTION ---
        assert_equal 'running', repository.find_workflow(workflow_id).state
        refute repository.find_workflow(workflow_id).has_failures
        assert_nil repository.find_workflow(workflow_id).finished_at
        assert_equal 'enqueued', step_f_record.reload.state # Expect 'enqueued'
        assert_equal 'pending', step_a_record.reload.state

        # Assert Events after Retry Call
        assert_equal 1, @test_notifier.published_events.count, 'Should publish 1 step.enqueued event'
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[0][:name]
        assert_equal [step_f_record.id], @test_notifier.published_events[0][:payload][:enqueued_ids]

        # Assert job queue
        assert_equal 1, enqueued_jobs.size
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_f_record.id, workflow_id, 'IntegrationStepFails'])

        # Act 2: Perform retried job (it will fail permanently again)
        @test_notifier.clear!
        perform_enqueued_jobs

        # Assert Events after Retried Job Fails
        assert_equal 4, @test_notifier.published_events.count, 'Should publish F.started, F.failed, A.cancelled, workflow.failed'

        # Assert Final State
        assert_equal 'failed', repository.find_workflow(workflow_id).state
        assert_equal 'failed', step_f_record.reload.state
        assert_equal 'cancelled', step_a_record.reload.state
        assert_equal 0, enqueued_jobs.size
      end


      def test_retry_failed_steps_does_nothing_for_succeeded_workflow
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs # Run A
        perform_enqueued_jobs # Run B
        wf = repository.find_workflow(workflow_id)
        assert_equal 'succeeded', wf.state, "Workflow should be succeeded before retry attempt"
        @test_notifier.clear!

        assert_raises Yantra::Errors::InvalidWorkflowState do
          Client.retry_failed_steps(workflow_id)
        end

        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
        assert_equal 0, @test_notifier.published_events.count
        assert_equal 0, enqueued_jobs.size
      end

      def test_retry_failed_steps_handles_not_found
        assert_raises Yantra::Errors::WorkflowNotFound do
          Client.retry_failed_steps(SecureRandom.uuid)
        end
        assert_equal 0, @test_notifier.published_events.count
      end

      def test_retry_failed_steps_handles_failed_workflow_with_no_failed_steps
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        step_a_record = repository.list_steps(workflow_id: workflow_id).find { |s| s.klass == 'IntegrationStepA' }
        repository.update_workflow_attributes(workflow_id, { state: 'failed', has_failures: true }, expected_old_state: 'pending')
        assert_equal 'failed', repository.find_workflow(workflow_id).state
        assert_equal 'pending', step_a_record.reload.state
        @test_notifier.clear!

        reenqueued_result = Client.retry_failed_steps(workflow_id)

        assert_equal 0, reenqueued_result, "Should return 0 when no steps are eligible for retry"
        assert_equal 'running', repository.find_workflow(workflow_id).state
        refute repository.find_workflow(workflow_id).has_failures
        assert_equal 'pending', step_a_record.reload.state
        assert_equal 0, @test_notifier.published_events.count
        assert_equal 0, enqueued_jobs.size
      end


      def test_parallel_start_workflow_enqueues_all_initial_steps
        workflow_id = Client.create_workflow(ParallelStartWorkflow)
        all_steps = repository.list_steps(workflow_id:)
        step_a = all_steps.find { |s| s.klass == 'IntegrationStepA' }
        step_e = all_steps.find { |s| s.klass == 'IntegrationStepE' }
        step_c = all_steps.find { |s| s.klass == 'IntegrationStepC' }
        refute_nil step_a; refute_nil step_e; refute_nil step_c

        @test_notifier.clear!
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count
        assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
        bulk_enqueued_event = @test_notifier.published_events[1]
        assert_equal 'yantra.step.bulk_enqueued', bulk_enqueued_event[:name]
        enqueued_ids = bulk_enqueued_event[:payload][:enqueued_ids]
        assert_equal 3, enqueued_ids.length
        assert_includes enqueued_ids, step_a.id
        assert_includes enqueued_ids, step_e.id
        assert_includes enqueued_ids, step_c.id

        # Assert Queue State
        assert_equal 3, enqueued_jobs.size
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_a.id, workflow_id, 'IntegrationStepA'])
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_e.id, workflow_id, 'IntegrationStepE'])
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_c.id, workflow_id, 'IntegrationStepC'])

        # Assert DB State
        assert_equal 'enqueued', step_a.reload.state
        assert_equal 'enqueued', step_e.reload.state
        assert_equal 'enqueued', step_c.reload.state
        assert_equal 'running', repository.find_workflow(workflow_id).state

        # Act 2: Perform all enqueued jobs
        @test_notifier.clear!
        perform_enqueued_jobs # Explicitly run A, E, C

        # Assert Final State
        assert_equal 0, enqueued_jobs.size
        assert_equal 'succeeded', step_a.reload.state
        assert_equal 'succeeded', step_e.reload.state
        assert_equal 'succeeded', step_c.reload.state
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state

        # Assert Events after completion
        assert_equal 3, @test_notifier.find_events('yantra.step.started').count
        assert_equal 3, @test_notifier.find_events('yantra.step.succeeded').count
        assert_equal 1, @test_notifier.find_events('yantra.workflow.succeeded').count
      end

      def test_multi_branch_workflow_independent_paths
        workflow_id = Client.create_workflow(MultiBranchWorkflow)
        all_steps = repository.list_steps(workflow_id:)
        step_a = all_steps.find { |s| s.klass == 'IntegrationStepA' }
        step_b = all_steps.find { |s| s.klass == 'IntegrationStepB' }
        step_c = all_steps.find { |s| s.klass == 'IntegrationStepC' }
        step_d = all_steps.find { |s| s.klass == 'IntegrationStepD' }
        refute_nil step_a; refute_nil step_b; refute_nil step_c; refute_nil step_d

        @test_notifier.clear!
        Client.start_workflow(workflow_id)

        # Assert 1: A and C enqueued
        assert_equal 2, @test_notifier.published_events.count
        bulk_event = @test_notifier.published_events[1]
        assert_equal 'yantra.step.bulk_enqueued', bulk_event[:name]
        assert_equal 2, bulk_event[:payload][:enqueued_ids].length
        assert_includes bulk_event[:payload][:enqueued_ids], step_a.id
        assert_includes bulk_event[:payload][:enqueued_ids], step_c.id

        assert_equal 2, enqueued_jobs.size
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_a.id, workflow_id, 'IntegrationStepA'])
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_c.id, workflow_id, 'IntegrationStepC'])
        assert_equal 'enqueued', step_a.reload.state
        assert_equal 'enqueued', step_c.reload.state
        assert_equal 'pending', step_b.reload.state
        assert_equal 'pending', step_d.reload.state
        assert_equal 'running', repository.find_workflow(workflow_id).state

        @test_notifier.clear!
        perform_enqueued_jobs # Explicitly run A and C

        # Assert 2: A and C succeeded, B and D enqueued
        assert_equal 'succeeded', step_a.reload.state
        assert_equal 'succeeded', step_c.reload.state
        assert_equal 'enqueued', step_b.reload.state
        assert_equal 'enqueued', step_d.reload.state
        assert_equal 2, enqueued_jobs.size
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_b.id, workflow_id, 'IntegrationStepB'])
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_d.id, workflow_id, 'IntegrationStepD'])

        # Assert 2 Events
        assert_equal 6, @test_notifier.published_events.count # 2 starts, 2 succeeds, 2 enqueues (B, D)
        assert_equal 2, @test_notifier.find_events('yantra.step.started').count
        assert_equal 2, @test_notifier.find_events('yantra.step.succeeded').count
        assert_equal 2, @test_notifier.find_events('yantra.step.bulk_enqueued').count
        assert @test_notifier.find_event('yantra.step.bulk_enqueued') { |ev| ev[:payload][:enqueued_ids] == [step_b.id] }
        assert @test_notifier.find_event('yantra.step.bulk_enqueued') { |ev| ev[:payload][:enqueued_ids] == [step_d.id] }

        @test_notifier.clear!
        perform_enqueued_jobs # Explicitly run B and D

        # Assert 3: B and D succeeded, Workflow succeeded
        assert_equal 'succeeded', step_b.reload.state
        assert_equal 'succeeded', step_d.reload.state
        assert_equal 0, enqueued_jobs.size
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state

        # Assert 3 Events
        assert_equal 5, @test_notifier.published_events.count # 2 starts, 2 succeeds, 1 workflow succeed
        assert_equal 2, @test_notifier.find_events('yantra.step.started').count
        assert_equal 2, @test_notifier.find_events('yantra.step.succeeded').count
        assert_equal 1, @test_notifier.find_events('yantra.workflow.succeeded').count
      end

      def test_delayed_step_workflow
        workflow_id = Client.create_workflow(DelayedStepWorkflow)
        step_a = repository.list_steps(workflow_id: workflow_id).find { |s| s.klass == 'IntegrationStepA' }
        step_e_delayed = repository.list_steps(workflow_id: workflow_id).find { |s| s.klass == 'IntegrationStepE' }
        refute_nil step_a; refute_nil step_e_delayed
        assert_equal 300, step_e_delayed.delay_seconds

        @test_notifier.clear!
        Client.start_workflow(workflow_id)

        # Assert 1: Only Step A is enqueued immediately
        assert_equal 1, enqueued_jobs.size
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_a.id, workflow_id, 'IntegrationStepA'])
        assert_equal 'enqueued', step_a.reload.state
        assert_equal 'pending', step_e_delayed.reload.state

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[1][:name]
        assert_equal [step_a.id], @test_notifier.published_events[1][:payload][:enqueued_ids]

        @test_notifier.clear!
        perform_enqueued_jobs # Explicitly run A

        # Assert 2: Step A succeeded, Step E is now 'enqueued' but delayed
        step_a.reload; step_e_delayed.reload
        assert_equal 'succeeded', step_a.state
        assert_equal 1, enqueued_jobs.size # E is delayed

        assert_equal 'enqueued', step_e_delayed.state
        refute_nil step_e_delayed.enqueued_at

        # Assert 2.2: Check Events after A runs
        assert_equal 3, @test_notifier.published_events.count
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[1][:name]
        assert_equal [step_e_delayed.id], @test_notifier.published_events[1][:payload][:enqueued_ids]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[2][:name]

        @test_notifier.clear!
        enqueued_step_e_job = enqueued_jobs.find { |j| j[:args][0] == step_e_delayed.id }
        refute_nil enqueued_step_e_job
        assert enqueued_step_e_job[:at]
        scheduled_at = Time.at(enqueued_step_e_job[:at])
        expected_scheduled_time = step_e_delayed.enqueued_at + 5.minutes
        assert_in_delta expected_scheduled_time.to_f, scheduled_at.to_f, 5.0

        # Use travel_to for reliable time advancement in tests
        travel_to scheduled_at + 1.second do # Travel past scheduled time
          perform_enqueued_jobs # Should now pick up and run Step E
        end

        # Assert 3: Step E succeeded, Workflow succeeded
        step_e_delayed.reload
        assert_equal 'succeeded', step_e_delayed.state
        assert_equal 0, enqueued_jobs.size
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state

        # Assert 3.1: Check Events after E runs
        assert_equal 3, @test_notifier.published_events.count
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]
      end

      def test_workflow_with_retries
        workflow_id = Client.create_workflow(RetryWorkflow)
        step_r_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationJobRetry' }
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        refute_nil step_r_record; refute_nil step_a_record

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count, 'Should publish workflow.started, step_r.enqueued'
        assert_equal [step_r_record.id], @test_notifier.published_events[1][:payload][:enqueued_ids]

        # Assert 1: Job R enqueued
        assert_equal 1, enqueued_jobs.size
        assert_equal 'enqueued', step_r_record.reload.state

        # Act 2: Perform Job R (Attempt 1 - Fails)
        @test_notifier.clear!
        # Perform the job. ActiveJob's retry_on should catch the error internally.
        # We should NOT expect an error to be raised out to the test.
        perform_enqueued_jobs(only: Worker::ActiveJob::StepJob)

        # Assert Events after Attempt 1 (Only step started should be published)
        assert_equal 1, @test_notifier.published_events.count, 'Should publish only R.started'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]

        # Assert 2: State/Retry updates (Result of internal failure handling)
        step_r_record.reload
        assert_equal 'running', step_r_record.state # Yantra state remains running
        assert_equal 1, step_r_record.retries      # Retry count incremented
        refute_nil step_r_record.error             # Error recorded
        assert_match /Integration job failed on attempt 1/, step_r_record.error['message']

        # Assert 2.1: Job should be re-enqueued by ActiveJob's retry mechanism
        assert_equal 1, enqueued_jobs.size, 'Job should be re-enqueued for retry'
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_r_record.id, workflow_id, 'IntegrationJobRetry'])

        # Act 3: Perform Job R (Attempt 2 - Succeeds)
        @test_notifier.clear!
        perform_enqueued_jobs # Runs R again, should succeed this time

        # Assert Events after Attempt 2
        assert_equal 2, @test_notifier.published_events.count, 'Should publish R.succeeded, A.enqueued'
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[0][:name] # A enqueued
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name] # R succeeded

        # Assert 3: Job R succeeded, Job A enqueued
        step_r_record.reload
        assert_equal 'succeeded', step_r_record.state
        assert_equal 1, step_r_record.retries # Retries don't increment on success
        assert_equal({ 'output_retry' => 'Success on attempt 2' }, step_r_record.output)
        assert_equal 1, enqueued_jobs.size # Job A is enqueued
        assert_equal 'enqueued', step_a_record.reload.state

        # Act 4: Perform Job A
        @test_notifier.clear!
        perform_enqueued_jobs # Runs A

        # Assert Events after Job A runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish A.started, A.succeeded, workflow.succeeded'

        # Assert 4: Job A succeeded, Workflow succeeded
        assert_equal 'succeeded', step_a_record.reload.state
        assert_equal 0, enqueued_jobs.size
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
      end


      def test_delayed_step_runs_after_workflow_cancelled
        workflow_id = Client.create_workflow(DelayedStepWorkflow)
        step_a = repository.list_steps(workflow_id: workflow_id).find { |s| s.klass == 'IntegrationStepA' }
        step_e = repository.list_steps(workflow_id: workflow_id).find { |s| s.klass == 'IntegrationStepE' }

        Client.start_workflow(workflow_id)
        perform_enqueued_jobs # Run A

        assert_equal 'enqueued', step_e.reload.state # Expect 'enqueued'
        assert step_e.enqueued_at

        # Cancel workflow before delayed step runs
        cancel_result = Client.cancel_workflow(workflow_id)
        assert cancel_result
        assert_equal 'cancelled', repository.find_workflow(workflow_id).reload.state

        # Act: Let delayed step run anyway
        enqueued_step_e_job = enqueued_jobs.find { |j| j[:args][0] == step_e.id }
        refute_nil enqueued_step_e_job, "Delayed job E should still be in queue" # Check it wasn't removed
        scheduled_at = Time.at(enqueued_step_e_job[:at])

        travel_to scheduled_at + 1.second do
          perform_enqueued_jobs
        end

        step_e.reload
        assert step_e.performed_at, "Step E should have performed_at set"
        assert_equal 'succeeded', step_e.state, "Even though workflow was cancelled, delayed step should still run"
      end
    end
  end # if defined?(YantraActiveRecordTestCase) ...
end # module Yantra

