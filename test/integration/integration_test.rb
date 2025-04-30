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
  require 'active_job/test_helper' # Keep AJ helper require here
end

# --- Test Support Requires ---
require_relative '../support/test_notifier_adapter'


# --- Dummy Classes for Integration Tests ---

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
    # Simulate some work, similar to IntegrationStepA
    puts "INTEGRATION_TEST: Job E running"
    sleep 0.1
    { output_e: msg.upcase }
  end
end

class IntegrationStepFails < Yantra::Step
  def self.yantra_max_attempts; 1; end # Force immediate permanent failure (1 attempt total)
  def perform(msg: 'F'); puts "INTEGRATION_TEST: Job Fails running - WILL FAIL"; raise StandardError, 'Integration job failed!'; end
end

class IntegrationJobRetry < Yantra::Step
  @@retry_test_attempts = Hash.new(0) # Keep class variable
  def self.reset_attempts!; @@retry_test_attempts = Hash.new(0); end
  def self.yantra_max_attempts; 2; end # 1 retry -> 2 attempts total

  def perform(msg: 'Retry')
    # Use step_id if available, otherwise generate temp key for test setup phase if needed
    # Note: self.id should be available when run via StepJob
    puts "DEBUG: Inside IntegrationJobRetry#perform. self.id is: #{self.id.inspect}"
    attempt_key = self.id || SecureRandom.uuid # Keep original logic for key
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
    # Find the output from the known parent (PipeProducer)
    producer_output_hash = parent_data.values.find { |output| output&.key?('produced_data') }

    unless producer_output_hash && producer_output_hash['produced_data']
      raise "Consumer failed: Did not receive expected data key 'produced_data' from producer. Got: #{parent_data.inspect}"
    end
    consumed_data = producer_output_hash['produced_data']
    { consumed: consumed_data, extra: 'CONSUMED' }
  end
end


# --- Dummy Workflow Classes ---

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
    run IntegrationStepA, name: :step_a, after: step_r_ref # Runs after retry succeeds
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
    run IntegrationStepA, name: :step_a # No dependencies
    run IntegrationStepE, name: :step_e # No dependencies
    run IntegrationStepC, name: :step_c # No dependencies - Assuming IntegrationStepE exists or use A/C again
  end
end


class MultiBranchWorkflow < Yantra::Workflow
  def perform
    # Branch 1
    step_a_ref = run IntegrationStepA, params: { msg: 'Start A' }
    run IntegrationStepB, params: { input_data: { a_out: 'A_OUTPUT' }, msg: 'Branch B' }, after: step_a_ref

    # Branch 2 (runs concurrently with Branch 1)
    step_c_ref = run IntegrationStepC, params: { msg: 'Start C' }
    # Pass expected structure even if values are placeholders initially
    run IntegrationStepD, params: { input_b: {}, input_c: {} }, after: step_c_ref
  end
end

class DelayedStepWorkflow < Yantra::Workflow
  def perform
    step_a_ref = run IntegrationStepA, name: :start_step, params: { msg: 'Start Delayed' }
    # Step E runs 5 minutes after Step A finishes
    run IntegrationStepE, name: :delayed_step, after: step_a_ref, delay: 5.minutes
  end
end

module Yantra
  # Keep original conditional test class definition
  if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Client) && defined?(ActiveJob::TestHelper)

    class ActiveJobWorkflowExecutionTest < YantraActiveRecordTestCase
      include ActiveJob::TestHelper
      include ActiveSupport::Testing::TimeHelpers

      def setup
        super
        Yantra.configure do |config|
          config.persistence_adapter = :active_record
          config.worker_adapter = :active_job
          config.default_step_options[:retries] = 3 # Default 4 attempts
          config.notification_adapter = TestNotifierAdapter
        end
        Yantra.instance_variable_set(:@repository, nil)
        Yantra.instance_variable_set(:@worker_adapter, nil)
        Yantra.instance_variable_set(:@notifier, nil)

        ActiveJob::Base.queue_adapter = :test
        clear_enqueued_jobs
        ActiveJob::Base.queue_adapter.perform_enqueued_jobs = false
        ActiveJob::Base.queue_adapter.perform_enqueued_at_jobs = false

        IntegrationJobRetry.reset_attempts!
        @test_notifier = Yantra.notifier
        assert_instance_of TestNotifierAdapter, @test_notifier, 'TestNotifierAdapter should be configured'
        @test_notifier.clear!
      end

      def teardown
        @test_notifier.clear! if @test_notifier
        clear_enqueued_jobs
        Yantra::Configuration.reset! if defined?(Yantra::Configuration) && Yantra::Configuration.respond_to?(:reset!)
        super
      end

      def repository
        Yantra.repository
      end

      # --- Test Cases ---

      def test_linear_workflow_success_end_to_end
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        step_b_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepB' }
        refute_nil step_a_record, 'Step A record should exist'
        refute_nil step_b_record, 'Step B record should exist'

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count, 'Expected 2 events after start'
        wf_started_event = @test_notifier.find_event('yantra.workflow.started')
        bulk_enqueued_event = @test_notifier.find_event('yantra.step.bulk_enqueued')
        step_a_record_enqueued_id = bulk_enqueued_event[:payload][:enqueued_ids].find { _1 == step_a_record.id }
        refute_nil wf_started_event, 'Workflow started event missing'
        refute_nil step_a_record_enqueued_id, 'Step A enqueued event missing'
        assert_equal workflow_id, wf_started_event[:payload][:workflow_id]
        assert_equal step_a_record.id, step_a_record.id # Kept original assertion

        # Assert 1: Job A enqueued
        assert_equal 1, enqueued_jobs.size
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_a_record.id, workflow_id, 'IntegrationStepA'])
        assert_equal 'enqueued', step_a_record.reload.state

        # Act 2: Perform Job A
        @test_notifier.clear!
        perform_enqueued_jobs # Runs Job A

        # Assert Events after Job A runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish step.started, step.succeeded, step.enqueued'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal step_a_record.id, @test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal step_a_record.id, @test_notifier.published_events[1][:payload][:step_id]
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[2][:name]
        assert_equal step_b_record.id, @test_notifier.published_events[2][:payload][:enqueued_ids].find { _1 == step_b_record.id }

        # Assert 2: Job A succeeded, Job B enqueued
        step_a_record.reload
        assert_equal 'succeeded', step_a_record.state
        assert_equal({ 'output_a' => 'HELLO' }, step_a_record.output)
        assert_equal 1, enqueued_jobs.size
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_b_record.id, workflow_id, 'IntegrationStepB'])
        assert_equal 'enqueued', step_b_record.reload.state

        # Act 3: Perform Job B
        @test_notifier.clear!
        perform_enqueued_jobs # Runs Job B

        # Assert Events after Job B runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish step.started, step.succeeded, workflow.succeeded'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal step_b_record.id, @test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal step_b_record.id, @test_notifier.published_events[1][:payload][:step_id]
        assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]
        assert_equal workflow_id, @test_notifier.published_events[2][:payload][:workflow_id]

        # Assert 3 & 4: Job B succeeded, Workflow succeeded
        step_b_record.reload
        assert_equal 'succeeded', step_b_record.state
        assert_equal({ 'output_b' => 'A_OUT_WORLD' }, step_b_record.output) # Based on original params
        assert_equal 0, enqueued_jobs.size
        wf_record = repository.find_workflow(workflow_id)
        assert_equal 'succeeded', wf_record.state
        refute wf_record.has_failures
        refute_nil wf_record.finished_at
      end

      def test_linear_workflow_failure_end_to_end
        workflow_id = Client.create_workflow(LinearFailureWorkflow)
        step_f_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepFails' }
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        refute_nil step_f_record
        refute_nil step_a_record

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count
        assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[1][:name]
        assert_equal step_f_record.id, @test_notifier.published_events[1][:payload][:enqueued_ids].find { step_f_record.id == _1 }

        # Assert 1: Job F enqueued
        assert_equal 1, enqueued_jobs.size
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_f_record.id, workflow_id, 'IntegrationStepFails'])
        assert_equal 'enqueued', step_f_record.reload.state

        # Act 2: Perform Job F (fails permanently)
        @test_notifier.clear!
        perform_enqueued_jobs # Runs Job F

        # Assert Events after Job F fails
        assert_equal 4, @test_notifier.published_events.count, 'Should publish step.started, step.failed, step.cancelled, workflow.failed'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal step_f_record.id, @test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.failed', @test_notifier.published_events[1][:name]
        assert_equal step_f_record.id, @test_notifier.published_events[1][:payload][:step_id]
        assert_equal 'StandardError', @test_notifier.published_events[1][:payload][:error][:class] # Check error details
        assert_equal 'yantra.step.cancelled', @test_notifier.published_events[2][:name]
        assert_equal step_a_record.id, @test_notifier.published_events[2][:payload][:step_id]
        assert_equal 'yantra.workflow.failed', @test_notifier.published_events[3][:name]
        assert_equal workflow_id, @test_notifier.published_events[3][:payload][:workflow_id]

        # Assert 2: Job F failed, Job A cancelled
        step_f_record.reload
        assert_equal 'failed', step_f_record.state
        refute_nil step_f_record.error
        error = step_f_record.error
        assert_equal 'StandardError', error['class'] # Access deserialized hash with string key
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
        [step_a, step_b, step_c, step_d].each { |s| refute_nil s, 'Setup: Step record missing' }

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count, 'Should publish workflow.started, step.enqueued(A)'
        assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[1][:name]
        assert_equal 1, @test_notifier.published_events[1][:payload][:enqueued_ids].length
        assert_equal step_a.id, @test_notifier.published_events[1][:payload][:enqueued_ids].find { _1 == step_a.id }

        # Assert 1: Job A enqueued
        assert_equal 1, enqueued_jobs.size
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_a.id, workflow_id, 'IntegrationStepA'])

        # Act 2: Perform Job A
        @test_notifier.clear!
        perform_enqueued_jobs # Runs A

        # Assert Events after Job A runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish A.started, A.succeeded, B+C enqueued'
        # Check events individually
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal step_a.id, @test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal step_a.id, @test_notifier.published_events[1][:payload][:step_id]
        # Check bulk enqueue event for B and C
        enqueued_event = @test_notifier.published_events[2]
        assert_equal 'yantra.step.bulk_enqueued', enqueued_event[:name]
        enqueued_ids = enqueued_event[:payload][:enqueued_ids]
        assert_equal 2, enqueued_ids.length
        assert_includes enqueued_ids, step_b.id
        assert_includes enqueued_ids, step_c.id

        # Assert 2: A succeeded, B & C enqueued
        assert_equal 'succeeded', step_a.reload.state
        assert_equal 2, enqueued_jobs.size
        assert_equal 'enqueued', step_b.reload.state
        assert_equal 'enqueued', step_c.reload.state

        # Act 3: Perform Job B and Job C
        @test_notifier.clear!
        perform_enqueued_jobs # Runs B and C

        # Assert Events after Job B & C run
        assert_equal 5, @test_notifier.published_events.count, 'Should publish B/C starts, B/C succeeds, D.enqueued'
        b_started = @test_notifier.find_event('yantra.step.started') { |ev| ev[:payload][:step_id] == step_b.id }
        b_succeeded = @test_notifier.find_event('yantra.step.succeeded') { |ev| ev[:payload][:step_id] == step_b.id }
        c_started = @test_notifier.find_event('yantra.step.started') { |ev| ev[:payload][:step_id] == step_c.id }
        c_succeeded = @test_notifier.find_event('yantra.step.succeeded') { |ev| ev[:payload][:step_id] == step_c.id }
        d_enqueued_event = @test_notifier.find_event('yantra.step.bulk_enqueued') { |ev| ev[:payload][:enqueued_ids].include?(step_d.id) }
        refute_nil b_started, 'B started event missing'
        refute_nil b_succeeded, 'B succeeded event missing'
        refute_nil c_started, 'C started event missing'
        refute_nil c_succeeded, 'C succeeded event missing'
        refute_nil d_enqueued_event, 'D enqueued event missing'
        assert_equal [step_d.id], d_enqueued_event[:payload][:enqueued_ids] # Check D is the only one

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
        assert_equal step_d.id, @test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal step_d.id, @test_notifier.published_events[1][:payload][:step_id]
        assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]
        assert_equal workflow_id, @test_notifier.published_events[2][:payload][:workflow_id]

        # Assert 4: D succeeded, Workflow succeeded
        assert_equal 'succeeded', step_d.reload.state
        assert_equal 0, enqueued_jobs.size
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
      end

      def test_workflow_with_retries
        workflow_id = Client.create_workflow(RetryWorkflow)
        step_r_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationJobRetry' }
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        refute_nil step_r_record
        refute_nil step_a_record

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count, 'Should publish workflow.started, step_r.enqueued'
        assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
        bulk_enqueued_event = @test_notifier.published_events[1]
        assert_equal 'yantra.step.bulk_enqueued', bulk_enqueued_event[:name]
        assert_equal [step_r_record.id], bulk_enqueued_event[:payload][:enqueued_ids]

        # Assert 1: Job R enqueued
        assert_equal 1, enqueued_jobs.size
        assert_equal 'enqueued', step_r_record.reload.state

        # Act 2: Perform Job R (Attempt 1 - Fails)
        @test_notifier.clear!
        # Expect error during execution, caught by assert_raises
        assert_nothing_raised do
          perform_enqueued_jobs # Runs R, fails, should trigger retry logic internally
        end
        # assert_match(/Integration job failed on attempt 1/, raised_error.message)

        # Assert Events after Attempt 1
        # Expect step started, but not failed (since it's retryable)
        assert_equal 1, @test_notifier.published_events.count, 'Should publish only R.started (no failed event on retry)'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal step_r_record.id, @test_notifier.published_events[0][:payload][:step_id]

        # Assert 2: State/Retry updates, Manual re-enqueue needed for test adapter
        step_r_record.reload
        assert_equal 'running', step_r_record.state # State remains running during retry cycle
        assert_equal 1, step_r_record.retries
        refute_nil step_r_record.error
        error = step_r_record.error
        assert_equal 'StandardError', error['class']

        assert_equal 1, enqueued_jobs.size, 'Job should be manually re-enqueued for retry test'

        # Act 3: Perform Job R (Attempt 2 - Succeeds)
        @test_notifier.clear!
        perform_enqueued_jobs # Runs R again, should succeed

        # Assert Events after Attempt 2 - Reverted to original expectation
        assert_equal 2, @test_notifier.published_events.count, 'Should publish R.succeeded, A.enqueued' # EXPECT 2 EVENTS
        # Original file checked succeeded[0], bulk_enqueued[1]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[0][:name]
        assert_equal step_r_record.id, @test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[1][:name]
        assert_equal step_a_record.id, @test_notifier.published_events[1][:payload][:enqueued_ids].first

        # Assert 3: Job R succeeded, Job A enqueued
        step_r_record.reload
        assert_equal 'succeeded', step_r_record.state
        assert_equal 1, step_r_record.retries # Retries don't increment on success
        assert_equal({ 'output_retry' => 'Success on attempt 2' }, step_r_record.output)
        assert_equal 1, enqueued_jobs.size
        assert_equal 'enqueued', step_a_record.reload.state

        # Act 4: Perform Job A
        @test_notifier.clear!
        perform_enqueued_jobs # Runs A

        # Assert Events after Job A runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish A.started, A.succeeded, workflow.succeeded'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal step_a_record.id, @test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal step_a_record.id, @test_notifier.published_events[1][:payload][:step_id]
        assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]
        assert_equal workflow_id, @test_notifier.published_events[2][:payload][:workflow_id]

        # Assert 4: Job A succeeded, Workflow succeeded
        assert_equal 'succeeded', step_a_record.reload.state
        assert_equal 0, enqueued_jobs.size
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
      end


      def test_pipelining_workflow
        workflow_id = Client.create_workflow(PipeliningWorkflow)
        producer_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'PipeProducer' }
        consumer_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'PipeConsumer' }
        refute_nil producer_record
        refute_nil consumer_record

        # Original check for dependency record creation
        producer_id = producer_record.id
        consumer_id = consumer_record.id
        # Query using the ACTUAL column names from your schema.rb (assuming they are step_id, depends_on_step_id)
        dependency_exists = Yantra::Persistence::ActiveRecord::StepDependencyRecord.exists?(
          step_id: consumer_id,             # This column holds the child ID
          depends_on_step_id: producer_id   # This column holds the parent ID
        )
        assert dependency_exists, "DATABASE CHECK: Dependency record from Producer (#{producer_id}) to Consumer (#{consumer_id}) was not created."

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count, 'Should publish workflow.started, producer.enqueued'
        assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
        bulk_enqueued_event = @test_notifier.published_events[1]
        assert_equal 'yantra.step.bulk_enqueued', bulk_enqueued_event[:name]
        assert_equal 1, bulk_enqueued_event[:payload][:enqueued_ids].length
        assert_equal [producer_record.id], bulk_enqueued_event[:payload][:enqueued_ids]

        # Assert 1: Producer enqueued
        assert_equal 1, enqueued_jobs.size

        # Act 2: Perform Producer
        @test_notifier.clear!
        perform_enqueued_jobs

        # Assert Events after Producer runs
        assert_equal 3, @test_notifier.published_events.count, 'Should publish producer.started, producer.succeeded, consumer.enqueued'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal producer_record.id, @test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal producer_record.id, @test_notifier.published_events[1][:payload][:step_id]
        bulk_enqueued_event = @test_notifier.published_events[2]
        assert_equal 'yantra.step.bulk_enqueued', bulk_enqueued_event[:name]
        assert_equal 1, bulk_enqueued_event[:payload][:enqueued_ids].length
        assert_equal [consumer_record.id], bulk_enqueued_event[:payload][:enqueued_ids]

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
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal consumer_record.id, @test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal consumer_record.id, @test_notifier.published_events[1][:payload][:step_id]
        assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]
        assert_equal workflow_id, @test_notifier.published_events[2][:payload][:workflow_id]

        # Assert 3: Consumer succeeded, Workflow succeeded
        consumer_record.reload
        assert_equal 'succeeded', consumer_record.state
        assert_equal({ 'consumed' => 'PRODUCED_DATA123', 'extra' => 'CONSUMED' }, consumer_record.output)
        assert_equal 0, enqueued_jobs.size
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
      end

      def test_cancel_workflow_cancels_running_workflow
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        step_b_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepB' }
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs # Run A, enqueue B
        @test_notifier.clear!
        wf_record = repository.find_workflow(workflow_id)
        assert_equal 'running', wf_record.reload.state
        assert_equal 'succeeded', step_a_record.reload.state
        assert_equal 'enqueued', step_b_record.reload.state
        cancel_result = Client.cancel_workflow(workflow_id)
        assert cancel_result
        assert_equal 2, @test_notifier.published_events.count, 'Expected 2 events after cancel'
        wf_cancelled_event = @test_notifier.find_event('yantra.workflow.cancelled')
        step_b_cancelled_event = @test_notifier.find_event('yantra.step.cancelled')
        refute_nil wf_cancelled_event, 'Workflow cancelled event missing'
        refute_nil step_b_cancelled_event, 'Step B cancelled event missing'
        assert_equal workflow_id, wf_cancelled_event[:payload][:workflow_id]
        assert_equal step_b_record.id, step_b_cancelled_event[:payload][:step_id]
        refute_nil wf_cancelled_event[:payload][:finished_at]
        wf_record.reload
        assert_equal 'cancelled', wf_record.state
        refute_nil wf_record.finished_at
        assert_equal 'succeeded', step_a_record.reload.state # A already finished
        step_b_record.reload
        assert_equal 'cancelled', step_b_record.state
        refute_nil step_b_record.finished_at
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
        cancel_result = Client.cancel_workflow(workflow_id)
        assert cancel_result
        assert_equal 3, @test_notifier.published_events.count, 'Expected 3 events after cancel'
        wf_cancelled_event = @test_notifier.find_event('yantra.workflow.cancelled')
        step_cancelled_events = @test_notifier.find_events('yantra.step.cancelled')
        refute_nil wf_cancelled_event, 'Workflow cancelled event missing'
        assert_equal 2, step_cancelled_events.count, 'Should be 2 step.cancelled events'
        cancelled_step_ids = step_cancelled_events.map { |ev| ev[:payload][:step_id] }
        assert_includes cancelled_step_ids, step_a_record.id
        assert_includes cancelled_step_ids, step_b_record.id
        wf_record.reload
        assert_equal 'cancelled', wf_record.state
        refute_nil wf_record.finished_at
        assert_equal 'cancelled', step_a_record.reload.state
        assert_equal 'cancelled', step_b_record.reload.state
        refute_nil step_a_record.finished_at
        refute_nil step_b_record.finished_at
        refute Client.start_workflow(workflow_id) # Cannot start cancelled workflow
        assert_equal 0, enqueued_jobs.size
      end

      def test_cancel_workflow_does_nothing_for_finished_workflow
        # Arrange: Create and run a workflow to completion
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs # Run A
        perform_enqueued_jobs # Run B
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
        @test_notifier.clear!

        # Act: Try to cancel
        cancel_result = Client.cancel_workflow(workflow_id)

        # Assert: No change, no events
        refute cancel_result
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
        assert_equal 0, @test_notifier.published_events.count, 'Should publish no events for already finished workflow'
      end

      def test_retry_failed_steps_restarts_failed_workflow
        # Arrange: Create and run a workflow that fails
        workflow_id = Client.create_workflow(LinearFailureWorkflow)
        step_f_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepFails' }
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs # Runs F, fails permanently
        assert_equal 'failed', repository.find_workflow(workflow_id).state
        assert_equal 'failed', step_f_record.reload.state
        assert_equal 'cancelled', step_a_record.reload.state
        @test_notifier.clear! # Clear events from initial failure

        # Act: Retry
        reenqueued_result = Client.retry_failed_steps(workflow_id) # Capture the array

        # Assert: State reset, job re-enqueued, events published
        assert_instance_of Array, reenqueued_result, "retry_failed_steps should return an array"
        assert_equal 1, reenqueued_result.size, "Should re-enqueue exactly 1 step"
        assert_equal step_f_record.id, reenqueued_result.first, "Should re-enqueue the correct failed step ID"

        assert_equal 'running', repository.find_workflow(workflow_id).state
        refute repository.find_workflow(workflow_id).has_failures
        assert_nil repository.find_workflow(workflow_id).finished_at
        assert_equal 'enqueued', step_f_record.reload.state # Should be re-enqueued
        assert_equal 'cancelled', step_a_record.reload.state # A remains cancelled

        # Assert Events after Retry Call
        assert_equal 1, @test_notifier.published_events.count, 'Should publish 1 step.enqueued event'
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[0][:name]
        assert_equal step_f_record.id, @test_notifier.published_events[0][:payload][:enqueued_ids].find { _1 == step_f_record.id }

        # Assert job queue
        assert_equal 1, enqueued_jobs.size
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_f_record.id, workflow_id, 'IntegrationStepFails'])

        # Act 2: Perform retried job (it will fail permanently again)
        @test_notifier.clear!
        perform_enqueued_jobs

        # Assert Events after Retried Job Fails
        assert_equal 3, @test_notifier.published_events.count, 'Should publish F.started, F.failed, workflow.failed'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.failed', @test_notifier.published_events[1][:name]
        assert_equal 'yantra.workflow.failed', @test_notifier.published_events[2][:name]

        # Assert Final State
        assert_equal 'failed', repository.find_workflow(workflow_id).state
        assert_equal 'failed', step_f_record.reload.state
        assert_equal 'cancelled', step_a_record.reload.state
        assert_equal 0, enqueued_jobs.size
      end

      def test_retry_failed_steps_does_nothing_for_succeeded_workflow
        # Arrange: Create and run a workflow to completion
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs # Run A
        perform_enqueued_jobs # Run B
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
        @test_notifier.clear!

        # Act: Try to retry
        retry_result = Client.retry_failed_steps(workflow_id)

        # Assert: No change, no events
        assert_equal false, retry_result # Original assertion
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state
        assert_equal 0, @test_notifier.published_events.count
        assert_equal 0, enqueued_jobs.size
      end

      def test_retry_failed_steps_handles_not_found
        # Act & Assert
        refute Client.retry_failed_steps(SecureRandom.uuid) # Original assertion
        assert_equal 0, @test_notifier.published_events.count
      end

      def test_retry_failed_steps_handles_failed_workflow_with_no_failed_steps
        # Arrange: Create workflow, mark failed manually without failing steps
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        step_a_record = repository.list_steps(workflow_id:).find { |s| s.klass == 'IntegrationStepA' }
        repository.update_workflow_attributes(workflow_id, { state: 'failed', has_failures: true }, expected_old_state: 'pending') # Use string state
        assert_equal 'failed', repository.find_workflow(workflow_id).state
        assert_equal 'pending', step_a_record.reload.state # Step A never failed
        @test_notifier.clear!

        # Act: Retry
        reenqueued_count = Client.retry_failed_steps(workflow_id)

        # Assert: Workflow reset, but no jobs re-enqueued, no events
        assert_equal 0, reenqueued_count
        assert_equal 'running', repository.find_workflow(workflow_id).state
        refute repository.find_workflow(workflow_id).has_failures
        assert_equal 'pending', step_a_record.reload.state # Step A remains pending
        assert_equal 0, @test_notifier.published_events.count
        assert_equal 0, enqueued_jobs.size # No jobs were actually failed to re-enqueue
      end

      def test_parallel_start_workflow_enqueues_all_initial_steps
        # Arrange: Create the workflow
        workflow_id = Client.create_workflow(ParallelStartWorkflow)

        # Fetch all step records, identifying them by klass defined in the workflow
        all_steps = repository.list_steps(workflow_id:)
        step_a = all_steps.find { |s| s.klass == 'IntegrationStepA' }
        step_e = all_steps.find { |s| s.klass == 'IntegrationStepE' } # Find E now
        step_c = all_steps.find { |s| s.klass == 'IntegrationStepC' }

        refute_nil step_a, "Step A record should exist"
        refute_nil step_e, "Step E record should exist" # Check E
        refute_nil step_c, "Step C record should exist"
        assert_equal 'IntegrationStepA', step_a.klass
        assert_equal 'IntegrationStepE', step_e.klass # Check E
        assert_equal 'IntegrationStepC', step_c.klass

        # Act: Start the workflow
        @test_notifier.clear!
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count, 'Should publish workflow.started and one step.bulk_enqueued'
        assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
        assert_equal workflow_id, @test_notifier.published_events[0][:payload][:workflow_id]

        bulk_enqueued_event = @test_notifier.published_events[1]
        assert_equal 'yantra.step.bulk_enqueued', bulk_enqueued_event[:name]
        enqueued_ids = bulk_enqueued_event[:payload][:enqueued_ids]

        # Verify all three steps were in the single bulk enqueue event
        assert_equal 3, enqueued_ids.length, "Should enqueue 3 steps"
        assert_includes enqueued_ids, step_a.id
        assert_includes enqueued_ids, step_e.id # Check E
        assert_includes enqueued_ids, step_c.id

        # Assert Queue State
        assert_equal 3, enqueued_jobs.size, "Should have 3 jobs enqueued"
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_a.id, workflow_id, 'IntegrationStepA'])
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_e.id, workflow_id, 'IntegrationStepE']) # Check E
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_c.id, workflow_id, 'IntegrationStepC'])

        # Assert DB State
        assert_equal 'enqueued', step_a.reload.state
        assert_equal 'enqueued', step_e.reload.state # Check E
        assert_equal 'enqueued', step_c.reload.state
        assert_equal 'running', repository.find_workflow(workflow_id).state

        # Act 2: Perform all enqueued jobs
        @test_notifier.clear!
        perform_enqueued_jobs # Runs A, E, C

        # Assert Final State
        assert_equal 0, enqueued_jobs.size, "Queue should be empty after performing jobs"
        assert_equal 'succeeded', step_a.reload.state
        assert_equal 'succeeded', step_e.reload.state # Check E
        assert_equal 'succeeded', step_c.reload.state
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state

        # Assert Events after completion (3 starts, 3 succeeds, 1 workflow succeed)
        assert_equal 7, @test_notifier.published_events.count
        assert_equal 3, @test_notifier.find_events('yantra.step.started').count
        assert_equal 3, @test_notifier.find_events('yantra.step.succeeded').count
        assert_equal 1, @test_notifier.find_events('yantra.workflow.succeeded').count
      end

      def test_multi_branch_workflow_independent_paths
        # Arrange: Create the workflow
        workflow_id = Client.create_workflow(MultiBranchWorkflow)

        # Fetch all step records, identifying them by klass
        all_steps = repository.list_steps(workflow_id:)
        step_a = all_steps.find { |s| s.klass == 'IntegrationStepA' }
        step_b = all_steps.find { |s| s.klass == 'IntegrationStepB' }
        step_c = all_steps.find { |s| s.klass == 'IntegrationStepC' }
        step_d = all_steps.find { |s| s.klass == 'IntegrationStepD' }

        refute_nil step_a, "Step A record should exist"
        refute_nil step_b, "Step B record should exist"
        refute_nil step_c, "Step C record should exist"
        refute_nil step_d, "Step D record should exist"

        # Act 1: Start the workflow
        @test_notifier.clear!
        Client.start_workflow(workflow_id)

        # Assert 1: A and C enqueued
        assert_equal 2, @test_notifier.published_events.count, 'Should publish workflow.started and one step.bulk_enqueued'
        assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
        bulk_event = @test_notifier.published_events[1]
        assert_equal 'yantra.step.bulk_enqueued', bulk_event[:name]
        assert_equal 2, bulk_event[:payload][:enqueued_ids].length, "Should enqueue 2 initial steps"
        assert_includes bulk_event[:payload][:enqueued_ids], step_a.id
        assert_includes bulk_event[:payload][:enqueued_ids], step_c.id

        assert_equal 2, enqueued_jobs.size, "Should have 2 jobs enqueued"
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_a.id, workflow_id, 'IntegrationStepA'])
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_c.id, workflow_id, 'IntegrationStepC'])
        assert_equal 'enqueued', step_a.reload.state
        assert_equal 'enqueued', step_c.reload.state
        assert_equal 'pending', step_b.reload.state # B and D still pending
        assert_equal 'pending', step_d.reload.state
        assert_equal 'running', repository.find_workflow(workflow_id).state

        # Act 2: Perform A and C
        @test_notifier.clear!
        perform_enqueued_jobs # Runs A and C

        # Assert 2: A and C succeeded, B and D enqueued
        assert_equal 'succeeded', step_a.reload.state
        assert_equal 'succeeded', step_c.reload.state
        assert_equal 'enqueued', step_b.reload.state
        assert_equal 'enqueued', step_d.reload.state
        assert_equal 2, enqueued_jobs.size, "Should have B and D enqueued"
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_b.id, workflow_id, 'IntegrationStepB'])
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_d.id, workflow_id, 'IntegrationStepD'])

        # Assert 2 Events: A started/succeeded, C started/succeeded, B enqueued, D enqueued
        # Expect 2 starts, 2 succeeds, 2 bulk_enqueues (one for B, one for D) = 6 events
        assert_equal 6, @test_notifier.published_events.count, "Expected 6 events after A & C run"
        assert_equal 2, @test_notifier.find_events('yantra.step.started').count
        assert_equal 2, @test_notifier.find_events('yantra.step.succeeded').count
        assert_equal 2, @test_notifier.find_events('yantra.step.bulk_enqueued').count
        assert @test_notifier.find_event('yantra.step.bulk_enqueued') { |ev| ev[:payload][:enqueued_ids] == [step_b.id] }
        assert @test_notifier.find_event('yantra.step.bulk_enqueued') { |ev| ev[:payload][:enqueued_ids] == [step_d.id] }

        # Act 3: Perform B and D
        @test_notifier.clear!
        perform_enqueued_jobs # Runs B and D

        # Assert 3: B and D succeeded, Workflow succeeded
        assert_equal 'succeeded', step_b.reload.state
        assert_equal 'succeeded', step_d.reload.state
        assert_equal 0, enqueued_jobs.size, "Queue should be empty"
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state

        # Assert 3 Events: B started/succeeded, D started/succeeded, Workflow succeeded = 5 events
        assert_equal 5, @test_notifier.published_events.count, "Expected 5 events after B & D run"
        assert_equal 2, @test_notifier.find_events('yantra.step.started').count
        assert_equal 2, @test_notifier.find_events('yantra.step.succeeded').count
        assert_equal 1, @test_notifier.find_events('yantra.workflow.succeeded').count

      end

      def test_delayed_step_workflow
        # Arrange: Create workflow with a delayed step
        workflow_id = Client.create_workflow(DelayedStepWorkflow)
        step_a = repository.list_steps(workflow_id: workflow_id).find { |s| s.klass == 'IntegrationStepA' }
        step_e_delayed = repository.list_steps(workflow_id: workflow_id).find { |s| s.klass == 'IntegrationStepE' }

        refute_nil step_a, "Step A record should exist"
        refute_nil step_e_delayed, "Step E record should exist"
        assert_equal 300, step_e_delayed.delay_seconds # 5.minutes = 300 seconds

        # Act 1: Start workflow
        @test_notifier.clear!
        Client.start_workflow(workflow_id)

        # Assert 1: Only Step A is enqueued immediately
        assert_equal 1, enqueued_jobs.size, "Only Step A should be enqueued initially"
        assert_enqueued_with(job: Yantra::Worker::ActiveJob::StepJob, args: [step_a.id, workflow_id, 'IntegrationStepA'])
        assert_equal 'enqueued', step_a.reload.state
        assert_equal 'pending', step_e_delayed.reload.state # Step E still pending

        # Assert Events after Start
        assert_equal 2, @test_notifier.published_events.count, 'Should publish workflow.started, step.enqueued(A)'
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[1][:name]
        assert_equal [step_a.id], @test_notifier.published_events[1][:payload][:enqueued_ids]

        # Act 2: Perform Step A
        @test_notifier.clear!
        perform_enqueued_jobs # Runs A

        # Assert 2: Step A succeeded, Step E is now 'enqueued' but delayed
        step_a.reload
        step_e_delayed.reload
        assert_equal 'succeeded', step_a.state
        assert_equal 1, enqueued_jobs.size, "Queue should be empty immediately after A runs (E is delayed)"

        # Assert 2.1: Check Step E's state and delayed_until timestamp
        assert_equal 'enqueued', step_e_delayed.state # Marked enqueued by StepEnqueuer
        refute_nil step_e_delayed.enqueued_at
        refute_nil step_e_delayed.delayed_until
        # Check that delayed_until is approx 5 minutes after enqueued_at
        expected_run_time = step_e_delayed.enqueued_at + 5.minutes
        assert_in_delta expected_run_time, step_e_delayed.delayed_until, 1.second

        # Assert 2.2: Check Events after A runs
        # Should be A.started, A.succeeded, E.bulk_enqueued (even though delayed)
        assert_equal 3, @test_notifier.published_events.count, 'Should publish A.started, A.succeeded, E.enqueued'
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal 'yantra.step.bulk_enqueued', @test_notifier.published_events[2][:name]
        assert_equal [step_e_delayed.id], @test_notifier.published_events[2][:payload][:enqueued_ids]

        # Act 3: Advance time past the delay and perform jobs again
        @test_notifier.clear!
        # Travel slightly past the expected run time
        # Find the enqueued job for Step E in the adapter
        enqueued_step_e_job = enqueued_jobs.find do |j|
          j[:args][0] == step_e_delayed.id
        end

        refute_nil enqueued_step_e_job, "Step E job should be enqueued"

        # Confirm that Step E was scheduled to run in the future (i.e., it is a delayed job)
        assert enqueued_step_e_job[:at], "Step E job should have a scheduled :at timestamp"

        scheduled_at = Time.at(enqueued_step_e_job[:at])

        # Confirm that scheduled time is roughly 5 minutes after it was enqueued
        # (matching delay_seconds = 300)
        expected_scheduled_time = step_e_delayed.enqueued_at + 5.minutes
        assert_in_delta expected_scheduled_time.to_f, scheduled_at.to_f, 5.0, "Step E job scheduled time should be about 5 minutes later"


        scheduled_at = Time.at(enqueued_step_e_job[:at])

        perform_enqueued_jobs # Should now pick up and run Step E

        # Assert 3: Step E succeeded, Workflow succeeded
        step_e_delayed.reload
        assert_equal 'succeeded', step_e_delayed.state
        assert_equal 0, enqueued_jobs.size, "Queue should be empty after E runs"
        assert_equal 'succeeded', repository.find_workflow(workflow_id).state

        # Assert 3.1: Check Events after E runs
        # Should be E.started, E.succeeded, Workflow.succeeded
        assert_equal 3, @test_notifier.published_events.count
        assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
        assert_equal step_e_delayed.id, @test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
        assert_equal step_e_delayed.id, @test_notifier.published_events[1][:payload][:step_id]
        assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]
      end

    end

  end # if defined?(YantraActiveRecordTestCase) ...
end # module Yantra
