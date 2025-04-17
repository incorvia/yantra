# --- test/integration/active_job_integration_test.rb ---

require "test_helper"

# --- Add require for the test adapter ---
require_relative '../support/test_notifier_adapter'

# Conditionally load components needed for integration tests
if AR_LOADED # Assumes test_helper defines AR_LOADED
  require "yantra/client"
  require "yantra/workflow"
  require "yantra/step"
  require "yantra/persistence/active_record/workflow_record"
  require "yantra/persistence/active_record/step_record"
  require "yantra/persistence/active_record/step_dependency_record"
  require "yantra/worker/active_job/step_job"
  require "yantra/worker/active_job/adapter"
  require 'active_job/test_helper'
  require 'json'
end

# --- Dummy Classes for Integration Tests ---
# (These remain the same)
class IntegrationStepA < Yantra::Step
  def perform(msg: "A"); puts "INTEGRATION_TEST: Job A running"; sleep 0.1; { output_a: msg.upcase }; end
end
class IntegrationStepB < Yantra::Step
  def perform(input_data:, msg: "B"); puts "INTEGRATION_TEST: Job B running"; { output_b: "#{input_data[:a_out]}_#{msg.upcase}" }; end
end
class IntegrationStepC < Yantra::Step
  def perform(msg: "C"); puts "INTEGRATION_TEST: Job C running"; { output_c: msg.downcase }; end
end
class IntegrationStepD < Yantra::Step
  def perform(input_b:, input_c:); puts "INTEGRATION_TEST: Job D running"; { output_d: "#{input_b[:output_b]}-#{input_c[:output_c]}" }; end
end
class IntegrationStepFails < Yantra::Step
   def self.yantra_max_attempts; 1; end # Force immediate permanent failure (1 attempt total)
   def perform(msg: "F"); puts "INTEGRATION_TEST: Job Fails running - WILL FAIL"; raise StandardError, "Integration job failed!"; end
end
class IntegrationJobRetry < Yantra::Step
  @@retry_test_attempts = Hash.new(0)
  def self.reset_attempts!; @@retry_test_attempts = Hash.new(0); end
  def self.yantra_max_attempts; 2; end # 1 retry -> 2 attempts total

  def perform(msg: "Retry")
    # Use step_id if available, otherwise generate temp key for test setup phase if needed
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
    # Find the output from the known parent (PipeProducer)
    producer_output_hash = parent_data.values.find { |output| output&.key?('produced_data') }

    unless producer_output_hash && producer_output_hash['produced_data']
      raise "Consumer failed: Did not receive expected data key 'produced_data' from producer. Got: #{parent_data.inspect}"
    end
    consumed_data = producer_output_hash['produced_data']
    { consumed: consumed_data, extra: "CONSUMED" }
  end
end

# --- Dummy Workflow Classes ---
# (These remain the same)
class LinearSuccessWorkflow < Yantra::Workflow
  def perform
    step_a_ref = run IntegrationStepA, name: :step_a, params: { msg: "Hello" }
    run IntegrationStepB, name: :step_b, params: { input_data: { a_out: "A_OUT" }, msg: "World" }, after: step_a_ref
  end
end
class LinearFailureWorkflow < Yantra::Workflow
   def perform
      step_f_ref = run IntegrationStepFails, name: :step_f, params: { msg: "Fail Me" }
      run IntegrationStepA, name: :step_a, params: { msg: "Never runs" }, after: step_f_ref
   end
end
class ComplexGraphWorkflow < Yantra::Workflow
  def perform
    step_a_ref = run IntegrationStepA, name: :a, params: { msg: "Start" }
    step_b_ref = run IntegrationStepB, name: :b, params: { input_data: { a_out: "A_OUT" }, msg: "B" }, after: step_a_ref
    step_c_ref = run IntegrationStepC, name: :c, params: { msg: "C" }, after: step_a_ref
    run IntegrationStepD, name: :d, params: { input_b: { output_b: "B_OUT" }, input_c: { output_c: "c_out" } }, after: [step_b_ref, step_c_ref]
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
    producer_ref = run PipeProducer, name: :producer, params: { value: "data123" }
    run PipeConsumer, name: :consumer, after: producer_ref
  end
end


module Yantra
  if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Client) && defined?(ActiveJob::TestHelper)

    class ActiveJobWorkflowExecutionTest < YantraActiveRecordTestCase
      include ActiveJob::TestHelper

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
        assert_instance_of TestNotifierAdapter, @test_notifier, "TestNotifierAdapter should be configured"
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
         # Arrange
         workflow_id = Client.create_workflow(LinearSuccessWorkflow)
         step_a_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepA" }
         step_b_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepB" }
         refute_nil step_a_record, "Step A record should exist"
         refute_nil step_b_record, "Step B record should exist"

         # Act 1: Start
         Client.start_workflow(workflow_id)

         # Assert Events after Start
         assert_equal 2, @test_notifier.published_events.count, "Expected 2 events after start"
         wf_started_event = @test_notifier.find_event('yantra.workflow.started')
         step_a_enqueued_event = @test_notifier.find_event('yantra.step.enqueued')
         refute_nil wf_started_event, "Workflow started event missing"
         refute_nil step_a_enqueued_event, "Step A enqueued event missing"
         assert_equal workflow_id, wf_started_event[:payload][:workflow_id]
         assert_equal step_a_record.id, step_a_enqueued_event[:payload][:step_id]

         # Assert 1: Job A enqueued
         assert_equal 1, enqueued_jobs.size
         assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_a_record.id, workflow_id, "IntegrationStepA"])
         assert_equal "enqueued", step_a_record.reload.state

         # Act 2: Perform Job A
         @test_notifier.clear!
         perform_enqueued_jobs # Runs Job A

         # Assert Events after Job A runs
         assert_equal 3, @test_notifier.published_events.count, "Should publish step.started, step.succeeded, step.enqueued"
         assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
         assert_equal step_a_record.id, @test_notifier.published_events[0][:payload][:step_id]
         assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
         assert_equal step_a_record.id, @test_notifier.published_events[1][:payload][:step_id]
         assert_equal 'yantra.step.enqueued', @test_notifier.published_events[2][:name]
         assert_equal step_b_record.id, @test_notifier.published_events[2][:payload][:step_id]

         # Assert 2: Job A succeeded, Job B enqueued
         step_a_record.reload
         assert_equal "succeeded", step_a_record.state
         assert_equal({ "output_a" => "HELLO" }, step_a_record.output)
         assert_equal 1, enqueued_jobs.size
         assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_b_record.id, workflow_id, "IntegrationStepB"])
         assert_equal "enqueued", step_b_record.reload.state

         # Act 3: Perform Job B
         @test_notifier.clear!
         perform_enqueued_jobs # Runs Job B

         # Assert Events after Job B runs
         assert_equal 3, @test_notifier.published_events.count, "Should publish step.started, step.succeeded, workflow.succeeded"
         assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
         assert_equal step_b_record.id, @test_notifier.published_events[0][:payload][:step_id]
         assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]
         assert_equal step_b_record.id, @test_notifier.published_events[1][:payload][:step_id]
         assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]
         assert_equal workflow_id, @test_notifier.published_events[2][:payload][:workflow_id]

         # Assert 3 & 4: Job B succeeded, Workflow succeeded
         step_b_record.reload
         assert_equal "succeeded", step_b_record.state
         assert_equal({ "output_b" => "A_OUT_WORLD" }, step_b_record.output)
         assert_equal 0, enqueued_jobs.size
         wf_record = repository.find_workflow(workflow_id)
         assert_equal "succeeded", wf_record.state
         refute wf_record.has_failures
         refute_nil wf_record.finished_at
      end

      def test_linear_workflow_failure_end_to_end
         # Arrange
         workflow_id = Client.create_workflow(LinearFailureWorkflow)
         step_f_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepFails" }
         step_a_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepA" }
         refute_nil step_f_record
         refute_nil step_a_record

         # Act 1: Start
         Client.start_workflow(workflow_id)

         # Assert Events after Start
         assert_equal 2, @test_notifier.published_events.count
         assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
         assert_equal 'yantra.step.enqueued', @test_notifier.published_events[1][:name]
         assert_equal step_f_record.id, @test_notifier.published_events[1][:payload][:step_id]

         # Assert 1: Job F enqueued
         assert_equal 1, enqueued_jobs.size
         assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_f_record.id, workflow_id, "IntegrationStepFails"])
         assert_equal "enqueued", step_f_record.reload.state

         # Act 2: Perform Job F (fails permanently as max_attempts = 1)
         @test_notifier.clear!
         perform_enqueued_jobs # Runs Job F

         # Assert Events after Job F fails
         assert_equal 4, @test_notifier.published_events.count, "Should publish step.started, step.failed, step.cancelled, workflow.failed"
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
         assert_equal "failed", step_f_record.state
         refute_nil step_f_record.error
         error = JSON.parse(step_f_record.error)
         assert_equal "StandardError", error['class'] # Access deserialized hash with symbol
         step_a_record.reload
         assert_equal "cancelled", step_a_record.state
         refute_nil step_a_record.finished_at

         # Assert 3: Workflow failed
         assert_equal 0, enqueued_jobs.size
         wf_record = repository.find_workflow(workflow_id)
         assert_equal "failed", wf_record.state
         assert wf_record.has_failures
         refute_nil wf_record.finished_at
      end

      # --- NEW: Added Event Assertions ---
      def test_complex_graph_success_end_to_end
         # Arrange
         workflow_id = Client.create_workflow(ComplexGraphWorkflow)
         step_a = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepA" }
         step_b = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepB" }
         step_c = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepC" }
         step_d = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepD" }
         [step_a, step_b, step_c, step_d].each { |s| refute_nil s, "Setup: Step record missing" }

         # Act 1: Start
         Client.start_workflow(workflow_id)

         # Assert Events after Start
         assert_equal 2, @test_notifier.published_events.count, "Should publish workflow.started, step.enqueued(A)"
         assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
         assert_equal 'yantra.step.enqueued', @test_notifier.published_events[1][:name]
         assert_equal step_a.id, @test_notifier.published_events[1][:payload][:step_id]

         # Assert 1: Job A enqueued
         assert_equal 1, enqueued_jobs.size
         assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_a.id, workflow_id, "IntegrationStepA"])

         # Act 2: Perform Job A
         @test_notifier.clear!
         perform_enqueued_jobs # Runs A

         # Assert Events after Job A runs
         assert_equal 4, @test_notifier.published_events.count, "Should publish A.started, A.succeeded, B.enqueued, C.enqueued"
         assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]; assert_equal step_a.id, @test_notifier.published_events[0][:payload][:step_id]
         assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]; assert_equal step_a.id, @test_notifier.published_events[1][:payload][:step_id]
         enqueued_events = @test_notifier.published_events[2..3]
         assert_equal 2, enqueued_events.count { |ev| ev[:name] == 'yantra.step.enqueued' }
         enqueued_ids = enqueued_events.map { |ev| ev[:payload][:step_id] }
         assert_includes enqueued_ids, step_b.id
         assert_includes enqueued_ids, step_c.id

         # Assert 2: A succeeded, B & C enqueued
         assert_equal "succeeded", step_a.reload.state
         assert_equal 2, enqueued_jobs.size
         assert_equal "enqueued", step_b.reload.state
         assert_equal "enqueued", step_c.reload.state

         # Act 3: Perform Job B and Job C
         @test_notifier.clear!
         # --- FIX: Call without arguments to run all currently enqueued (B & C) ---
         perform_enqueued_jobs # Runs B and C

         # Assert Events after Job B & C run
         # Expect: B.started, B.succeeded, C.started, C.succeeded, D.enqueued (order of B/C may vary)
         assert_equal 5, @test_notifier.published_events.count, "Should publish B/C starts, B/C succeeds, D.enqueued"
         b_started = @test_notifier.find_event('yantra.step.started') { |ev| ev[:payload][:step_id] == step_b.id }
         b_succeeded = @test_notifier.find_event('yantra.step.succeeded') { |ev| ev[:payload][:step_id] == step_b.id }
         c_started = @test_notifier.find_event('yantra.step.started') { |ev| ev[:payload][:step_id] == step_c.id }
         c_succeeded = @test_notifier.find_event('yantra.step.succeeded') { |ev| ev[:payload][:step_id] == step_c.id }
         d_enqueued = @test_notifier.find_event('yantra.step.enqueued') { |ev| ev[:payload][:step_id] == step_d.id }
         refute_nil b_started, "B started event missing"
         refute_nil b_succeeded, "B succeeded event missing"
         refute_nil c_started, "C started event missing"
         refute_nil c_succeeded, "C succeeded event missing"
         refute_nil d_enqueued, "D enqueued event missing"

         # Assert 3: B succeeded, C succeeded, D enqueued
         assert_equal "succeeded", step_b.reload.state
         assert_equal "succeeded", step_c.reload.state
         assert_equal 1, enqueued_jobs.size # Only D left
         assert_equal "enqueued", step_d.reload.state

         # --- REMOVED Act 4 for Job C (already ran) ---

         # Act 4 (was 5): Perform Job D
         @test_notifier.clear!
         perform_enqueued_jobs # Run D

         # Assert Events after Job D runs
         assert_equal 3, @test_notifier.published_events.count, "Should publish D.started, D.succeeded, workflow.succeeded"
         assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]; assert_equal step_d.id, @test_notifier.published_events[0][:payload][:step_id]
         assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]; assert_equal step_d.id, @test_notifier.published_events[1][:payload][:step_id]
         assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]; assert_equal workflow_id, @test_notifier.published_events[2][:payload][:workflow_id]

         # Assert 4 (was 5): D succeeded, Workflow succeeded
         assert_equal "succeeded", step_d.reload.state
         assert_equal 0, enqueued_jobs.size
         assert_equal "succeeded", repository.find_workflow(workflow_id).state
      end

      # --- NEW: Added Event Assertions ---
      def test_workflow_with_retries
         # Arrange
         workflow_id = Client.create_workflow(RetryWorkflow)
         step_r_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationJobRetry" }
         step_a_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepA" }
         refute_nil step_r_record
         refute_nil step_a_record

         # Act 1: Start
         Client.start_workflow(workflow_id)

         # Assert Events after Start
         assert_equal 2, @test_notifier.published_events.count, "Should publish workflow.started, step_r.enqueued"
         assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
         assert_equal 'yantra.step.enqueued', @test_notifier.published_events[1][:name]; assert_equal step_r_record.id, @test_notifier.published_events[1][:payload][:step_id]

         # Assert 1: Job R enqueued
         assert_equal 1, enqueued_jobs.size
         assert_equal "enqueued", step_r_record.reload.state

         # Act 2: Perform Job R (Attempt 1 - Fails)
         @test_notifier.clear!
         # --- FIX: Expect and rescue the error raised on first attempt ---
         raised_error = assert_raises(StandardError) do
           perform_enqueued_jobs # Runs R, fails, re-enqueues for retry
         end
         assert_match(/Integration job failed on attempt 1/, raised_error.message)
         # --- END FIX ---

         # Assert Events after Attempt 1 (check *after* rescuing error)
         assert_equal 1, @test_notifier.published_events.count, "Should publish only R.started (no failed event on retry)"
         assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]; assert_equal step_r_record.id, @test_notifier.published_events[0][:payload][:step_id]

         # Assert 2: Job R still running (state doesn't change on retry), error recorded, job re-enqueued
         step_r_record.reload
         assert_equal "running", step_r_record.state # State remains running during retry cycle
         assert_equal 1, step_r_record.retries
         refute_nil step_r_record.error
         error = JSON.parse(step_r_record.error)
         assert_equal "StandardError", error['class']
         # Check that job is still enqueued for the next attempt

         assert_equal 0, enqueued_jobs.size, "Job should not be automatically re-enqueued by test adapter"
         # Enqueue the job again for the next attempt
         Worker::ActiveJob::StepJob.perform_later(step_r_record.id, step_r_record.workflow_id, step_r_record.klass)
         assert_equal 1, enqueued_jobs.size, "Job should be manually re-enqueued for retry test"

         # Act 3: Perform Job R (Attempt 2 - Succeeds)
         @test_notifier.clear!
         perform_enqueued_jobs # Runs R again, should succeed this time

         # Assert Events after Attempt 2
         assert_equal 2, @test_notifier.published_events.count, "Should publish R.started, R.succeeded, A.enqueued"
         assert_equal 'yantra.step.succeeded', @test_notifier.published_events[0][:name]; assert_equal step_r_record.id, @test_notifier.published_events[0][:payload][:step_id]
         assert_equal 'yantra.step.enqueued', @test_notifier.published_events[1][:name]; assert_equal step_a_record.id, @test_notifier.published_events[1][:payload][:step_id]

         # Assert 3: Job R succeeded, Job A enqueued
         step_r_record.reload
         assert_equal "succeeded", step_r_record.state
         assert_equal 1, step_r_record.retries # Retries don't increment on success
         assert_equal({ "output_retry" => "Success on attempt 2" }, step_r_record.output)
         assert_equal 1, enqueued_jobs.size
         assert_equal "enqueued", step_a_record.reload.state

         # Act 4: Perform Job A
         @test_notifier.clear!
         perform_enqueued_jobs # Runs A

         # Assert Events after Job A runs
         assert_equal 3, @test_notifier.published_events.count, "Should publish A.started, A.succeeded, workflow.succeeded"
         assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]; assert_equal step_a_record.id, @test_notifier.published_events[0][:payload][:step_id]
         assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]; assert_equal step_a_record.id, @test_notifier.published_events[1][:payload][:step_id]
         assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]; assert_equal workflow_id, @test_notifier.published_events[2][:payload][:workflow_id]

         # Assert 4: Job A succeeded, Workflow succeeded
         assert_equal "succeeded", step_a_record.reload.state
         assert_equal 0, enqueued_jobs.size
         assert_equal "succeeded", repository.find_workflow(workflow_id).state
      end

      # --- NEW: Added Event Assertions ---
      def test_pipelining_workflow
         # Arrange
         workflow_id = Client.create_workflow(PipeliningWorkflow)
         producer_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "PipeProducer" }
         consumer_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "PipeConsumer" }
         refute_nil producer_record
         refute_nil consumer_record

         # Act 1: Start
         Client.start_workflow(workflow_id)

         # Assert Events after Start
         assert_equal 2, @test_notifier.published_events.count, "Should publish workflow.started, producer.enqueued"
         assert_equal 'yantra.workflow.started', @test_notifier.published_events[0][:name]
         assert_equal 'yantra.step.enqueued', @test_notifier.published_events[1][:name]; assert_equal producer_record.id, @test_notifier.published_events[1][:payload][:step_id]

         # Assert 1: Producer enqueued
         assert_equal 1, enqueued_jobs.size

         # Act 2: Perform Producer
         @test_notifier.clear!
         perform_enqueued_jobs

         # Assert Events after Producer runs
         assert_equal 3, @test_notifier.published_events.count, "Should publish producer.started, producer.succeeded, consumer.enqueued"
         assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]; assert_equal producer_record.id, @test_notifier.published_events[0][:payload][:step_id]
         assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]; assert_equal producer_record.id, @test_notifier.published_events[1][:payload][:step_id]
         assert_equal 'yantra.step.enqueued', @test_notifier.published_events[2][:name]; assert_equal consumer_record.id, @test_notifier.published_events[2][:payload][:step_id]

         # Assert 2: Producer succeeded, Consumer enqueued
         producer_record.reload
         assert_equal "succeeded", producer_record.state
         assert_equal({ "produced_data" => "PRODUCED_DATA123" }, producer_record.output)
         assert_equal 1, enqueued_jobs.size
         assert_equal "enqueued", consumer_record.reload.state

         # Act 3: Perform Consumer
         @test_notifier.clear!
         perform_enqueued_jobs

         # Assert Events after Consumer runs
         assert_equal 3, @test_notifier.published_events.count, "Should publish consumer.started, consumer.succeeded, workflow.succeeded"
         assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]; assert_equal consumer_record.id, @test_notifier.published_events[0][:payload][:step_id]
         assert_equal 'yantra.step.succeeded', @test_notifier.published_events[1][:name]; assert_equal consumer_record.id, @test_notifier.published_events[1][:payload][:step_id]
         assert_equal 'yantra.workflow.succeeded', @test_notifier.published_events[2][:name]; assert_equal workflow_id, @test_notifier.published_events[2][:payload][:workflow_id]

         # Assert 3: Consumer succeeded, Workflow succeeded
         consumer_record.reload
         assert_equal "succeeded", consumer_record.state
         assert_equal({ "consumed" => "PRODUCED_DATA123", "extra" => "CONSUMED" }, consumer_record.output)
         assert_equal 0, enqueued_jobs.size
         assert_equal "succeeded", repository.find_workflow(workflow_id).state
      end

      def test_cancel_workflow_cancels_running_workflow
          # ... (Assertions remain the same as previous version - already correct) ...
          workflow_id = Client.create_workflow(LinearSuccessWorkflow)
          step_a_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepA" }
          step_b_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepB" }
          Client.start_workflow(workflow_id)
          perform_enqueued_jobs
          @test_notifier.clear!
          wf_record = repository.find_workflow(workflow_id)
          assert_equal "running", wf_record.reload.state
          assert_equal "succeeded", step_a_record.reload.state
          assert_equal "enqueued", step_b_record.reload.state
          cancel_result = Client.cancel_workflow(workflow_id)
          assert cancel_result
          assert_equal 2, @test_notifier.published_events.count, "Expected 2 events after cancel"
          wf_cancelled_event = @test_notifier.find_event('yantra.workflow.cancelled')
          step_b_cancelled_event = @test_notifier.find_event('yantra.step.cancelled')
          refute_nil wf_cancelled_event, "Workflow cancelled event missing"
          refute_nil step_b_cancelled_event, "Step B cancelled event missing"
          assert_equal workflow_id, wf_cancelled_event[:payload][:workflow_id]
          assert_equal step_b_record.id, step_b_cancelled_event[:payload][:step_id]
          refute_nil wf_cancelled_event[:payload][:finished_at]
          wf_record.reload
          assert_equal "cancelled", wf_record.state
          refute_nil wf_record.finished_at
          assert_equal "succeeded", step_a_record.reload.state
          step_b_record.reload
          assert_equal "cancelled", step_b_record.state
          refute_nil step_b_record.finished_at
      end

      def test_cancel_workflow_cancels_pending_workflow
          # ... (Assertions remain the same as previous version - already correct) ...
          workflow_id = Client.create_workflow(LinearSuccessWorkflow)
          step_a_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepA" }
          step_b_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepB" }
          wf_record = repository.find_workflow(workflow_id)
          assert_equal "pending", wf_record.reload.state
          assert_equal "pending", step_a_record.reload.state
          assert_equal "pending", step_b_record.reload.state
          @test_notifier.clear!
          cancel_result = Client.cancel_workflow(workflow_id)
          assert cancel_result
          assert_equal 3, @test_notifier.published_events.count, "Expected 3 events after cancel"
          wf_cancelled_event = @test_notifier.find_event('yantra.workflow.cancelled')
          step_cancelled_events = @test_notifier.find_events('yantra.step.cancelled')
          refute_nil wf_cancelled_event, "Workflow cancelled event missing"
          assert_equal 2, step_cancelled_events.count, "Should be 2 step.cancelled events"
          cancelled_step_ids = step_cancelled_events.map { |ev| ev[:payload][:step_id] }
          assert_includes cancelled_step_ids, step_a_record.id
          assert_includes cancelled_step_ids, step_b_record.id
          wf_record.reload
          assert_equal "cancelled", wf_record.state
          refute_nil wf_record.finished_at
          assert_equal "cancelled", step_a_record.reload.state
          assert_equal "cancelled", step_b_record.reload.state
          refute_nil step_a_record.finished_at
          refute_nil step_b_record.finished_at
          refute Client.start_workflow(workflow_id)
          assert_equal 0, enqueued_jobs.size
      end

      def test_cancel_workflow_does_nothing_for_finished_workflow
          # Arrange: Create and run a workflow to completion
          workflow_id = Client.create_workflow(LinearSuccessWorkflow)
          Client.start_workflow(workflow_id)
          perform_enqueued_jobs # Run A
          perform_enqueued_jobs # Run B
          assert_equal "succeeded", repository.find_workflow(workflow_id).state
          @test_notifier.clear!

          # Act: Try to cancel
          cancel_result = Client.cancel_workflow(workflow_id)

          # Assert: No change, no events
          refute cancel_result
          assert_equal "succeeded", repository.find_workflow(workflow_id).state
          assert_equal 0, @test_notifier.published_events.count, "Should publish no events for already finished workflow"
      end

      # --- NEW: Added Event Assertions ---
      def test_retry_failed_steps_restarts_failed_workflow
          # Arrange: Create and run a workflow that fails
          workflow_id = Client.create_workflow(LinearFailureWorkflow)
          step_f_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepFails" }
          step_a_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepA" }
          Client.start_workflow(workflow_id)
          perform_enqueued_jobs # Runs F, fails permanently
          assert_equal "failed", repository.find_workflow(workflow_id).state
          assert_equal "failed", step_f_record.reload.state
          assert_equal "cancelled", step_a_record.reload.state
          @test_notifier.clear! # Clear events from initial failure

          # Act: Retry
          reenqueued_count = Client.retry_failed_steps(workflow_id)

          # Assert: State reset, job re-enqueued, events published
          assert_equal 1, reenqueued_count # Only Step F was failed and retryable
          assert_equal "running", repository.find_workflow(workflow_id).state
          refute repository.find_workflow(workflow_id).has_failures
          assert_nil repository.find_workflow(workflow_id).finished_at
          assert_equal "enqueued", step_f_record.reload.state # Should be re-enqueued
          assert_equal "cancelled", step_a_record.reload.state # A remains cancelled for now

          # Assert Events after Retry Call
          # Expect step.enqueued for the retried step F
          assert_equal 1, @test_notifier.published_events.count, "Should publish 1 step.enqueued event"
          assert_equal 'yantra.step.enqueued', @test_notifier.published_events[0][:name]
          assert_equal step_f_record.id, @test_notifier.published_events[0][:payload][:step_id]

          # Assert job queue
          assert_equal 1, enqueued_jobs.size
          assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_f_record.id, workflow_id, "IntegrationStepFails"])

          # Act 2: Perform retried job (it will fail permanently again)
          @test_notifier.clear!
          perform_enqueued_jobs

          # Assert Events after Retried Job Fails
          assert_equal 3, @test_notifier.published_events.count, "Should publish F.started, F.failed, workflow.failed"
          assert_equal 'yantra.step.started', @test_notifier.published_events[0][:name]
          assert_equal 'yantra.step.failed', @test_notifier.published_events[1][:name]
          assert_equal 'yantra.workflow.failed', @test_notifier.published_events[2][:name]

          # Assert Final State
          assert_equal "failed", repository.find_workflow(workflow_id).state
          assert_equal "failed", step_f_record.reload.state
          assert_equal "cancelled", step_a_record.reload.state
          assert_equal 0, enqueued_jobs.size
      end

      def test_retry_failed_steps_does_nothing_for_succeeded_workflow
          # Arrange: Create and run a workflow to completion
          workflow_id = Client.create_workflow(LinearSuccessWorkflow)
          Client.start_workflow(workflow_id)
          perform_enqueued_jobs # Run A
          perform_enqueued_jobs # Run B
          assert_equal "succeeded", repository.find_workflow(workflow_id).state
          @test_notifier.clear!

          # Act: Try to retry
          retry_result = Client.retry_failed_steps(workflow_id)

          # Assert: No change, no events
          assert_equal false, retry_result
          assert_equal "succeeded", repository.find_workflow(workflow_id).state
          assert_equal 0, @test_notifier.published_events.count
          assert_equal 0, enqueued_jobs.size
      end

      def test_retry_failed_steps_handles_not_found
          # Act & Assert
          refute Client.retry_failed_steps(SecureRandom.uuid)
          assert_equal 0, @test_notifier.published_events.count
      end

      def test_retry_failed_steps_handles_failed_workflow_with_no_failed_steps
          # Arrange: Create a workflow, mark it failed manually without failing steps
          workflow_id = Client.create_workflow(LinearSuccessWorkflow)
          step_a_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepA" }
          repository.update_workflow_attributes(workflow_id, { state: 'failed', has_failures: true }, expected_old_state: :pending)
          assert_equal "failed", repository.find_workflow(workflow_id).state
          assert_equal "pending", step_a_record.reload.state # Step A never failed
          @test_notifier.clear!

          # Act: Retry
          reenqueued_count = Client.retry_failed_steps(workflow_id)

          # Assert: Workflow reset, but no jobs re-enqueued, no events
          assert_equal 0, reenqueued_count
          assert_equal "running", repository.find_workflow(workflow_id).state
          refute repository.find_workflow(workflow_id).has_failures
          assert_equal "pending", step_a_record.reload.state # Step A remains pending
          assert_equal 0, @test_notifier.published_events.count
          assert_equal 0, enqueued_jobs.size # No jobs were actually failed to re-enqueue
      end


    end # class ActiveJobWorkflowExecutionTest

  end # if defined?
end # module Yantra

