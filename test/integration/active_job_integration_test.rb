# --- test/integration/active_step_integration_test.rb ---

require "test_helper"

# Conditionally load components needed for integration tests
if AR_LOADED # Assumes test_helper defines AR_LOADED based on ActiveRecord/SQLite3/DB Cleaner availability
  require "yantra/client"
  require "yantra/workflow"
  require "yantra/step"
  require "yantra/persistence/active_record/workflow_record"
  require "yantra/persistence/active_record/step_record"
  require "yantra/persistence/active_record/step_dependency_record"
  require "yantra/worker/active_job/step_job"
  require "yantra/worker/active_job/adapter"
  require 'active_job/test_helper'
  require 'json' # Ensure JSON is available for parsing/checking arguments if needed
end

require_relative '../support/test_notifier_adapter'

# --- Dummy Classes for Integration Tests ---
# (These remain the same as the previous version)
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
  # Note: This assumes input_b and input_c are hashes passed directly as arguments
  def perform(input_b:, input_c:); puts "INTEGRATION_TEST: Job D running"; { output_d: "#{input_b[:output_b]}-#{input_c[:output_c]}" }; end
end
class IntegrationStepFails < Yantra::Step
  def self.yantra_max_attempts; 1; end # Force immediate permanent failure
  def perform(msg: "F"); puts "INTEGRATION_TEST: Job Fails running - WILL FAIL"; raise StandardError, "Integration job failed!"; end
end
class IntegrationJobRetry < Yantra::Step
  @@retry_test_attempts = Hash.new(0)
  def self.reset_attempts!; @@retry_test_attempts = Hash.new(0); end
  def self.yantra_max_attempts; 2; end # Allow one retry

  def perform(msg: "Retry")
    attempt_key = self.id
    @@retry_test_attempts[attempt_key] = @@retry_test_attempts[attempt_key].to_i + 1
    current_attempt = @@retry_test_attempts[attempt_key]
    puts "INTEGRATION_TEST: Job Retry running (Attempt #{current_attempt}) for job #{self.id}"
    if current_attempt < 2
      raise StandardError, "Integration job failed on attempt #{current_attempt}!"
    else
      puts "INTEGRATION_TEST: Job Retry succeeding on attempt #{current_attempt}"
      { output_retry: "Success on attempt #{current_attempt}" }
    end
  end
end
class PipeProducer < Yantra::Step
  def perform(value:)
    puts "INTEGRATION_TEST: PipeProducer running"
    { produced_data: "PRODUCED_#{value.upcase}" }
  end
end
class PipeConsumer < Yantra::Step
  def perform() # Takes no arguments itself
    puts "INTEGRATION_TEST: PipeConsumer running"
    parent_data = parent_outputs # Hash like { "producer_id" => {"produced_data"=>"..."} }
    producer_output_hash = parent_data.values.first
    unless producer_output_hash && producer_output_hash['produced_data']
      raise "Consumer failed: Did not receive expected data key from producer. Got: #{parent_data.inspect}"
    end
    consumed_data = producer_output_hash['produced_data']
    { consumed: consumed_data, extra: "CONSUMED" }
  end
end

# --- Dummy Workflow Classes ---
# (These remain the same as the previous version)
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
          config.default_step_options[:retries] = 3
          config.notification_adapter = TestNotifierAdapter # Use the class constant
        end
        # Reset memoized adapters AFTER configuration
        Yantra.instance_variable_set(:@repository, nil)
        Yantra.instance_variable_set(:@worker_adapter, nil)
        Yantra.instance_variable_set(:@notifier, nil) # <<< ADD THIS LINE

        # ... rest of setup ...
        ActiveJob::Base.queue_adapter = :test
        clear_enqueued_jobs
        ActiveJob::Base.queue_adapter.perform_enqueued_jobs = false
        ActiveJob::Base.queue_adapter.perform_enqueued_at_jobs = false

        IntegrationJobRetry.reset_attempts!
        # This clear! will now correctly instantiate and memoize an *instance*
        Yantra.notifier.clear! if Yantra.notifier.respond_to?(:clear!)
      end

      def teardown
        clear_enqueued_jobs
        Yantra::Configuration.reset! if defined?(Yantra::Configuration) && Yantra::Configuration.respond_to?(:reset!)
        super
      end

      # --- Helper to access repository ---
      def repository
        Yantra.repository
      end

      # Helper to access the test notifier instance
      def test_notifier
         notifier = Yantra.notifier
         # Ensure the correct adapter is loaded, helps catch setup issues
         assert_instance_of TestNotifierAdapter, notifier, "TestNotifierAdapter not configured correctly"
         notifier
      end

      # --- Test Methods (Copied from previous version for completeness) ---
      def test_linear_workflow_success_end_to_end
        # Arrange
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        step_a_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepA" }
        step_b_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepB" }
        refute_nil step_a_record, "Setup: Step A record missing"
        refute_nil step_b_record, "Setup: Step B record missing"

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, test_notifier.published_events.count, "Expected 2 events after start"
        wf_started_event = test_notifier.find_event('yantra.workflow.started')
        step_a_enqueued_event = test_notifier.find_event('yantra.step.enqueued')
        refute_nil wf_started_event, "Workflow started event missing"
        refute_nil step_a_enqueued_event, "Step A enqueued event missing"
        assert_equal workflow_id, wf_started_event[:payload][:workflow_id]
        assert_equal step_a_record.id, step_a_enqueued_event[:payload][:step_id]

        # Assert 1: Job A enqueued (Keep existing assertions)
        assert_equal 1, enqueued_jobs.size
        assert_equal "enqueued", step_a_record.reload.state

        # Act 2: Perform Job A
        test_notifier.clear! # Clear events before next action
        perform_enqueued_jobs # Runs Job A

        # Assert Events after Job A runs
        assert_equal 3, test_notifier.published_events.count, "Expected 3 events after Step A runs"
        assert_equal 'yantra.step.started', test_notifier.published_events[0][:name]
        assert_equal step_a_record.id, test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.succeeded', test_notifier.published_events[1][:name]
        assert_equal step_a_record.id, test_notifier.published_events[1][:payload][:step_id]
        assert_equal 'yantra.step.enqueued', test_notifier.published_events[2][:name]
        assert_equal step_b_record.id, test_notifier.published_events[2][:payload][:step_id]

        # Assert 2: Job A succeeded, Job B enqueued (Keep existing assertions)
        step_a_record.reload
        assert_equal "succeeded", step_a_record.state
        assert_equal({ "output_a" => "HELLO" }, step_a_record.output)
        assert_equal 1, enqueued_jobs.size
        assert_equal "enqueued", step_b_record.reload.state

        # Act 3: Perform Job B
        test_notifier.clear! # Clear events
        perform_enqueued_jobs # Runs Job B

        # Assert Events after Job B runs
        assert_equal 3, test_notifier.published_events.count, "Expected 3 events after Step B runs"
        assert_equal 'yantra.step.started', test_notifier.published_events[0][:name]
        assert_equal step_b_record.id, test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.succeeded', test_notifier.published_events[1][:name]
        assert_equal step_b_record.id, test_notifier.published_events[1][:payload][:step_id]
        assert_equal 'yantra.workflow.succeeded', test_notifier.published_events[2][:name]
        assert_equal workflow_id, test_notifier.published_events[2][:payload][:workflow_id]

        # Assert 3 & 4: Job B succeeded, Workflow succeeded (Keep existing assertions)
        step_b_record.reload
        assert_equal "succeeded", step_b_record.state
        assert_equal({ "output_b" => "A_OUT_WORLD" }, step_b_record.output)
        assert_equal 0, enqueued_jobs.size
        wf_record = repository.find_workflow(workflow_id)
        assert_equal "succeeded", wf_record.state
        refute wf_record.has_failures
        refute_nil wf_record.finished_at
      end

      # --- UPDATED: Added Event Assertions ---
      def test_linear_workflow_failure_end_to_end
        # Arrange
        workflow_id = Client.create_workflow(LinearFailureWorkflow)
        step_f_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepFails" }
        step_a_record = repository.get_workflow_steps(workflow_id).find { |s| s.klass == "IntegrationStepA" }
        refute_nil step_f_record, "Setup: Step F record missing"
        refute_nil step_a_record, "Setup: Step A record missing"

        # Act 1: Start
        Client.start_workflow(workflow_id)

        # Assert Events after Start
        assert_equal 2, test_notifier.published_events.count
        assert_equal 'yantra.workflow.started', test_notifier.published_events[0][:name]
        assert_equal 'yantra.step.enqueued', test_notifier.published_events[1][:name]
        assert_equal step_f_record.id, test_notifier.published_events[1][:payload][:step_id]

        # Assert 1: Job F enqueued
        assert_equal 1, enqueued_jobs.size
        assert_equal "enqueued", step_f_record.reload.state

        # Act 2: Perform Job F (fails permanently as max_attempts = 1)
        test_notifier.clear!
        perform_enqueued_jobs # Runs Job F

        # Assert Events after Job F fails
        # Expect: step.started, step.failed, step.cancelled (for step A), workflow.failed
        assert_equal 4, test_notifier.published_events.count, "Expected 4 events after Step F fails"
        assert_equal 'yantra.step.started', test_notifier.published_events[0][:name]
        assert_equal step_f_record.id, test_notifier.published_events[0][:payload][:step_id]
        assert_equal 'yantra.step.failed', test_notifier.published_events[1][:name]
        assert_equal step_f_record.id, test_notifier.published_events[1][:payload][:step_id]
        assert_equal 'StandardError', test_notifier.published_events[1][:payload][:error][:class]
        assert_equal 'yantra.step.cancelled', test_notifier.published_events[2][:name]
        assert_equal step_a_record.id, test_notifier.published_events[2][:payload][:step_id]
        assert_equal 'yantra.workflow.failed', test_notifier.published_events[3][:name]
        assert_equal workflow_id, test_notifier.published_events[3][:payload][:workflow_id]

        # Assert 2: Job F failed, Job A cancelled (Keep existing assertions)
        step_f_record.reload
        assert_equal "failed", step_f_record.state
        refute_nil step_f_record.error
        error = JSON.parse(step_f_record.error)
        assert_equal "StandardError", error['class']
        assert_match(/Integration job failed!/, error["message"])
        step_a_record.reload
        assert_equal "cancelled", step_a_record.state
        refute_nil step_a_record.finished_at

        # Assert 3: Workflow failed (Keep existing assertions)
        assert_equal 0, enqueued_jobs.size
        wf_record = repository.find_workflow(workflow_id)
        assert_equal "failed", wf_record.state
        assert wf_record.has_failures
        refute_nil wf_record.finished_at
      end

      def test_workflow_with_retries
         # Arrange
         IntegrationJobRetry.reset_attempts!
         workflow_id = Client.create_workflow(RetryWorkflow)
         step_r_klass_name = "IntegrationJobRetry"

         # Act 1: Start
         Client.start_workflow(workflow_id) # Enqueues Job R

         # Assert 1: Job R enqueued
         assert_equal 1, enqueued_jobs.size
         job_info = enqueued_jobs.first
         step_r_id = job_info[:args][0]
         step_r_record = repository.find_step(step_r_id)
         assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_r_id, workflow_id, step_r_klass_name])
         assert_equal "enqueued", step_r_record.reload.state

         # Act 2: Manually perform the first (failing) attempt directly via StepJob instance
         clear_enqueued_jobs # Clear queue before manual run
         step_job_instance = Worker::ActiveJob::StepJob.new
         step_job_instance.executions = 1 # Manually set attempt number for RetryHandler

         assert_raises(StandardError, /Integration job failed on attempt 1/) do
           step_job_instance.perform(step_r_id, workflow_id, step_r_klass_name)
         end

         # Assert 2: Verify state after the first failed attempt
         step_r_record.reload
         assert_equal "running", step_r_record.state, "State should be running after first failed attempt (pending retry)"
         assert_equal 1, step_r_record.retries, "Retries count should be 1 after first failure"
         assert_equal 0, enqueued_jobs.size # Queue should be empty as we ran manually

         # Act 3: Manually enqueue the job for the second attempt (simulating backend retry)
         puts "INFO: [TEST] Manually enqueuing job #{step_r_id} for retry attempt."
         Worker::ActiveJob::StepJob.set(queue: step_r_record.queue || 'default').perform_later(step_r_id, workflow_id, step_r_klass_name)
         assert_equal 1, enqueued_jobs.size, "Job should be manually re-enqueued"

         # Act 4: Perform Job R again (Attempt 2 - Succeeds) via test helper
         perform_enqueued_jobs # Run the manually re-enqueued job

         # Assert 4: Job R succeeded, Job A enqueued
         step_r_record.reload
         assert_equal "succeeded", step_r_record.state, "State should be succeeded after retry"
         # Use string keys for comparison
         assert_equal({"output_retry"=>"Success on attempt 2"}, step_r_record.output)
         assert_equal 1, step_r_record.retries

         # Assert 4b: Job A should now be enqueued
         assert_equal 1, enqueued_jobs.size, "Job A should be enqueued after Job R succeeded"
         step_a_record = Persistence::ActiveRecord::StepRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationStepA")
         assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_a_record.id, workflow_id, "IntegrationStepA"])

         # Act 5: Perform Job A
         perform_enqueued_jobs

         # Assert 5: Workflow succeeds
         assert_equal 0, enqueued_jobs.size
         wf_record = repository.find_workflow(workflow_id)
         assert_equal "succeeded", wf_record.state, "Retry workflow state should be 'succeeded'"
         refute wf_record.has_failures
         refute_nil wf_record.finished_at
      end

      def test_pipelining_workflow
        # Arrange: Producer -> Consumer (uses parent_outputs)
        workflow_id = Client.create_workflow(PipeliningWorkflow)

        # Act 1: Start Workflow
        Client.start_workflow(workflow_id)

        # Assert 1: Producer enqueued
        assert_equal 1, enqueued_jobs.size
        producer_job_info = enqueued_jobs.first
        producer_step_id = producer_job_info[:args][0]
        producer_record = repository.find_step(producer_step_id)
        assert_equal "PipeProducer", producer_record.klass
        assert_equal "enqueued", producer_record.reload.state
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [producer_step_id, workflow_id, "PipeProducer"])

        # Act 2: Run Producer
        perform_enqueued_jobs

        # Assert 2: Producer succeeded, Consumer enqueued
        producer_record.reload
        assert_equal "succeeded", producer_record.state
        # Use string keys for comparison
        expected_producer_output = { "produced_data" => "PRODUCED_DATA123" }
        assert_equal expected_producer_output, producer_record.output

        assert_equal 1, enqueued_jobs.size
        consumer_job_info = enqueued_jobs.first
        consumer_step_id = consumer_job_info[:args][0]
        consumer_record = repository.find_step(consumer_step_id)
        assert_equal "PipeConsumer", consumer_record.klass
        assert_equal "enqueued", consumer_record.reload.state
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [consumer_step_id, workflow_id, "PipeConsumer"])

        # Act 3: Run Consumer
        perform_enqueued_jobs

        # Assert 3: Consumer succeeded using Producer's output
        consumer_record.reload
        assert_equal "succeeded", consumer_record.state
        # Use string keys for comparison
        expected_consumer_output = { "consumed" => "PRODUCED_DATA123", "extra" => "CONSUMED" }
        assert_equal expected_consumer_output, consumer_record.output
        assert_equal 0, enqueued_jobs.size # Queue empty

        # Assert 4: Workflow succeeded
        wf_record = repository.find_workflow(workflow_id)
        assert_equal "succeeded", wf_record.state
        refute wf_record.has_failures
      end

      # --- Cancel Tests ---
      def test_cancel_workflow_cancels_running_workflow
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        Client.start_workflow(workflow_id) # -> wf.started, step_a.enqueued
        perform_enqueued_jobs          # -> step_a.started, step_a.succeeded, step_b.enqueued
        wf_record = repository.find_workflow(workflow_id)
        step_a_record = Persistence::ActiveRecord::StepRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationStepA")
        step_b_record = Persistence::ActiveRecord::StepRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationStepB")
        assert_equal "running", wf_record.reload.state
        assert_equal "succeeded", step_a_record.reload.state
        assert_equal "enqueued", step_b_record.reload.state

        test_notifier.clear! # Clear events before cancel action

        # Act: Cancel
        cancel_result = Client.cancel_workflow(workflow_id)

        # Assert: Cancellation result
        assert cancel_result

        # Assert Events after Cancel
        # Expect: workflow.cancelled, step.cancelled (for step B)
        assert_equal 2, test_notifier.published_events.count, "Expected 2 events after cancel"
        wf_cancelled_event = test_notifier.find_event('yantra.workflow.cancelled')
        step_b_cancelled_event = test_notifier.find_event('yantra.step.cancelled')
        refute_nil wf_cancelled_event, "Workflow cancelled event missing"
        refute_nil step_b_cancelled_event, "Step B cancelled event missing"
        assert_equal workflow_id, wf_cancelled_event[:payload][:workflow_id]
        assert_equal step_b_record.id, step_b_cancelled_event[:payload][:step_id]

        # Assert: Final states (Keep existing assertions)
        wf_record.reload
        assert_equal "cancelled", wf_record.state
        refute_nil wf_record.finished_at
        assert_equal "succeeded", step_a_record.reload.state # A was already done
        step_b_record.reload
        assert_equal "cancelled", step_b_record.state
        refute_nil step_b_record.finished_at
      end


      def test_cancel_workflow_cancels_pending_workflow
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        wf_record = repository.find_workflow(workflow_id)
        step_a_record = Persistence::ActiveRecord::StepRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationStepA")
        step_b_record = Persistence::ActiveRecord::StepRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationStepB")
        assert_equal "pending", wf_record.reload.state
        assert_equal "pending", step_a_record.reload.state
        assert_equal "pending", step_b_record.reload.state
        cancel_result = Client.cancel_workflow(workflow_id)
        assert cancel_result
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
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs
        perform_enqueued_jobs
        wf_record = repository.find_workflow(workflow_id)
        step_a_record = Persistence::ActiveRecord::StepRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationStepA")
        step_b_record = Persistence::ActiveRecord::StepRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationStepB")
        assert_equal "succeeded", wf_record.reload.state
        cancel_result = Client.cancel_workflow(workflow_id)
        refute cancel_result
        wf_record.reload
        assert_equal "succeeded", wf_record.state
        assert_equal "succeeded", step_a_record.reload.state
        assert_equal "succeeded", step_b_record.reload.state
      end


      # --- Retry Failed Steps Tests ---
      def test_retry_failed_steps_restarts_failed_workflow
        workflow_id = Client.create_workflow(LinearFailureWorkflow)
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs
        wf_record = repository.find_workflow(workflow_id)
        step_f_record = Persistence::ActiveRecord::StepRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationStepFails")
        step_a_record = Persistence::ActiveRecord::StepRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationStepA")
        assert_equal "failed", wf_record.reload.state
        assert_equal "failed", step_f_record.reload.state
        assert_equal "cancelled", step_a_record.reload.state
        retry_result = Client.retry_failed_steps(workflow_id)
        assert_equal 1, retry_result
        wf_record.reload
        assert_equal "running", wf_record.state
        refute wf_record.has_failures
        assert_nil wf_record.finished_at
        step_f_record.reload
        assert_equal "enqueued", step_f_record.state
        assert_nil step_f_record.finished_at
        assert_equal "cancelled", step_a_record.reload.state
        assert_equal 1, enqueued_jobs.size
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_f_record.id, workflow_id, "IntegrationStepFails"])
        perform_enqueued_jobs
        step_f_record.reload
        assert_equal "failed", step_f_record.state
        assert_equal "cancelled", step_a_record.reload.state
        wf_record.reload
        assert_equal "failed", wf_record.state
        assert wf_record.has_failures
        refute_nil wf_record.finished_at
      end

       def test_retry_failed_steps_does_nothing_for_succeeded_workflow
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs
        perform_enqueued_jobs
        wf_record = repository.find_workflow(workflow_id)
        assert_equal "succeeded", wf_record.reload.state
        retry_result = Client.retry_failed_steps(workflow_id)
        assert_equal false, retry_result
        assert_equal "succeeded", wf_record.reload.state
      end

      def test_retry_failed_steps_handles_not_found
        non_existent_id = SecureRandom.uuid
        retry_result = Client.retry_failed_steps(non_existent_id)
        assert_equal false, retry_result
      end

      def test_retry_failed_steps_handles_failed_workflow_with_no_failed_steps
         workflow_id = SecureRandom.uuid
         wf_record = Persistence::ActiveRecord::WorkflowRecord.create!(id: workflow_id, klass: "TestWf", state: "failed", has_failures: true, finished_at: Time.current)
         step_a_record = Persistence::ActiveRecord::StepRecord.create!(id: SecureRandom.uuid, workflow_id: workflow_id, klass: "TestStep", state: "succeeded", finished_at: Time.current)
         retry_result = Client.retry_failed_steps(workflow_id)
         assert_equal 0, retry_result
         wf_record.reload
         assert_equal "running", wf_record.state
         refute wf_record.has_failures
         assert_nil wf_record.finished_at
         assert_equal "succeeded", step_a_record.reload.state
      end

    end

  end # if defined?
end # module Yantra

