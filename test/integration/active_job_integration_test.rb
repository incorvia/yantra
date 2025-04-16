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

# --- Dummy Classes for Integration Tests ---
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

# --- NEW Steps for Pipelining Test ---
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

    # Find the producer's output hash (assuming one parent)
    producer_output_hash = parent_data.values.first

    # *** FIX HERE: Access using string key ***
    # Check if the hash exists and if the key (likely a string from JSON) exists
    unless producer_output_hash && producer_output_hash['produced_data']
      raise "Consumer failed: Did not receive expected data key from producer. Got: #{parent_data.inspect}"
    end

    # Use the parent's output accessed via string key
    consumed_data = producer_output_hash['produced_data']
    { consumed: consumed_data, extra: "CONSUMED" }
  end
end
# --- END NEW Steps ---


# --- Dummy Workflow Classes ---
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

# --- NEW Workflow for Pipelining Test ---
class PipeliningWorkflow < Yantra::Workflow
  def perform
    producer_ref = run PipeProducer, name: :producer, params: { value: "data123" }
    run PipeConsumer, name: :consumer, after: producer_ref
  end
end
# --- END NEW Workflow ---


module Yantra
  if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Client) && defined?(ActiveJob::TestHelper)

    class ActiveJobWorkflowExecutionTest < YantraActiveRecordTestCase
      include ActiveJob::TestHelper

      def setup
        # ... (setup remains the same) ...
        super
        Yantra.configure do |config|
          config.persistence_adapter = :active_record
          config.worker_adapter = :active_job
          config.default_max_step_attempts = 3
        end
        Yantra.instance_variable_set(:@repository, nil)
        Yantra.instance_variable_set(:@worker_adapter, nil)

        ActiveJob::Base.queue_adapter = :test
        clear_enqueued_jobs
        ActiveJob::Base.queue_adapter.perform_enqueued_jobs = false
        ActiveJob::Base.queue_adapter.perform_enqueued_at_jobs = false

        IntegrationJobRetry.reset_attempts!
      end

      def teardown
         # ... (teardown remains the same) ...
         clear_enqueued_jobs
         Yantra::Configuration.reset! if Yantra::Configuration.respond_to?(:reset!)
         super
      end

      # --- Helper to access repository ---
      def repository
        Yantra.repository
      end

      # --- Test Cases ---
      # ... (other tests remain the same) ...

      def test_linear_workflow_success_end_to_end
         # Arrange
         workflow_id = Client.create_workflow(LinearSuccessWorkflow)
         # Act 1: Start
         Client.start_workflow(workflow_id)
         # Assert 1: Job A enqueued
         assert_equal 1, enqueued_jobs.size
         step_a_record = repository.find_step(enqueued_jobs.first[:args][0])
         assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_a_record.id, workflow_id, "IntegrationStepA"])
         assert_equal "enqueued", step_a_record.reload.state
         # Act 2: Perform Job A
         perform_enqueued_jobs # Runs Job A
         # Assert 2: Job A succeeded, Job B enqueued
         step_a_record.reload
         assert_equal "succeeded", step_a_record.state
         # Use string keys for comparison as that's likely what AR returns from JSON
         assert_equal({ "output_a" => "HELLO" }, step_a_record.output)
         assert_equal 1, enqueued_jobs.size
         step_b_record = repository.find_step(enqueued_jobs.first[:args][0])
         assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_b_record.id, workflow_id, "IntegrationStepB"])
         assert_equal "enqueued", step_b_record.reload.state
         # Act 3: Perform Job B
         perform_enqueued_jobs # Runs Job B
         # Assert 3: Job B succeeded, Queue empty
         step_b_record.reload
         assert_equal "succeeded", step_b_record.state
         assert_equal({ "output_b" => "A_OUT_WORLD" }, step_b_record.output)
         assert_equal 0, enqueued_jobs.size
         # Assert 4: Workflow succeeded
         wf_record = repository.find_workflow(workflow_id)
         assert_equal "succeeded", wf_record.state, "Workflow state should be 'succeeded' after last job finishes"
         refute wf_record.has_failures
         refute_nil wf_record.finished_at
      end


      def test_linear_workflow_failure_end_to_end
         # Arrange
         workflow_id = Client.create_workflow(LinearFailureWorkflow)
         # Act 1: Start
         Client.start_workflow(workflow_id)
         # Assert 1: Job F enqueued
         assert_equal 1, enqueued_jobs.size
         step_f_record = repository.find_step(enqueued_jobs.first[:args][0])
         assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_f_record.id, workflow_id, "IntegrationStepFails"])
         assert_equal "enqueued", step_f_record.reload.state
         # Act 2: Perform Job F (fails permanently as max_attempts = 1)
         perform_enqueued_jobs # Runs Job F
         # Assert 2: Job F failed, Job A cancelled
         step_f_record.reload
         assert_equal "failed", step_f_record.state
         refute_nil step_f_record.error
         assert_equal "StandardError", step_f_record.error["class"]
         assert_match(/Integration job failed!/, step_f_record.error["message"])
         step_a_record = Persistence::ActiveRecord::StepRecord.find_by(workflow_id: workflow_id, klass: "IntegrationStepA")
         assert_equal "cancelled", step_a_record.reload.state
         refute_nil step_a_record.finished_at
         # Assert 3: Workflow failed
         assert_equal 0, enqueued_jobs.size
         wf_record = repository.find_workflow(workflow_id)
         assert_equal "failed", wf_record.state
         assert wf_record.has_failures
         refute_nil wf_record.finished_at
      end

      def test_complex_graph_success_end_to_end
        # Arrange: A -> (B, C) -> D
        workflow_id = Client.create_workflow(ComplexGraphWorkflow)
        # Act 1: Start
        Client.start_workflow(workflow_id)
        # Assert 1: Job A enqueued
        assert_equal 1, enqueued_jobs.size
        step_a_record = repository.find_step(enqueued_jobs.first[:args][0])
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_a_record.id, workflow_id, "IntegrationStepA"])
        assert_equal "enqueued", step_a_record.reload.state
        # Act 2: Perform Job A
        perform_enqueued_jobs # Runs Job A
        # Assert 2: Job A succeeded, Jobs B and C enqueued
        step_a_record.reload
        assert_equal "succeeded", step_a_record.state
        assert_equal 2, enqueued_jobs.size
        enqueued_step_ids = enqueued_jobs.map { |j| j[:args][0] }
        step_b_record = repository.find_step(enqueued_step_ids.find { |id| repository.find_step(id).klass == "IntegrationStepB" })
        step_c_record = repository.find_step(enqueued_step_ids.find { |id| repository.find_step(id).klass == "IntegrationStepC" })
        assert_equal "enqueued", step_b_record.reload.state
        assert_equal "enqueued", step_c_record.reload.state
        assert_enqueued_jobs 2
        # Act 3: Perform Jobs B and C
        perform_enqueued_jobs # Runs both B and C
        # Assert 3: Jobs B and C succeeded, Job D is enqueued
        step_b_record.reload
        step_c_record.reload
        assert_equal "succeeded", step_b_record.state
        assert_equal "succeeded", step_c_record.state
        assert_equal 1, enqueued_jobs.size
        step_d_record = repository.find_step(enqueued_jobs.first[:args][0])
        step_d_record.reload
        assert_enqueued_with(job: Worker::ActiveJob::StepJob, args: [step_d_record.id, workflow_id, "IntegrationStepD"])
        assert_equal "enqueued", step_d_record.state
        # Act 4: Perform Job D
        perform_enqueued_jobs # Runs D
        # Assert 4: Job D succeeded, Workflow succeeded
        step_d_record.reload
        assert_equal "succeeded", step_d_record.state
        # Use string keys for comparison
        assert_equal({ "output_d" => "B_OUT-c_out" }, step_d_record.output)
        assert_equal 0, enqueued_jobs.size
        wf_record = repository.find_workflow(workflow_id)
        assert_equal "succeeded", wf_record.state, "Complex workflow state should be 'succeeded'"
        refute wf_record.has_failures
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

      # --- NEW Pipelining Test ---
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
      # --- END NEW Pipelining Test ---


      # --- Cancel Tests ---
      # ... (cancel tests remain the same) ...
      def test_cancel_workflow_cancels_running_workflow
        workflow_id = Client.create_workflow(LinearSuccessWorkflow)
        Client.start_workflow(workflow_id)
        perform_enqueued_jobs
        wf_record = repository.find_workflow(workflow_id)
        step_a_record = Persistence::ActiveRecord::StepRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationStepA")
        step_b_record = Persistence::ActiveRecord::StepRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationStepB")
        assert_equal "running", wf_record.reload.state
        assert_equal "succeeded", step_a_record.reload.state
        assert_equal "enqueued", step_b_record.reload.state
        cancel_result = Client.cancel_workflow(workflow_id)
        assert cancel_result
        wf_record.reload
        assert_equal "cancelled", wf_record.state
        refute_nil wf_record.finished_at
        assert_equal "succeeded", step_a_record.reload.state
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
      # ... (retry tests remain the same) ...
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

