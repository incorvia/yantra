# test/integration/active_job_integration_test.rb
require "test_helper"

# Conditionally load components needed for integration tests
if AR_LOADED # Assumes test_helper defines AR_LOADED based on ActiveRecord/SQLite3/DB Cleaner availability
  require "yantra/client"
  require "yantra/workflow"
  require "yantra/job"
  require "yantra/persistence/active_record/workflow_record"
  require "yantra/persistence/active_record/job_record"
  require "yantra/persistence/active_record/job_dependency_record"
  # Ensure worker components are loaded if not already handled by require 'yantra'
  require "yantra/worker/active_job/async_job"
  require "yantra/worker/active_job/adapter"

  # Require ActiveJob test helpers
  require 'active_job/test_helper'
end

# --- Dummy Classes for Integration Tests ---
class IntegrationJobA < Yantra::Job
  def perform(msg: "A")
    puts "INTEGRATION_TEST: IntegrationJobA performing with msg: #{msg}"
    { output_a: msg.upcase }
  end
end

class IntegrationJobB < Yantra::Job
  def perform(input_data:, msg: "B")
    puts "INTEGRATION_TEST: IntegrationJobB performing with msg: #{msg}, input: #{input_data.inspect}"
    output_value_a = input_data[:a_out] # Use symbol key now
    { output_b: "#{output_value_a}_#{msg.upcase}" }
  end
end

class IntegrationJobFails < Yantra::Job
   # Optional: Define max attempts for this job class
   # def self.yantra_max_attempts; 1; end # Force immediate failure for test

   def perform(msg: "F")
      puts "INTEGRATION_TEST: IntegrationJobFails performing with msg: #{msg} - WILL FAIL"
      raise StandardError, "Integration job failed!"
   end
end


class LinearSuccessWorkflow < Yantra::Workflow
  def perform
    job_a_ref = run IntegrationJobA, name: :job_a, params: { msg: "Hello" }
    run IntegrationJobB, name: :job_b, params: { input_data: { a_out: "A_OUT" }, msg: "World" }, after: job_a_ref
  end
end

class LinearFailureWorkflow < Yantra::Workflow
   def perform
      job_f_ref = run IntegrationJobFails, name: :job_f, params: { msg: "Fail Me" }
      run IntegrationJobA, name: :job_a, params: { msg: "Never runs" }, after: job_f_ref # This should be cancelled
   end
end


module Yantra
  # Run tests only if components loaded
  if defined?(YantraActiveRecordTestCase) && AR_LOADED && defined?(Yantra::Client) && defined?(ActiveJob::TestHelper)

    class ActiveJobWorkflowExecutionTest < YantraActiveRecordTestCase
      # Include ActiveJob helpers to assert/perform jobs
      include ActiveJob::TestHelper

      def setup
        super # DB Cleaning
        # Ensure correct adapters are configured for these tests
        Yantra.configure do |config|
          config.persistence_adapter = :active_record
          config.worker_adapter = :active_job
          # Set default attempts low for failure tests, or override on Job class
          config.default_max_job_attempts = 1
        end
        Yantra.instance_variable_set(:@repository, nil) # Reset memoization
        Yantra.instance_variable_set(:@worker_adapter, nil) # Reset memoization

        # Set the ActiveJob queue adapter to :test
        ActiveJob::Base.queue_adapter = :test
        clear_enqueued_jobs
        ActiveJob::Base.queue_adapter.perform_enqueued_jobs = false
        ActiveJob::Base.queue_adapter.perform_enqueued_at_jobs = false
      end

      def teardown
         clear_enqueued_jobs
         # Reset config? Optional.
         # Yantra.configure do |config|
         #   config.default_max_job_attempts = 3
         # end
         super
      end

      # --- Test Cases ---

      def test_linear_workflow_success_end_to_end
         # Arrange
         workflow_id = Client.create_workflow(LinearSuccessWorkflow)
         assert_equal 0, enqueued_jobs.size

         # Act 1: Start
         Client.start_workflow(workflow_id)

         # Assert 1: Job A enqueued
         assert_equal 1, enqueued_jobs.size
         job_a_record = Persistence::ActiveRecord::JobRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationJobA")
         assert_enqueued_with(job: Worker::ActiveJob::AsyncJob, args: [job_a_record.id, workflow_id, "IntegrationJobA"])
         assert_equal "enqueued", job_a_record.reload.state

         # Act 2: Perform Job A
         perform_enqueued_jobs

         # Assert 2: Job A succeeded, Job B enqueued
         job_a_record.reload
         assert_equal "succeeded", job_a_record.state
         assert_equal({ "output_a" => "HELLO" }, job_a_record.output)
         assert_equal 1, enqueued_jobs.size
         job_b_record = Persistence::ActiveRecord::JobRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationJobB")
         assert_enqueued_with(job: Worker::ActiveJob::AsyncJob, args: [job_b_record.id, workflow_id, "IntegrationJobB"])
         assert_equal "enqueued", job_b_record.reload.state

         # Act 3: Perform Job B
         perform_enqueued_jobs

         # Assert 3: Job B succeeded, Workflow succeeded
         job_b_record.reload
         assert_equal "succeeded", job_b_record.state
         assert_equal({ "output_b" => "A_OUT_WORLD" }, job_b_record.output)
         assert_equal 0, enqueued_jobs.size
         wf_record = Persistence::ActiveRecord::WorkflowRecord.find(workflow_id)
         assert_equal "succeeded", wf_record.state
         refute wf_record.has_failures
         refute_nil wf_record.finished_at
      end

      # --- NEW TEST ---
      def test_linear_workflow_failure_end_to_end
         # Arrange: Create the workflow with the job that fails
         workflow_id = Client.create_workflow(LinearFailureWorkflow)
         assert_equal 0, enqueued_jobs.size

         # Act 1: Start the workflow
         Client.start_workflow(workflow_id)

         # Assert 1: Check initial job (JobF) is enqueued
         assert_equal 1, enqueued_jobs.size
         job_f_record = Persistence::ActiveRecord::JobRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationJobFails")
         assert_enqueued_with(job: Worker::ActiveJob::AsyncJob, args: [job_f_record.id, workflow_id, "IntegrationJobFails"])
         assert_equal "enqueued", job_f_record.reload.state

         # Act 2: Perform Job F (which will fail)
         # Because default_max_job_attempts is 1 (set in setup), it should fail permanently
         perform_enqueued_jobs # Runs Job F, it fails, AsyncJob calls Orchestrator->RetryHandler->fail_permanently->job_finished

         # Assert 2: Check Job F status (failed) and Job A status (cancelled)
         job_f_record.reload
         assert_equal "failed", job_f_record.state, "Job F state should be failed"
         refute_nil job_f_record.started_at, "Job F started_at should be set"
         refute_nil job_f_record.finished_at, "Job F finished_at should be set"
         refute_nil job_f_record.error, "Job F error should be recorded"
         assert_match /Integration job failed!/, job_f_record.error["message"] # Check error message

         job_a_record = Persistence::ActiveRecord::JobRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationJobA")
         # job_finished(job_f) should trigger cancel_downstream_jobs(job_f)
         # which calls cancel_jobs_bulk([job_a])
         assert_equal "cancelled", job_a_record.reload.state, "Job A state should be cancelled"
         refute_nil job_a_record.finished_at, "Job A finished_at should be set (cancellation)"

         # Assert 3: Check Workflow completion (failed)
         assert_equal 0, enqueued_jobs.size, "No jobs should be enqueued after JobF fails"

         wf_record = Persistence::ActiveRecord::WorkflowRecord.find(workflow_id)
         assert_equal "failed", wf_record.state, "Workflow state should be failed"
         refute_nil wf_record.finished_at, "Workflow finished_at should be set"
         assert wf_record.has_failures, "Workflow has_failures should be true"
      end
      # --- END NEW TEST ---

      # TODO: Add tests for more complex graphs (parallel, fan-in/out)
      # TODO: Add tests for workflows with retries > 1 attempt

    end

  end # if defined?
end # module Yantra

