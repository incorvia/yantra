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
    { output_a: msg.upcase } # Use symbol key for output hash
  end
end

class IntegrationJobB < Yantra::Job
  def perform(input_data:, msg: "B")
    puts "INTEGRATION_TEST: IntegrationJobB performing with msg: #{msg}, input: #{input_data.inspect}"
    # Simulate work using input
    # Use symbol key :a_out now, as AsyncJob performs deep symbolization
    output_value_a = input_data[:a_out] # <<< CHANGED BACK TO SYMBOL KEY
    { output_b: "#{output_value_a}_#{msg.upcase}" }
  end
end

class IntegrationJobFails < Yantra::Job
   def perform(msg: "F")
      puts "INTEGRATION_TEST: IntegrationJobFails performing with msg: #{msg} - WILL FAIL"
      raise StandardError, "Integration job failed!"
   end
end


class LinearSuccessWorkflow < Yantra::Workflow
  def perform
    job_a_ref = run IntegrationJobA, name: :job_a, params: { msg: "Hello" }
    # For now, pass static data to Job B as output passing isn't implemented
    run IntegrationJobB, name: :job_b, params: { input_data: { a_out: "A_OUT" }, msg: "World" }, after: job_a_ref
  end
end

class LinearFailureWorkflow < Yantra::Workflow
   def perform
      job_f_ref = run IntegrationJobFails, name: :job_f # This job will fail
      run IntegrationJobA, name: :job_a, after: job_f_ref # This should be cancelled
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
        end
        Yantra.instance_variable_set(:@repository, nil) # Reset memoization
        Yantra.instance_variable_set(:@worker_adapter, nil) # Reset memoization

        # Set the ActiveJob queue adapter to :test
        ActiveJob::Base.queue_adapter = :test
        # Clear any enqueued jobs from previous tests
        clear_enqueued_jobs
        ActiveJob::Base.queue_adapter.perform_enqueued_jobs = false
        ActiveJob::Base.queue_adapter.perform_enqueued_at_jobs = false
      end

      def teardown
         clear_enqueued_jobs
         super
      end

      # --- Test Cases ---

      def test_linear_workflow_success_end_to_end
         # Arrange: Create the workflow
         workflow_id = Client.create_workflow(LinearSuccessWorkflow)
         assert_equal 0, enqueued_jobs.size, "No jobs should be enqueued initially"

         # Act 1: Start the workflow
         Client.start_workflow(workflow_id)

         # Assert 1: Check initial job (JobA) is enqueued
         assert_equal 1, enqueued_jobs.size, "One job (JobA) should be enqueued after start"
         job_a_record = Persistence::ActiveRecord::JobRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationJobA")
         assert_enqueued_with(job: Worker::ActiveJob::AsyncJob, args: [job_a_record.id, workflow_id, "IntegrationJobA"])
         assert_equal "enqueued", job_a_record.reload.state, "Job A DB state should be enqueued"

         # Act 2: Perform Job A using ActiveJob Test Helper
         perform_enqueued_jobs # Runs Job A synchronously

         # Assert 2: Check Job A status and Job B enqueueing
         job_a_record.reload
         assert_equal "succeeded", job_a_record.state, "Job A state should be succeeded"
         refute_nil job_a_record.started_at, "Job A started_at should be set"
         refute_nil job_a_record.finished_at, "Job A finished_at should be set"
         # Output hash keys should be symbols now if deep symbolize used, but JSON might still stringify
         # Let's check against string keys from DB for safety unless we confirm deep symbolization of output
         assert_equal({ "output_a" => "HELLO" }, job_a_record.output, "Job A output should be persisted")

         assert_equal 1, enqueued_jobs.size, "One job (JobB) should be enqueued after JobA finishes"
         job_b_record = Persistence::ActiveRecord::JobRecord.find_by!(workflow_id: workflow_id, klass: "IntegrationJobB")
         assert_enqueued_with(job: Worker::ActiveJob::AsyncJob, args: [job_b_record.id, workflow_id, "IntegrationJobB"])
         assert_equal "enqueued", job_b_record.reload.state, "Job B DB state should be enqueued"

         # Act 3: Perform Job B
         perform_enqueued_jobs # Runs Job B synchronously

         # Assert 3: Check Job B status and Workflow completion
         job_b_record.reload
         assert_equal "succeeded", job_b_record.state, "Job B state should be succeeded"
         refute_nil job_b_record.finished_at, "Job B finished_at should be set"
         # The output generated by Job B should now be correct
         assert_equal({ "output_b" => "A_OUT_WORLD" }, job_b_record.output, "Job B output should be persisted")

         assert_equal 0, enqueued_jobs.size, "No jobs should be enqueued after JobB finishes"

         wf_record = Persistence::ActiveRecord::WorkflowRecord.find(workflow_id)
         assert_equal "succeeded", wf_record.state, "Workflow state should be succeeded"
         refute_nil wf_record.finished_at, "Workflow finished_at should be set"
         refute wf_record.has_failures, "Workflow has_failures should be false"
      end

      # TODO: Add test_linear_workflow_failure (similar structure)
      # TODO: Add tests for more complex graphs (parallel, fan-in/out)
      # TODO: Add tests for workflows with retries (check retry counts, final state after retries)

    end

  end # if defined?
end # module Yantra

