# test/client_test.rb

require "test_helper"

# Explicitly require the files needed for these tests
if AR_LOADED # Only require if AR itself was loaded successfully
  require "yantra/client"
  require "yantra/workflow"
  require "yantra/job"
  require "yantra/persistence/active_record/workflow_record"
  require "yantra/persistence/active_record/job_record"
  require "yantra/persistence/active_record/job_dependency_record"
end

# --- Define Dummy Classes for Testing ---

# A simple job for testing workflow structure persistence
class ClientTestJob < Yantra::Job # Renamed to avoid potential collision
  def perform(data: nil)
    # Test jobs don't need complex logic, just need to exist
    # puts "MyTestJob performing with #{data}" # Keep puts commented unless debugging
  end
end

# A simple workflow definition specific to this test file
class ClientTestWorkflow < Yantra::Workflow # <<< RENAMED HERE
  # This perform method expects keyword arguments
  def perform(user_id:, report_type: 'default')
    # Use arguments passed during creation
    # puts "ClientTestWorkflow performing for user #{user_id} with report type #{report_type}"

    # Define some jobs and dependencies using the DSL
    # Use the job class defined above
    run ClientTestJob, name: :fetch_data, params: { data: user_id }
    run ClientTestJob, name: :process_data, params: { data: report_type }, after: :fetch_data
    run ClientTestJob, name: :final_step, after: :process_data
  end
end

# A class that doesn't inherit correctly, for testing validation
class NotAWorkflow
end

# --- Client Tests ---

module Yantra
  # Check if base class and models were loaded before defining tests
  if defined?(YantraActiveRecordTestCase) && AR_LOADED

    class ClientTest < YantraActiveRecordTestCase

      # Ensure the correct adapter is configured before each test in this file
      def setup
        super # Run base class setup (skip checks, DB cleaning)
        Yantra.configure { |c| c.persistence_adapter = :active_record }
        # Reset repository memoization to ensure the correct adapter is used
        Yantra.instance_variable_set(:@repository, nil)
      end

      # Test successful creation and persistence of all components
      def test_create_workflow_persists_workflow_jobs_and_dependencies
        # Arrange
        user_id_arg = 123
        report_type_kwarg = 'summary'
        globals_hash = { tenant_id: 'abc' }

        # Act: Call the client method directly, using the renamed workflow class
        # Pass arguments intended for perform as keyword arguments
        workflow_id = Client.create_workflow(
          ClientTestWorkflow, # <<< USE RENAMED CLASS HERE
          # No positional args needed for this workflow's perform
          user_id: user_id_arg,           # <<< Pass user_id as keyword arg
          report_type: report_type_kwarg, # Pass report_type as keyword arg
          globals: globals_hash          # Globals hash is handled separately
        )

        # Assert: Workflow Record was created correctly
        assert workflow_id.is_a?(String) && !workflow_id.empty?, "Should return a workflow ID string"
        # Use the correct namespace when querying models in tests
        wf_record = Persistence::ActiveRecord::WorkflowRecord.find_by(id: workflow_id)
        refute_nil wf_record, "WorkflowRecord should be created in DB"
        assert_equal "ClientTestWorkflow", wf_record.klass # <<< CHECK RENAMED CLASS HERE
        assert_equal "pending", wf_record.state # Initial state
        # Check persisted positional args - should be empty now
        assert_equal [], wf_record.arguments
        # Check persisted keyword args (ensure your persist_workflow handles kwargs if needed - currently it doesn't)
        # assert_equal({ "user_id" => user_id_arg, "report_type" => report_type_kwarg }, wf_record.kwargs) # If kwargs were persisted
        assert_equal({ "tenant_id" => "abc" }, wf_record.globals) # Check persisted globals
        refute wf_record.has_failures # Should initially be false

        # Assert: Job Records were created correctly
        job_records = Persistence::ActiveRecord::JobRecord.where(workflow_id: workflow_id).order(:created_at) # Order matters if checking sequence

        # This assertion should now pass (Expected 3)
        assert_equal 3, job_records.count, "Should create 3 JobRecords"

        # Find jobs based on expected arguments set in ClientTestWorkflow#perform
        # Ensure we check for the correct job class name now
        fetch_job = job_records.find { |j| j.klass == "ClientTestJob" && j.arguments == {"data" => user_id_arg} }
        process_job = job_records.find { |j| j.klass == "ClientTestJob" && j.arguments == {"data" => report_type_kwarg} }
        final_job = job_records.find { |j| j.klass == "ClientTestJob" && j.arguments == {} }

        refute_nil fetch_job, "Fetch Data job record should exist"
        refute_nil process_job, "Process Data job record should exist"
        refute_nil final_job, "Final Step job record should exist"
        assert job_records.all? { |j| j.state == "pending" }, "All jobs should start in pending state"
        # Check is_terminal flag persistence
        # Note: This assertion will likely fail until the logic to determine/set is_terminal is added
        # assert final_job.is_terminal, "Final job should be marked terminal"
        # refute fetch_job.is_terminal, "Non-terminal job should not be marked terminal"
        # refute process_job.is_terminal, "Non-terminal job should not be marked terminal"


        # Assert: Dependency Records were created correctly
        # Query dependencies based on the job IDs we found
        dependencies = Persistence::ActiveRecord::JobDependencyRecord.where(
          job_id: [fetch_job&.id, process_job&.id, final_job&.id].compact
        )
        # The workflow defines 2 dependencies
        assert_equal 2, dependencies.count, "Should create 2 JobDependencyRecords"

        dep1 = dependencies.find { |d| d.job_id == process_job&.id }
        assert_equal fetch_job&.id, dep1&.depends_on_job_id, "Process Data should depend on Fetch Data"

        dep2 = dependencies.find { |d| d.job_id == final_job&.id }
        assert_equal process_job&.id, dep2&.depends_on_job_id, "Final Step should depend on Process Data"
      end

      # Test input validation
      def test_create_workflow_raises_error_for_invalid_class
        assert_raises(ArgumentError, /must be a Class inheriting from Yantra::Workflow/) do
          Client.create_workflow(NotAWorkflow) # Pass a class that doesn't inherit
        end
      end

      # TODO: Add tests for persistence errors (e.g., mock repo methods to raise errors)
      # TODO: Add tests for other Client methods (find_workflow, start_workflow, etc.) once implemented

    end

  end # if defined?
end # module Yantra

