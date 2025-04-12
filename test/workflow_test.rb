# test/workflow_test.rb
require "test_helper"

# Dummy Job classes for DSL testing
class JobOne < Yantra::Job; def perform; end; end
class JobTwo < Yantra::Job; def perform; end; end
class JobThree < Yantra::Job; def perform; end; end

# Dummy Workflow for testing structure and DSL
class MyTestWorkflow < Yantra::Workflow
  attr_reader :perform_args, :perform_kwargs

  def perform(*args, **kwargs)
    @perform_args = args
    @perform_kwargs = kwargs

    run JobOne, name: "task_one", params: { p1: true }
    run JobTwo, name: "task_two", params: { p2: true }, after: ["task_one"]
    run JobThree, name: "task_three", params: { p3: true }, after: ["task_one"]
    run JobTwo, name: "task_two_again", params: { p2a: true }, after: ["task_one"] # Test duplicate class, diff name
    run JobOne, name: "task_one_final", params: { p1f: true }, after: ["task_two", "task_three", "task_two_again"]
  end
end

# Workflow to check perform execution skipping
class PerformCheckWorkflow < Yantra::Workflow
  attr_reader :perform_was_called
  def initialize(*args, **kwargs)
    @perform_was_called = false
    super
  end
  def perform
    @perform_was_called = true
  end
end

class WorkflowTest < Minitest::Test
  def test_initialization_with_defaults_and_args
    args = [1, 2]
    kwargs = { config: "value" }
    globals = { global_setting: true }
    wf = MyTestWorkflow.new(*args, **kwargs, globals: globals, internal_state: { skip_setup: true }) # Skip perform

    assert_instance_of String, wf.id
    assert_equal MyTestWorkflow, wf.klass
    assert_equal args, wf.arguments
    assert_equal kwargs, wf.kwargs
    assert_equal globals, wf.globals
    assert_empty wf.jobs
    assert_empty wf.dependencies
    assert_equal :pending, wf.current_state # Assuming default state
  end

  def test_initialization_calls_perform_by_default
    wf = PerformCheckWorkflow.new
    assert wf.perform_was_called, "Expected #perform to be called during initialization"
  end

  def test_initialization_skips_perform_when_persisted
    wf = PerformCheckWorkflow.new(internal_state: { persisted: true })
    refute wf.perform_was_called, "Expected #perform NOT to be called when internal_state[:persisted] is true"
  end

   def test_initialization_skips_perform_when_skip_setup
    wf = PerformCheckWorkflow.new(internal_state: { skip_setup: true })
    refute wf.perform_was_called, "Expected #perform NOT to be called when internal_state[:skip_setup] is true"
  end

  def test_initialization_with_internal_state_id
     wf = MyTestWorkflow.new(internal_state: { id: "wf-test-id-123", skip_setup: true })
     assert_equal "wf-test-id-123", wf.id
  end

  def test_base_perform_raises_not_implemented_error
    base_wf = Yantra::Workflow.new(internal_state: { skip_setup: true }) # Instantiate base class
    assert_raises(NotImplementedError) do
      base_wf.perform
    end
  end

  def test_run_dsl_builds_jobs_and_dependencies
    wf = MyTestWorkflow.new # Let perform run

    assert_equal 5, wf.jobs.size
    assert_instance_of JobOne, wf.find_job_by_ref("task_one")
    assert_instance_of JobTwo, wf.find_job_by_ref("task_two")
    assert_instance_of JobThree, wf.find_job_by_ref("task_three")
    assert_instance_of JobTwo, wf.find_job_by_ref("task_two_again") # Note: second instance of JobTwo
    assert_instance_of JobOne, wf.find_job_by_ref("task_one_final")

    # Check specific job details
    task_one = wf.find_job_by_ref("task_one")
    task_two = wf.find_job_by_ref("task_two")
    task_three = wf.find_job_by_ref("task_three")
    task_two_again = wf.find_job_by_ref("task_two_again")
    task_one_final = wf.find_job_by_ref("task_one_final")

    assert_equal({ p1: true }, task_one.arguments)
    assert_equal({ p2: true }, task_two.arguments)
    assert_equal({ p2a: true }, task_two_again.arguments)

    # Check dependencies (map uses job IDs)
    assert_nil wf.dependencies[task_one.id] # No dependencies
    assert_equal [task_one.id], wf.dependencies[task_two.id]
    assert_equal [task_one.id], wf.dependencies[task_three.id]
    assert_equal [task_one.id], wf.dependencies[task_two_again.id]
    # Final task depends on the three parallel tasks
    assert_equal [task_two.id, task_three.id, task_two_again.id].sort, wf.dependencies[task_one_final.id].sort
  end

  # This test verifies that if the `run` DSL method is called multiple times
  # with the same Job class *without* providing an explicit `name:` option,
  # it generates unique internal reference names (e.g., "JobClassName", "JobClassName_1").
  def test_run_dsl_handles_duplicate_class_names_without_explicit_name
    # 1. Create a base workflow instance.
    #    IMPORTANT: Use `skip_setup: true` to prevent `initialize` from calling
    #    the base `perform` method, which would raise NotImplementedError.
    wf = Yantra::Workflow.new(internal_state: { skip_setup: true })

    # 2. Directly call the `run` method multiple times on this instance.
    #    We expect the internal naming logic within `run` to handle the duplicates.
    job1 = wf.run(JobOne) # First call, should get default ref name "JobOne"
    job2 = wf.run(JobOne) # Second call, should get suffixed ref name "JobOne_1"

    # 3. Assertions to verify the outcome:
    assert_equal 2, wf.jobs.size, "Should have created 2 jobs in the workflow's list"

    # Verify jobs can be retrieved using the expected reference names
    assert_equal job1, wf.find_job_by_ref("JobOne"), "Should find first job instance by default class name reference"
    assert_equal job2, wf.find_job_by_ref("JobOne_1"), "Should find second job instance by suffixed reference name"

    # Verify the `dsl_name` attribute was set correctly on the job instances themselves
    assert_equal "JobOne", job1.dsl_name, "First job instance's dsl_name should be the default"
    assert_equal "JobOne_1", job2.dsl_name, "Second job instance's dsl_name should have the suffix"

    # Optional: Verify the jobs array contains the correct instances
    assert_includes wf.jobs, job1
    assert_includes wf.jobs, job2
  end

  def test_run_dsl_raises_error_for_invalid_job_class
    wf = MyTestWorkflow.new(internal_state: { skip_setup: true }) # Don't run default perform
    assert_raises(ArgumentError) do
      wf.run(String) # Pass a non-Yantra::Job class
    end
  end

  def test_run_dsl_raises_error_for_missing_dependency
    wf = MyTestWorkflow.new(internal_state: { skip_setup: true })
    wf.run(JobOne, name: "actual_job") # Define one job
    assert_raises(Yantra::Errors::DependencyNotFound) do
      # Try to depend on a job ref that wasn't defined
      wf.run(JobTwo, after: ["non_existent_job_ref"])
    end
  end

  def test_find_job_by_ref
    wf = MyTestWorkflow.new # Run perform to define jobs
    task_one = wf.find_job_by_ref("task_one")
    refute_nil task_one
    assert_instance_of JobOne, task_one

    assert_nil wf.find_job_by_ref("this_does_not_exist")
  end

   def test_to_hash
     args = ["arg1"]
     kwargs = { k: "v" }
     globals = { g: true }
     wf = MyTestWorkflow.new(*args, **kwargs, globals: globals, internal_state: { id: "wf-hash-test", skip_setup: true })

     hash = wf.to_hash

     assert_equal "wf-hash-test", hash[:id]
     assert_equal "MyTestWorkflow", hash[:klass]
     assert_equal args, hash[:arguments]
     assert_equal kwargs, hash[:kwargs]
     assert_equal globals, hash[:globals]
     assert_equal :pending, hash[:state] # Assumes current_state returns :pending
   end
end

