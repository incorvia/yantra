# test/workflow_test.rb

require "test_helper" # Use basic helper, no AR needed here
require "yantra/workflow"
require "yantra/job"
require "yantra/errors" # For DependencyNotFound

# --- Dummy Job Classes for testing DSL ---
# These don't need real perform logic for these tests.
class TestJobA < Yantra::Job; def perform; end; end
class TestJobB < Yantra::Job; def perform; end; end
class TestJobC < Yantra::Job; def perform; end; end

# --- Test Workflow Definitions ---

# A workflow with various dependency structures
class ComplexTestWorkflow < Yantra::Workflow
  def perform(pos_arg = nil, key_arg: 'default')
    run TestJobA, name: :task_a, params: { p: pos_arg }
    run TestJobB, name: :task_b, params: { p: key_arg }, after: :task_a
    run TestJobC, name: :task_c, after: :task_a # Parallel branch
    # Fan-in, reuse TestJobA class, depends on B and C
    run TestJobA, name: :task_d, after: [:task_b, :task_c]
  end
end

# A simple linear workflow
class LinearTestWorkflow < Yantra::Workflow
  def perform
    run TestJobA, name: :a
    run TestJobB, name: :b, after: :a
    run TestJobC, name: :c, after: :b # C is terminal
  end
end

# Workflow that tries to use a dependency that wasn't defined
class InvalidDependencyWorkflow < Yantra::Workflow
  def perform
    run TestJobB, after: :non_existent_job
  end
end

# Workflow that tries to use an invalid job class
class InvalidJobClassWorkflow < Yantra::Workflow
  def perform
    run String # Not a Yantra::Job subclass
  end
end

# --- Workflow Test Class ---

# Use standard Minitest::Test, no AR needed
class WorkflowTest < Minitest::Test

  def test_initialization_stores_arguments_and_globals
    wf = ComplexTestWorkflow.new(123, key_arg: 'abc', globals: { g: 1 })
    assert_equal [123], wf.arguments
    # Note: :globals is extracted, remaining kwargs are stored
    assert_equal({ key_arg: 'abc' }, wf.kwargs)
    assert_equal({ g: 1 }, wf.globals)
    assert_kind_of String, wf.id
    assert_equal ComplexTestWorkflow, wf.klass
  end

  def test_dsl_builds_jobs_correctly
    wf = ComplexTestWorkflow.new(100) # Pass positional arg
    assert_equal 4, wf.jobs.count, "Should create 4 job instances"

    # Check job attributes created by the DSL
    job_a = wf.find_job_by_ref('task_a')
    job_b = wf.find_job_by_ref('task_b')
    job_c = wf.find_job_by_ref('task_c')
    job_d = wf.find_job_by_ref('task_d') # This is the second TestJobA instance

    refute_nil job_a
    refute_nil job_b
    refute_nil job_c
    refute_nil job_d

    assert_instance_of TestJobA, job_a
    assert_instance_of TestJobB, job_b
    assert_instance_of TestJobC, job_c
    assert_instance_of TestJobA, job_d # Check reused class

    assert_equal({ p: 100 }, job_a.arguments)
    assert_equal({ p: 'default' }, job_b.arguments) # Uses default kwarg value
    assert_equal({}, job_c.arguments)
    assert_equal({}, job_d.arguments)

    # Check that jobs have unique IDs and correct workflow ID
    all_ids = wf.jobs.map(&:id)
    assert_equal 4, all_ids.uniq.count, "Job IDs should be unique"
    assert wf.jobs.all? { |j| j.workflow_id == wf.id }, "All jobs should have correct workflow ID"
  end

  def test_dsl_builds_dependencies_correctly
    wf = ComplexTestWorkflow.new
    job_a = wf.find_job_by_ref('task_a')
    job_b = wf.find_job_by_ref('task_b')
    job_c = wf.find_job_by_ref('task_c')
    job_d = wf.find_job_by_ref('task_d')

    # Check the @dependencies hash structure
    assert_equal [job_a.id], wf.dependencies[job_b.id]
    assert_equal [job_a.id], wf.dependencies[job_c.id]
    # Use sort for comparison as order doesn't matter for dependencies
    assert_equal [job_b.id, job_c.id].sort, wf.dependencies[job_d.id].sort
    assert_nil wf.dependencies[job_a.id], "Job A should have no dependencies"
    assert_equal 3, wf.dependencies.keys.count # Only jobs B, C, D have dependency entries
  end

  def test_find_job_by_ref_works
    wf = ComplexTestWorkflow.new
    job_a_instance = wf.jobs.find { |j| j.dsl_name == 'task_a' } # Find instance reliably
    refute_nil job_a_instance
    assert_equal job_a_instance, wf.find_job_by_ref('task_a')
    assert_nil wf.find_job_by_ref('non_existent_ref')
  end

  def test_run_raises_error_for_invalid_dependency_reference
    # Checks that the `run` method fails if `after:` points to an unknown job ref
    assert_raises(Yantra::Errors::DependencyNotFound) do
      InvalidDependencyWorkflow.new
    end
  end

  def test_run_raises_error_for_non_job_class
    # Checks that the `run` method validates the job class
    assert_raises(ArgumentError, /must be a Class inheriting from Yantra::Job/) do
      InvalidJobClassWorkflow.new
    end
  end

  def test_calculate_terminal_status_marks_correct_jobs
    # Test the complex workflow
    wf_complex = ComplexTestWorkflow.new
    job_a = wf_complex.find_job_by_ref('task_a')
    job_b = wf_complex.find_job_by_ref('task_b')
    job_c = wf_complex.find_job_by_ref('task_c')
    job_d = wf_complex.find_job_by_ref('task_d') # This is the only terminal job

    # Note: calculate_terminal_status! should be called automatically by initialize now
    assert job_d.terminal?, "task_d should be terminal"
    refute job_a.terminal?, "task_a should not be terminal"
    refute job_b.terminal?, "task_b should not be terminal"
    refute job_c.terminal?, "task_c should not be terminal"

    # Test the linear workflow
    wf_linear = LinearTestWorkflow.new
    job_a_lin = wf_linear.find_job_by_ref('a')
    job_b_lin = wf_linear.find_job_by_ref('b')
    job_c_lin = wf_linear.find_job_by_ref('c') # This is terminal

    assert job_c_lin.terminal?, "Job C (linear) should be terminal"
    refute job_a_lin.terminal?, "Job A (linear) should not be terminal"
    refute job_b_lin.terminal?, "Job B (linear) should not be terminal"
  end

  # Add tests for job name collision handling if that logic is refined
  # Add tests for how globals are accessed if needed

end

