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

# Workflow for testing dynamic dependencies
class DynamicDepWorkflow < Yantra::Workflow
  def perform(count: 3)
    job_a_refs = (1..count).map do |i|
      # Run job without explicit name, collect returned reference symbol
      run TestJobA, params: { index: i }
    end
    # This job should depend on all JobA instances created above
    run TestJobB, name: :final_job, after: job_a_refs
  end
end


# --- Workflow Test Class ---

# Use standard Minitest::Test, no AR needed
class WorkflowTest < Minitest::Test

  def test_initialization_stores_arguments_and_globals
    wf = ComplexTestWorkflow.new(123, key_arg: 'abc', globals: { g: 1 })
    assert_equal [123], wf.arguments
    assert_equal({ key_arg: 'abc' }, wf.kwargs)
    assert_equal({ g: 1 }, wf.globals)
    assert_kind_of String, wf.id
    assert_equal ComplexTestWorkflow, wf.klass
  end

  def test_dsl_run_method_returns_reference_symbol # <<< NEW TEST (or assertion)
    wf = ComplexTestWorkflow.new(100)
    # Manually run one step to check return value
    # Need to call perform first to initialize internal state used by run
    # wf.perform(100) # Or just instantiate, as initialize calls perform

    # Re-instantiate to test within perform context implicitly called by initialize
    wf_instance = Class.new(Yantra::Workflow) do
      attr_reader :return_value_test
      def perform
        @return_value_test = run TestJobA, name: :first_job
        run TestJobB, after: :first_job
      end
    end.new

    assert_equal :first_job, wf_instance.return_value_test, "run method should return the reference symbol"

    # Test generated name return value
    wf_instance_gen = Class.new(Yantra::Workflow) do
       attr_reader :return_value_gen1, :return_value_gen2
       def perform
         @return_value_gen1 = run TestJobA # Returns :TestJobA
         @return_value_gen2 = run TestJobA # Returns :TestJobA_1
       end
    end.new
    assert_equal :TestJobA, wf_instance_gen.return_value_gen1
    assert_equal :TestJobA_1, wf_instance_gen.return_value_gen2

  end


  def test_dsl_builds_jobs_correctly
    wf = ComplexTestWorkflow.new(100) # Pass positional arg
    assert_equal 4, wf.jobs.count, "Should create 4 job instances"

    job_a = wf.find_job_by_ref('task_a')
    job_b = wf.find_job_by_ref('task_b')
    job_c = wf.find_job_by_ref('task_c')
    job_d = wf.find_job_by_ref('task_d')

    refute_nil job_a
    refute_nil job_b
    refute_nil job_c
    refute_nil job_d

    assert_instance_of TestJobA, job_a
    assert_instance_of TestJobB, job_b
    assert_instance_of TestJobC, job_c
    assert_instance_of TestJobA, job_d

    assert_equal({ p: 100 }, job_a.arguments)
    assert_equal({ p: 'default' }, job_b.arguments)
    assert_equal({}, job_c.arguments)
    assert_equal({}, job_d.arguments)

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

    assert_equal [job_a.id], wf.dependencies[job_b.id]
    assert_equal [job_a.id], wf.dependencies[job_c.id]
    assert_equal [job_b.id, job_c.id].sort, wf.dependencies[job_d.id].sort
    assert_nil wf.dependencies[job_a.id]
    assert_equal 3, wf.dependencies.keys.count
  end

  def test_find_job_by_ref_works
    wf = ComplexTestWorkflow.new
    job_a_instance = wf.jobs.find { |j| j.dsl_name == 'task_a' }
    refute_nil job_a_instance
    assert_equal job_a_instance, wf.find_job_by_ref('task_a')
    assert_nil wf.find_job_by_ref('non_existent_ref')
  end

  def test_run_raises_error_for_invalid_dependency_reference
    assert_raises(Yantra::Errors::DependencyNotFound) do
      InvalidDependencyWorkflow.new
    end
  end

  def test_run_raises_error_for_non_job_class
    assert_raises(ArgumentError, /must be a Class inheriting from Yantra::Job/) do
      InvalidJobClassWorkflow.new
    end
  end

  def test_calculate_terminal_status_correctly_identifies_terminal_jobs
    wf_complex = ComplexTestWorkflow.new
    job_d = wf_complex.find_job_by_ref('task_d')
    job_a = wf_complex.find_job_by_ref('task_a')
    job_b = wf_complex.find_job_by_ref('task_b')
    job_c = wf_complex.find_job_by_ref('task_c')

    assert job_d.terminal?, "task_d should be terminal"
    refute job_a.terminal?, "task_a should not be terminal"
    refute job_b.terminal?, "task_b should not be terminal"
    refute job_c.terminal?, "task_c should not be terminal"

    wf_linear = LinearTestWorkflow.new
    job_c_linear = wf_linear.find_job_by_ref('c')
    job_b_linear = wf_linear.find_job_by_ref('b')
    assert job_c_linear.terminal?, "Job C (linear) should be terminal"
    refute job_b_linear.terminal?, "Job B (linear) should not be terminal"
  end

  def test_dynamic_dependencies_from_loop # <<< NEW TEST
    # Arrange: Define workflow that creates jobs in a loop
    wf = DynamicDepWorkflow.new(count: 3)

    # Assert: Check jobs created
    assert_equal 4, wf.jobs.count # 3 JobA, 1 JobB
    job_a_instances = wf.jobs.select { |j| j.klass == TestJobA }
    job_b_instance = wf.jobs.find { |j| j.klass == TestJobB }
    assert_equal 3, job_a_instances.count
    refute_nil job_b_instance

    # Assert: Check dependencies for the final job
    expected_dep_ids = job_a_instances.map(&:id).sort
    actual_dep_ids = wf.dependencies[job_b_instance.id].sort
    assert_equal expected_dep_ids, actual_dep_ids, "Final job should depend on all jobs created in loop"
  end

end

