# test/workflow_test.rb

require "test_helper" # Use basic helper, no AR needed here
require "yantra/workflow"
require "yantra/step"
require "yantra/errors" # For DependencyNotFound

# --- Dummy Job Classes for testing DSL ---
# These don't need real perform logic for these tests.
class TestStepA < Yantra::Step; def perform; end; end
class TestStepB < Yantra::Step; def perform; end; end
class TestStepC < Yantra::Step; def perform; end; end

# --- Test Workflow Definitions ---

# A workflow with various dependency structures
class ComplexTestWorkflow < Yantra::Workflow
  def perform(pos_arg = nil, key_arg: 'default')
    run TestStepA, name: :task_a, params: { p: pos_arg }
    run TestStepB, name: :task_b, params: { p: key_arg }, after: :task_a
    run TestStepC, name: :task_c, after: :task_a # Parallel branch
    # Fan-in, reuse TestStepA class, depends on B and C
    run TestStepA, name: :task_d, after: [:task_b, :task_c]
  end
end

# A simple linear workflow
class LinearTestWorkflow < Yantra::Workflow
  def perform
    run TestStepA, name: :a
    run TestStepB, name: :b, after: :a
    run TestStepC, name: :c, after: :b # C is terminal
  end
end

# Workflow that tries to use a dependency that wasn't defined
class InvalidDependencyWorkflow < Yantra::Workflow
  def perform
    run TestStepB, after: :non_existent_job
  end
end

# Workflow that tries to use an invalid job class
class InvalidStepClassWorkflow < Yantra::Workflow
  def perform
    run String # Not a Yantra::Step subclass
  end
end

# Workflow for testing dynamic dependencies
class DynamicDepWorkflow < Yantra::Workflow
  def perform(count: 3)
    step_a_refs = (1..count).map do |i|
      # Run job without explicit name, collect returned reference symbol
      run TestStepA, params: { index: i }
    end
    # This job should depend on all StepA instances created above
    run TestStepB, name: :final_job, after: step_a_refs
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
        @return_value_test = run TestStepA, name: :first_job
        run TestStepB, after: :first_job
      end
    end.new

    assert_equal :first_job, wf_instance.return_value_test, "run method should return the reference symbol"

    # Test generated name return value
    wf_instance_gen = Class.new(Yantra::Workflow) do
       attr_reader :return_value_gen1, :return_value_gen2
       def perform
         @return_value_gen1 = run TestStepA # Returns :TestStepA
         @return_value_gen2 = run TestStepA # Returns :TestStepA_1
       end
    end.new
    assert_equal :TestStepA, wf_instance_gen.return_value_gen1
    assert_equal :TestStepA_1, wf_instance_gen.return_value_gen2

  end


  def test_dsl_builds_jobs_correctly
    wf = ComplexTestWorkflow.new(100) # Pass positional arg
    assert_equal 4, wf.steps.count, "Should create 4 job instances"

    step_a = wf.find_step_by_ref('task_a')
    step_b = wf.find_step_by_ref('task_b')
    step_c = wf.find_step_by_ref('task_c')
    step_d = wf.find_step_by_ref('task_d')

    refute_nil step_a
    refute_nil step_b
    refute_nil step_c
    refute_nil step_d

    assert_instance_of TestStepA, step_a
    assert_instance_of TestStepB, step_b
    assert_instance_of TestStepC, step_c
    assert_instance_of TestStepA, step_d

    assert_equal({ p: 100 }, step_a.arguments)
    assert_equal({ p: 'default' }, step_b.arguments)
    assert_equal({}, step_c.arguments)
    assert_equal({}, step_d.arguments)

    all_ids = wf.steps.map(&:id)
    assert_equal 4, all_ids.uniq.count, "Job IDs should be unique"
    assert wf.steps.all? { |j| j.workflow_id == wf.id }, "All jobs should have correct workflow ID"
  end

  def test_dsl_builds_dependencies_correctly
    wf = ComplexTestWorkflow.new
    step_a = wf.find_step_by_ref('task_a')
    step_b = wf.find_step_by_ref('task_b')
    step_c = wf.find_step_by_ref('task_c')
    step_d = wf.find_step_by_ref('task_d')

    assert_equal [step_a.id], wf.dependencies[step_b.id]
    assert_equal [step_a.id], wf.dependencies[step_c.id]
    assert_equal [step_b.id, step_c.id].sort, wf.dependencies[step_d.id].sort
    assert_nil wf.dependencies[step_a.id]
    assert_equal 3, wf.dependencies.keys.count
  end

  def test_find_step_by_ref_works
    wf = ComplexTestWorkflow.new
    step_a_instance = wf.steps.find { |j| j.dsl_name == 'task_a' }
    refute_nil step_a_instance
    assert_equal step_a_instance, wf.find_step_by_ref('task_a')
    assert_nil wf.find_step_by_ref('non_existent_ref')
  end

  def test_run_raises_error_for_invalid_dependency_reference
    assert_raises(Yantra::Errors::DependencyNotFound) do
      InvalidDependencyWorkflow.new
    end
  end

  def test_run_raises_error_for_non_step_class
    assert_raises(ArgumentError, /must be a Class inheriting from Yantra::Step/) do
      InvalidStepClassWorkflow.new
    end
  end

  def test_dynamic_dependencies_from_loop # <<< NEW TEST
    # Arrange: Define workflow that creates jobs in a loop
    wf = DynamicDepWorkflow.new(count: 3)

    # Assert: Check jobs created
    assert_equal 4, wf.steps.count # 3 StepA, 1 StepB
    step_a_instances = wf.steps.select { |j| j.klass == TestStepA }
    step_b_instance = wf.steps.find { |j| j.klass == TestStepB }
    assert_equal 3, step_a_instances.count
    refute_nil step_b_instance

    # Assert: Check dependencies for the final job
    expected_dep_ids = step_a_instances.map(&:id).sort
    actual_dep_ids = wf.dependencies[step_b_instance.id].sort
    assert_equal expected_dep_ids, actual_dep_ids, "Final job should depend on all jobs created in loop"
  end

end

