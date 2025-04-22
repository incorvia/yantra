# test/step_test.rb
require "test_helper"
require "ostruct" # For creating simple mock objects

# Conditionally load ActiveJob related files
if AR_LOADED
  require "yantra/errors"
  require "yantra/step" # Need base Yantra::Step
  require "minitest/mock"
end


# Dummy Step class for testing instantiation context
class MyTestStep < Yantra::Step
  # No need to implement perform for these tests
end

class StepTest < Minitest::Test
  # Define UUID regex constant for reuse
  UUID_REGEX = /\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/

  def setup
    @workflow_id = SecureRandom.uuid
    @klass = MyTestStep
    @arguments = { sample: "data", count: 1 }
    @dsl_name = "test_step_instance"

    # Mock repo still needed for other tests
    @mock_repo = Minitest::Mock.new
  end

  def teardown
    @mock_repo.verify
    Yantra::Configuration.reset! if defined?(Yantra::Configuration) && Yantra::Configuration.respond_to?(:reset!)
    super
  end

  # --- Initialization Tests ---
  # (Remain the same)

  def test_initialization_with_defaults
    step = @klass.new(workflow_id: @workflow_id, klass: @klass)
    assert_instance_of String, step.id
    assert_match(UUID_REGEX, step.id)
    assert_equal @workflow_id, step.workflow_id
    assert_equal @klass, step.klass
    assert_empty step.arguments
    assert_nil step.dsl_name
    assert_equal "my_test_step_queue", step.queue_name
    assert_empty step.parent_ids
  end

  def test_initialization_with_specific_arguments
    step = @klass.new(
      workflow_id: @workflow_id,
      klass: @klass,
      arguments: @arguments,
      dsl_name: @dsl_name,
      queue_name: "custom_queue",
      parent_ids: ["parent-uuid-1"]
    )
    assert_equal @arguments, step.arguments
    assert_equal @dsl_name, step.dsl_name
    assert_equal "custom_queue", step.queue_name
    assert_equal ["parent-uuid-1"], step.parent_ids
  end

  def test_initialization_with_explicit_id_and_other_args
    persisted_data = { id: "job-uuid-123", workflow_id: "wf-uuid-456", arguments: @arguments, dsl_name: @dsl_name, parent_ids: ["parent-1", "parent-2"] }
    step = @klass.new(klass: @klass, workflow_id: persisted_data[:workflow_id], step_id: persisted_data[:id], arguments: persisted_data[:arguments], dsl_name: persisted_data[:dsl_name], parent_ids: persisted_data[:parent_ids])
    assert_equal "job-uuid-123", step.id
    assert_equal "wf-uuid-456", step.workflow_id
    assert_equal @klass, step.klass
    assert_equal @arguments, step.arguments
    assert_equal @dsl_name, step.dsl_name
    assert_equal ["parent-1", "parent-2"], step.parent_ids
  end

  # --- Helper Method Tests ---
  # (Remain the same)

  def test_to_hash_serialization
    step = @klass.new(workflow_id: @workflow_id, klass: @klass, arguments: @arguments, dsl_name: @dsl_name, parent_ids: ["p1"])
    hash = step.to_hash
    assert_equal step.id, hash[:id]
    assert_equal step.workflow_id, hash[:workflow_id]
    assert_equal step.klass.to_s, hash[:klass]
    assert_equal step.arguments, hash[:arguments]
    assert_equal step.dsl_name, hash[:dsl_name]
    assert_equal step.queue_name, hash[:queue_name]
    assert_equal step.parent_ids, hash[:parent_ids]
    refute hash.key?(:state)
  end

  def test_name_helper
    step_no_dsl = @klass.new(workflow_id: @workflow_id, klass: @klass)
    step_with_dsl = @klass.new(workflow_id: @workflow_id, klass: @klass, dsl_name: "MySpecificStepName")
    assert_equal "MyTestStep", step_no_dsl.name
    assert_equal "MySpecificStepName", step_with_dsl.name
  end

  def test_base_perform_raises_not_implemented_error
    base_step = Yantra::Step.new(workflow_id: @workflow_id, klass: Yantra::Step)
    assert_raises(NotImplementedError) { base_step.perform }
  end

  # --- Pipelining Tests (`parent_outputs`) ---
  # (Tests using @mock_repo remain the same)

  def test_parent_outputs_returns_empty_hash_for_no_parents
    step = @klass.new(workflow_id: @workflow_id, klass: @klass, parent_ids: [])
    step.stub(:repository, @mock_repo) do
      assert_equal({}, step.parent_outputs)
    end
  end

  def test_parent_outputs_fetches_and_returns_output_for_one_parent
    parent_id = SecureRandom.uuid
    expected_output = { "some_key" => "some_value" }
    step = @klass.new(workflow_id: @workflow_id, klass: @klass, parent_ids: [parent_id], repository: @mock_repo)
    @mock_repo.expect(:get_step_outputs, { parent_id => expected_output }, [[parent_id]])
    step.stub(:repository, @mock_repo) do
      result = step.parent_outputs
      assert_equal({ parent_id => expected_output }, result)
    end
  end

  def test_parent_outputs_fetches_and_returns_outputs_for_multiple_parents
    parent_id_1 = SecureRandom.uuid
    parent_id_2 = SecureRandom.uuid
    output_1 = { value: 1 }
    parent_ids = [parent_id_1, parent_id_2]
    # Adjust expected result based on how get_step_outputs handles nil output
    expected_result = { parent_id_1.to_s => output_1, parent_id_2.to_s => {} } # Assuming nil becomes {}

    # --- FIX: Inject repository during initialization ---
    step = @klass.new(
      id: SecureRandom.uuid, # Add step's own ID if needed
      workflow_id: @workflow_id,
      klass: @klass,
      parent_ids: parent_ids,
      repository: @mock_repo # Pass mock repository here
    )
    # --- END FIX ---

    # Set expectation directly on the injected mock repo
    @mock_repo.expect(:get_step_outputs, expected_result, [parent_ids])

    # Call method directly
    result = step.parent_outputs
    assert_equal(expected_result, result)

    @mock_repo.verify # Ensure expectation was met
  end


  def test_parent_outputs_lazy_loads_and_caches_result
    parent_id = SecureRandom.uuid
    expected_output = { value: "cached" }
    step = @klass.new(workflow_id: @workflow_id, klass: @klass, parent_ids: [parent_id], repository: @mock_repo)
    @mock_repo.expect(:get_step_outputs, { parent_id => expected_output }, [[parent_id]])
    step.stub(:repository, @mock_repo) do
      result1 = step.parent_outputs # First call
      result2 = step.parent_outputs # Second call (should use cache)
      assert_equal({ parent_id => expected_output }, result1)
      assert_equal({ parent_id => expected_output }, result2)
    end
  end

  def test_parent_outputs_handles_repository_error_gracefully
    parent_id = SecureRandom.uuid
    step = @klass.new(workflow_id: @workflow_id, klass: @klass, parent_ids: [parent_id])

    # *** FIX HERE: Use a simple object that raises instead of Minitest::Mock ***
    error_repo = Object.new
    # Define the method directly on the object instance
    def error_repo.get_step_outputs(ids)
      raise Yantra::Errors::PersistenceError, "DB connection failed"
    end

    # Stub the repository method on the step instance to return our error_repo
    step.stub(:repository, error_repo) do
      # Call the method under test
      result = step.parent_outputs
      # Assert that it returns empty hash because the rescue block should catch the error
      assert_equal({}, result, "Should return empty hash on repository error")
    end
    # No need to verify @mock_repo here as it wasn't used.
  end

end

