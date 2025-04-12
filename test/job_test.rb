# test/job_test.rb
require "test_helper"

# Dummy Job class for testing instantiation context
class MyTestJob < Yantra::Job
  # No need to implement perform for these tests
end

class JobTest < Minitest::Test
  def setup
    @workflow_id = SecureRandom.uuid
    @klass = MyTestJob
    @arguments = { sample: "data", count: 1 }
    @dsl_name = "test_job_instance"
  end

  def test_initialization_with_defaults
    job = @klass.new(workflow_id: @workflow_id, klass: @klass)

    assert_instance_of String, job.id
    assert_match(/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/, job.id) # UUID format check
    assert_equal @workflow_id, job.workflow_id
    assert_equal @klass, job.klass
    assert_equal :pending, job.state
    assert_empty job.arguments
    assert_nil job.dsl_name
    assert_in_delta Time.now.utc, job.created_at, 1 # Check within 1 second
    assert_nil job.enqueued_at
    assert_nil job.started_at
    assert_nil job.finished_at
    assert_nil job.output
    assert_nil job.error
    assert_equal 0, job.retries
  end

  def test_initialization_with_specific_arguments
    job = @klass.new(
      workflow_id: @workflow_id,
      klass: @klass,
      arguments: @arguments,
      dsl_name: @dsl_name,
      state: :enqueued # Override default state
    )

    assert_equal @arguments, job.arguments
    assert_equal @dsl_name, job.dsl_name
    assert_equal :enqueued, job.state
  end

  def test_initialization_with_internal_state_reconstruction
    timestamp = Time.now.utc - 3600 # An hour ago
    error_info = { class: "StandardError", message: "It failed" }
    internal_data = {
      id: "job-uuid-123",
      workflow_id: "wf-uuid-456",
      klass: @klass, # Note: Class needs to be passed again or handled differently
      arguments: @arguments,
      state: :failed,
      dsl_name: @dsl_name,
      created_at: timestamp,
      enqueued_at: timestamp + 60,
      started_at: timestamp + 120,
      finished_at: timestamp + 180,
      output: { result: "partial" },
      error: error_info,
      retries: 2
    }

    # Pass the class again, as internal_state typically won't have the actual constant
    job = @klass.new(klass: @klass, workflow_id: "ignored", internal_state: internal_data)

    assert_equal "job-uuid-123", job.id
    assert_equal "wf-uuid-456", job.workflow_id
    assert_equal @klass, job.klass
    assert_equal @arguments, job.arguments
    assert_equal :failed, job.state
    assert_equal @dsl_name, job.dsl_name
    assert_equal timestamp, job.created_at
    assert_equal timestamp + 60, job.enqueued_at
    assert_equal timestamp + 120, job.started_at
    assert_equal timestamp + 180, job.finished_at
    assert_equal({ result: "partial" }, job.output)
    assert_equal error_info, job.error
    assert_equal 2, job.retries
  end

  def test_to_hash_serialization
    job = @klass.new(
      workflow_id: @workflow_id,
      klass: @klass,
      arguments: @arguments,
      dsl_name: @dsl_name,
      state: :succeeded
    )
    # Simulate setting some state during execution
    job.instance_variable_set(:@started_at, Time.now.utc - 10)
    job.instance_variable_set(:@finished_at, Time.now.utc)
    job.instance_variable_set(:@output, { final: "value" })

    hash = job.to_hash

    assert_equal job.id, hash[:id]
    assert_equal job.workflow_id, hash[:workflow_id]
    assert_equal job.klass.to_s, hash[:klass] # Should store class name as string
    assert_equal job.arguments, hash[:arguments]
    assert_equal job.state, hash[:state]
    assert_equal job.dsl_name, hash[:dsl_name]
    assert_equal job.created_at, hash[:created_at]
    assert_equal job.enqueued_at, hash[:enqueued_at] # nil in this case
    assert_equal job.started_at, hash[:started_at]
    assert_equal job.finished_at, hash[:finished_at]
    assert_equal job.output, hash[:output]
    assert_equal job.error, hash[:error] # nil in this case
    assert_equal job.retries, hash[:retries]
  end

  def test_name_helper
    job_no_dsl = @klass.new(workflow_id: @workflow_id, klass: @klass)
    job_with_dsl = @klass.new(workflow_id: @workflow_id, klass: @klass, dsl_name: "MySpecificJobName")

    assert_equal "MyTestJob", job_no_dsl.name
    assert_equal "MySpecificJobName", job_with_dsl.name
  end

  def test_base_perform_raises_not_implemented_error
    # Instantiate the base class directly
    base_job = Yantra::Job.new(workflow_id: @workflow_id, klass: Yantra::Job)
    assert_raises(NotImplementedError) do
      base_job.perform
    end
  end
end

