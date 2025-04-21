# test/performance/orchestrator_perf_test.rb
require 'test_helper'
require 'benchmark'
require 'securerandom'
require 'yantra' # Load main library

# --- Ensure Adapters/Core are Loaded ---
require 'yantra/persistence/active_record/adapter'
require 'yantra/events/null/adapter'
require 'yantra/core/orchestrator'
require 'yantra/core/step_executor' # Needed by TestWorkerAdapter simulation if used
require 'yantra/worker/retry_handler' # Needed by TestWorkerAdapter simulation if used
require_relative '../support/test_worker_adapter' # <<< Require the Test Adapter

# Adjust paths for ActiveRecord models if needed
require 'yantra/persistence/active_record/workflow_record'
require 'yantra/persistence/active_record/step_record'
require 'yantra/persistence/active_record/step_dependency_record'


# --- Define Dummy Job Classes ---
class PerfStartJob; end unless defined?(PerfStartJob)
class PerfParallelJob; end unless defined?(PerfParallelJob)
class PerfFinalJob; end unless defined?(PerfFinalJob)

class OrchestratorPerfTest < YantraActiveRecordTestCase
  # self.use_transactional_tests = true # Consider transactions

  def setup
    super

    # --- Use REAL Repository, Test Worker, Null Notifier ---
    @repository = Yantra::Persistence::ActiveRecord::Adapter.new
    @test_worker_adapter = TestWorkerAdapter.new # <<< Use Test Worker Adapter
    @notifier = Yantra::Events::Null::Adapter.new

    # Initialize Orchestrator with TestWorkerAdapter
    @orchestrator = Yantra::Core::Orchestrator.new(
      repository: @repository,
      worker_adapter: @test_worker_adapter, # <<< Pass instance
      notifier: @notifier
    )

    # --- Test Parameters ---
    # Default to 1k, allow override via ENV var PERF_N
    @num_parallel_steps = ENV.fetch('PERF_N', 10000).to_i

    # Optional: Silence Yantra's global logger for performance runs
    @original_logger = Yantra.logger
    Yantra.logger = Logger.new(IO::NULL)

    # Create test data directly
    @workflow_uuid, @start_step_uuid, @parallel_step_uuids, @final_step_uuid = \
      create_fan_out_workflow_with_uuids(@num_parallel_steps)
  end

  def teardown
    # Clean database if not using transactions
    # Yantra::Persistence::ActiveRecord::StepDependencyRecord.delete_all
    # Yantra::Persistence::ActiveRecord::StepRecord.delete_all
    # Yantra::Persistence::ActiveRecord::WorkflowRecord.delete_all

    # Restore original logger if changed in setup
    Yantra.logger = @original_logger if @original_logger

    super
  end

  # --- Test 1: Fan-Out Performance ---
  # Measures time to process start step completion and "enqueue" N parallel steps.
  def test_perf_fan_out_enqueue
    puts "\n--- Testing Fan-Out Enqueue (N=#{@num_parallel_steps}) ---"
    # Start the workflow first (enqueues start step)
    measurement_start = Benchmark.measure do
      @orchestrator.start_workflow(@workflow_uuid)
    end
    puts format_benchmark("Fan-Out (start_workflow -> 1)", measurement_start)
    assert_equal 'running', @repository.find_workflow(@workflow_uuid)&.state, "Workflow should be running"
    assert_equal 1, @test_worker_adapter.enqueued_jobs.size, "Should have recorded start job"
    @test_worker_adapter.clear! # Clear start job

    # Simulate the starting step finishing successfully
    update_step_state(@start_step_uuid, :succeeded)

    # Measure the time for the orchestrator to process the finished step
    measurement_finish = Benchmark.measure do
      @orchestrator.step_finished(@start_step_uuid)
    end
    puts format_benchmark("Fan-Out (step_finished -> record N)", measurement_finish)

    # Assert that the correct number of jobs were recorded by the test adapter
    assert_equal @num_parallel_steps, @test_worker_adapter.enqueued_jobs.size, "Should have recorded N parallel jobs"

    # --- <<< CHANGED: Scalable Performance Assertion >>> ---
    # Define max time allowed per step (e.g., 100 microseconds)
    max_time_per_step = 0.0001
    # Define a fixed overhead (e.g., 0.5 seconds)
    fixed_overhead = 0.5
    # Calculate max total time based on N
    max_time_seconds = (@num_parallel_steps * max_time_per_step) + fixed_overhead

    assert measurement_finish.real < max_time_seconds, \
      "Fan-out performance regression detected! Took #{measurement_finish.real.round(4)}s, expected < #{max_time_seconds.round(2)}s for N=#{@num_parallel_steps} (using TestWorkerAdapter)."
    # --- <<< END CHANGED >>> ---
  end

  # --- Test 2: Failure Cascade Performance ---
  # Measures time to process a step failure, including cancellation logic.
  def test_perf_failure_cascade
    puts "\n--- Testing Failure Cascade (N=#{@num_parallel_steps}) ---"
    @orchestrator.start_workflow(@workflow_uuid)
    assert_equal 'running', @repository.find_workflow(@workflow_uuid)&.state, "Workflow should be running before failure"
    @test_worker_adapter.clear! # Clear start job

    # Simulate the START step failing after starting
    step_to_fail_uuid = @start_step_uuid
    update_step_state(step_to_fail_uuid, :running) # Step must be running for step_failed

    simulated_error = { class: "StandardError", message: "Simulated failure for cascade test", backtrace: ["line 1"] }

    # Measure the time for the orchestrator to handle the failure report
    measurement = Benchmark.measure do
      @orchestrator.step_failed(step_to_fail_uuid, simulated_error)
    end
    puts format_benchmark("Failure Handling & Cascade (step_failed -> step_finished)", measurement)

    # Verify workflow has failures flag is set and state is failed
    wf_check = @repository.find_workflow(@workflow_uuid)
    assert_equal true, wf_check&.has_failures, "Workflow has_failures flag should be true"
    assert_equal 'failed', wf_check&.state, "Workflow state should be marked failed"

    # Assert no jobs were recorded by worker adapter after failure
    assert_equal 0, @test_worker_adapter.enqueued_jobs.size, "No jobs should be enqueued after failure cascade"

    # Add a scalable performance check for cancellation if desired
    # Cancellation involves finding descendants (DB reads) and bulk cancelling (DB write)
    # Might scale roughly linearly with N, but potentially slower than enqueue
    # Example: Allow more time per step for cancellation logic
    # max_cancel_time_per_step = 0.0005 # 500 microseconds
    # fixed_cancel_overhead = 1.0
    # max_cancel_time = (@num_parallel_steps * max_cancel_time_per_step) + fixed_cancel_overhead
    # assert measurement.real < max_cancel_time, \
    #  "Failure cascade performance regression! Took #{measurement.real.round(4)}s, expected < #{max_cancel_time.round(2)}s for N=#{@num_parallel_steps}."
  end

  # --- Test 3: Fan-In Readiness Check Performance ---
  # Measures time to process the completion of the LAST prerequisite for a fan-in step.
  def test_perf_fan_in_readiness_check
    puts "\n--- Testing Fan-In Readiness Check (N=#{@num_parallel_steps}) ---"
    # Setup: Start workflow, process start step completion (triggers parallel enqueues)
    @orchestrator.start_workflow(@workflow_uuid) # Enqueues Start Job (1)
    update_step_state(@start_step_uuid, :succeeded)
    @test_worker_adapter.clear! # Clear the start job
    @orchestrator.step_finished(@start_step_uuid) # This now "enqueues" N parallel jobs to an empty queue
    assert_equal @num_parallel_steps, @test_worker_adapter.enqueued_jobs.size, "Should have recorded N parallel jobs after start finished" # Verify parallel jobs recorded
    @test_worker_adapter.clear! # Clear parallel jobs before simulating their completion

    # Process N-1 parallel steps silently by updating DB state
    steps_to_finish_first = @parallel_step_uuids.take(@num_parallel_steps - 1)
    last_parallel_step_uuid = @parallel_step_uuids.last

    if steps_to_finish_first.any?
      attributes_to_update = { state: Yantra::Core::StateMachine::SUCCEEDED.to_s }
      # Use bulk_update_steps for efficiency
      @repository.bulk_update_steps(steps_to_finish_first, attributes_to_update)
    end

    # Now, simulate the *last* parallel step finishing
    update_step_state(last_parallel_step_uuid, :succeeded)

    # Measure the time for processing the last parallel step's completion.
    # This involves: get_dependents(last_parallel) -> [final], fetch_dependencies(final) -> [all parallel],
    # fetch_states(final + all parallel), is_ready?(final)
    measurement = Benchmark.measure do
      @orchestrator.step_finished(last_parallel_step_uuid)
    end
    puts format_benchmark("Fan-In Readiness Check (last step_finished -> record final)", measurement)

    # Assert that ONLY the final step was recorded by the test adapter
    assert_equal 1, @test_worker_adapter.enqueued_jobs.size, "Only final step should be enqueued"
    assert_equal @final_step_uuid, @test_worker_adapter.enqueued_jobs.first[:step_id], "Final step ID should match"

    # Add scalable performance check for fan-in
    # This involves DB reads scaling with N (fetch_dependencies, fetch_states)
    # Allow more time per step than fan-out enqueue
    # max_fan_in_time_per_step = 0.0003 # 300 microseconds
    # fixed_fan_in_overhead = 0.5
    # max_fan_in_time = (@num_parallel_steps * max_fan_in_time_per_step) + fixed_fan_in_overhead
    # assert measurement.real < max_fan_in_time, \
    #  "Fan-in performance regression! Took #{measurement.real.round(4)}s, expected < #{max_fan_in_time.round(2)}s for N=#{@num_parallel_steps}."
  end


  private

  # Helper updated to match the provided db/schema.rb
  def create_fan_out_workflow_with_uuids(num_parallel)
    # ... (Implementation remains the same) ...
    workflow_uuid = SecureRandom.uuid
    start_step_uuid = SecureRandom.uuid
    final_step_uuid = SecureRandom.uuid
    parallel_step_uuids = Array.new(num_parallel) { SecureRandom.uuid }
    now = Time.current
    Yantra::Persistence::ActiveRecord::WorkflowRecord.create!(
      id: workflow_uuid, klass: 'PerfWorkflow', arguments: { perf_n: num_parallel }.to_json,
      state: 'pending', globals: {}.to_json, has_failures: false,
      created_at: now, updated_at: now
    )
    steps_to_insert = []
    steps_to_insert << {
      id: start_step_uuid, workflow_id: workflow_uuid, klass: 'PerfStartJob', state: 'pending',
      arguments: {}.to_json, queue: 'perf_queue', retries: 0, max_attempts: 3,
      created_at: now, updated_at: now
    }
    parallel_step_uuids.each_with_index do |p_uuid, i|
      steps_to_insert << {
        id: p_uuid, workflow_id: workflow_uuid, klass: 'PerfParallelJob', state: 'pending',
        arguments: { index: i }.to_json, queue: 'perf_queue', retries: 0, max_attempts: 3,
        created_at: now, updated_at: now
      }
    end
    steps_to_insert << {
      id: final_step_uuid, workflow_id: workflow_uuid, klass: 'PerfFinalJob', state: 'pending',
      arguments: {}.to_json, queue: 'perf_queue', retries: 0, max_attempts: 3,
      created_at: now, updated_at: now
    }
    Yantra::Persistence::ActiveRecord::StepRecord.insert_all!(steps_to_insert)
    dependencies_to_insert = []
    parallel_step_uuids.each { |p_step_uuid| dependencies_to_insert << { step_id: p_step_uuid, depends_on_step_id: start_step_uuid } }
    parallel_step_uuids.each { |p_step_uuid| dependencies_to_insert << { step_id: final_step_uuid, depends_on_step_id: p_step_uuid } }
    Yantra::Persistence::ActiveRecord::StepDependencyRecord.insert_all!(dependencies_to_insert) if dependencies_to_insert.any?
    [workflow_uuid, start_step_uuid, parallel_step_uuids, final_step_uuid]
  end

  # Helper modified to find step by string UUID 'id'
  def update_step_state(step_uuid, state_symbol)
    # Use update_columns to skip validations/callbacks for performance tests
    Yantra::Persistence::ActiveRecord::StepRecord.where(id: step_uuid)
      .update_all(state: state_symbol.to_s, updated_at: Time.current)
  end

  # Helper to format benchmark output
  def format_benchmark(label, measurement)
    "#{label}: #{measurement.real.round(4)}s real, #{measurement.total.round(4)}s cpu"
  end

end

