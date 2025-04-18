# test/performance/orchestrator_perf_test.rb
require 'test_helper'
require 'benchmark'
require 'securerandom'
require 'yantra' # Load main library

# --- Ensure Real Adapters are Loaded ---
# Adjust paths as necessary if not automatically loaded by test_helper
require 'yantra/persistence/active_record/adapter'
require 'yantra/events/null/adapter'
require 'yantra/core/orchestrator'
# Adjust paths for ActiveRecord models if needed (as user mentioned scoping)
require 'yantra/persistence/active_record/workflow_record'
require 'yantra/persistence/active_record/step_record'
require 'yantra/persistence/active_record/step_dependency_record'
# Require the interface definition if not loaded automatically
require 'yantra/worker/enqueuing_interface'


# --- Define Dummy Job Classes ---
# Needed for creating Step records
class PerfStartJob; end unless defined?(PerfStartJob)
class PerfParallelJob; end unless defined?(PerfParallelJob)
class PerfFinalJob; end unless defined?(PerfFinalJob)

class OrchestratorPerfTest < ActiveSupport::TestCase # Or Minitest::Test
  # Consider using database cleaner or transactions if configured in test_helper
  # self.use_transactional_tests = true

  def setup
    # --- Use REAL Repository, Mock Worker ---
    @repository = Yantra::Persistence::ActiveRecord::Adapter.new
    @worker_adapter = mock("stub_worker_adapter") # Mock worker
    @notifier = Yantra::Events::Null::Adapter.new # Use Null notifier

    # --- Add Stub for Interface Check ---
    @worker_adapter.stubs(:is_a?).with(Yantra::Worker::EnqueuingInterface).returns(true)
    # --- **FIX 1**: Stub enqueue by default to allow setup calls ---
    # Individual tests can override this with specific 'expects' if needed.
    @worker_adapter.stubs(:enqueue)

    # Initialize Orchestrator
    @orchestrator = Yantra::Core::Orchestrator.new(
      repository: @repository,
      worker_adapter: @worker_adapter,
      notifier: @notifier
    )

    # --- Test Parameters ---
    @num_parallel_steps = ENV.fetch('PERF_N', 1000).to_i

    # Optional: Silence Yantra's global logger
    # @original_logger = Yantra.logger
    # Yantra.logger = Logger.new(IO::NULL)

    # Common setup for workflow IDs used in multiple tests
    # Note: Workflow/steps are created in 'pending' state here.
    @workflow_uuid, @start_step_uuid, @parallel_step_uuids, @final_step_uuid = \
      create_fan_out_workflow_with_uuids(@num_parallel_steps)

  end

  def teardown
    # Clean database if not using transactions
    # Yantra::WorkflowRecord.delete_all
    # Yantra::StepRecord.delete_all
    # Yantra::StepDependencyRecord.delete_all

    # Restore original logger if changed in setup
    # Yantra.logger = @original_logger if @original_logger

    # Standard Mocha teardown
    Mocha::Mockery.instance.teardown
  end

  # --- Test 1: Fan-Out Performance ---
  # Measures time to process start step completion and enqueue N parallel steps.
  focus
  def test_perf_fan_out_enqueue
    puts "\n--- Testing Fan-Out Enqueue (N=#{@num_parallel_steps}) ---"
    # Start the workflow first
    @orchestrator.start_workflow(@workflow_uuid)
    assert_equal 'running', @repository.find_workflow(@workflow_uuid)&.state, "Workflow should be running"

    # Simulate the starting step finishing successfully
    update_step_state(@start_step_uuid, :succeeded)

    # Expect the worker adapter's 'enqueue' method to be called N times
    # Override the general stub from setup with a specific expectation for this test.
    @worker_adapter.expects(:enqueue)
                   .with(any_of(*@parallel_step_uuids), @workflow_uuid, PerfParallelJob.name, anything)
                   .times(@num_parallel_steps)

    # Measure the time for the orchestrator to process the finished step
    measurement = Benchmark.measure do
      @orchestrator.step_finished(@start_step_uuid)
    end
    puts format_benchmark("Fan-Out (step_finished -> enqueue N)", measurement)
    # Implicit assertion via Mocha expects
  end

  # --- Test 2: Failure Cascade Performance ---
  # Measures time to process a step failure, including updating state,
  # setting flags, and potentially cancelling downstream steps via step_finished.
  focus
  def test_perf_failure_cascade
     puts "\n--- Testing Failure Cascade (N=#{@num_parallel_steps}) ---"
     # **FIX 2**: Start the workflow before simulating failure
     @orchestrator.start_workflow(@workflow_uuid)
     assert_equal 'running', @repository.find_workflow(@workflow_uuid)&.state, "Workflow should be running before failure"

     # Simulate the START step failing after starting
     step_to_fail_uuid = @start_step_uuid
     update_step_state(step_to_fail_uuid, :running) # Step must be running for step_failed

     simulated_error = { class: "StandardError", message: "Simulated failure for cascade test", backtrace: ["line 1"] }

     # Allow any enqueue calls that might happen during cancellation (though likely none)
     @worker_adapter.stubs(:enqueue)

     # Measure the time for the orchestrator to handle the failure report
     measurement = Benchmark.measure do
        @orchestrator.step_failed(step_to_fail_uuid, simulated_error)
     end
     puts format_benchmark("Failure Handling & Cascade (step_failed -> step_finished)", measurement)

     # Verify workflow has failures flag is set and state is failed (since start failed)
     wf_check = @repository.find_workflow(@workflow_uuid)
     assert_equal true, wf_check&.has_failures, "Workflow has_failures flag should be true"
     # Now that workflow was started, the final state update should succeed
     assert_equal 'failed', wf_check&.state, "Workflow state should be marked failed"

     # Optional: Verify parallel/final steps are cancelled if desired
     # cancelled_states = Yantra::Persistence::ActiveRecord::StepRecord.where(id: @parallel_step_uuids + [@final_step_uuid]).pluck(:state)
     # assert cancelled_states.all? { |s| s == 'cancelled' }, "Downstream steps should be cancelled"
  end

   # --- Test 3: Fan-In Readiness Check Performance ---
   # Measures time to process the completion of the LAST prerequisite for a fan-in step,
   # including checking all parents and enqueuing the final step.
  focus
  def test_perf_fan_in_readiness_check
    puts "\n--- Testing Fan-In Readiness Check (N=#{@num_parallel_steps}) ---"
    # Setup: Start workflow, process start step completion (triggers parallel enqueues - allowed by setup stub)
    @orchestrator.start_workflow(@workflow_uuid)
    update_step_state(@start_step_uuid, :succeeded)
    # Allow parallel step enqueues triggered by this call
    @worker_adapter.stubs(:enqueue).with(any_of(*@parallel_step_uuids), anything, anything, anything)
    @orchestrator.step_finished(@start_step_uuid)

    # Process N-1 parallel steps silently
    steps_to_finish_first = @parallel_step_uuids.take(@num_parallel_steps - 1)
    last_parallel_step_uuid = @parallel_step_uuids.last

    steps_to_finish_first.each do |step_uuid|
      update_step_state(step_uuid, :succeeded)
      @orchestrator.step_finished(step_uuid) # Process completion silently
    end

    # Now, simulate the *last* parallel step finishing
    update_step_state(last_parallel_step_uuid, :succeeded)

    # Expect ONLY the final step to be enqueued now
    # Override the general stub with a specific expectation for the final step.
    @worker_adapter.expects(:enqueue)
                   .with(@final_step_uuid, @workflow_uuid, PerfFinalJob.name, anything)
                   .once

    # Measure the time for processing the last parallel step's completion.
    measurement = Benchmark.measure do
      @orchestrator.step_finished(last_parallel_step_uuid)
    end
    puts format_benchmark("Fan-In Readiness Check (last step_finished -> enqueue final)", measurement)

    # Implicit assertion via Mocha expects for enqueue.
  end


  private

  # Helper updated to match the provided db/schema.rb
  def create_fan_out_workflow_with_uuids(num_parallel)
    # 1. Generate UUIDs
    workflow_uuid = SecureRandom.uuid
    start_step_uuid = SecureRandom.uuid
    final_step_uuid = SecureRandom.uuid
    parallel_step_uuids = Array.new(num_parallel) { SecureRandom.uuid }
    now = Time.current # Use consistent timestamp for bulk insert

    # 2. Create Workflow Record using schema columns
    Yantra::Persistence::ActiveRecord::WorkflowRecord.create!(
      id: workflow_uuid, # Assign generated UUID to primary key 'id'
      klass: 'PerfWorkflow', # Schema has 'klass'
      arguments: { perf_n: num_parallel }.to_json, # Schema has 'arguments' (JSONB)
      state: 'pending', # Schema has 'state'
      globals: {}.to_json, # Schema has 'globals' (JSONB) - example empty hash
      has_failures: false, # Schema has 'has_failures'
      created_at: now, # Add timestamps if not handled automatically by AR + insert_all
      updated_at: now
      # Schema does NOT have 'name' or 'details'
    )

    # 3. Prepare Step Data using schema columns
    steps_to_insert = []
    # Start Step data
    steps_to_insert << {
      id: start_step_uuid, workflow_id: workflow_uuid, # Schema uses 'workflow_id' FK
      klass: 'PerfStartJob', state: 'pending',
      arguments: {}.to_json, queue: 'perf_queue', retries: 0, max_attempts: 3, # Added schema columns
      created_at: now, updated_at: now
      # Schema does NOT have 'name'
    }
    # Parallel Steps data
    parallel_step_uuids.each_with_index do |p_uuid, i|
        steps_to_insert << {
          id: p_uuid, workflow_id: workflow_uuid,
          klass: 'PerfParallelJob', state: 'pending',
          arguments: { index: i }.to_json, queue: 'perf_queue', retries: 0, max_attempts: 3, # Added schema columns
          created_at: now, updated_at: now
          # Schema does NOT have 'name'
        }
    end
    # Final Step data
    steps_to_insert << {
      id: final_step_uuid, workflow_id: workflow_uuid,
      klass: 'PerfFinalJob', state: 'pending',
      arguments: {}.to_json, queue: 'perf_queue', retries: 0, max_attempts: 3, # Added schema columns
      created_at: now, updated_at: now
      # Schema does NOT have 'name'
    }
    # Bulk Insert Steps
    Yantra::Persistence::ActiveRecord::StepRecord.insert_all!(steps_to_insert)


    # 4. Prepare Dependency Data using schema columns
    # Schema uses 'step_id' (dependent) and 'depends_on_step_id' (prerequisite)
    # Schema does NOT have 'yantra_workflow_id' in this table
    dependencies_to_insert = []
    # Parallel depends on Start
    parallel_step_uuids.each do |p_step_uuid|
      dependencies_to_insert << {
         step_id: p_step_uuid, depends_on_step_id: start_step_uuid
        # created_at/updated_at not in schema for dependencies
      }
    end
    # Final depends on Parallel
    parallel_step_uuids.each do |p_step_uuid|
      dependencies_to_insert << {
        step_id: final_step_uuid, depends_on_step_id: p_step_uuid
        # created_at/updated_at not in schema for dependencies
      }
    end
    # Bulk Insert Dependencies
    Yantra::Persistence::ActiveRecord::StepDependencyRecord.insert_all!(dependencies_to_insert) if dependencies_to_insert.any?

    # 5. Return the generated UUIDs
    [workflow_uuid, start_step_uuid, parallel_step_uuids, final_step_uuid]
  end

  # Helper modified to find step by string UUID 'id'
  def update_step_state(step_uuid, state_symbol)
    Yantra::Persistence::ActiveRecord::StepRecord.find(step_uuid) # Find by primary key UUID 'id'
                      .update_column(:state, state_symbol.to_s)
  end

  # Helper to format benchmark output
  def format_benchmark(label, measurement)
    "#{label}: #{measurement.real.round(4)}s real, #{measurement.total.round(4)}s cpu"
  end

end

