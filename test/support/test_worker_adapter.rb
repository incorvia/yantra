# test/support/test_worker_adapter.rb

require 'yantra/worker/enqueuing_interface'
require 'yantra/errors'

# A test adapter for the worker system that conforms to the EnqueuingInterface.
# It does not actually enqueue jobs but records the calls made to it,
# allowing tests (especially performance tests) to run without the overhead
# or dependency of a real background job system like ActiveJob or Sidekiq.
class TestWorkerAdapter
  include Yantra::Worker::EnqueuingInterface

  # Stores hashes representing jobs that were requested to be enqueued.
  # Each hash contains: { step_id:, workflow_id:, klass:, queue:, args: nil }
  # Note: args are typically not passed directly during Yantra enqueue.
  attr_reader :enqueued_jobs

  def initialize
    clear!
  end

  # Clears the record of enqueued jobs. Call this in test setup.
  def clear!
    @enqueued_jobs = []
  end

  # Simulates enqueuing a single job by recording its details.
  #
  # @param step_id [String] The unique ID of the step.
  # @param workflow_id [String] The ID of the workflow.
  # @param step_klass_name [String] The class name of the step.
  # @param queue_name [String] The target queue name.
  # @return [Boolean] Always returns true in this test adapter.
  def enqueue(step_id, workflow_id, step_klass_name, queue_name)
    job_data = {
      step_id: step_id,
      workflow_id: workflow_id,
      klass: step_klass_name,
      queue: queue_name,
      args: [step_id, workflow_id, step_klass_name] # Store args for potential inspection
    }
    @enqueued_jobs << job_data
    # Yantra.logger&.debug { "[TestWorkerAdapter] Recorded enqueue: #{job_data.inspect}" } # Optional logging
    true # Simulate successful enqueue submission
  end
end

