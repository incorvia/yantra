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

  # Simulates enqueuing multiple jobs by recording their details.
  #
  # @param jobs_data_array [Array<Hash>] Array of job data hashes.
  #   Expected keys per hash: :step_id, :workflow_id, :klass, :queue
  # @return [Boolean] Always returns true in this test adapter.
  def enqueue_bulk(jobs_data_array)
    return true if jobs_data_array.nil? || jobs_data_array.empty?

    jobs_data_array.each do |job_data|
       # Ensure basic keys exist before recording
       recorded_data = {
         step_id: job_data[:step_id],
         workflow_id: job_data[:workflow_id],
         klass: job_data[:klass],
         queue: job_data[:queue],
         args: [job_data[:step_id], job_data[:workflow_id], job_data[:klass]]
       }
       @enqueued_jobs << recorded_data
    end
    # Yantra.logger&.debug { "[TestWorkerAdapter] Recorded bulk enqueue: #{jobs_data_array.count} jobs." } # Optional logging
    true # Simulate successful bulk submission
  end

end

