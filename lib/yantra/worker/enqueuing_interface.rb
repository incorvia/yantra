# lib/yantra/worker/enqueuing_interface.rb

module Yantra
  module Worker
    # Defines the contract for background worker adapters used by Yantra.
    # Concrete adapters (e.g., SidekiqAdapter, ResqueAdapter, ActiveStepAdapter)
    # must implement the `enqueue` method defined here.
    #
    # Adapters are responsible for taking the details of a Yantra job that is
    # ready to run and submitting it to the specific background job processing
    # system (Sidekiq, Resque, etc.). They typically enqueue an instance of
    # Yantra::Worker::BaseExecutionJob (or a similar wrapper).
    module EnqueuingInterface

      # Enqueues a Yantra job for background execution using the configured system.
      #
      # The background system will typically instantiate and run
      # Yantra::Worker::BaseExecutionJob, passing it the step_id.
      #
      # @param step_id [String] The UUID of the Yantra job to execute.
      # @param workflow_id [String] The UUID of the parent workflow.
      # @param step_klass_name [String] The class name of the user's Yantra::Step subclass
      #   (needed by BaseExecutionJob to instantiate the correct user job class).
      # @param queue_name [String] The target queue name where the job should be placed.
      # @return [void]
      # @raise [StandardError] Adapters should raise an appropriate error if
      #   enqueuing fails catastrophically (e.g., cannot connect to queue backend).
      #   The Orchestrator's `enqueue_step` method may need to handle this.
      def enqueue(step_id, workflow_id, step_klass_name, queue_name)
        raise NotImplementedError, "#{self.class.name}#enqueue is not implemented"
      end

      # Enqueues multiple background jobs efficiently, if supported by the adapter.
      # This method is optional for adapters to implement. The core library
      # (e.g., StepEnqueuingService) should check `respond_to?(:enqueue_bulk)`
      # and fall back to calling `enqueue` individually if not implemented.
      #
      # @param jobs_data_array [Array<Hash>] An array where each element is a Hash
      #   containing the necessary details for a single job enqueue.
      #   Expected Hash keys:
      #   - :step_id [String] (Required) ID of the step.
      #   - :workflow_id [String] (Required) ID of the workflow.
      #   - :klass [String] (Required) Class name of the step.
      #   - :queue [String] (Required) Queue name for the job.
      #   - :args [Hash] (Optional) Arguments for the step job, if the adapter/job needs them directly.
      #     (Note: Yantra::StepJob typically fetches args via step_id, so this might not be needed).
      #
      # @return [Boolean] True if the bulk operation was successfully submitted, false otherwise.
      #   Note: This might not guarantee individual job success, depending on the adapter.
      # @raise [Yantra::Errors::WorkerError] If a critical error occurs during bulk enqueuing.
      # @raise [NotImplementedError] If the adapter does not support bulk enqueuing.
      # def enqueue_bulk(jobs_data_array)
        # Default implementation raises NotImplementedError.
        # Adapters supporting bulk enqueue should override this.
        # Adapters *not* supporting it can simply omit this method,
        # and callers should use respond_to? check.
        # Alternatively, provide a fallback loop here calling self.enqueue,
        # but raising NotImplementedError is clearer if it's truly optional.
        # raise NotImplementedError, "#{self.class.name} does not support 'enqueue_bulk'"
      # end
    end
  end
end

