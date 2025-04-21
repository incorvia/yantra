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
    end
  end
end

