# lib/yantra/worker/active_job/adapter.rb

require_relative '../enqueuing_interface'
# Attempt to load the actual ActiveJob class, handle LoadError if AJ not present
begin
  # Make sure this path matches the actual filename if you renamed it
  require_relative 'step_job'
rescue LoadError

end

# Require Yantra's custom errors if needed for rescue block
require_relative '../../errors'

module Yantra
  module Worker
    module ActiveJob
      # Implements the EnqueuingInterface using standard Rails ActiveJob.
      # Assumes ActiveJob has been configured in the host application.
      class Adapter
        include Yantra::Worker::EnqueuingInterface

        # Enqueues a Yantra job using ActiveJob's perform_later.
        #
        # @param step_id [String] The UUID of the Yantra job to execute.
        # @param workflow_id [String] The UUID of the parent workflow.
        # @param step_klass_name [String] The class name of the user's Yantra::Step subclass.
        # @param queue_name [String] The target queue name.
        def enqueue(step_id, workflow_id, step_klass_name, queue_name)
          # Define the expected constant name
          step_const_name = :StepJob # Or ExecutionJob/Job depending on final name
          step_class_module = Yantra::Worker::ActiveJob

          # Check if the required job class constant exists *before* trying to use it.
          # Use `const_defined?(..., false)` to check only within the specific module.
          unless step_class_module.const_defined?(step_const_name, false)
             raise Yantra::Errors::ConfigurationError, "ActiveJob is configured but #{step_class_module}::#{step_const_name} class could not be found/loaded."
          end

          # If defined, get the class constant
          step_class_to_enqueue = step_class_module.const_get(step_const_name)


          begin
            # Use .set to specify the queue, then enqueue the job with necessary args
            step_class_to_enqueue.set(queue: queue_name).perform_later(step_id, workflow_id, step_klass_name)
          rescue StandardError => e
            # Catch potential errors during enqueueing (e.g., AJ backend misconfiguration)

            # Wrap in a Yantra-specific error
            raise Yantra::Errors::WorkerError, "ActiveJob enqueuing failed: #{e.message}"
          end
        end

        def enqueue_in(delay_seconds, step_id, workflow_id, step_klass_name, queue_name)
          # Ensure delay is positive, otherwise enqueue immediately
          if delay_seconds.nil? || delay_seconds <= 0
            log_info("Delay is zero or nil for step #{step_id}, enqueuing immediately.")
            return enqueue(step_id, workflow_id, step_klass_name, queue_name)
          end

          job_args = [step_id, workflow_id, step_klass_name]

          options = { wait: delay_seconds.seconds }
          options[:queue] = queue_name.to_sym if queue_name.present?
          StepJob.set(options).perform_later(*job_args)

          true # Indicate success
        rescue StandardError => e
          # Log error appropriately
          logger = defined?(Yantra.logger) && Yantra.logger ? Yantra.logger : Logger.new(STDOUT)
          logger.error { "[AJ Adapter] Failed to enqueue delayed job for step #{step_id}: #{e.class} - #{e.message}" }
          false # Indicate failure
        end

        private

        # --- Added Logging Helpers ---
        # Logging helpers expecting STRING
        def log_info(msg);  Yantra.logger&.info("[SidekiqAdapter] #{msg}") end
        def log_debug(msg); Yantra.logger&.debug("[SidekiqAdapter] #{msg}") end
        def log_warn(msg);  Yantra.logger&.warn("[SidekiqAdapter] #{msg}") end
        def log_error(msg); Yantra.logger&.error("[SidekiqAdapter] #{msg}") end
        # --- End Added Logging Helpers ---
      end
    end
  end
end

