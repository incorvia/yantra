# lib/yantra/worker/active_job/adapter.rb

require_relative '../enqueuing_interface'
# Attempt to load the actual ActiveJob class, handle LoadError if AJ not present
begin
  # Make sure this path matches the actual filename if you renamed it
  require_relative 'async_job' # <<< UPDATED REQUIRE PATH (assuming file renamed)
rescue LoadError
  puts "WARN: Could not load Yantra ActiveJob AsyncJob. ActiveJob adapter may not function."
end


module Yantra
  module Worker
    module ActiveJob
      # Implements the EnqueuingInterface using standard Rails ActiveJob.
      # Assumes ActiveJob has been configured in the host application.
      class Adapter
        include Yantra::Worker::EnqueuingInterface

        # Enqueues a Yantra job using ActiveJob's perform_later.
        #
        # @param job_id [String] The UUID of the Yantra job to execute.
        # @param workflow_id [String] The UUID of the parent workflow.
        # @param job_klass_name [String] The class name of the user's Yantra::Job subclass.
        # @param queue_name [String] The target queue name.
        def enqueue(job_id, workflow_id, job_klass_name, queue_name)
          # Use the renamed class name here
          unless defined?(AsyncJob) # <<< CHECK FOR RENAMED CLASS
             raise Yantra::Errors::ConfigurationError, "ActiveJob is configured but Yantra::Worker::ActiveJob::AsyncJob class failed to load."
          end

          puts "INFO: [ActiveJob::Adapter] Enqueuing job #{job_id} (Klass: #{job_klass_name}, WF: #{workflow_id}) to queue '#{queue_name}'"
          begin
            # Use .set to specify the queue, then enqueue the job with necessary args
            # Call the renamed class
            AsyncJob.set(queue: queue_name).perform_later(job_id, workflow_id, job_klass_name) # <<< USE RENAMED CLASS HERE
          rescue StandardError => e
            # Catch potential errors during enqueueing (e.g., AJ backend misconfiguration)
            puts "ERROR: [ActiveJob::Adapter] Failed to enqueue job #{job_id} via ActiveJob: #{e.message}"
            # Re-raise or wrap in a Yantra error? Re-raising for now.
            raise Yantra::Errors::WorkerError, "ActiveJob enqueuing failed: #{e.message}" # Use a specific error?
          end
        end
      end
    end
  end
end

