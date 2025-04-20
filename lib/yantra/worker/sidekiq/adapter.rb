# lib/yantra/worker/sidekiq/adapter.rb

# Ensure sidekiq is available
begin
  require 'sidekiq'
rescue LoadError
  raise "The 'sidekiq' gem is required to use the Yantra Sidekiq worker adapter."
end

require_relative '../enqueuing_interface'
require_relative '../../errors'
require_relative 'step_worker' # Require the corresponding Sidekiq worker class

module Yantra
  module Worker
    module Sidekiq
      # Worker adapter for enqueuing Yantra steps using Sidekiq directly.
      # Implements the EnqueuingInterface, including optimized bulk enqueuing.
      class Adapter
        include Yantra::Worker::EnqueuingInterface

        # @see Yantra::Worker::EnqueuingInterface#enqueue
        def enqueue(step_id, workflow_id, step_klass_name, queue_name)
          log_debug { "Enqueuing single job via Sidekiq: W:#{workflow_id} S:#{step_id} K:#{step_klass_name} Q:#{queue_name}" }
          # Use set to specify the queue for this specific job push
          job_id = StepWorker.set(queue: queue_name).perform_async(step_id, workflow_id, step_klass_name)
          log_info { "Successfully enqueued step #{step_id} to Sidekiq queue '#{queue_name}' with JID #{job_id}" }
          !job_id.nil? # perform_async returns JID on success, nil on failure (e.g., middleware abort)
        rescue StandardError => e
          log_error { "Failed to enqueue step #{step_id} to Sidekiq: #{e.class} - #{e.message}" }
          # Optionally wrap in Yantra::Errors::WorkerError
          # raise Yantra::Errors::WorkerError, "Sidekiq enqueue failed: #{e.message}"
          false # Indicate failure
        end

        # @see Yantra::Worker::EnqueuingInterface#enqueue_bulk
        def enqueue_bulk(jobs_data_array)
          return true if jobs_data_array.nil? || jobs_data_array.empty?

          log_debug { "Attempting bulk enqueue of #{jobs_data_array.count} jobs via Sidekiq." }

          # Prepare arguments for Sidekiq::Client.push_bulk
          # It expects an array of hashes, each hash representing a job.
          # Each hash needs 'class' and 'args' (array). Optionally 'queue'.
          jobs_to_push = jobs_data_array.map do |job_data|
            {
              'class' => StepWorker.name, # The Sidekiq worker class name (string)
              'args'  => [job_data[:step_id], job_data[:workflow_id], job_data[:klass]], # Array of args for perform
              'queue' => job_data[:queue], # Use queue specified for the job
              'retry' => false # Ensure Yantra handles retries, not Sidekiq directly
              # Add other Sidekiq options if needed (e.g., 'jid', 'at')
            }
          end

          # Use Sidekiq::Client.push_bulk for efficiency
          # push_bulk returns an array of JIDs for the enqueued jobs, or raises errors.
          batch_jids = ::Sidekiq::Client.push_bulk(jobs_to_push)

          if batch_jids && batch_jids.count == jobs_data_array.count
             log_info { "Successfully bulk enqueued #{batch_jids.count} steps to Sidekiq." }
             true # Indicate success
          else
             # This case might indicate partial success or middleware intervention,
             # though push_bulk often raises errors on failure.
             log_warn { "Sidekiq push_bulk may not have enqueued all requested jobs. Requested: #{jobs_data_array.count}, Got JIDs: #{batch_jids&.count}" }
             false # Indicate potential issue
          end
        rescue StandardError => e
          log_error { "Failed to bulk enqueue steps to Sidekiq: #{e.class} - #{e.message}" }
          # Optionally wrap in Yantra::Errors::WorkerError
          # raise Yantra::Errors::WorkerError, "Sidekiq bulk enqueue failed: #{e.message}"
          false # Indicate failure
        end

        private

        # Logging helpers
        def log_info(&block);  Yantra.logger&.info("[SidekiqAdapter]", &block) end
        def log_debug(&block); Yantra.logger&.debug("[SidekiqAdapter]", &block) end
        def log_warn(&block);  Yantra.logger&.warn("[SidekiqAdapter]", &block) end
        def log_error(&block); Yantra.logger&.error("[SidekiqAdapter]", &block) end

      end # class Adapter
    end # module Sidekiq
  end # module Worker
end # module Yantra

