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
          # <<< Use string logging >>>
          log_debug "Enqueuing single job via Sidekiq: W:#{workflow_id} S:#{step_id} K:#{step_klass_name} Q:#{queue_name}"
          # Use set to specify the queue for this specific job push
          job_id = StepWorker.set(queue: queue_name).perform_async(step_id, workflow_id, step_klass_name)
          # <<< Use string logging >>>
          log_info "Successfully enqueued step #{step_id} to Sidekiq queue '#{queue_name}' with JID #{job_id}"
          !job_id.nil? # perform_async returns JID on success, nil on failure (e.g., middleware abort)
        rescue StandardError => e
          # <<< Use string logging >>>
          log_error "Failed to enqueue step #{step_id} to Sidekiq: #{e.class} - #{e.message}"
          false # Indicate failure
        end

        # @see Yantra::Worker::EnqueuingInterface#enqueue_bulk
        def enqueue_bulk(jobs_data_array)
          return true if jobs_data_array.nil? || jobs_data_array.empty?

          # <<< Use string logging >>>
          log_debug "Attempting bulk enqueue of #{jobs_data_array.count} jobs via Sidekiq."

          jobs_to_push = jobs_data_array.map do |job_data|
            {
              'class' => StepWorker.name,
              'args'  => [job_data[:step_id], job_data[:workflow_id], job_data[:klass]],
              'queue' => job_data[:queue],
              'retry' => false
            }
          end

          batch_jids = ::Sidekiq::Client.push_bulk(jobs_to_push)

          if batch_jids && batch_jids.count == jobs_data_array.count
             # <<< Use string logging >>>
             log_info "Successfully bulk enqueued #{batch_jids.count} steps to Sidekiq."
             true # Indicate success
          else
             # <<< Use string logging >>>
             log_warn "Sidekiq push_bulk may not have enqueued all requested jobs. Requested: #{jobs_data_array.count}, Got JIDs: #{batch_jids&.count}"
             false # Indicate potential issue
          end
        rescue StandardError => e
          # <<< Use string logging >>>
          log_error "Failed to bulk enqueue steps to Sidekiq: #{e.class} - #{e.message}"
          false # Indicate failure
        end

        private

        # <<< CHANGED: Logging helpers expecting STRING >>>
        def log_info(msg);  Yantra.logger&.info("[SidekiqAdapter] #{msg}") end
        def log_debug(msg); Yantra.logger&.debug("[SidekiqAdapter] #{msg}") end
        def log_warn(msg);  Yantra.logger&.warn("[SidekiqAdapter] #{msg}") end
        def log_error(msg); Yantra.logger&.error("[SidekiqAdapter] #{msg}") end
        # <<< END CHANGED >>>

      end # class Adapter
    end # module Sidekiq
  end # module Worker
end # module Yantra

