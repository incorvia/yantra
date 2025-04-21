# lib/yantra/worker/sidekiq/adapter.rb

# Ensure sidekiq is available
begin
  require 'sidekiq'
  require 'sidekiq/api' # Needed for Client.push_bulk
rescue LoadError
  raise "The 'sidekiq' gem is required to use the Yantra Sidekiq worker adapter."
end

require_relative '../enqueuing_interface'
require_relative '../../errors'
require_relative 'step_job' # Require the corresponding Sidekiq worker class

module Yantra
  module Worker
    module Sidekiq
      # Worker adapter for enqueuing Yantra steps using Sidekiq directly.
      # Implements the EnqueuingInterface, including optimized bulk enqueuing.
      class Adapter
        include Yantra::Worker::EnqueuingInterface

        # @see Yantra::Worker::EnqueuingInterface#enqueue
        def enqueue(step_id, workflow_id, step_klass_name, queue_name)
          log_debug "Enqueuing single job via Sidekiq: W:#{workflow_id} S:#{step_id} K:#{step_klass_name} Q:#{queue_name}"
          job_id = StepJob.set(queue: queue_name).perform_async(step_id, workflow_id, step_klass_name)
          log_info "Successfully enqueued step #{step_id} to Sidekiq queue '#{queue_name}' with JID #{job_id}"
          !job_id.nil?
        rescue StandardError => e
          log_error "Failed to enqueue step #{step_id} to Sidekiq: #{e.class} - #{e.message}"
          false
        end

        private

        # Logging helpers expecting STRING
        def log_info(msg);  Yantra.logger&.info("[SidekiqAdapter] #{msg}") end
        def log_debug(msg); Yantra.logger&.debug("[SidekiqAdapter] #{msg}") end
        def log_warn(msg);  Yantra.logger&.warn("[SidekiqAdapter] #{msg}") end
        def log_error(msg); Yantra.logger&.error("[SidekiqAdapter] #{msg}") end

      end # class Adapter
    end # module Sidekiq
  end # module Worker
end # module Yantra

