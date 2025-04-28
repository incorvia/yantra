# lib/yantra/worker/sidekiq/adapter.rb
# frozen_string_literal: true

require 'sidekiq'
require_relative '../enqueuing_interface'
require_relative 'step_job' # Ensure the actual job class is loaded
require 'logger' # Ensure Logger is available for fallback

module Yantra
  module Worker
    module Sidekiq
      # Implements the EnqueuingInterface using Sidekiq directly.
      class Adapter
        include Yantra::Worker::EnqueuingInterface

        # Enqueues immediately using Sidekiq::Client.push or StepJob.perform_async.
        # @see Yantra::Worker::EnqueuingInterface#enqueue
        def enqueue(step_id, workflow_id, step_klass_name, queue_name)
          job_args = [step_id, workflow_id, step_klass_name]
          # Use perform_async which handles building the payload
          # Set queue if provided
          options = queue_name.present? ? { 'queue' => queue_name.to_s } : {}
          StepJob.set(options).perform_async(*job_args)
          true # perform_async returns JID or nil, return true for consistency
        rescue StandardError => e
          # Use logging helper
          log_error("Failed to enqueue job for step #{step_id}: #{e.class} - #{e.message}")
          false # Indicate failure
        end

        # Enqueues with a delay using Sidekiq's `perform_in`.
        # @see Yantra::Worker::EnqueuingInterface#enqueue_in
        def enqueue_in(delay_seconds, step_id, workflow_id, step_klass_name, queue_name)
          # Ensure delay is positive, otherwise enqueue immediately
          if delay_seconds.nil? || delay_seconds <= 0
            log_info("Delay is zero or nil for step #{step_id}, enqueuing immediately.")
            return enqueue(step_id, workflow_id, step_klass_name, queue_name)
          end

          job_args = [step_id, workflow_id, step_klass_name]
          # Use perform_in for scheduling
          # Set queue if provided
          options = queue_name.present? ? { 'queue' => queue_name.to_s } : {}
          StepJob.set(options).perform_in(delay_seconds, *job_args)
          log_info("Enqueued delayed job for step #{step_id} to run in #{delay_seconds} seconds.")
          true # Indicate success
        rescue StandardError => e
          # Use logging helper
          log_error("Failed to enqueue delayed job for step #{step_id}: #{e.class} - #{e.message}")
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
