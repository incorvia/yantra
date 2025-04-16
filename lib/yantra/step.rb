# lib/yantra/step.rb

require 'securerandom'
require 'time' # Ensure Time is available

module Yantra
  # Base class for defining Yantra jobs (units of work).
  # Subclasses MUST implement the #perform method.
  class Step
    # Attributes accessible for each job instance
    attr_reader :id, :workflow_id, :klass, :arguments, :state, :dsl_name
    attr_reader :created_at, :enqueued_at, :started_at, :finished_at
    attr_reader :output, :error, :retries

    # Initializes a new job instance. Typically called internally by Workflow DSL.
    #
    # @param id [String] Unique ID for the job (defaults to SecureRandom.uuid).
    # @param workflow_id [String] ID of the parent workflow.
    # @param klass [Class] The specific Job subclass being instantiated.
    # @param arguments [Hash] Parameters passed to the #perform method.
    # @param state [Symbol] Initial state (defaults to :pending).
    # @param dsl_name [String] The reference name used in the workflow DSL.
    # @param internal_state [Hash] Used internally for reconstruction from persistence.
    def initialize(id: SecureRandom.uuid, workflow_id:, klass:, arguments: {}, state: :pending, dsl_name: nil, internal_state: {})
      # Prioritize internal_state for rehydration if loading from persistence
      @id = internal_state.fetch(:id, id)
      @workflow_id = internal_state.fetch(:workflow_id, workflow_id)
      @klass = internal_state.fetch(:klass, klass)
      @arguments = internal_state.fetch(:arguments, arguments)
      @state = internal_state.fetch(:state, state).to_sym
      @dsl_name = internal_state.fetch(:dsl_name, dsl_name) # Store reference name
      # Store terminal status, defaulting to false if not provided

      # Timestamps and execution details usually populated/updated by the system/repository
      @created_at = internal_state.fetch(:created_at, Time.now.utc)
      @enqueued_at = internal_state[:enqueued_at]
      @started_at = internal_state[:started_at]
      @finished_at = internal_state[:finished_at]
      @output = internal_state[:output]
      @error = internal_state[:error] # Store error info (e.g., { class:, message:, backtrace: })
      @retries = internal_state.fetch(:retries, 0)
    end

    # The main execution method for the job.
    # Subclasses MUST implement this method.
    # Arguments passed via `params:` in the DSL's `run` method
    # are available via the `arguments` reader hash.
    def perform
      # Example access: task_id = arguments[:task_id]
      raise NotImplementedError, "#{self.class.name} must implement the `perform` method."
    end

    # --- Methods expected by Persistence Adapters & Core Logic ---

    # Specifies the target queue for background workers.
    # Subclasses can override this method to specify a different queue.
    # @return [String] The name of the queue.
    def queue_name
      'default'
    end

    # --- Helper Methods ---

    # Placeholder: Method to convert job instance to persistable hash.
    # May be used by Repository adapters if they work with hashes,
    # though often they access attributes directly.
    def to_hash
      {
        id: @id,
        workflow_id: @workflow_id,
        klass: @klass.to_s,
        arguments: @arguments,
        state: @state,
        dsl_name: @dsl_name,
        queue: queue_name,        # Include queue from method
        created_at: @created_at,
        enqueued_at: @enqueued_at,
        started_at: @started_at,
        finished_at: @finished_at,
        output: @output,
        error: @error,
        retries: @retries
      }
    end

    # Helper for logging/debugging, uses DSL name if available.
    # @return [String] A representative name for the job.
    def name
      @dsl_name || @klass.to_s
    end
  end
end

