# lib/yantra/job.rb

require 'securerandom'

module Yantra
  # Base class for defining Yantra jobs (units of work).
  # Subclasses MUST implement the #perform method.
  class Job
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
      @id = internal_state.fetch(:id, id)
      @workflow_id = internal_state.fetch(:workflow_id, workflow_id)
      @klass = internal_state.fetch(:klass, klass)
      @arguments = internal_state.fetch(:arguments, arguments)
      @state = internal_state.fetch(:state, state).to_sym
      @dsl_name = internal_state.fetch(:dsl_name, dsl_name) # Store reference name

      # Timestamps and execution details usually populated by the system/repository
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
    # are available within this method.
    def perform(*args, **kwargs)
      # Note: How arguments from @arguments hash get passed here
      # might need refinement (e.g., pass @arguments hash directly,
      # or expect perform to access `arguments` reader).
      # Let's assume for now subclasses access `arguments` reader.
      raise NotImplementedError, "#{self.class.name} must implement the `perform` method."
    end

    # Placeholder: Method to convert job instance to persistable hash.
    # Used by the Repository.
    def to_hash
      {
        id: @id,
        workflow_id: @workflow_id,
        klass: @klass.to_s,
        arguments: @arguments,
        state: @state,
        dsl_name: @dsl_name,
        created_at: @created_at,
        enqueued_at: @enqueued_at,
        started_at: @started_at,
        finished_at: @finished_at,
        output: @output,
        error: @error,
        retries: @retries
        # Add queue, etc. if needed
      }
    end

    # Helper for logging/debugging
    def name
      @dsl_name || @klass.to_s
    end
  end
end

