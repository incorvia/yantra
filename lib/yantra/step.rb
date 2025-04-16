# lib/yantra/step.rb

require 'securerandom'
require 'active_support/core_ext/string/inflections' # for underscore
# Ensure Yantra configuration and logger are available if not loaded elsewhere
# require_relative '../yantra' # Adjust path as necessary if Yantra isn't globally required

module Yantra
  # Base class for defining workflow steps (jobs).
  # Users should inherit from this class and implement the `perform` method.
  class Step
    attr_reader :id, :workflow_id, :arguments, :queue_name, :parent_ids, :klass, :dsl_name
    # Allow repository to be accessed if needed, but don't mandate it for external use
    attr_reader :repository

    # @param workflow_id [String] The UUID of the parent workflow.
    # @param klass [Class] The actual class of the step being instantiated.
    # @param arguments [Hash] Arguments passed to the step.
    # @param step_id [String, nil] Optional pre-assigned UUID for the step.
    # @param queue_name [String, nil] Optional queue name override.
    # @param parent_ids [Array<String>] Optional array of parent step IDs (for pipelining).
    # @param dsl_name [Symbol, String, nil] Optional reference name used in the Workflow DSL.
    # @param repository [Object, nil] Injected repository instance (used by StepJob).
    # @param _options [Hash] Catches any other keyword arguments passed during initialization
    def initialize(workflow_id:, klass:, arguments: {}, step_id: nil, queue_name: nil, parent_ids: [], dsl_name: nil, repository: nil, **_options) # <-- Added repository:
      @id = step_id || SecureRandom.uuid
      @workflow_id = workflow_id
      @arguments = arguments || {} # Ensure arguments is always a hash
      @klass = klass # Store klass first
      @queue_name = queue_name || default_queue_name # Now call default_queue_name
      @parent_ids = parent_ids || [] # Store parent IDs passed during instantiation
      @dsl_name = dsl_name # Will be nil if not provided (e.g., by StepJob)
      @repository = repository # Store injected repository

      # Initialize cache for lazy-loaded parent outputs
      @_parent_outputs_cache = nil

      # Note: The `_options` hash is intentionally ignored here.
    end

    # The main execution method to be implemented by subclasses.
    def perform
      raise NotImplementedError, "#{self.class.name} must implement the `perform` method."
    end

    # --- Pipelining Support ---

    # Lazily fetches and returns the outputs of all direct parent steps.
    # @return [Hash{String => Object}] A hash mapping parent step IDs to their outputs.
    #   Returns an empty hash if there are no parents or if outputs couldn't be fetched.
    def parent_outputs
      # Return from cache if already loaded
      return @_parent_outputs_cache unless @_parent_outputs_cache.nil?

      # If no parent IDs were provided, there's nothing to fetch
      if @parent_ids.empty?
        @_parent_outputs_cache = {}
        return @_parent_outputs_cache
      end

      # Fetch outputs from the repository using the stored parent IDs
      fetched_outputs = nil # Initialize
      begin
        # Use the injected repository instance FIRST, fallback to global lookup
        repo = repository # Calls the helper method below
        unless repo
          # Log using the logger helper method
          logger.error("Yantra persistence adapter not configured while trying to fetch parent outputs for step #{id}.")
          @_parent_outputs_cache = {} # Cache empty hash on error
          return @_parent_outputs_cache
        end

        # Call the repository method to get outputs for all parents in one go
        fetched_outputs = repo.fetch_step_outputs(@parent_ids)

        # Cache the result (use || {} to ensure cache is always a hash)
        @_parent_outputs_cache = fetched_outputs || {}

      rescue => e
        # Log error and cache an empty hash to prevent retries within the same instance
        logger.error("Error fetching parent outputs for step #{id}: #{e.message}")
        logger.error(e.backtrace.join("\n"))
        @_parent_outputs_cache = {}
      end

      @_parent_outputs_cache
    end

    # --- Default Configuration ---

    # Default queue name derived from the class name.
    def default_queue_name
      # Use @klass instance variable directly for safety during initialization
      @klass ? "#{@klass.name.underscore}_queue" : "yantra_default_queue"
    end

    # --- Helpers ---

    # Returns the reference name used in the DSL if provided, otherwise the class name.
    def name
      dsl_name&.to_s || klass&.name || "UnknownStep"
    end

    # Returns a hash representation of the step instance's identifying attributes.
    def to_hash
      {
        id: id,
        workflow_id: workflow_id,
        klass: klass&.to_s,
        arguments: arguments,
        dsl_name: dsl_name,
        queue_name: queue_name,
        parent_ids: parent_ids
      }
    end

    # Helper method to access the configured Yantra logger
    def logger
      # Ensure Yantra.logger is accessible. Might need `require 'yantra'`
      # at the top or ensure it's loaded globally.
      # Note: Still relies on Yantra.logger existing if not injected,
      # which might need addressing separately if logger is implemented.
      Yantra.logger
    rescue NameError # Fallback if Yantra or Yantra.logger isn't defined
      @_fallback_logger ||= Logger.new($stdout, level: Logger::INFO)
    end

    # Provides access to the configured repository adapter
    # Prioritizes injected repository, falls back to global config.
    def repository
      @repository || Yantra.config&.persistence_adapter
    rescue NameError # Fallback if Yantra or Yantra.config isn't defined
      @repository || nil # Return injected repo if global fails
    end
  end
end

