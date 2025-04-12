# lib/yantra/workflow.rb

require 'securerandom'
require_relative 'job' # Assuming job.rb is in the same directory for now
require_relative 'errors'

module Yantra
  # Base class for defining Yantra workflows.
  # Subclasses should implement the #perform method to define the
  # Directed Acyclic Graph (DAG) of jobs using the `run` DSL method.
  class Workflow
    attr_reader :id, :klass, :arguments, :kwargs, :globals, :jobs, :dependencies

    # Initializes a new workflow instance.
    #
    # @param args [Array] Positional arguments passed to the workflow perform method.
    # @param kwargs [Hash] Keyword arguments passed to the workflow perform method.
    # @param globals [Hash] Global configuration or data accessible to all jobs.
    # @param internal_state [Hash] Used internally for reconstructing workflow state
    #   from persistence. Avoid passing this manually. Expected keys:
    #   :id [String] Existing workflow ID.
    #   :persisted [Boolean] If the workflow is being loaded from storage.
    #   :skip_setup [Boolean] If true, skips running the #perform method (used during reconstruction).
    #   :jobs [Array<Yantra::Job>] Pre-loaded job instances.
    #   :dependencies [Hash] Pre-loaded dependency map { job_id => [dep_job_id, ...] }.
    def initialize(*args, **kwargs)
      internal_state = kwargs.delete(:internal_state) || {}
      @globals = kwargs.delete(:globals) || {} # Separate globals explicitly

      @id = internal_state.fetch(:id, SecureRandom.uuid)
      @klass = self.class
      @arguments = args
      @kwargs = kwargs

      # Internal tracking during DAG definition
      @jobs = [] # Holds Yantra::Job instances
      @job_lookup = {} # Maps internal DSL name/ref to Yantra::Job instance
      @dependencies = {} # Maps job.id => [dependency_job_id, ...]

      # Rehydrate state if loaded from persistence
      if internal_state[:persisted]
        @jobs = internal_state.fetch(:jobs, [])
        @dependencies = internal_state.fetch(:dependencies, {})
        # Rebuild lookup for potential internal use, though less critical after creation
        @jobs.each { |job| @job_lookup[job.klass.to_s] ||= job } # Simplified lookup rebuild
      end

      # Run the user-defined DAG definition unless skipped (e.g., during reconstruction)
      perform unless internal_state[:skip_setup] || internal_state[:persisted]
    end

    # Entry point for defining the workflow DAG.
    # Subclasses MUST implement this method using the `run` DSL.
    def perform
      raise NotImplementedError, "#{self.class.name} must implement the `perform` method."
    end

    # DSL method to define a job within the workflow.
    #
    # @param job_klass [Class] The Yantra::Job subclass to run.
    # @param params [Hash] Arguments/payload to pass to the job's perform method.
    # @param after [Array<String, Symbol, Class>] References to jobs that must complete before this one starts.
    #   References can be the job class itself (if unique in the workflow) or a custom name/symbol.
    # @param name [String, Symbol] An optional unique name/reference for this job within the workflow DSL.
    #   Defaults to the job_klass.to_s if not provided. Useful if running the same job class multiple times.
    # @return [Yantra::Job] The newly created job instance.
    def run(job_klass, params: {}, after: [], name: nil)
      unless job_klass.is_a?(Class) && job_klass < Yantra::Job
         raise ArgumentError, "#{job_klass} must be a Class inheriting from Yantra::Job"
      end

      # Use provided name or default to class name for internal lookup during build
      job_ref_name = (name || job_klass.to_s).to_s
      if @job_lookup.key?(job_ref_name)
        # Handle cases where the same job class/name is used multiple times
        # Simple strategy: append a counter. More complex strategies could exist.
        count = @jobs.count { |j| (j.dsl_name || j.klass.to_s) == job_ref_name }
        job_ref_name = "#{job_ref_name}_#{count}"
        # Alternatively, raise an error if unique names are required unless explicitly provided
        # raise ArgumentError, "Job reference name '#{job_ref_name}' already used. Provide a unique `name:` option."
      end

      job_id = SecureRandom.uuid
      job = job_klass.new(
        id: job_id,
        workflow_id: @id,
        klass: job_klass,
        arguments: params,
        dsl_name: job_ref_name # Store the name used in DSL for potential lookup
        # Other attributes like queue, retries could be added here or configured globally
      )

      @jobs << job
      @job_lookup[job_ref_name] = job

      # Resolve dependencies based on their DSL reference names
      dependency_ids = Array(after).map do |dep_ref|
        dep_job = find_job_by_ref(dep_ref.to_s)
        unless dep_job
          raise Yantra::Errors::DependencyNotFound, "Dependency '#{dep_ref}' not found for job '#{job_ref_name}'."
        end
        dep_job.id
      end

      @dependencies[job.id] = dependency_ids unless dependency_ids.empty?

      job # Return the job instance
    end

    # Finds a job instance within this workflow based on the reference
    # name used in the DSL (`run` method's `name` option or class name).
    # Used internally to resolve dependencies during DAG definition.
    #
    # @param ref_name [String] The reference name.
    # @return [Yantra::Job, nil] The found job instance or nil.
    def find_job_by_ref(ref_name)
      @job_lookup[ref_name]
    end

    # Placeholder: Method to convert workflow definition to persistable hash
    # This would be used by the Repository/Client before saving.
    def to_hash
      {
        id: @id,
        klass: @klass.to_s,
        arguments: @arguments,
        kwargs: @kwargs,
        globals: @globals,
        # Note: Jobs and Dependencies are usually persisted separately by the repository
        # based on the @jobs array and @dependencies hash built during initialization.
        # We might only store core workflow metadata here.
        state: current_state # Assuming state tracking exists
      }
    end

    # Placeholder for state retrieval
    def current_state
      # Logic to determine current state (e.g., from internal state or repo)
      :pending # Default initial state perhaps
    end
  end
end

