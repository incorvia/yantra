# lib/yantra/workflow.rb

require 'securerandom'
require 'set' # Needed for Set in calculate_terminal_status!
require_relative 'job'
require_relative 'errors'

module Yantra
  # Base class for defining Yantra workflows.
  # Subclasses should implement the #perform method to define the
  # Directed Acyclic Graph (DAG) of jobs using the `run` DSL method.
  class Workflow
    # Attributes accessible after initialization
    attr_reader :id, :klass, :arguments, :kwargs, :globals, :jobs, :dependencies
    # Add state reader if loading state during rehydration
    attr_reader :state

    # Initializes a new workflow instance.
    # If creating NEW: Runs the subclass's #perform method to build the job graph
    #                  and then calculates terminal job statuses.
    # If REHYDRATING (internal_state[:persisted] is true): Loads only core attributes,
    #                  does NOT load jobs/dependencies, does NOT run perform/calculate.
    #
    # @param args [Array] Positional arguments passed to the workflow perform method (on create).
    # @param kwargs [Hash] Keyword arguments passed to the workflow perform method (on create).
    # @option kwargs [Hash] :globals Global configuration or data accessible to all jobs.
    # @option kwargs [Hash] :internal_state Used internally for reconstruction. Avoid manual use.
    #   Expected keys for rehydration: :id, :klass, :arguments, :kwargs, :globals, :state, :persisted=true
    def initialize(*args, **kwargs)
      internal_state = kwargs.delete(:internal_state) || {}
      @globals = internal_state.fetch(:globals, kwargs.delete(:globals) || {}) # Load globals

      @id = internal_state.fetch(:id, SecureRandom.uuid)
      @klass = internal_state.fetch(:klass, self.class)
      @arguments = internal_state.fetch(:arguments, args) # Load positional args
      @kwargs = internal_state.fetch(:kwargs, kwargs)  # Load keyword args
      @state = internal_state.fetch(:state, :pending).to_sym # Load state, default pending

      # Jobs and Dependencies are NOT loaded by default during rehydration
      # They remain empty unless explicitly loaded later via another method.
      @jobs = [] # Holds Yantra::Job instances (only populated during initial creation)
      @job_lookup = {} # Only populated during initial creation
      @dependencies = {} # Only populated during initial creation

      # Run perform and calculate terminal status ONLY on initial creation,
      # NOT during rehydration from persistence.
      if !internal_state[:persisted] && !internal_state[:skip_setup]
        # Call the subclass's perform method, passing through original args/kwargs
        perform(*@arguments, **@kwargs)
        # Calculate terminal status immediately after the graph is built
        calculate_terminal_status!
      elsif internal_state[:persisted]
        # Optional: Could rebuild job_lookup if needed based on *persisted* job data
        # fetched separately, but likely not needed for the base Workflow object.
        puts "INFO: Rehydrated workflow #{@id} (State: #{@state}). Jobs/Deps not loaded into memory."
      end
    end

    # Entry point for defining the workflow DAG.
    # Subclasses MUST implement this method using the `run` DSL.
    def perform(*args, **kwargs)
      raise NotImplementedError, "#{self.class.name} must implement the `perform` method."
    end

    # DSL method to define a job within the workflow.
    # (Code for `run` method remains the same as before)
    def run(job_klass, params: {}, after: [], name: nil)
      unless job_klass.is_a?(Class) && job_klass < Yantra::Job
        raise ArgumentError, "#{job_klass} must be a Class inheriting from Yantra::Job"
      end

      base_ref_name = (name || job_klass.to_s).to_s
      job_ref_name = base_ref_name
      if @job_lookup.key?(job_ref_name)
        count = @jobs.count { |j| (j.dsl_name || j.klass.to_s) == base_ref_name }
        job_ref_name = "#{base_ref_name}_#{count}"
      end

      job_id = SecureRandom.uuid
      job = job_klass.new(
        id: job_id,
        workflow_id: @id,
        klass: job_klass,
        arguments: params,
        dsl_name: job_ref_name,
        is_terminal: false # Default, calculated by calculate_terminal_status!
      )

      @jobs << job
      @job_lookup[job_ref_name] = job

      dependency_ids = Array(after).map do |dep_ref|
        dep_job = find_job_by_ref(dep_ref.to_s)
        unless dep_job
          raise Yantra::Errors::DependencyNotFound, "Dependency '#{dep_ref}' not found for job '#{job_ref_name}'."
        end
        dep_job.id
      end

      @dependencies[job.id] = dependency_ids unless dependency_ids.empty?

      job
    end

    # Finds a job instance within this workflow based on the reference
    # name used in the DSL during initial creation.
    # Note: This only works on newly created instances before persistence
    # or if the lookup is rebuilt during rehydration (which it currently isn't by default).
    # @param ref_name [String] The reference name.
    # @return [Yantra::Job, nil] The found job instance or nil.
    def find_job_by_ref(ref_name)
      @job_lookup[ref_name]
    end

    # Calculates which jobs in the workflow are terminal nodes (have no dependents)
    # and updates the @is_terminal flag on the job instances.
    # This is called automatically by initialize after perform completes during initial creation.
    # Runs in O(N+E) time, where N is number of jobs, E is number of dependency edges.
    def calculate_terminal_status!
      return if @jobs.empty?

      prerequisite_job_ids = Set.new
      @dependencies.each_value do |dependency_id_array|
        dependency_id_array.each { |dep_id| prerequisite_job_ids.add(dep_id) }
      end

      @jobs.each do |job|
        is_terminal = !prerequisite_job_ids.include?(job.id)
        job.instance_variable_set(:@is_terminal, is_terminal)
      end
    end

    # Placeholder: Method to convert workflow definition to persistable hash.
    # May not be needed if adapter takes the instance directly for core attributes.
    def to_hash
      {
        id: @id,
        klass: @klass.to_s,
        arguments: @arguments,
        kwargs: @kwargs,
        globals: @globals,
        state: @state # Use the loaded/current state
      }
    end

    # Removed current_state method, using @state directly now.

  end
end

