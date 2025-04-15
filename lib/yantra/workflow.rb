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
    # Runs the subclass's #perform method to build the job graph and then
    # calculates terminal job statuses.
    #
    # @param args [Array] Positional arguments passed to the workflow perform method (on create).
    # @param kwargs [Hash] Keyword arguments passed to the workflow perform method (on create).
    # @option kwargs [Hash] :globals Global configuration or data accessible to all jobs.
    # @option kwargs [Hash] :internal_state Used internally for reconstruction. Avoid manual use.
    def initialize(*args, **kwargs)
      internal_state = kwargs.delete(:internal_state) || {}
      @globals = internal_state.fetch(:globals, kwargs.delete(:globals) || {}) # Load globals

      @id = internal_state.fetch(:id, SecureRandom.uuid)
      @klass = internal_state.fetch(:klass, self.class)
      @arguments = internal_state.fetch(:arguments, args) # Load positional args
      @kwargs = internal_state.fetch(:kwargs, kwargs)  # Load keyword args
      @state = internal_state.fetch(:state, :pending).to_sym # Load state, default pending

      # Internal tracking during DAG definition
      @jobs = [] # Holds Yantra::Job instances created by `run`
      @job_lookup = {} # Maps internal DSL name/ref to Yantra::Job instance
      @dependencies = {} # Maps job.id => [dependency_job_id, ...]
      @job_name_counts = Hash.new(0) # <<< ADDED: Track counts for base names

      # Rehydrate state if loaded from persistence
      if internal_state[:persisted]
        # Note: Jobs/Deps are NOT loaded by default anymore
        puts "INFO: Rehydrated workflow #{@id} (State: #{@state}). Jobs/Deps not loaded into memory."
      end

      # Run perform and calculate terminal status ONLY on initial creation,
      # NOT during rehydration from persistence.
      if !internal_state[:persisted] && !internal_state[:skip_setup]
        # Call the subclass's perform method, passing through original args/kwargs
        perform(*@arguments, **@kwargs)
        # Calculate terminal status immediately after the graph is built
        calculate_terminal_status!
      end
    end

    # Entry point for defining the workflow DAG.
    # Subclasses MUST implement this method using the `run` DSL.
    def perform(*args, **kwargs)
      raise NotImplementedError, "#{self.class.name} must implement the `perform` method."
    end

    # DSL method to define a job within the workflow.
    #
    # @param job_klass [Class] The Yantra::Job subclass to run.
    # @param params [Hash] Arguments/payload to pass to the job's perform method.
    # @param after [Array<String, Symbol, Class>] References to jobs that must complete first.
    # @param name [String, Symbol] An optional unique name/reference for this job within the workflow DSL.
    # @return [Symbol] The unique reference symbol/name assigned to the job in the DSL.
    def run(job_klass, params: {}, after: [], name: nil)
      unless job_klass.is_a?(Class) && job_klass < Yantra::Job
        raise ArgumentError, "#{job_klass} must be a Class inheriting from Yantra::Job"
      end

      # --- UPDATED Name Collision Logic ---
      base_ref_name = (name || job_klass.to_s).to_s
      job_ref_name = base_ref_name
      current_count = @job_name_counts[base_ref_name]

      if current_count > 0 # Base name has been used before
         job_ref_name = "#{base_ref_name}_#{current_count}"
      end
      # Ensure generated name is also unique if user provides explicit conflicting name
      # e.g. run JobA; run JobB, name: "JobA_1" <- could conflict
      while @job_lookup.key?(job_ref_name)
          current_count += 1
          job_ref_name = "#{base_ref_name}_#{current_count}"
          # Safety break, should not happen with incrementing count unless > MAX_INT jobs
          break if current_count > 1_000_000
      end
      # Increment count for the base name for the *next* time it's used
      @job_name_counts[base_ref_name] += 1
      # --- END UPDATED Name Collision Logic ---


      job_id = SecureRandom.uuid
      job = job_klass.new(
        id: job_id,
        workflow_id: @id,
        klass: job_klass,
        arguments: params,
        dsl_name: job_ref_name, # Store the potentially modified, unique name
        is_terminal: false # Default, calculated later by calculate_terminal_status!
      )

      @jobs << job
      @job_lookup[job_ref_name] = job # Store using the unique name

      # Resolve dependencies based on their DSL reference names
      dependency_ids = Array(after).map do |dep_ref|
        dep_job = find_job_by_ref(dep_ref.to_s)
        unless dep_job
          raise Yantra::Errors::DependencyNotFound, "Dependency '#{dep_ref}' not found for job '#{job_ref_name}'."
        end
        dep_job.id
      end

      @dependencies[job.id] = dependency_ids unless dependency_ids.empty?

      # Return the unique reference name (as a symbol)
      job_ref_name.to_sym
    end

    # Finds a job instance within this workflow based on the reference name used in the DSL.
    # @param ref_name [String] The reference name.
    # @return [Yantra::Job, nil] The found job instance or nil.
    def find_job_by_ref(ref_name)
      @job_lookup[ref_name]
    end

    # Calculates which jobs are terminal and updates their instance variable.
    # Called automatically by initialize after perform completes.
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
    def to_hash
      {
        id: @id,
        klass: @klass.to_s,
        arguments: @arguments,
        kwargs: @kwargs,
        globals: @globals,
        state: @state
      }
    end

    # Removed current_state method, using @state directly now.

  end
end

