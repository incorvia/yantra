# lib/yantra/workflow.rb

require 'securerandom'
require 'set' # Needed for Set in calculate_terminal_status!
require_relative 'step'
require_relative 'errors'

module Yantra
  # Base class for defining Yantra workflows.
  # Subclasses should implement the #perform method to define the
  # Directed Acyclic Graph (DAG) of steps using the `run` DSL method.
  class Workflow
    # Attributes accessible after initialization
    attr_reader :id, :klass, :arguments, :kwargs, :globals, :steps, :dependencies
    # Add state reader if loading state during rehydration
    attr_reader :state

    # Initializes a new workflow instance.
    # Runs the subclass's #perform method to build the step graph and then
    # calculates terminal step statuses.
    #
    # @param args [Array] Positional arguments passed to the workflow perform method (on create).
    # @param kwargs [Hash] Keyword arguments passed to the workflow perform method (on create).
    # @option kwargs [Hash] :globals Global configuration or data accessible to all steps.
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
      @steps = [] # Holds Yantra::Step instances created by `run`
      @step_lookup = {} # Maps internal DSL name/ref to Yantra::Step instance
      @dependencies = {} # Maps step.id => [dependency_step_id, ...]
      @step_name_counts = Hash.new(0) # <<< ADDED: Track counts for base names

      # Rehydrate state if loaded from persistence
      if internal_state[:persisted]
        # Note: steps/Deps are NOT loaded by default anymore

      end

      # Run perform and calculate terminal status ONLY on initial creation,
      # NOT during rehydration from persistence.
      if !internal_state[:persisted] && !internal_state[:skip_setup]
        # Call the subclass's perform method, passing through original args/kwargs
        perform(*@arguments, **@kwargs)
        # Calculate terminal status immediately after the graph is built
      end
    end

    # Entry point for defining the workflow DAG.
    # Subclasses MUST implement this method using the `run` DSL.
    def perform(*args, **kwargs)
      raise NotImplementedError, "#{self.class.name} must implement the `perform` method."
    end

    # DSL method to define a step within the workflow.
    #
    # @param step_klass [Class] The Yantra::Step subclass to run.
    # @param params [Hash] Arguments/payload to pass to the step's perform method.
    # @param after [Array<String, Symbol, Class>] References to steps that must complete first.
    # @param name [String, Symbol] An optional unique name/reference for this step within the workflow DSL.
    # @return [Symbol] The unique reference symbol/name assigned to the step in the DSL.
    def run(step_klass, params: {}, after: [], name: nil)
      unless step_klass.is_a?(Class) && step_klass < Yantra::Step
        raise ArgumentError, "#{step_klass} must be a Class inheriting from Yantra::Step"
      end

      # --- UPDATED Name Collision Logic ---
      base_ref_name = (name || step_klass.to_s).to_s
      step_ref_name = base_ref_name
      current_count = @step_name_counts[base_ref_name]

      if current_count > 0 # Base name has been used before
         step_ref_name = "#{base_ref_name}_#{current_count}"
      end
      # Ensure generated name is also unique if user provides explicit conflicting name
      # e.g. run StepA; run StepB, name: "StepA_1" <- could conflict
      while @step_lookup.key?(step_ref_name)
          current_count += 1
          step_ref_name = "#{base_ref_name}_#{current_count}"
          # Safety break, should not happen with incrementing count unless > MAX_INT steps
          break if current_count > 1_000_000
      end
      # Increment count for the base name for the *next* time it's used
      @step_name_counts[base_ref_name] += 1
      # --- END UPDATED Name Collision Logic ---


      step_id = SecureRandom.uuid
      step = step_klass.new(
        id: step_id,
        workflow_id: @id,
        klass: step_klass,
        arguments: params,
        dsl_name: step_ref_name, # Store the potentially modified, unique name
      )

      @steps << step
      @step_lookup[step_ref_name] = step # Store using the unique name

      # Resolve dependencies based on their DSL reference names
      dependency_ids = Array(after).flatten.map do |dep_ref|
        dep_step = find_step_by_ref(dep_ref.to_s)
        unless dep_step
          raise Yantra::Errors::DependencyNotFound, "Dependency '#{dep_ref}' not found for step '#{step_ref_name}'."
        end
        dep_step.id
      end

      @dependencies[step.id] = dependency_ids unless dependency_ids.empty?

      # Return the unique reference name (as a symbol)
      step_ref_name.to_sym
    end

    # Finds a step instance within this workflow based on the reference name used in the DSL.
    # @param ref_name [String] The reference name.
    # @return [Yantra::Step, nil] The found step instance or nil.
    def find_step_by_ref(ref_name)
      @step_lookup[ref_name]
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

