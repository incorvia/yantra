# lib/yantra/workflow.rb

require 'securerandom'
require 'set'
require_relative 'step'
require_relative 'errors'

module Yantra
  # Base class for defining Yantra workflows.
  # Subclasses must implement `#perform` to define steps via the `run` DSL method.
  class Workflow
    attr_reader :id, :klass, :arguments, :kwargs, :globals, :steps, :dependencies, :state

    def initialize(*args, **kwargs)
      internal_state = kwargs.delete(:internal_state) || {}
      @globals       = internal_state.fetch(:globals, kwargs.delete(:globals) || {})
      @id            = internal_state.fetch(:id, SecureRandom.uuid)
      @klass         = internal_state.fetch(:klass, self.class)
      @arguments     = internal_state.fetch(:arguments, args)
      @kwargs        = internal_state.fetch(:kwargs, kwargs)
      @state         = internal_state.fetch(:state, :pending).to_sym

      @steps             = []
      @step_lookup       = {}
      @dependencies      = {}
      @step_name_counts  = Hash.new(0)

      unless internal_state[:persisted] || internal_state[:skip_setup]
        perform(*@arguments, **@kwargs)
      end
    end

    def perform(*, **)
      raise NotImplementedError, "#{self.class.name} must implement the `perform` method."
    end

    # Defines a step in the workflow.
    #
    # @param step_klass [Class] subclass of Yantra::Step
    # @param params [Hash] arguments for the step
    # @param after [Array] dependencies (by name)
    # @param name [String, Symbol] optional name for the step
    # @return [Symbol] the reference name for the step
    def run(step_klass, params: {}, after: [], name: nil)
      unless step_klass.is_a?(Class) && step_klass < Yantra::Step
        raise ArgumentError, "#{step_klass} must be a subclass of Yantra::Step"
      end

      base_ref_name = (name || step_klass.to_s).to_s
      step_ref_name = base_ref_name
      current_count = @step_name_counts[base_ref_name]

      step_ref_name = "#{base_ref_name}_#{current_count}" if current_count > 0

      while @step_lookup.key?(step_ref_name)
        current_count += 1
        step_ref_name = "#{base_ref_name}_#{current_count}"
        break if current_count > 1_000_000
      end

      @step_name_counts[base_ref_name] += 1

      step_id = SecureRandom.uuid
      step = step_klass.new(
        id: step_id,
        workflow_id: @id,
        klass: step_klass,
        arguments: params,
        dsl_name: step_ref_name
      )

      @steps << step
      @step_lookup[step_ref_name] = step

      dependency_ids = Array(after).flatten.map do |ref|
        dep = find_step_by_ref(ref.to_s)
        raise Yantra::Errors::DependencyNotFound, "Dependency '#{ref}' not found for step '#{step_ref_name}'." unless dep
        dep.id
      end

      @dependencies[step.id] = dependency_ids unless dependency_ids.empty?
      step_ref_name.to_sym
    end

    # Looks up a step by its reference name.
    def find_step_by_ref(ref_name)
      @step_lookup[ref_name]
    end

    # Serializes workflow metadata.
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
  end
end

