# lib/yantra/step.rb

require 'securerandom'
require 'active_support/core_ext/string/inflections'

module Yantra
  # Base class for defining steps in a workflow.
  class Step
    attr_reader :id, :workflow_id, :arguments, :queue_name, :parent_ids, :klass, :dsl_name
    attr_reader :repository

    def initialize(
      workflow_id:,
      klass:,
      arguments: {},
      step_id: nil,
      queue_name: nil,
      parent_ids: [],
      dsl_name: nil,
      repository: nil,
      **_options
    )
      @id           = step_id || SecureRandom.uuid
      @workflow_id  = workflow_id
      @arguments    = arguments || {}
      @klass        = klass
      @queue_name   = queue_name || default_queue_name
      @parent_ids   = parent_ids || []
      @dsl_name     = dsl_name
      @repository   = repository
      @_parent_outputs_cache = nil
    end

    def perform
      raise NotImplementedError, "#{self.class.name} must implement the `perform` method."
    end

    def parent_outputs
      return @_parent_outputs_cache if defined?(@_parent_outputs_cache) && @_parent_outputs_cache

      ids_to_fetch = if @parent_ids.any?
        log_debug "[Step#parent_outputs] Using provided parent_ids: #{@parent_ids.inspect}"
        @parent_ids
      else
        unless @id && repository&.respond_to?(:get_dependency_ids)
          log_warn "[Step#parent_outputs] Missing step ID or unsupported repository interface."
          return @_parent_outputs_cache = {}
        end

        begin
          repository.get_dependency_ids(@id).tap do |ids|
            log_debug "[Step#parent_outputs] Dynamically fetched parent IDs: #{ids.inspect}"
          end
        rescue => e
          log_error "[Step#parent_outputs] Error fetching parent IDs: #{e.message}"
          return @_parent_outputs_cache = {}
        end
      end

      return @_parent_outputs_cache = {} if ids_to_fetch.empty?

      unless repository&.respond_to?(:get_step_outputs)
        log_error "[Step#parent_outputs] Repository does not support get_step_outputs."
        return @_parent_outputs_cache = {}
      end

      begin
        fetched_outputs = repository.get_step_outputs(ids_to_fetch)
        log_debug "[Step#parent_outputs] Outputs fetched: #{fetched_outputs.inspect}"
        @_parent_outputs_cache = fetched_outputs || {}
      rescue => e
        log_error "[Step#parent_outputs] Error fetching outputs: #{e.message}"
        @_parent_outputs_cache = {}
      end
    end

    def default_queue_name
      @klass ? "#{@klass.name.underscore}_queue" : "yantra_default_queue"
    end

    def name
      dsl_name&.to_s || klass&.name || "UnknownStep"
    end

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

    def logger
      Yantra.logger
    rescue NameError
      @_fallback_logger ||= Logger.new($stdout, level: Logger::INFO)
    end

    def repository
      @repository || Yantra.configuration&.persistence_adapter
    rescue NameError
      @repository
    end

    private

    def log_debug(msg)
      logger&.debug { msg }
    end

    def log_warn(msg)
      logger&.warn { msg }
    end

    def log_error(msg)
      logger&.error { msg }
    end
  end
end

