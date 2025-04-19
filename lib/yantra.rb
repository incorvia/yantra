# lib/yantra.rb

require_relative "yantra/version"
require_relative "yantra/errors"
require_relative "yantra/configuration"
require_relative "yantra/persistence/repository_interface"
require_relative "yantra/worker/enqueuing_interface"
require_relative "yantra/events/notifier_interface"

module Yantra
  class << self
    attr_writer :configuration

    BUILTIN_ADAPTER_PATHS = {
      persistence: {
        active_record: 'yantra/persistence/active_record/adapter'
      }.freeze,
      worker: {
        active_job: 'yantra/worker/active_job/adapter'
      }.freeze,
      notifier: {
        null: 'yantra/events/null/adapter',
        logger: 'yantra/events/logger/adapter',
        active_support_notifications: 'yantra/events/active_support_notifications/adapter'
      }.freeze
    }.freeze

    def configuration
      @configuration ||= Configuration.new
    end

    def configure
      yield(configuration)
    end

    def logger
      @logger || configuration.logger
    end
    attr_writer :logger

    def repository
      @repository ||= load_adapter(
        :persistence,
        configuration.persistence_adapter,
        configuration.persistence_options
      )
    end

    def worker_adapter
      @worker_adapter ||= load_adapter(
        :worker,
        configuration.worker_adapter,
        configuration.worker_adapter_options
      )
    end

    def notifier
      @notifier ||= load_adapter(
        :notifier,
        configuration.notification_adapter,
        configuration.notification_adapter_options
      )
    end

    private

    def load_adapter(type, adapter_config, options = {})
      if adapter_config.is_a?(Symbol)
        require_path = BUILTIN_ADAPTER_PATHS.dig(type, adapter_config)
        if require_path
          begin
            require_relative require_path
            logger&.debug { "[Yantra.load_adapter] Loaded built-in adapter: #{require_path}" }
          rescue LoadError => e
            msg = "[Yantra.load_adapter] Could not load adapter :#{adapter_config} (#{require_path}): #{e.message}"
            log_error(msg)
            raise Yantra::Errors::ConfigurationError, "Could not load required file for ':#{adapter_config}'. #{e.message}"
          end
        else
          logger&.debug { "[Yantra.load_adapter] Unknown built-in adapter :#{adapter_config} for #{type}, assuming custom." }
        end
      end

      adapter_instance = case adapter_config
                         when Symbol
                           klass = find_adapter_class(type, adapter_config)
                           initialize_adapter(klass, options)
                         when Class
                           validate_adapter_interface(type, adapter_config)
                           initialize_adapter(adapter_config, options)
                         when nil
                           raise Yantra::Errors::ConfigurationError, "#{type.capitalize} adapter configuration is nil."
                         else
                           validate_adapter_interface(type, adapter_config)
                           adapter_config
                         end

      validate_adapter_interface(type, adapter_instance)
      adapter_instance

    rescue Yantra::Errors::ConfigurationError => e
      raise e
    rescue ArgumentError => e
      name = adapter_config.is_a?(Symbol) ? adapter_config.to_s.camelize : adapter_config.class.name rescue adapter_config.inspect
      raise Yantra::Errors::ConfigurationError, "Error initializing #{type} adapter '#{name}' with #{options.inspect}: #{e.message}"
    rescue LoadError => e
      raise Yantra::Errors::ConfigurationError, "Failed to load dependency for #{type} adapter ':#{adapter_config}': #{e.message}"
    end

    def initialize_adapter(klass, options)
      arity = klass.instance_method(:initialize).arity
      if options && !options.empty? && arity != 0
        arity < 0 || arity >= 1 ? klass.new(**options) : klass.new(options)
      else
        klass.new
      end
    end

    def validate_adapter_interface(type, obj)
      mod = case type
            when :persistence then Persistence::RepositoryInterface
            when :worker      then Worker::EnqueuingInterface
            when :notifier    then Events::NotifierInterface
            else raise ArgumentError, "Unknown adapter type: #{type}"
            end

      instance = obj.is_a?(Class) ? obj.allocate : obj

      unless instance.is_a?(mod)
        method_name = {
          persistence: :find_workflow,
          worker: :enqueue,
          notifier: :publish
        }[type]

        unless instance.respond_to?(method_name)
          raise Yantra::Errors::ConfigurationError,
                "Invalid #{type} adapter. #{obj.inspect} does not include #{mod.name} or respond to ##{method_name}."
        end

        log_warn("[Yantra.validate_adapter_interface] #{type} adapter #{obj.inspect} does not include #{mod.name}, but responds to ##{method_name}.")
      end

      true
    end

    def find_adapter_class(type, name)
      base = {
        persistence: Yantra::Persistence,
        worker: Yantra::Worker,
        notifier: Yantra::Events
      }[type] || raise(ArgumentError, "Unknown adapter type: #{type}")

      mod_name = name.to_s.camelize
      full_name = "#{base.name}::#{mod_name}::Adapter"

      begin
        base.const_get(mod_name).const_get("Adapter")
      rescue NameError => e
        raise Yantra::Errors::ConfigurationError,
              "Cannot find adapter class #{full_name} for type :#{type}, name :#{name}: #{e.message}"
      end
    end

    def log_warn(msg)
      logger&.warn { msg } || warn("WARN: #{msg}")
    end

    def log_error(msg)
      logger&.error { msg } || warn("ERROR: #{msg}")
    end

    unless String.method_defined?(:camelize)
      class ::String
        def camelize
          split('_').map(&:capitalize).join
        end unless method_defined?(:camelize)
      end

      unless defined?(@_camelize_warned)
        warn("[Yantra] ActiveSupport not found. Using simple camelize fallback.") unless ENV["YANTRA_SILENT"]
        @_camelize_warned = true
      end
    end
  end
end

Yantra.configuration

