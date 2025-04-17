# lib/yantra.rb

# --- Core Requires (No Zeitwerk) ---
require_relative "yantra/version"
require_relative "yantra/errors"
require_relative "yantra/configuration"
require_relative "yantra/persistence/repository_interface"
require_relative "yantra/worker/enqueuing_interface"
require_relative "yantra/events/notifier_interface"

# Note: Built-in adapter files are now required conditionally
# within the load_adapter method below using the BUILTIN_ADAPTER_PATHS map.

module Yantra
  class << self
    attr_writer :configuration

    # --- START: Definition of Built-in Adapter Map ---
    # Defines the mapping between adapter symbols and their relative file paths.
    # This map is used for conditionally loading built-in adapters.
    BUILTIN_ADAPTER_PATHS = {
      persistence: {
        active_record: 'yantra/persistence/active_record/adapter'
        # Add other built-in persistence adapters here when created
        # redis: 'yantra/persistence/redis/adapter'
      }.freeze,
      worker: {
        active_job: 'yantra/worker/active_job/adapter'
        # Add other built-in worker adapters here when created
        # sidekiq: 'yantra/worker/sidekiq/adapter'
      }.freeze,
      notifier: {
        null: 'yantra/events/null/adapter',
        logger: 'yantra/events/logger/adapter',
        active_support_notifications: 'yantra/events/active_support_notifications/adapter'
        # Add other built-in notifier adapters here when created
      }.freeze
    }.freeze
    # --- END: Definition of Built-in Adapter Map ---


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

    # Memoized accessor for the persistence adapter instance.
    def repository
      @repository ||= load_adapter(
        :persistence,
        configuration.persistence_adapter,
        configuration.persistence_options
      )
    end

    # Memoized accessor for the worker adapter instance.
    def worker_adapter
      @worker_adapter ||= load_adapter(
        :worker,
        configuration.worker_adapter,
        configuration.worker_adapter_options
      )
    end

    # Memoized accessor for the notifier adapter instance.
    def notifier
      @notifier ||= load_adapter(
        :notifier,
        configuration.notification_adapter,
        configuration.notification_adapter_options
      )
    end

    private

    # Internal helper to load and instantiate adapters based on configuration.
    # Conditionally requires built-in adapters when first requested via symbol, using a map.
    def load_adapter(type, adapter_config, options = {})
      # --- Conditional Require for Built-in Symbol Adapters (Using Map) ---
      if adapter_config.is_a?(Symbol)
        # Look up the path in our predefined map
        require_path = BUILTIN_ADAPTER_PATHS.dig(type, adapter_config)

        if require_path
          begin
            # Use require_relative based on the current file's location (lib/yantra.rb)
            require_relative require_path
            Yantra.logger.debug { "[Yantra.load_adapter] Conditionally loaded built-in adapter file: #{require_path}" } if Yantra.logger
          rescue LoadError => e
            log_msg = "[Yantra.load_adapter] Failed to load built-in adapter file '#{require_path}' for :#{adapter_config}. Error: #{e.message}"
            if defined?(Yantra.logger) && Yantra.logger
               Yantra.logger.error { log_msg }
            else
               puts "ERROR: #{log_msg}"
            end
            raise Yantra::Errors::ConfigurationError, "Could not load required file for built-in adapter ':#{adapter_config}'. #{e.message}"
          end
        else
           # Symbol not found in our built-in map, assume it's custom.
           Yantra.logger.debug { "[Yantra.load_adapter] Adapter symbol :#{adapter_config} is not a known built-in for type :#{type}. Assuming custom adapter (user must require file)." } if Yantra.logger
        end
      end
      # --- End Conditional Require ---

      # --- Find/Initialize Adapter Instance (Remains the same) ---
      adapter_instance = case adapter_config
                         when Symbol
                           adapter_class = find_adapter_class(type, adapter_config)
                           initialize_adapter(adapter_class, options)
                         when nil
                           raise Yantra::Errors::ConfigurationError, "#{type.capitalize} adapter configuration is nil."
                         else
                           validate_adapter_interface(type, adapter_config)
                           adapter_config
                         end

      # Validate the final instance conforms to the interface (Remains the same)
      validate_adapter_interface(type, adapter_instance)
      adapter_instance

    # --- Error Handling (Remains the same) ---
    rescue Yantra::Errors::ConfigurationError => e
       raise e
    rescue ArgumentError => e
       adapter_class_name = adapter_config.is_a?(Symbol) ? adapter_config.to_s.camelize : adapter_config.class.name rescue adapter_config.inspect
       raise Yantra::Errors::ConfigurationError, "Error initializing #{type.capitalize} adapter '#{adapter_class_name}' with options #{options.inspect}. Check initializer arguments. Original error: #{e.message}"
    rescue LoadError => e
       raise Yantra::Errors::ConfigurationError, "Failed to load dependency for #{type.capitalize} adapter ':#{adapter_config}'. Is the required underlying gem (e.g., 'activerecord', 'activejob') installed? Original error: #{e.message}"
    end

    # Initializes an adapter class with options (Remains the same)
    def initialize_adapter(adapter_class, options)
       init_arity = adapter_class.instance_method(:initialize).arity
       if options && !options.empty? && (init_arity != 0)
          if init_arity < 0 || init_arity >= 1
             adapter_class.new(**options)
          else
             adapter_class.new(options)
          end
       else
          adapter_class.new
       end
    end

    # Validates that the loaded adapter instance conforms to the expected interface (Remains the same)
    def validate_adapter_interface(type, instance)
       interface_module = case type
                          when :persistence then Persistence::RepositoryInterface
                          when :worker then Worker::EnqueuingInterface
                          when :notifier then Events::NotifierInterface
                          else raise ArgumentError, "Unknown adapter type: #{type}"
                          end
       object_to_check = instance.is_a?(Class) ? instance.allocate : instance
       unless object_to_check.is_a?(interface_module)
         required_method = case type
                           when :persistence then :find_workflow
                           when :worker then :enqueue
                           when :notifier then :publish
                           end
         unless object_to_check.respond_to?(required_method)
            raise Yantra::Errors::ConfigurationError, "Invalid #{type} adapter configured. Instance/Class #{instance.inspect} does not implement the required interface (#{interface_module.name} or respond to ##{required_method})."
         end
         log_msg = "[Yantra.validate_adapter_interface] Configured #{type} adapter #{instance.inspect} does not explicitly include #{interface_module.name}, but responds to ##{required_method}."
         if defined?(Yantra.logger) && Yantra.logger
            Yantra.logger.warn { log_msg }
         else
            puts "WARN: #{log_msg}"
         end
       end
       true
    end


    # Internal helper to find the adapter class constant (Remains the same)
    def find_adapter_class(type, name)
      base_module = case type
                    when :persistence then Yantra::Persistence
                    when :worker then Yantra::Worker
                    when :notifier then Yantra::Events
                    else raise ArgumentError, "Unknown adapter type: #{type}"
                    end
      adapter_sub_namespace_name = name.to_s.camelize
      adapter_class_name = "Adapter"
      full_class_name_for_error = "#{base_module.name}::#{adapter_sub_namespace_name}::#{adapter_class_name}"
      begin
        adapter_module = base_module.const_get(adapter_sub_namespace_name)
        adapter_module.const_get(adapter_class_name)
      rescue NameError => e
         raise Yantra::Errors::ConfigurationError, "Cannot find adapter class #{full_class_name_for_error} for type :#{type}, name :#{name}. If this is a custom adapter, ensure its file is required before Yantra is configured/used. Original error: #{e.message}"
      end
    end

    # Simple camelize fallback (Remains the same)
    unless String.method_defined?(:camelize)
       class ::String
          def camelize
             self.split('_').map(&:capitalize).join
          end unless method_defined?(:camelize)
       end
       @_camelize_warning_logged ||= begin
          log_msg = "[Yantra] WARN: ActiveSupport::Inflector not available. Using basic String#camelize fallback."
          if defined?(Yantra.logger) && Yantra.logger
             Yantra.logger.warn { log_msg }
          else
             puts log_msg
          end
          true
       end
    end

  end # class << self
end # module Yantra

# Initialize default configuration when file is loaded
Yantra.configuration

