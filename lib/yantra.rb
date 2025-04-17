# lib/yantra.rb

require 'zeitwerk'

# # --- Zeitwerk Setup ---
# loader = Zeitwerk::Loader.for_gem
# loader.setup
# # --- END Zeitwerk Setup ---

require_relative "yantra/version"
require_relative "yantra/errors"
require_relative "yantra/configuration"
require_relative "yantra/persistence/repository_interface"
require_relative "yantra/worker/enqueuing_interface"
require_relative "yantra/events/notifier_interface"

module Yantra
  class << self
    attr_writer :configuration

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

    # Internal helper to load and instantiate adapters based on configuration.
    def load_adapter(type, adapter_config, options = {})
      adapter_instance = case adapter_config
                         when Symbol
                           adapter_class = find_adapter_class(type, adapter_config)
                           initialize_adapter(adapter_class, options)
                         when nil
                           raise Yantra::Errors::ConfigurationError, "#{type.capitalize} adapter configuration is nil."
                         else
                           validate_adapter_interface(type, adapter_config) # Validate provided instance
                           adapter_config
                         end

      validate_adapter_interface(type, adapter_instance) # Validate final instance
      adapter_instance

    rescue NameError => e
       class_name_str = adapter_config.is_a?(Symbol) ? adapter_config.to_s.camelize : 'Unknown'
       namespace_str = "Yantra::#{type.capitalize}::#{class_name_str}::Adapter" # Consistent lookup path
       raise Yantra::Errors::ConfigurationError, "Could not find #{type.capitalize} adapter class for ':#{adapter_config}' configuration (tried resolving #{namespace_str}). Autoloading issue or incorrect configuration? #{e.message}"
    rescue ArgumentError => e
       adapter_class_name = adapter_config.is_a?(Symbol) ? find_adapter_class(type, adapter_config).name : adapter_config.class.name rescue adapter_config.inspect
       raise Yantra::Errors::ConfigurationError, "Error initializing #{type.capitalize} adapter '#{adapter_class_name}' with options #{options.inspect}. Check initializer arguments. Original error: #{e.message}"
    rescue LoadError => e
       raise Yantra::Errors::ConfigurationError, "Failed to load dependency for #{type.capitalize} adapter ':#{adapter_config}'. Is the required gem installed? Original error: #{e.message}"
    end

    # Initializes an adapter class with options, handling different initializer arities.
    def initialize_adapter(adapter_class, options)
       # Check arity more carefully: arity == 0 means no args, arity < 0 means optional/keyword args
       init_arity = adapter_class.instance_method(:initialize).arity
       if options && !options.empty? && init_arity != 0 # Pass options if given AND initializer accepts args
          adapter_class.new(**options)
       else
          adapter_class.new
       end
    end

    # Validates that the loaded adapter instance conforms to the expected interface.
    def validate_adapter_interface(type, instance)
       interface_module = case type
                          when :persistence then Persistence::RepositoryInterface
                          when :worker then Worker::EnqueuingInterface
                          when :notifier then Events::NotifierInterface
                          else raise ArgumentError, "Unknown adapter type: #{type}"
                          end

       unless instance.is_a?(interface_module)
         required_method = case type
                           when :persistence then :find_workflow
                           when :worker then :enqueue
                           when :notifier then :publish
                           end
         unless instance.respond_to?(required_method)
            raise Yantra::Errors::ConfigurationError, "Invalid #{type} adapter configured. Instance of #{instance.class} does not implement the required interface (#{interface_module.name} or respond to ##{required_method})."
         end
         puts "WARN: [Yantra.validate_adapter_interface] Configured #{type} adapter #{instance.class} does not explicitly include #{interface_module.name}, but responds to ##{required_method}."
       end
       # Add return true for clarity if validation passes
       true
    end


    # *** UPDATED: Simplified and Consistent Adapter Lookup ***
    # Internal helper to find the adapter class constant based on type and name.
    def find_adapter_class(type, name)
      base_module = case type
                    when :persistence then Yantra::Persistence
                    when :worker then Yantra::Worker
                    when :notifier then Yantra::Events # Base module for notifiers
                    else raise ArgumentError, "Unknown adapter type: #{type}"
                    end

      # Consistent naming convention: Yantra::<Type>::<Name>::Adapter
      class_name_part = name.to_s.camelize
      full_class_name = "#{base_module.name}::#{class_name_part}::Adapter"

      begin
        Object.const_get(full_class_name)
      rescue NameError => e
         # Reraise with a more informative message if lookup fails
         raise Yantra::Errors::ConfigurationError, "Cannot find adapter class #{full_class_name} for type :#{type}, name :#{name}. #{e.message}"
      end
    end
    # *** END UPDATE ***

    # Simple camelize fallback if ActiveSupport is not available
    unless String.method_defined?(:camelize)
      puts "WARN: ActiveSupport::Inflector not available. Using basic String#camelize fallback."
      class ::String; def camelize; self.split('_').map(&:capitalize).join; end; end
    end

  end # class << self
end # module Yantra

# Initialize default configuration
Yantra.configuration

