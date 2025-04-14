# frozen_string_literal: true

# lib/yantra.rb
# Main entry point for the Yantra gem.
# Handles requiring core components and dynamically loading configured adapters.

require_relative "yantra/version"
require_relative "yantra/errors"
require_relative "yantra/configuration" # Needed early for configuration access
require_relative "yantra/job"           # Base class for user jobs
require_relative "yantra/workflow"       # Base class for user workflows
# require_relative "yantra/client"         # Public API class

# --- Require Core Interfaces and Logic ---
# These are essential parts of the Yantra framework itself.
require_relative "yantra/persistence/repository_interface" # Persistence contract
# require_relative "yantra/worker/enqueuing_interface"       # Worker contract
# require_relative "yantra/core/orchestrator"            # Core workflow logic
# require_relative "yantra/core/state_machine"         # If defined separately
# ... require other core components ...

# --- DO NOT require specific adapters here ---
# Specific adapters (Redis, ActiveRecord, etc.) are loaded dynamically
# based on the user's configuration to keep the core gem decoupled.

module Yantra
  # Provides convenient access to the singleton configuration instance.
  # @return [Yantra::Configuration] the configuration instance
  def self.configuration
    Configuration.instance
  end

  # Provides a block syntax for configuring Yantra.
  # @yield [Yantra::Configuration] the configuration instance
  # @example
  #   Yantra.configure do |config|
  #     config.persistence_adapter = :redis
  #     config.redis_url = "redis://localhost:6379/1"
  #   end
  def self.configure
    yield(configuration)
  end

  # Central place to access the configured repository instance.
  # Dynamically loads and instantiates the correct persistence adapter
  # based on the configuration. Memoizes the instance.
  #
  # @raise [Errors::ConfigurationError] if adapter is not configured,
  #   if the adapter file cannot be loaded, or if the expected
  #   module/class structure is not found.
  # @return [Object] an instance of the configured persistence adapter,
  #   which should include Yantra::Persistence::RepositoryInterface.
  def self.repository
    # Memoize the instance for performance
    @repository ||= begin
      adapter_sym = configuration.persistence_adapter&.to_sym
      unless adapter_sym
        raise Errors::ConfigurationError, "Yantra persistence_adapter not configured."
      end

      # Construct paths and names based on the structure:
      # persistence/<adapter_type>/adapter.rb
      # module Yantra::Persistence::<AdapterTypeModule>::Adapter
      adapter_require_path = "yantra/persistence/#{adapter_sym}/adapter"
      # Convert symbol like :active_record to module name "ActiveRecord"
      adapter_module_name = adapter_sym.to_s.split('_').map(&:capitalize).join

      begin
        # 1. Attempt to load the specific adapter file ONLY when needed.
        #    Example: require "yantra/persistence/active_record/adapter"
        require adapter_require_path
      rescue LoadError => e
        # 2. Handle failure: Could be missing adapter file or missing dependency gem.
        gem_name = case adapter_sym
                   when :active_record then "'activerecord' (and potentially DB driver like 'pg', 'mysql2')"
                   when :redis then "'redis'"
                   else "'#{adapter_sym}' adapter support gem(s)"
                   end
        raise Errors::ConfigurationError,
              "Could not load Yantra persistence adapter file at '#{adapter_require_path}.rb'. " \
              "Ensure the adapter file exists and the necessary gem(s) #{gem_name} are in your Gemfile " \
              "when configuring the ':#{adapter_sym}' adapter. Original error: #{e.message}"
      end

      begin
        # 3. Get the adapter module (e.g., Yantra::Persistence::ActiveRecord)
        #    Must be defined within the Yantra::Persistence namespace.
        adapter_module = Yantra::Persistence.const_get(adapter_module_name)
      rescue NameError
         raise Errors::ConfigurationError, "Could not find Yantra persistence module 'Yantra::Persistence::#{adapter_module_name}' for adapter ':#{adapter_sym}'. Is '#{adapter_require_path}.rb' correctly defining this module?"
      end

      begin
         # 4. Get the adapter class within the module (must be named 'Adapter')
         #    Example: Yantra::Persistence::ActiveRecord::Adapter
         adapter_class = adapter_module.const_get("Adapter")
      rescue NameError
         raise Errors::ConfigurationError, "Could not find Yantra persistence adapter class 'Adapter' within module 'Yantra::Persistence::#{adapter_module_name}' for adapter ':#{adapter_sym}'. Is '#{adapter_require_path}.rb' correctly defining the 'Adapter' class inside the module?"
      end

      # 5. Instantiate the adapter. Pass configuration if the adapter's
      #    initializer requires it.
      adapter_class.new # Or: adapter_class.new(configuration)
    end
  end

  # Placeholder for similar dynamic loading logic for the worker adapter,
  # if needed and configured similarly.
  # def self.worker_adapter
  #   @worker_adapter ||= begin
  #     # ... similar dynamic loading logic based on config.worker_adapter ...
  #   end
  # end

end

