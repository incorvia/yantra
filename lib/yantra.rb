# frozen_string_literal: true

# lib/yantra.rb
# Main entry point for the Yantra gem.
# Handles requiring core components and dynamically loading configured adapters.

require_relative "yantra/version"
require_relative "yantra/errors"
require_relative "yantra/configuration" # Needed early for configuration access
require_relative "yantra/step"           # Base class for user jobs
require_relative "yantra/workflow"       # Base class for user workflows
require_relative "yantra/client"         # Public API class

# --- Require Core Interfaces and Logic ---
require_relative "yantra/persistence/repository_interface" # Persistence contract
require_relative "yantra/worker/enqueuing_interface"     # Worker contract
require_relative "yantra/core/state_machine"           # State Machine logic
require_relative "yantra/core/orchestrator"            # Core workflow logic
# ... require other core components ...

module Yantra
  # Provides convenient access to the singleton configuration instance.
  # @return [Yantra::Configuration] the configuration instance
  def self.configuration
    Configuration.instance
  end

  # Provides a block syntax for configuring Yantra.
  # @yield [Yantra::Configuration] the configuration instance
  def self.configure
    yield(configuration)
  end

  # Central place to access the configured repository instance.
  # Dynamically loads and instantiates the correct persistence adapter.
  # @return [Object] an instance of the configured persistence adapter.
  def self.repository
    @repository ||= load_adapter(
      config_key:         :persistence_adapter,
      base_path_segment:  'persistence',
      base_module:        Yantra::Persistence,
      interface_module:   Yantra::Persistence::RepositoryInterface,
      adapter_type_name:  'persistence',
      gem_suggestions:    { # Hints for LoadError messages
        active_record: "'activerecord' (and potentially DB driver like 'pg', 'mysql2')",
        redis:         "'redis'"
      }
    )
  end

  # Central place to access the configured worker adapter instance.
  # Dynamically loads and instantiates the correct worker adapter.
  # @return [Object] an instance of the configured worker adapter.
  def self.worker_adapter
    @worker_adapter ||= load_adapter(
      config_key:         :worker_adapter,
      base_path_segment:  'worker',
      base_module:        Yantra::Worker,
      interface_module:   Yantra::Worker::EnqueuingInterface,
      adapter_type_name:  'worker',
      gem_suggestions:    { # Hints for LoadError messages
        active_job: "'activejob' (usually part of Rails)",
        sidekiq:    "'sidekiq'",
        resque:     "'resque'"
      }
    )
  end

  # --- Private Helper Method for Adapter Loading ---
  class << self
    private

    # Dynamically loads and instantiates the configured adapter.
    # @param config_key [Symbol] e.g., :persistence_adapter
    # @param base_path_segment [String] e.g., 'persistence'
    # @param base_module [Module] e.g., Yantra::Persistence
    # @param interface_module [Module] e.g., Yantra::Persistence::RepositoryInterface
    # @param adapter_type_name [String] e.g., 'persistence' (for error messages)
    # @param gem_suggestions [Hash] e.g., { redis: "'redis'" } (for error messages)
    # @return [Object] An instance of the loaded adapter class.
    # @raise [Errors::ConfigurationError] If configuration or loading fails.
    def load_adapter(config_key:, base_path_segment:, base_module:, interface_module:, adapter_type_name:, gem_suggestions:)
      adapter_sym = configuration.send(config_key)&.to_sym
      unless adapter_sym
        raise Errors::ConfigurationError, "Yantra #{adapter_type_name}_adapter not configured via config.#{config_key}"
      end

      # Construct paths and names based on convention: <base>/<type>/adapter.rb
      adapter_require_path = "yantra/#{base_path_segment}/#{adapter_sym}/adapter"
      # Convert :some_adapter to SomeAdapter
      adapter_module_name = adapter_sym.to_s.split('_').map(&:capitalize).join

      # 1. Require the adapter file
      begin
        require adapter_require_path
      rescue LoadError => e
        gem_name = gem_suggestions[adapter_sym] || "'#{adapter_sym}' adapter support gem(s)"
        raise Errors::ConfigurationError,
              "Could not load Yantra #{adapter_type_name} adapter file at '#{adapter_require_path}.rb'. " \
              "Ensure the adapter file exists and the necessary gem(s) #{gem_name} are available " \
              "when configuring the ':#{adapter_sym}' adapter. Original error: #{e.message}"
      end

      # 2. Find the specific adapter module (e.g., Yantra::Persistence::ActiveRecord)
      begin
        adapter_module = base_module.const_get(adapter_module_name)
      rescue NameError
         raise Errors::ConfigurationError, "Could not find Yantra #{adapter_type_name} module '#{base_module}::#{adapter_module_name}' for adapter ':#{adapter_sym}'. Is '#{adapter_require_path}.rb' correctly defining this module?"
      end

      # 3. Find the Adapter class within that module
      begin
         adapter_class = adapter_module.const_get("Adapter")
      rescue NameError
         raise Errors::ConfigurationError, "Could not find Yantra #{adapter_type_name} adapter class 'Adapter' within module '#{base_module}::#{adapter_module_name}' for adapter ':#{adapter_sym}'. Is '#{adapter_require_path}.rb' correctly defining the 'Adapter' class inside the module?"
      end

      # 4. Verify the adapter implements the required interface
      unless adapter_class.include?(interface_module)
         raise Errors::ConfigurationError, "Adapter class '#{adapter_class.name}' does not include the required interface '#{interface_module.name}'."
      end

      # 5. Instantiate the adapter
      adapter_class.new # Or: adapter_class.new(configuration) if adapter needs config
    end

  end # class << self
  # --- End Private Helper ---

end

