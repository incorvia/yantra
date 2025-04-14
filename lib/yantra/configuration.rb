# lib/yantra/configuration.rb

require 'logger'
require 'singleton' # Using stdlib Singleton for simplicity

module Yantra
  # Manages Yantra configuration options.
  # Provides a singleton instance accessible via `Yantra.configuration`.
  # Configuration is typically done via an initializer using `Yantra.configure`.
  #
  # Example:
  #   Yantra.configure do |config|
  #     config.persistence_adapter = :sql
  #     config.sql_connection_details = ENV['DATABASE_URL']
  #     config.redis_namespace = 'my_app_flow'
  #     config.logger.level = Logger::DEBUG
  #   end
  class Configuration
    include Singleton # Ensures only one instance of configuration exists

    # --- Available Configuration Options ---

    # Specifies the persistence adapter to use. The core library will
    # attempt to load and instantiate the corresponding adapter class
    # from Yantra::Persistence (e.g., :redis loads RedisAdapter).
    # @return [Symbol] The adapter type (e.g., :redis, :sql).
    # Default: :redis
    attr_accessor :persistence_adapter

    # URL string for connecting to the Redis server.
    # Used by the RedisAdapter if `persistence_adapter` is :redis.
    # Defaults check common environment variables.
    # @return [String] Redis connection URL.
    # Default: ENV['YANTRA_REDIS_URL'] || ENV['REDIS_URL'] || 'redis://localhost:6379'
    attr_accessor :redis_url

    # A hash of additional options to pass to the underlying Redis client
    # during initialization (e.g., { ssl_params: { ca_file: '...' } }).
    # @return [Hash] Additional Redis client options.
    # Default: {}
    attr_accessor :redis_options

    # Connection details for SQL databases (e.g., PostgreSQL, MySQL).
    # Used by the SqlAdapter if `persistence_adapter` is :sql.
    # Can be a connection string URL or potentially a hash of options
    # depending on the underlying ORM/library used by the adapter.
    # Defaults check common environment variables.
    # @return [String, Hash, nil] SQL connection details.
    # Default: ENV['YANTRA_DATABASE_URL'] || ENV['DATABASE_URL']
    attr_accessor :sql_connection_details

    # The logger instance Yantra should use for internal logging.
    # Must respond to standard logger methods like :info, :warn, :error, :debug.
    # @return [Logger] Logger instance.
    # Default: Logger.new(STDOUT, level: Logger::INFO)
    attr_accessor :logger

    # The backend responsible for publishing Yantra lifecycle events.
    # Must respond to `publish(event_name, payload)`.
    # If ActiveSupport::Notifications is available, it's used by default.
    # Otherwise, a simple null notifier is used.
    # @return [Object] The notification backend.
    attr_accessor :notification_backend

    # A namespace prefix added to all Redis keys used by Yantra.
    # Helps avoid key collisions if the Redis instance is shared.
    # @return [String] The namespace string.
    # Default: "yantra"
    attr_accessor :redis_namespace

    def self.reset!
      Singleton.__init__(self) # Re-initialize the singleton state
      instance.send(:initialize) # Re-run initialize to set defaults
    end

    # --- Initialization ---
    def initialize
      # Set default values for all configuration options
      @persistence_adapter = :redis # Default adapter
      @redis_url = ENV['YANTRA_REDIS_URL'] || ENV['REDIS_URL'] || 'redis://localhost:6379'
      @redis_options = {} # Options passed to Redis.new
      @redis_namespace = 'yantra' # Namespace for Redis keys

      # ActiveRecord adapter uses Rails' connection by default, no separate config needed here.

      @logger = Logger.new(STDOUT)
      @logger.level = Logger::INFO # Default log level

      @notification_backend = default_notification_backend # For Yantra events
    end

    private

    # Provides a default notification backend. Prefers ActiveSupport::Notifications
    # if it's loaded, otherwise provides a simple null object that does nothing.
    def default_notification_backend
      if defined?(ActiveSupport::Notifications)
        ActiveSupport::Notifications
      else
        # Simple fallback notifier (Null Object pattern)
        Object.new.tap do |notifier|
          def notifier.publish(_event_name, _payload)
            # No-op
          end
        end
      end
    end
  end

  # --- Convenience Accessors ---

  # Provides global access to the singleton configuration instance.
  # @return [Yantra::Configuration]
  def self.configuration
    Configuration.instance
  end

  # Allows configuration settings to be modified via a block.
  # Typically used in an initializer file (e.g., config/initializers/yantra.rb).
  #   Yantra.configure do |config|
  #     config.persistence_adapter = :sql
  #     # ... other settings
  #   end
  def self.configure
    yield(configuration)
  end
end

