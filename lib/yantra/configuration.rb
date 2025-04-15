# lib/yantra/configuration.rb

require 'singleton'
require 'logger'
# Attempt to load ActiveSupport::Notifications for default notifier, but don't fail if not present
begin
  require 'active_support/notifications'
rescue LoadError
  # ActiveSupport not available
end

module Yantra
  # Handles configuration settings for the Yantra gem.
  # Access the singleton instance via `Yantra.configuration` or `Yantra::Configuration.instance`.
  #
  # @example
  #   Yantra.configure do |config|
  #     config.persistence_adapter = :redis # Override default
  #     config.worker_adapter = :sidekiq # Override default
  #     config.redis_url = "redis://my-redis:6379/2"
  #     config.logger.level = Logger::DEBUG
  #   end
  class Configuration
    include Singleton # Use Singleton pattern for easy global access

    # --- Persistence Configuration ---

    # Symbol identifying the chosen persistence adapter (:redis, :active_record).
    # Adapters must implement Yantra::Persistence::RepositoryInterface.
    # Default: `:active_record`
    attr_accessor :persistence_adapter

    # Connection URL for the Redis adapter. Used if `persistence_adapter` is `:redis`.
    # Reads from ENV['YANTRA_REDIS_URL'] then ENV['REDIS_URL'] as fallbacks.
    # Default: `'redis://localhost:6379/0'`
    attr_accessor :redis_url

    # Additional options hash passed directly to `Redis.new`.
    # See Redis gem documentation for available options (e.g., :ssl_params, :timeout).
    # Default: `{}`
    attr_accessor :redis_options

    # Namespace prefix for all Redis keys used by Yantra.
    # Default: `'yantra'`
    attr_accessor :redis_namespace

    # Note: ActiveRecord connection details are NOT configured here.
    # The ActiveRecord adapter assumes connection management via the host
    # application's setup (e.g., Rails' database.yml and connection pools).

    # --- Worker Configuration ---

    # Symbol identifying the chosen background worker adapter (:sidekiq, :resque, :active_job).
    # Adapters must implement Yantra::Worker::EnqueuingInterface.
    # Default: `:active_job`
    attr_accessor :worker_adapter

    # TODO: Add configuration options specific to worker adapters if needed,
    # e.g., default Sidekiq options hash.

    # --- General Configuration ---

    # Logger instance used by Yantra components. Must adhere to Ruby Logger interface.
    # Default: `Logger.new($stdout, level: Logger::INFO)`
    attr_accessor :logger

    # Backend used for emitting lifecycle events (e.g., `yantra.workflow.started`).
    # Must respond to `instrument(event_name, payload = {})`.
    # Default: `ActiveSupport::Notifications` if available, otherwise `nil`.
    attr_accessor :notification_backend

    # Internal: Initialize with default values.
    def initialize
      set_defaults
    end

    # Resets configuration to defaults. Primarily useful for testing.
    def self.reset!
      instance.send(:set_defaults) # Call private method on the singleton instance
    end

    private

    # Sets the default values for all configuration options.
    def set_defaults
      # Persistence Defaults
      @persistence_adapter = :active_record # <<< UPDATED DEFAULT
      @redis_url = ENV['YANTRA_REDIS_URL'] || ENV['REDIS_URL'] || 'redis://localhost:6379/0'
      @redis_options = {}
      @redis_namespace = 'yantra'

      # Worker Defaults
      @worker_adapter = :active_job # <<< UPDATED DEFAULT

      # General Defaults
      @logger = Logger.new($stdout) # Default logger to standard output
      @logger.level = Logger::INFO  # Default log level
      @notification_backend = default_notification_backend
    end

    # Detects a suitable default notification backend.
    # @return [Object, nil] ActiveSupport::Notifications or nil.
    def default_notification_backend
      defined?(ActiveSupport::Notifications) ? ActiveSupport::Notifications : nil
    end
  end

  # --- Convenience Accessors ---
  # These methods are typically defined in the main gem file (e.g., lib/yantra.rb)
  # but are shown here for context on how the Configuration singleton is accessed.
  #
  # def self.configuration
  #   Configuration.instance
  # end
  #
  # def self.configure
  #   yield(configuration)
  # end

end

