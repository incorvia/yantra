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
  class Configuration
    include Singleton # Use Singleton pattern for easy global access

    # --- Persistence Configuration ---
    attr_accessor :persistence_adapter # Default: :active_record
    attr_accessor :redis_url           # Default: 'redis://localhost:6379/0'
    attr_accessor :redis_options       # Default: {}
    attr_accessor :redis_namespace     # Default: 'yantra'

    # --- Worker Configuration ---
    attr_accessor :worker_adapter      # Default: :active_job
    # TODO: Add specific worker options if needed

    # --- Retry Configuration ---
    # Default maximum number of times a job will be attempted (including first run).
    # Can be overridden per job class by defining `self.yantra_max_attempts`.
    # Default: 3
    attr_accessor :default_max_job_attempts # <<< NEW CONFIGURATION

    # --- General Configuration ---
    attr_accessor :logger                 # Default: Logger.new($stdout, level: Logger::INFO)
    attr_accessor :notification_backend   # Default: ActiveSupport::Notifications or nil
    # TODO: Add config for RetryHandler class to allow swapping?
    # attr_accessor :retry_handler_class

    def initialize
      set_defaults
    end

    def self.reset!
      instance.send(:set_defaults)
    end

    private

    def set_defaults
      # Persistence Defaults
      @persistence_adapter = :active_record
      @redis_url = ENV['YANTRA_REDIS_URL'] || ENV['REDIS_URL'] || 'redis://localhost:6379/0'
      @redis_options = {}
      @redis_namespace = 'yantra'

      # Worker Defaults
      @worker_adapter = :active_job

      # Retry Defaults
      @default_max_job_attempts = 3 # <<< SET DEFAULT HERE

      # General Defaults
      @logger = Logger.new($stdout)
      @logger.level = Logger::INFO
      @notification_backend = default_notification_backend
      # @retry_handler_class = Yantra::Worker::RetryHandler # Default handler class
    end

    def default_notification_backend
      defined?(ActiveSupport::Notifications) ? ActiveSupport::Notifications : nil
    end
  end
end

