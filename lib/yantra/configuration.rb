# lib/yantra/configuration.rb

require 'logger'
require_relative 'errors'

module Yantra
  # Handles configuration settings for the Yantra gem.
  # Provides default values and allows overrides via `Yantra.configure`.
  class Configuration
    # --- Persistence Configuration ---
    attr_accessor :persistence_adapter
    attr_accessor :persistence_options

    # --- Worker Configuration ---
    attr_accessor :worker_adapter
    attr_accessor :worker_adapter_options

    # --- Event Notification Configuration ---
    attr_accessor :notification_adapter
    attr_accessor :notification_adapter_options

    # --- General Configuration ---
    attr_accessor :default_step_options
    attr_accessor :logger # Logger instance used by Yantra internals.

    # --- Retry Handler Configuration ---
    attr_accessor :retry_handler_class

    def initialize
      # --- Defaults ---
      @persistence_adapter = :active_record
      @persistence_options = {}

      @worker_adapter = :active_job
      @worker_adapter_options = {}

      @notification_adapter = :null
      @notification_adapter_options = {}

      @default_step_options = { retries: 3 }

      # *** FIX HERE: Default logger to standard Logger writing to STDOUT ***
      # Users in a Rails environment can explicitly set config.logger = Rails.logger
      # in their initializer if desired.
      @logger = ::Logger.new($stdout, level: ::Logger::INFO)
      # Apply a simple formatter
      @logger.formatter ||= proc do |severity, datetime, progname, msg|
        "[#{datetime.strftime('%Y-%m-%d %H:%M:%S.%L')}] #{severity} -- : #{msg}\n"
      end


      @retry_handler_class = nil # Use default RetryHandler unless overridden
    end

    # Allows resetting configuration, primarily for testing.
    def self.reset!
      Yantra.instance_variable_set(:@configuration, nil)
      Yantra.instance_variable_set(:@repository, nil)
      Yantra.instance_variable_set(:@worker_adapter, nil)
      Yantra.instance_variable_set(:@notifier, nil)
    end

  end
end
