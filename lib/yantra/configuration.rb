# lib/yantra/configuration.rb

require 'logger'
require_relative 'errors'

module Yantra
  # Handles configuration for the Yantra gem.
  class Configuration
    # Persistence
    attr_accessor :persistence_adapter, :persistence_options

    # Worker
    attr_accessor :worker_adapter, :worker_adapter_options

    # Notifications
    attr_accessor :notification_adapter, :notification_adapter_options

    # General
    attr_accessor :default_step_options, :logger

    # Retry logic
    attr_accessor :retry_handler_class

    def initialize
      @persistence_adapter           = :active_record
      @persistence_options           = {}

      @worker_adapter                = :active_job
      @worker_adapter_options        = {}

      @notification_adapter          = :null
      @notification_adapter_options  = {}

      @default_step_options          = { retries: 3 }

      @logger = Logger.new($stdout, level: Logger::INFO)
      @logger.formatter = proc do |severity, datetime, _progname, msg|
        "[#{datetime.strftime('%Y-%m-%d %H:%M:%S.%L')}] #{severity} -- : #{msg}\n"
      end

      @retry_handler_class = nil
    end

    # Resets global configuration (used in tests or reinitialization).
    def self.reset!
      Yantra.instance_variable_set(:@configuration, nil)
      Yantra.instance_variable_set(:@repository, nil)
      Yantra.instance_variable_set(:@worker_adapter, nil)
      Yantra.instance_variable_set(:@notifier, nil)
    end
  end
end

