# lib/yantra/events/logger/adapter.rb # <= New Path

require 'logger'
# require_relative '../../events/notifier_interface' # Assuming autoloading

module Yantra
  module Events
    module Logger # <= New Module Level
      # A notifier adapter that logs events using a standard Ruby Logger.
      class Adapter # <= Renamed Class
        include Yantra::Events::NotifierInterface

        attr_reader :logger

        def initialize(logger: nil)
          @logger = logger || ::Logger.new($stdout, level: ::Logger::INFO)
          @logger.formatter ||= proc do |severity, datetime, progname, msg|
            "[#{datetime.strftime('%Y-%m-%d %H:%M:%S.%L')}] #{severity} -- : #{msg}\n"
          end
        end

        def publish(event_name, payload)
          begin
            @logger.info("[Yantra Event] Name: #{event_name}, Payload: #{payload.inspect}")
          rescue => e
            @logger.error("[Yantra LoggerAdapter] Failed to log event '#{event_name}': #{e.class} - #{e.message}")
          end
        end

      end
    end
  end
end

