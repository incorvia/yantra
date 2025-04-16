# lib/yantra/events/active_support_notifications/adapter.rb # <= New Path

# require_relative '../../events/notifier_interface' # Assuming autoloading
require_relative '../../errors'

begin
  require 'active_support/notifications'
rescue LoadError
  # Handled in initializer check
end

module Yantra
  module Events
    module ActiveSupportNotifications # <= New Module Level
      # Publishes events using ActiveSupport::Notifications.
      class Adapter # <= Renamed Class
        include Yantra::Events::NotifierInterface

        def initialize
          unless defined?(::ActiveSupport::Notifications)
            raise Yantra::Errors::ConfigurationError,
                  "The 'activesupport' gem is required to use Yantra::Events::ActiveSupportNotifications::Adapter, but it could not be loaded."
          end
        end

        def publish(event_name, payload)
          begin
            event_name_str = event_name.to_s
            ::ActiveSupport::Notifications.instrument(event_name_str, payload)
          rescue => e
            puts "ERROR: [Yantra AS::NotificationsAdapter] Failed to instrument event '#{event_name_str}': #{e.class} - #{e.message}"
          end
        end

      end
    end
  end
end

