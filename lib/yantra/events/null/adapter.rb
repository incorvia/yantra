# lib/yantra/events/null/adapter.rb # <= New Path

# Note: Interface path needs ../../events/notifier_interface if required from here
# but assuming Zeitwerk handles loading, explicit require might not be needed.
# require_relative '../../events/notifier_interface'

module Yantra
  module Events
    module Null # <= New Module Level
      # A notifier adapter that does nothing.
      class Adapter # <= Renamed Class
        # Assuming NotifierInterface is autoloaded or required elsewhere
        include Yantra::Events::NotifierInterface

        # Implements the publish interface method but performs no action.
        def publish(event_name, payload)
          # No operation
        end

      end
    end
  end
end

