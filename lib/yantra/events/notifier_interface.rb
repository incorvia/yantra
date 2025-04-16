# lib/yantra/events/notifier_interface.rb

module Yantra
  module Events
    # Defines the interface that all notification adapters must implement.
    # Adapters are responsible for taking event information and publishing it
    # to a specific backend (e.g., ActiveSupport::Notifications, Logger, Null).
    module NotifierInterface

      # Publishes an event with a given name and payload.
      #
      # Adapters implementing this interface MUST define this method.
      #
      # @param event_name [String, Symbol] The unique name identifying the event
      #   (e.g., 'yantra.workflow.started', 'yantra.step.failed').
      # @param payload [Hash] A hash containing data relevant to the event
      #   (e.g., { workflow_id: '...', step_id: '...', error_class: '...', timestamp: ... }).
      #   The exact payload structure depends on the specific event being published.
      # @return [void]
      def publish(event_name, payload)
        raise NotImplementedError, "#{self.class.name} must implement the `publish` method."
      end

    end
  end
end
