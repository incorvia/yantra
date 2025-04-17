# test/support/test_notifier_adapter.rb

# Ensure the interface is available. Adjust path if test_helper doesn't load lib.
# require_relative '../../lib/yantra/events/notifier_interface'

# A simple notifier adapter implementation specifically for testing purposes.
# It does not send notifications anywhere but records them in memory
# so that tests can assert that the correct events were published.
class TestNotifierAdapter
  # Include the interface to ensure compatibility (optional but good practice)
  include Yantra::Events::NotifierInterface

  # Stores the events published during a test run.
  # Each element is a hash like: { name: "event_name", payload: {...} }
  attr_reader :published_events

  def initialize(**options)
    # Options are ignored for this simple adapter, but accept them for interface compatibility.
    @published_events = []
  end

  # Records the event name and payload when called.
  # Implements the NotifierInterface contract.
  #
  # @param event_name [String, Symbol] The name of the event.
  # @param payload [Hash] The event payload.
  def publish(event_name, payload)
    @published_events << { name: event_name.to_s, payload: payload }
    # Optional: Log the event for debugging during tests
    # puts "[TestNotifierAdapter] Event Published: #{event_name} | Payload: #{payload.inspect}"
  end

  # Clears the list of recorded events.
  # Useful to call in test setup or teardown to isolate tests.
  def clear!
    @published_events = []
  end

  # Helper method to find the first event published with a specific name.
  #
  # @param event_name [String, Symbol] The name of the event to find.
  # @return [Hash, nil] The first matching event hash or nil if not found.
  def find_event(event_name)
    target_name = event_name.to_s
    @published_events.find { |event| event[:name] == target_name }
  end

  # Helper method to find all events published with a specific name.
  #
  # @param event_name [String, Symbol] The name of the event to find.
  # @return [Array<Hash>] An array of matching event hashes.
  def find_events(event_name)
    target_name = event_name.to_s
    @published_events.select { |event| event[:name] == target_name }
  end
end

