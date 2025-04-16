# test/configuration_test.rb
require "test_helper"
require 'logger' # Ensure logger is available

# If ActiveSupport is available, you might need to handle its definition for the notification test.
class ConfigurationTest < Minitest::Test

  # Reset configuration to defaults before each test
  def setup
    # If you add the reset! method to Yantra::Configuration:
    Yantra::Configuration.reset! if Yantra::Configuration.respond_to?(:reset!)
  end

  def teardown
    # Reset again after test if needed, depending on your reset strategy
    Yantra::Configuration.reset! if Yantra::Configuration.respond_to?(:reset!)
    super # Call Minitest teardown
  end

  def test_default_configuration_values
    config = Yantra.configuration

    # Check main adapter defaults
    assert_equal :active_record, config.persistence_adapter
    assert_equal :active_job, config.worker_adapter
    assert_equal :null, config.notification_adapter

    # Check default options hashes are empty
    assert_equal({}, config.persistence_options)
    assert_equal({}, config.worker_adapter_options)
    assert_equal({}, config.notification_adapter_options)

    # Check default step options
    assert_equal({ retries: 3 }, config.default_step_options)

    # Check default logger
    assert_instance_of Logger, config.logger
    assert_equal Logger::INFO, config.logger.level

    # Check default retry handler class
    assert_nil config.retry_handler_class
  end

  def test_configure_block_sets_values
    # Example custom logger
    custom_logger = Logger.new(nil) # Null logger for test

    Yantra.configure do |config|
      config.persistence_adapter = :redis # Change adapter type
      # *** FIX: Set options via persistence_options hash ***
      config.persistence_options = { url: 'redis://custom:6379/1', namespace: 'my_test_app', timeout: 5 }
      config.worker_adapter = :sidekiq
      config.worker_adapter_options = { concurrency: 10 }
      config.notification_adapter = :logger
      config.notification_adapter_options = { logger: custom_logger }
      config.default_step_options[:retries] = 5 # Modify value within the hash
      config.logger = custom_logger
      config.retry_handler_class = "MyCustomRetryHandler" # Example custom class name
    end

    config = Yantra.configuration

    # Assert main adapter types
    assert_equal :redis, config.persistence_adapter
    assert_equal :sidekiq, config.worker_adapter
    assert_equal :logger, config.notification_adapter

    # *** FIX: Assert options hashes ***
    expected_persistence_options = { url: 'redis://custom:6379/1', namespace: 'my_test_app', timeout: 5 }
    assert_equal expected_persistence_options, config.persistence_options

    expected_worker_options = { concurrency: 10 }
    assert_equal expected_worker_options, config.worker_adapter_options

    expected_notification_options = { logger: custom_logger }
    assert_equal expected_notification_options, config.notification_adapter_options

    # Assert other settings
    assert_equal({ retries: 5 }, config.default_step_options)
    assert_same custom_logger, config.logger # Use assert_same for object identity
    assert_equal "MyCustomRetryHandler", config.retry_handler_class
  end

  # Removed test_redis_url_uses_environment_variables_in_order
  # as this logic should reside within the specific Redis adapter, not the base Configuration.

end

