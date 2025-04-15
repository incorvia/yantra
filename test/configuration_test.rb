# test/configuration_test.rb
require "test_helper"

# If ActiveSupport is available, you might need to handle its definition for the notification test.
class ConfigurationTest < Minitest::Test

  # Reset configuration to defaults before each test
  def setup
    # If you add the reset! method to Yantra::Configuration:
    Yantra::Configuration.reset! if Yantra::Configuration.respond_to?(:reset!)

    # Manually clear relevant ENV vars before each test (or use a helper)
    @original_yantra_redis_url = ENV.delete('YANTRA_REDIS_URL')
    @original_redis_url = ENV.delete('REDIS_URL')
    @original_yantra_db_url = ENV.delete('YANTRA_DATABASE_URL')
    @original_db_url = ENV.delete('DATABASE_URL')
  end

  # Restore ENV vars after each test
  def teardown
    ENV['YANTRA_REDIS_URL'] = @original_yantra_redis_url if @original_yantra_redis_url
    ENV['REDIS_URL'] = @original_redis_url if @original_redis_url
    ENV['YANTRA_DATABASE_URL'] = @original_yantra_db_url if @original_yantra_db_url
    ENV['DATABASE_URL'] = @original_db_url if @original_db_url

    # Reset again after test if needed, depending on your reset strategy
    Yantra::Configuration.reset! if Yantra::Configuration.respond_to?(:reset!)
  end

  def test_default_configuration_values
    config = Yantra.configuration

    assert_equal :active_record, config.persistence_adapter
    assert_equal 'redis://localhost:6379/0', config.redis_url # Assuming no relevant ENV vars set
    assert_equal({}, config.redis_options)
    assert_instance_of Logger, config.logger
    assert_equal Logger::INFO, config.logger.level
    assert_equal 'yantra', config.redis_namespace

    # Check default notification backend (might need mocking defined? for full test)
    assert config.notification_backend.respond_to?(:publish)
  end

  def test_configure_block_sets_values
    Yantra.configure do |config|
      config.persistence_adapter = :sql
      config.redis_namespace = 'my_test_app'
      config.logger.level = Logger::DEBUG
      config.redis_options = { timeout: 5 }
    end

    config = Yantra.configuration

    assert_equal :sql, config.persistence_adapter
    assert_equal 'my_test_app', config.redis_namespace
    assert_equal Logger::DEBUG, config.logger.level
    assert_equal({ timeout: 5 }, config.redis_options)

    # Check a default value wasn't accidentally changed
    assert_equal 'redis://localhost:6379/0', config.redis_url
  end

  def test_redis_url_uses_environment_variables_in_order
    default_url = 'redis://localhost:6379/0'
    env_redis_url = 'redis://env-redis:1111'
    env_yantra_redis_url = 'redis://yantra-env-redis:2222'

    # 1. No ENV vars set
    Yantra::Configuration.reset!
    assert_equal default_url, Yantra.configuration.redis_url

    # 2. Only REDIS_URL set
    with_env('REDIS_URL' => env_redis_url) do
      Yantra::Configuration.reset!
      assert_equal env_redis_url, Yantra.configuration.redis_url
    end

    # 3. YANTRA_REDIS_URL takes precedence over REDIS_URL
    with_env('REDIS_URL' => env_redis_url, 'YANTRA_REDIS_URL' => env_yantra_redis_url) do
      Yantra::Configuration.reset!
      assert_equal env_yantra_redis_url, Yantra.configuration.redis_url
    end
  end

  # Helper to temporarily set ENV vars for a test
  def with_env(options = {}, &block)
    original_values = {}
    options.each_key { |k| original_values[k] = ENV[k] }
    options.each { |k, v| ENV[k] = v }
    begin
      yield
    ensure
      options.each_key { |k| ENV[k] = original_values[k] }
    end
  end

  # Example test for notification backend (requires stubbing/mocking `defined?`)
  # def test_notification_backend_default_logic
  #   # Test case 1: ActiveSupport::Notifications is defined
  #   # Need a way to stub `defined?(ActiveSupport::Notifications)` to return true
  #   # Yantra::Configuration.reset!
  #   # assert_equal ActiveSupport::Notifications, Yantra.configuration.notification_backend
  #
  #   # Test case 2: ActiveSupport::Notifications is not defined
  #   # Need a way to stub `defined?(ActiveSupport::Notifications)` to return false/nil
  #   # Yantra::Configuration.reset!
  #   # assert Yantra.configuration.notification_backend.respond_to?(:publish)
  #   # refute_equal ActiveSupport::Notifications, Yantra.configuration.notification_backend
  # end
end

