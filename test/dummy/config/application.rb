# test/dummy/config/application.rb
require_relative "boot"

# <<<--- This line requires the Rails framework --->>>
require "rails/all"
# Or require specific frameworks if needed:
# require "active_record/railtie"
# require "action_controller/railtie"
# etc.

# Require your gem's main file
require "yantra"

# Define a minimal Rails application module
module Dummy
  class Application < Rails::Application
    # Initialize configuration defaults for your target Rails version.
    # Replace 7.0 with the version you are targeting if different.
    config.load_defaults 7.0

    # Minimal configuration settings
    config.eager_load = false
    config.active_support.deprecation = :stderr
    config.root = File.expand_path("..", __dir__) # Set root to dummy app dir

    # Add additional config if needed by Yantra or its dependencies
  end
end

