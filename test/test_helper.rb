# test/test_helper.rb
$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "yantra" # Make sure this matches your main gem require file

require "minitest/autorun"
require "securerandom" # Needed for generating IDs in tests
require "logger"       # Needed for logger below
require 'mocha/minitest'
require 'minitest/focus'
require 'active_support/testing/time_helpers'
require File.expand_path('dummy/config/environment', __dir__)



# --- ActiveRecord Test Setup ---

# Attempt to load ActiveRecord and SQLite3, set a flag indicating success.
AR_LOADED = begin
  require 'active_record'
  require 'database_cleaner/active_record' # Require DatabaseCleaner here too
  true # Indicates successful loading
rescue LoadError # <<< CHANGED: No longer assigns to unused 'e'
  # Output a warning if gems are missing (useful for contributors)


  false # Indicates gems are not available
end

# Only proceed with AR setup if the gems were loaded successfully
if AR_LOADED
  begin
    # Ensure the connection pool is established using the config loaded by Rails env
    # Accessing .connection forces initialization.
    ActiveRecord::Base.connection
    puts "INFO: ActiveRecord connected successfully using database.yml."

    # Configure Database Cleaner (example, if you use it)
    DatabaseCleaner.strategy = :transaction
    DatabaseCleaner.clean_with(:truncation)

  rescue ActiveRecord::NoDatabaseError => e
    puts "\nERROR: Test database specified in database.yml does not exist."
    puts "Please run the following command from the 'test/dummy' directory:"
    puts "  bundle exec rails db:create RAILS_ENV=test"
    raise e
  rescue ActiveRecord::ConnectionNotEstablished, PG::ConnectionBad => e
    puts "\nERROR: Could not connect to PostgreSQL database specified in database.yml."
    puts "Ensure PostgreSQL server is running and accessible with the configured credentials."
    puts "Config used: #{Rails.application.config.database_configuration[Rails.env].inspect}"
    raise e
  rescue => e
    puts "\nERROR: An unexpected error occurred during ActiveRecord setup: #{e.message}"
    raise e
  end

  ActiveRecord::Base.logger = Logger.new(IO::NULL)

  # Helper method to load the database schema from a schema.rb file.
  def load_yantra_schema
    schema_path = File.expand_path('./support/db/schema.rb', __dir__) # Using user's corrected path
    unless File.exist?(schema_path)
      raise "Cannot load schema: Schema file not found at #{schema_path}. " \
            "Generate it using 'rake db:schema:dump' (likely within the test/dummy app) " \
            "and place it in your gem's test support directory (e.g., test/support/db/)."
    end


    begin
      original_stdout = $stdout.dup
      $stdout.reopen(IO::NULL)
      load(schema_path)
    rescue => e
      $stdout.reopen(original_stdout)


      raise
    ensure
      $stdout.reopen(original_stdout)
    end

  end

  # --- Load Schema ---
  load_yantra_schema


  # --- Configure Database Cleaner ---
  DatabaseCleaner.strategy = :transaction


  # --- Base Test Class for ActiveRecord Tests ---
  class YantraActiveRecordTestCase < Minitest::Test
    def setup
      super
      skip "ActiveRecord/SQLite3/DatabaseCleaner not available" unless AR_LOADED
      DatabaseCleaner.start
    end

    def teardown
      DatabaseCleaner.clean
      super
    end
  end

else
  # Define a dummy base class if AR didn't load.
  class YantraActiveRecordTestCase < Minitest::Test
     def setup
       super
       skip "ActiveRecord/SQLite3/DatabaseCleaner not available"
     end
  end
end



