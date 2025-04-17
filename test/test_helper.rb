# test/test_helper.rb
$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "yantra" # Make sure this matches your main gem require file

require "minitest/autorun"
require "securerandom" # Needed for generating IDs in tests
require "logger"       # Needed for logger below
require 'mocha/minitest'
require 'minitest/focus'

puts "INFO: Loading test helper..."

# --- ActiveRecord Test Setup ---

# Attempt to load ActiveRecord and SQLite3, set a flag indicating success.
AR_LOADED = begin
  require 'active_record'
  require 'sqlite3'
  require 'database_cleaner/active_record' # Require DatabaseCleaner here too
  true # Indicates successful loading
rescue LoadError # <<< CHANGED: No longer assigns to unused 'e'
  # Output a warning if gems are missing (useful for contributors)
  puts "\nWARN: ActiveRecord, sqlite3, or database_cleaner-active_record gem not found. Skipping ActiveRecord adapter tests."
  puts "      Install development dependencies (bundle install) to run them."
  false # Indicates gems are not available
end

# Only proceed with AR setup if the gems were loaded successfully
if AR_LOADED
  puts "INFO: Setting up ActiveRecord with in-memory SQLite for Minitest..."

  ActiveRecord::Base.logger = Logger.new(IO::NULL)
  ActiveRecord::Base.establish_connection(adapter: 'sqlite3', database: ':memory:')

  # Helper method to load the database schema from a schema.rb file.
  def load_yantra_schema
    schema_path = File.expand_path('./support/db/schema.rb', __dir__) # Using user's corrected path
    unless File.exist?(schema_path)
      raise "Cannot load schema: Schema file not found at #{schema_path}. " \
            "Generate it using 'rake db:schema:dump' (likely within the test/dummy app) " \
            "and place it in your gem's test support directory (e.g., test/support/db/)."
    end

    puts "INFO: Loading Yantra schema from #{schema_path}..."
    begin
      original_stdout = $stdout.dup
      $stdout.reopen(IO::NULL)
      load(schema_path)
    rescue => e
      $stdout.reopen(original_stdout)
      puts "ERROR: Failed to load schema from #{schema_path}: #{e.message}"
      puts e.backtrace.take(15).join("\n")
      raise
    ensure
      $stdout.reopen(original_stdout)
    end
    puts "INFO: Yantra schema loaded successfully."
  end

  # --- Load Schema ---
  load_yantra_schema
  puts "INFO: Database schema setup complete."

  # --- Configure Database Cleaner ---
  DatabaseCleaner.strategy = :transaction
  puts "INFO: DatabaseCleaner strategy set to :transaction"

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

puts "INFO: Test helper loading complete."

