# test/test_helper.rb
$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "yantra" # Make sure this matches your main gem require file

require "minitest/autorun"
require "securerandom" # Needed for generating IDs in tests
require "logger"       # Needed for logger below

puts "INFO: Loading test helper..."

# --- ActiveRecord Test Setup ---

# Attempt to load ActiveRecord and SQLite3, set a flag indicating success.
AR_LOADED = begin
  require 'active_record'
  require 'sqlite3'
  require 'database_cleaner/active_record' # Require DatabaseCleaner here too
  true # Indicates successful loading
rescue LoadError => e
  # Output a warning if gems are missing (useful for contributors)
  puts "\nWARN: ActiveRecord, sqlite3, or database_cleaner-active_record gem not found. Skipping ActiveRecord adapter tests."
  puts "      Install development dependencies (bundle install) to run them."
  # puts "      Error: #{e.message}" # Uncomment for more debug info
  false # Indicates gems are not available
end

# Only proceed with AR setup if the gems were loaded successfully
if AR_LOADED
  puts "INFO: Setting up ActiveRecord with in-memory SQLite for Minitest..."

  # Suppress ActiveRecord logging clutter during tests (optional)
  ActiveRecord::Base.logger = Logger.new(IO::NULL)

  # Establish connection to a temporary in-memory SQLite database
  # Each test run using this helper will get a fresh database.
  ActiveRecord::Base.establish_connection(adapter: 'sqlite3', database: ':memory:')

  # Helper method to load the database schema from a schema.rb file.
  def load_yantra_schema
    # Define the path to your schema.rb file within the gem's test support.
    # *** IMPORTANT: You need to generate this file (e.g., via rake db:schema:dump in dummy app) ***
    # *** and place it at this location within your gem structure.          ***
    schema_path = File.expand_path('./support/db/schema.rb', __dir__) #<-- ADJUST PATH & CREATE FILE

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
      $stdout.reopen(original_stdout) # Restore stdout on error
      puts "ERROR: Failed to load schema from #{schema_path}: #{e.message}"
      puts e.backtrace.take(15).join("\n")
      raise
    ensure
      $stdout.reopen(original_stdout) # Ensure stdout is restored
    end
    puts "INFO: Yantra schema loaded successfully."
  end

  # --- Load Schema ---
  # Load the schema once when the helper loads for the in-memory DB.
  load_yantra_schema
  puts "INFO: Database schema setup complete."

  # --- Configure Database Cleaner ---
  # Configure the strategy ONCE
  DatabaseCleaner.strategy = :transaction
  puts "INFO: DatabaseCleaner strategy set to :transaction"

  # --- Base Test Class for ActiveRecord Tests ---
  # Inherit from this class for tests that require the AR setup and schema.
  class YantraActiveRecordTestCase < Minitest::Test
    # Automatically skip tests in this class if AR wasn't loaded
    # Run DatabaseCleaner before each test
    def setup
      super
      skip "ActiveRecord/SQLite3/DatabaseCleaner not available" unless AR_LOADED
      # puts "--- DB CLEANER START ---" # Add this
      DatabaseCleaner.start
    end

    def teardown
      # puts "--- DB CLEANER END ---" # Add this
      DatabaseCleaner.clean
      super
    end


    # --- Optional: Transaction wrapping via around ---
    # If using DatabaseCleaner with :transaction, you generally don't need
    # this manual around hook as well. Use EITHER DC hooks OR the around hook.
    # def around(&block)
    #   skip "ActiveRecord/SQLite3 not available" unless AR_LOADED
    #   ActiveRecord::Base.transaction do
    #     super(&block) # Runs the actual test method
    #     raise ActiveRecord::Rollback # Rollback transaction after each test
    #   end
    # end
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

