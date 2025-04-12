# test/test_helper.rb
$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "yantra" # Make sure this matches your main gem require file

require "minitest/autorun"
require "securerandom" # Needed for generating IDs in tests
