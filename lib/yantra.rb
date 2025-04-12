# frozen_string_literal: true

# lib/yantra.rb
require_relative "yantra/version"
require_relative "yantra/errors"
require_relative "yantra/job"      # <-- Make sure this line is present
require_relative "yantra/workflow" # <-- Make sure this line is present
# Add requires for other core files as you create them (Client, Configuration, etc.)


module Yantra
  class Error < StandardError; end
  # Your code goes here...
end
