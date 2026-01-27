# test/minimal_verification.rb

module Yantra
  class << self
    attr_accessor :configuration
  end
end

# Mock ActiveSupport
class String
  def underscore
    self.gsub(/::/, '/').
    gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
    gsub(/([a-z\d])([A-Z])/,'\1_\2').
    tr("-", "_").
    downcase
  end
end

class Integer
  def hour
    self * 3600
  end
end

require 'logger'
require 'securerandom'
require 'set'

# Load core files manually
require_relative '../lib/yantra/errors'
require_relative '../lib/yantra/configuration'
require_relative '../lib/yantra/step'
require_relative '../lib/yantra/workflow'
require_relative '../lib/yantra/reporting'
require_relative '../lib/yantra/stats'
require_relative '../lib/yantra/auto_rerunner'

# Setup Configuration
Yantra.configuration = Yantra::Configuration.new

puts "--- Verifying Reporting ---"

class TestStep < Yantra::Step
  def perform
    report(custom_metric: 123)
  end
end

step = TestStep.new(workflow_id: 'w1', klass: TestStep)
step.report(initial_data: 'test')

puts "Step reporting_data: #{step.reporting_data.inspect}"
raise "Reporting failed" unless step.reporting_data == { initial_data: 'test' }

step.perform
puts "Step reporting_data after perform: #{step.reporting_data.inspect}"
raise "Reporting merge failed" unless step.reporting_data == { initial_data: 'test', custom_metric: 123 }

puts "--- Verifying AutoRerunner ---"

# Mock Step class for querying
class MockStep
  attr_accessor :state, :retries, :id, :error

  def initialize(state:, retries:, id:)
    @state = state
    @retries = retries
    @id = id
  end

  def update(attributes)
    attributes.each do |k, v|
      send("#{k}=", v)
    end
    puts "Updated step #{id} to #{attributes.inspect}"
  end
end

module Yantra
  class Step
    def self.where(conditions)
      # Mock return
      [MockStep.new(state: 'failed', retries: 2, id: 's1')]
    end
  end
end

Yantra::AutoRerunner.configure do
  rule "Retry failed steps" do
    criteria { true }
    target { Yantra::Step.where(state: 'failed') }
    action :retry
  end
end

puts "Running AutoRerunner..."
Yantra::AutoRerunner.run!

puts "Verification Complete!"
