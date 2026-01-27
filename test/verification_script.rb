# test/verification_script.rb
require "bundler/setup"

require_relative '../lib/yantra'
require_relative '../lib/yantra/reporting'
require_relative '../lib/yantra/stats'
require_relative '../lib/yantra/auto_rerunner'
require 'ostruct'
require 'active_support/all'

# Mock ActiveRecord setup for standalone testing if needed
# For now, we assume this runs in an environment where AR is set up (like rails runner or test helper)
# If running standalone, we might need to setup a memory DB.

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
module Yantra
  class Step
    def self.where(conditions)
      # Mock return
      [OpenStruct.new(state: 'failed', retries: 2, id: 's1')]
    end

    def update(attributes)
      puts "Updating step with: #{attributes.inspect}"
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
