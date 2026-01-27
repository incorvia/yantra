# lib/yantra/auto_rerunner.rb

module Yantra
  class AutoRerunner
    class Rule
      attr_reader :name, :criteria_block, :target_block, :action_type

      def initialize(name)
        @name = name
      end

      def criteria(&block)
        @criteria_block = block
      end

      def target(&block)
        @target_block = block
      end

      def action(type)
        @action_type = type
      end
    end

    def self.configure(&block)
      instance.instance_eval(&block)
    end

    def self.run!
      instance.run_rules!
    end

    def self.instance
      @instance ||= new
    end

    def initialize
      @rules = []
    end

    def rule(name, &block)
      rule = Rule.new(name)
      rule.instance_eval(&block)
      @rules << rule
    end

    def run_rules!
      @rules.each do |rule|
        next unless rule.criteria_block.call

        targets = rule.target_block.call

        targets.each do |step|
          case rule.action_type
          when :retry
            retry_step(step)
          else
            # Log unknown action
          end
        end
      end
    end

    private

    def retry_step(step)
      # Logic to retry the step
      # This typically involves resetting the state to pending and re-enqueuing
      # For now, we'll just update the state and rely on an external poller or
      # explicitly call the orchestrator if available.
      # Ideally, we should use Yantra::Orchestrator to handle this safely.

      # Assuming a simple state reset for now as per design
      step.update(state: 'pending', retries: 0, error: nil)

      # Re-enqueue logic would go here.
      # Since we don't have direct access to the Orchestrator instance easily here without more context,
      # we might need to instantiate one or use a service.
      # For this implementation, we'll assume the state change is enough for a poller to pick it up,
      # or we can try to re-enqueue if the worker adapter supports it.

      # TODO: Integrate with Orchestrator for proper re-enqueuing
    end
  end
end
