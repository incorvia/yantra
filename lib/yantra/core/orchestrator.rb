# lib/yantra/core/orchestrator.rb
require_relative 'state_machine' # Add this near the top
# ... other requires ...

module Yantra
  module Core
    class Orchestrator
      def some_method(job_id, current_state, desired_state)
        StateMachine.validate_transition!(current_state, desired_state) # Use the module
        # ... tell repository to update state ...
      end
    end
  end
end
