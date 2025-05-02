# lib/yantra/errors.rb

module Yantra
  # Base error class for all Yantra-specific errors.
  class Error < StandardError
    attr_reader :original_exception
    attr_accessor :handled

    def initialize(message = nil, original_exception: nil)
      super(message)
      @original_exception = original_exception
      @handled = false
    end
  end

  module Errors
    class WorkflowNotFound        < Yantra::Error; end
    class StepNotFound            < Yantra::Error; end
    class DependencyNotFound      < Yantra::Error; end
    class ConfigurationError      < Yantra::Error; end
    class InvalidState            < Yantra::Error; end
    class InvalidStateTransition  < Yantra::Error; end
    class PersistenceError        < Yantra::Error; end
    class WorkerError             < Yantra::Error; end
    class StepDefinitionError     < Yantra::Error; end
    class InvalidWorkflowState    < Yantra::Error; end
    class WorkflowError           < Yantra::Error; end
    class UpdateConflictError     < PersistenceError; end
    class WorkflowDefinitionError < StandardError; end

    class EnqueueFailed < StandardError
      attr_reader :failed_ids

      def initialize(message = nil, failed_ids: [])
        super(message)
        @failed_ids = failed_ids
      end
    end
  end
end

