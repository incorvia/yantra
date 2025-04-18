# lib/yantra/errors.rb

module Yantra
  # Base error class for all Yantra specific errors.
  # Add accessor for original_exception and handled flag.
  class Error < StandardError
    attr_reader :original_exception
    attr_accessor :handled # Flag to indicate if error should stop AJ retries

    def initialize(message = nil, original_exception: nil)
      super(message)
      @original_exception = original_exception
      @handled = false # Default to not handled
    end
  end

  # Module to namespace specific error types
  module Errors
    # Raised when a workflow cannot be found by its ID.
    class WorkflowNotFound < Yantra::Error; end

    # Raised when a job cannot be found by its ID.
    class StepNotFound < Yantra::Error; end

    # Raised during workflow definition when a specified dependency cannot be found.
    class DependencyNotFound < Yantra::Error; end

    # Raised when Yantra configuration is invalid or missing required settings.
    class ConfigurationError < Yantra::Error; end

    # Raised when an operation encounters an invalid state or attempts an invalid state transition
    class InvalidState < Yantra::Error; end

    # Raised when attempting an invalid state transition
    class InvalidStateTransition < Yantra::Error; end # Note: Inherits from Yantra::Error now

    # A generic error for persistence layer issues, potentially wrapping adapter-specific errors.
    class PersistenceError < Yantra::Error; end

    # Optional: A generic error for worker adapter issues.
    class WorkerError < Yantra::Error; end

    # Raised when a job class definition is invalid or cannot be loaded.
    class StepDefinitionError < Yantra::Error; end

    # Raised on update conflict during optimistic locking
    class UpdateConflictError < PersistenceError; end

  end
end

