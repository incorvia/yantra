# lib/yantra/errors.rb

module Yantra
  # Base error class for all Yantra specific errors.
  # Allows users to rescue Yantra::Error to catch any error originating from this gem.
  class Error < StandardError; end

  # Module to namespace specific error types
  module Errors
    # Raised when a workflow cannot be found by its ID.
    class WorkflowNotFound < Yantra::Error; end # Inherit from Yantra::Error

    # Raised when a job cannot be found by its ID.
    class JobNotFound < Yantra::Error; end

    # Raised during workflow definition when a specified dependency cannot be found.
    class DependencyNotFound < Yantra::Error; end

    # Raised when Yantra configuration is invalid or missing required settings.
    class ConfigurationError < Yantra::Error; end

    # Raised when an operation encounters an invalid state or attempts an invalid state transition
    # (though the StateMachine might handle transitions more specifically).
    class InvalidState < Yantra::Error; end

    # Raised when attempting an invalid state transition
    class InvalidStateTransition < Error; end # <<< ADD THIS CLASS

    # Optional: A generic error for persistence layer issues, potentially wrapping adapter-specific errors.
    # Alternatively, adapters could raise their own specific errors inheriting from Yantra::Error.
    # class PersistenceError < Yantra::Error; end

    # Optional: A generic error for worker adapter issues.
    # class WorkerError < Yantra::Error; end
  end
end

