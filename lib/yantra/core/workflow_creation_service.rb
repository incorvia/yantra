# lib/yantra/core/workflow_creation_service.rb
# frozen_string_literal: true

require_relative '../errors'
require 'logger'

module Yantra
  module Core
    # Service responsible for persisting a workflow instance along with its steps and dependencies.
    # Handles idempotency for all workflows - returns existing workflow ID if idempotency_key matches.
    class WorkflowCreationService
      attr_reader :repository, :logger

      def initialize(repository:)
        @repository = repository
        @logger = Yantra.logger || Logger.new(IO::NULL)
      end

      # Creates a workflow with its steps and dependencies.
      # When idempotency_key is provided, returns existing workflow ID if one exists.
      #
      # @param workflow_instance [Yantra::Workflow] The workflow instance to persist
      # @param parent_workflow_id [String, nil] Parent workflow ID for child workflows
      # @param idempotency_key [String, nil] Idempotency key for deduplication
      # @return [String] The workflow ID (new or existing)
      # @raise [Yantra::Errors::PersistenceError] If persistence fails
      def call(workflow_instance, parent_workflow_id: nil, idempotency_key: nil)
        if parent_workflow_id
          create_child_workflow(workflow_instance, parent_workflow_id, idempotency_key:)
        else
          create_workflow(workflow_instance, idempotency_key:)
        end
      end

      private

      def create_child_workflow(workflow_instance, parent_workflow_id, idempotency_key: nil)
        # create_child_workflow returns the workflow ID (either new or existing)
        returned_workflow_id = repository.create_child_workflow(
          workflow_instance,
          parent_workflow_id,
          idempotency_key:
        )

        unless returned_workflow_id
          raise Yantra::Errors::PersistenceError,
                "Failed to persist child workflow record for ID: #{workflow_instance.id}"
        end

        # If returned ID doesn't match our instance ID, an existing workflow was found
        # Skip creating steps/dependencies as they already exist
        if returned_workflow_id != workflow_instance.id
          log_info("Returning existing workflow #{returned_workflow_id} for idempotency_key: #{idempotency_key}")
          return returned_workflow_id
        end

        persist_steps_and_dependencies(workflow_instance)
        workflow_instance.id
      end

      def create_workflow(workflow_instance, idempotency_key: nil)
        # Returns workflow ID (either new or existing)
        returned_workflow_id = repository.create_workflow(
          workflow_instance,
          idempotency_key:
        )

        unless returned_workflow_id
          raise Yantra::Errors::PersistenceError,
                "Failed to persist workflow record for ID: #{workflow_instance.id}"
        end

        # If returned ID doesn't match our instance ID, an existing workflow was found
        # Skip creating steps/dependencies as they already exist
        if returned_workflow_id != workflow_instance.id
          log_info("Returning existing workflow #{returned_workflow_id} for idempotency_key: #{idempotency_key}")
          return returned_workflow_id
        end

        persist_steps_and_dependencies(workflow_instance)
        workflow_instance.id
      end

      def persist_steps_and_dependencies(workflow_instance)
        persist_steps(workflow_instance)
        persist_dependencies(workflow_instance)
      end

      def persist_steps(workflow_instance)
        return if workflow_instance.steps.empty?

        unless repository.create_steps_bulk(workflow_instance.steps)
          raise Yantra::Errors::PersistenceError,
                "Failed to persist step records for workflow ID: #{workflow_instance.id}"
        end
      end

      def persist_dependencies(workflow_instance)
        return if workflow_instance.dependencies.empty?

        links_array = workflow_instance.dependencies.flat_map do |step_id, dep_ids|
          dep_ids.map { |dep_id| { step_id: step_id, depends_on_step_id: dep_id } }
        end

        unless repository.add_step_dependencies_bulk(links_array)
          raise Yantra::Errors::PersistenceError,
                "Failed to persist dependency links for workflow ID: #{workflow_instance.id}"
        end
      end

      def log_info(msg)
        @logger&.info { "[WorkflowCreationService] #{msg}" }
      end
    end
  end
end
