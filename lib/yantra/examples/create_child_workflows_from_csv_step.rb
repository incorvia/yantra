# lib/yantra/examples/create_child_workflows_from_csv_step.rb
# frozen_string_literal: true

require 'set'
require_relative '../step'

module Yantra
  module Examples
    # Example step that demonstrates how to create child workflows from CSV data
    # with idempotency support. This step downloads a CSV from S3, processes each row,
    # and creates child workflows only for rows that haven't been processed before.
    #
    # Usage:
    #   run CreateChildWorkflowsFromCsvStep, 
    #       name: :process_csv, 
    #       params: { 
    #         s3_url: "https://s3.amazonaws.com/bucket/file.csv",
    #         parent_workflow_id: "parent-workflow-id",
    #         child_workflow_class: ProcessMealShortWorkflow,
    #         idempotency_key_generator: ->(row) { "meal-short-#{row[:order_id]}-#{row[:sku]}" }
    #       }
    class CreateChildWorkflowsFromCsvStep < Yantra::Step
      
      # @param s3_url [String] URL to the CSV file in S3
      # @param parent_workflow_id [String] ID of the parent workflow
      # @param child_workflow_class [Class] The workflow class to instantiate for each row
      # @param idempotency_key_generator [Proc] Lambda that generates idempotency keys from row data
      # @param csv_parser [Object] Optional custom CSV parser (must respond to #parse_csv)
      def perform(s3_url:, parent_workflow_id:, child_workflow_class:, 
                  idempotency_key_generator:, csv_parser: nil)
        
        # Validate inputs
        validate_inputs(s3_url, parent_workflow_id, child_workflow_class, idempotency_key_generator)
        
        # Download and parse CSV
        csv_rows = download_and_parse_csv(s3_url, csv_parser)
        
        # Generate all potential idempotency keys from the CSV data
        potential_keys = csv_rows.map { |row| idempotency_key_generator.call(row) }
        
        # Find existing workflows to avoid duplicates
        existing_keys = Yantra.repository.find_existing_idempotency_keys(parent_workflow_id, potential_keys)
        existing_keys_set = Set.new(existing_keys)
        
        # Create child workflows only for new rows
        created_count = 0
        skipped_count = 0
        
        csv_rows.each do |row|
          idempotency_key = idempotency_key_generator.call(row)
          
          # Skip if we already have a workflow for this key
          if existing_keys_set.include?(idempotency_key)
            skipped_count += 1
            next
          end
          
          # Create child workflow for this row
          begin
            Yantra.create_child_workflow(
              child_workflow_class,
              parent_workflow_id,
              idempotency_key: idempotency_key,
              **row # Pass row data as keyword arguments to the child workflow
            )
            created_count += 1
          rescue Yantra::Errors::PersistenceError => e
            # Log error but continue processing other rows
            log_error("Failed to create child workflow for key #{idempotency_key}: #{e.message}")
          end
        end
        
        # Return summary of what was processed
        {
          total_rows: csv_rows.count,
          created_workflows: created_count,
          skipped_duplicates: skipped_count,
          existing_keys_found: existing_keys.count
        }
      end
      
      private
      
      def validate_inputs(s3_url, parent_workflow_id, child_workflow_class, idempotency_key_generator)
        unless s3_url.is_a?(String) && !s3_url.empty?
          raise ArgumentError, "s3_url must be a non-empty string"
        end
        
        unless parent_workflow_id.is_a?(String) && !parent_workflow_id.empty?
          raise ArgumentError, "parent_workflow_id must be a non-empty string"
        end
        
        unless child_workflow_class.is_a?(Class) && child_workflow_class < Yantra::Workflow
          raise ArgumentError, "child_workflow_class must be a subclass of Yantra::Workflow"
        end
        
        unless idempotency_key_generator.respond_to?(:call)
          raise ArgumentError, "idempotency_key_generator must be callable"
        end
      end
      
      def download_and_parse_csv(s3_url, csv_parser)
        # Use provided parser or default to a simple implementation
        parser = csv_parser || DefaultCsvParser.new
        
        begin
          parser.parse_csv(s3_url)
        rescue => e
          raise Yantra::Errors::WorkflowError, "Failed to download or parse CSV from #{s3_url}: #{e.message}"
        end
      end
      
      def log_error(message)
        Yantra.logger&.error("[CreateChildWorkflowsFromCsvStep] #{message}")
      end
      
      # Default CSV parser implementation
      # In a real application, you would implement this based on your S3 client
      class DefaultCsvParser
        def parse_csv(s3_url)
          # This is a placeholder implementation
          # In practice, you would:
          # 1. Download the file from S3
          # 2. Parse it as CSV
          # 3. Return an array of hashes
          
          # For demonstration, return empty array
          # Replace this with actual S3 download and CSV parsing logic
          []
        end
      end
    end
  end
end 