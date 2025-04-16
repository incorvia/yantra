# --- lib/yantra/worker/active_job/step_job.rb ---

begin
  require 'active_job'
rescue LoadError
  puts "WARN: 'active_job' gem not found. Yantra ActiveJob adapter requires it."
  # Define minimal ActiveJob::Base if not found, for environments without it.
  unless defined?(ActiveJob::Base)
    module ActiveJob; class Base; def self.set(*); self; end; def self.perform_later(*); end; attr_accessor :executions; end; end
  end
end

# Require for deep_symbolize_keys. Add 'activesupport' as a dependency if not already present.
AS_DEEP_SYMBOLIZE_LOADED = begin
  require 'active_support/core_ext/hash/keys'
  true
rescue LoadError
  # Define a simple fallback if ActiveSupport is not available
  unless Hash.method_defined?(:deep_symbolize_keys)
    puts "WARN: ActiveSupport not found. Defining basic deep_symbolize_keys fallback for Hash."
    class ::Hash
      def deep_symbolize_keys
        each_with_object({}) do |(key, value), result| # Start block
          new_key = key.to_sym rescue key
          new_value = value.is_a?(Hash) ? value.deep_symbolize_keys : value
          result[new_key] = new_value
        end # End block
      end
    end # End class ::Hash
  end # End unless
  false # Indicate AS version was not loaded
end


require_relative '../../step'
require_relative '../../core/orchestrator'
require_relative '../../core/state_machine'
require_relative '../../errors'
require_relative '../retry_handler'

module Yantra
  module Worker
    module ActiveJob
      class StepJob < ::ActiveJob::Base
        # Make executions accessible if not already provided by ActiveJob::Base version
        attr_accessor :executions unless defined?(executions)

        # Main execution logic called by ActiveJob backend.
        def perform(yantra_step_id, yantra_workflow_id, yantra_step_klass_name)
          # Initialize executions if it's nil (might happen in some test scenarios)
          self.executions ||= 1 # Assume first execution if not set by ActiveJob

          puts "INFO: [AJ::StepJob] Attempt ##{self.executions} for Yantra step: #{yantra_step_id} WF: #{yantra_workflow_id} Klass: #{yantra_step_klass_name}"
          repo = Yantra.repository
          orchestrator = Yantra::Core::Orchestrator.new

          step_record = nil
          user_step_klass = nil

          # --- 1. Notify Orchestrator: Starting ---
          unless orchestrator.step_starting(yantra_step_id)
             puts "WARN: [AJ::StepJob] Orchestrator#step_starting indicated job #{yantra_step_id} should not proceed. Aborting."
             return
          end

          # --- 2. Execute User Code ---
          begin
            # Fetch job record
            step_record = repo.find_step(yantra_step_id)
            unless step_record
               raise Yantra::Errors::StepNotFound, "Job record #{yantra_step_id} not found after starting."
            end
            yantra_step_klass_name = step_record.klass # Use persisted class name

            begin
              user_step_klass = Object.const_get(yantra_step_klass_name)
            rescue NameError => e
              raise Yantra::Errors::StepDefinitionError, "Class #{yantra_step_klass_name} could not be loaded: #{e.message}"
            end

            unless user_step_klass && user_step_klass < Yantra::Step
               raise Yantra::Errors::StepDefinitionError, "Class #{yantra_step_klass_name} is not a Yantra::Step subclass."
            end

            # --- Robust Argument Handling & Logging ---
            raw_arguments = step_record.arguments # Get raw value from DB/adapter
            puts "DEBUG: [AJ::StepJob] Raw arguments loaded: #{raw_arguments.inspect} (Class: #{raw_arguments.class})" # DEBUG LOG

            arguments_hash = {} # Default to empty hash
            if raw_arguments.is_a?(Hash)
              arguments_hash = raw_arguments
            elsif raw_arguments.is_a?(String) && !raw_arguments.empty?
              begin
                # Attempt to parse if it looks like JSON
                arguments_hash = JSON.parse(raw_arguments)
                unless arguments_hash.is_a?(Hash)
                  puts "WARN: [AJ::StepJob] Parsed arguments from string but result was not a Hash: #{arguments_hash.inspect}"
                  arguments_hash = {}
                end
              rescue JSON::ParserError => json_err
                puts "WARN: [AJ::StepJob] Failed to parse arguments string as JSON: #{json_err.message}. Raw: #{raw_arguments.inspect}"
                arguments_hash = {}
              end
            elsif raw_arguments.nil?
              arguments_hash = {} # Explicitly handle nil
            else
              puts "WARN: [AJ::StepJob] Unexpected arguments type loaded: #{raw_arguments.class}. Treating as empty."
              arguments_hash = {}
            end

            # Deep Symbolize Keys if possible
            if arguments_hash.respond_to?(:deep_symbolize_keys)
              begin
                arguments_hash = arguments_hash.deep_symbolize_keys
              rescue => e # Catch potential errors during symbolization
                puts "WARN: [AJ::StepJob] Failed during deep_symbolize_keys: #{e.message}. Using original hash."
                # Fallback to basic symbolization if deep fails
                arguments_hash = arguments_hash.transform_keys(&:to_sym) rescue arguments_hash
              end
            else
              # Basic fallback symbolization
              arguments_hash = arguments_hash.transform_keys(&:to_sym) rescue arguments_hash
            end
            puts "DEBUG: [AJ::StepJob] Final arguments_hash for perform: #{arguments_hash.inspect}" # DEBUG LOG
            # --- End Argument Handling ---


            parent_ids = repo.get_step_dependencies(yantra_step_id)

            user_step_instance = user_step_klass.new(
              step_id: step_record.id,
              workflow_id: step_record.workflow_id,
              klass: user_step_klass,
              arguments: arguments_hash, # Pass symbolized hash to initializer
              parent_ids: parent_ids,
              queue_name: step_record.queue,
              repository: repo
            )

            # Pass symbolized hash to perform using splat
            result = user_step_instance.perform(**arguments_hash)

            # --- 3a. Notify Orchestrator: Success ---
            orchestrator.step_succeeded(yantra_step_id, result)

          rescue StandardError => e
            # --- 3b. Handle Failure via RetryHandler ---
            # (Error handling logic remains the same - re-raises error for backend)
            puts "ERROR: [AJ::StepJob] Job #{yantra_step_id} failed on attempt #{self.executions}. Delegating to RetryHandler."
            unless step_record
              puts "FATAL: [AJ::StepJob] Cannot handle error for job #{yantra_step_id} - step_record not loaded."
              raise e
            end
            user_klass_for_handler = user_step_klass || Yantra::Step

            retry_handler_class = Yantra.configuration.try(:retry_handler_class) || Yantra::Worker::RetryHandler
            handler = retry_handler_class.new(
              repository: repo,
              step_record: step_record,
              error: e,
              executions: self.executions,
              user_step_klass: user_klass_for_handler
            )

            begin
              outcome = handler.handle_error!
              if outcome == :failed
                puts "INFO: [AJ::StepJob] Notifying orchestrator job finished (failed permanently) for #{yantra_step_id}"
                final_record = repo.find_step(yantra_step_id)
                if final_record&.state&.to_sym == Yantra::Core::StateMachine::FAILED
                   orchestrator.step_finished(yantra_step_id)
                else
                   puts "WARN: [AJ::StepJob] RetryHandler indicated permanent failure, but step state is not 'failed' in repo. State: #{final_record&.state}"
                end
              end
            rescue => retry_error
              unless retry_error.equal?(e)
                 puts "WARN: [AJ::StepJob] RetryHandler raised a different error than expected. Propagating original error."
                 retry_error = e
              end
              puts "INFO: [AJ::StepJob] Retry allowed for job #{yantra_step_id}. Re-raising error for background job system to handle retry scheduling."
              raise retry_error
            end # Inner begin/rescue for handler call
          end # Outer begin/rescue for perform
        end # end perform
      end # class StepJob
    end
  end
end

