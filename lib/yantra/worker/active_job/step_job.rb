# --- lib/yantra/worker/active_job/step_job.rb ---

begin
  require 'active_job'
rescue LoadError
  puts "WARN: 'active_job' gem not found. Yantra ActiveJob adapter requires it."
  unless defined?(ActiveJob::Base)
    module ActiveJob; class Base; def self.set(*); self; end; def self.perform_later(*); end; attr_accessor :executions; end; end
  end
end

# Require for deep_symbolize_keys
AS_DEEP_SYMBOLIZE_LOADED = begin
  require 'active_support/core_ext/hash/keys'
  true
rescue LoadError
  unless Hash.method_defined?(:deep_symbolize_keys)
    puts "WARN: ActiveSupport not found. Defining basic deep_symbolize_keys fallback for Hash."
    class ::Hash
      def deep_symbolize_keys
        each_with_object({}) do |(key, value), result|
          new_key = key.to_sym rescue key
          new_value = value.is_a?(Hash) ? value.deep_symbolize_keys : value
          result[new_key] = new_value
        end
      end
    end
  end
  false
end


require_relative '../../step'
require_relative '../../core/orchestrator'
require_relative '../../core/state_machine'
require_relative '../../errors'
require_relative '../retry_handler' # Ensure RetryHandler is loaded

module Yantra
  module Worker
    module ActiveJob
      class StepJob < ::ActiveJob::Base
        attr_accessor :executions unless defined?(executions)

        def perform(yantra_step_id, yantra_workflow_id, yantra_step_klass_name)
          self.executions ||= 1

          puts "INFO: [AJ::StepJob] Attempt ##{self.executions} for Yantra step: #{yantra_step_id} WF: #{yantra_workflow_id} Klass: #{yantra_step_klass_name}"
          # --- Get dependencies (Repo, Notifier) ---
          # Note: Still using global accessors here. Injecting these into StepJob
          # itself would be a larger refactor.
          repo = Yantra.repository
          notifier = Yantra.notifier # Get notifier instance
          unless repo
             raise Yantra::Errors::ConfigurationError, "Yantra repository not configured."
          end
          # Notifier can be nil if configured as :null, handle that downstream
          # --- End Get dependencies ---

          orchestrator = Yantra::Core::Orchestrator.new(repository: repo, notifier: notifier) # Pass notifier to Orchestrator too

          step_record = nil
          user_step_klass = nil

          # --- 1. Notify Orchestrator: Starting ---
          unless orchestrator.step_starting(yantra_step_id)
             puts "WARN: [AJ::StepJob] Orchestrator#step_starting indicated job #{yantra_step_id} should not proceed. Aborting."
             return
          end

          # --- 2. Execute User Code ---
          begin
            step_record = repo.find_step(yantra_step_id)
            unless step_record
               raise Yantra::Errors::StepNotFound, "Job record #{yantra_step_id} not found after starting."
            end
            yantra_step_klass_name = step_record.klass # Use klass from record

            begin
              user_step_klass = Object.const_get(yantra_step_klass_name)
            rescue NameError => e
              raise Yantra::Errors::StepDefinitionError, "Class #{yantra_step_klass_name} could not be loaded: #{e.message}"
            end

            unless user_step_klass && user_step_klass < Yantra::Step
               raise Yantra::Errors::StepDefinitionError, "Class #{yantra_step_klass_name} is not a Yantra::Step subclass."
            end

            # --- Argument Handling (remains the same) ---
            raw_arguments = step_record.arguments
            arguments_hash = {}
            if raw_arguments.is_a?(Hash)
              arguments_hash = raw_arguments
            elsif raw_arguments.is_a?(String) && !raw_arguments.empty?
              begin; arguments_hash = JSON.parse(raw_arguments); arguments_hash = {} unless arguments_hash.is_a?(Hash); rescue JSON::ParserError; arguments_hash = {}; end
            elsif raw_arguments.nil?; arguments_hash = {}
            else; arguments_hash = {}; end
            final_arguments_hash = if arguments_hash.respond_to?(:deep_symbolize_keys); begin; arguments_hash.deep_symbolize_keys; rescue => e; puts "WARN: [AJ::StepJob] deep_symbolize_keys failed: #{e.message}. Falling back."; arguments_hash.transform_keys(&:to_sym) rescue {}; end; else; arguments_hash.transform_keys(&:to_sym) rescue {}; end
            # --- END Argument Handling ---

            parent_ids = repo.get_step_dependencies(yantra_step_id)

            user_step_instance = user_step_klass.new(
              step_id: step_record.id,
              workflow_id: step_record.workflow_id,
              klass: user_step_klass,
              arguments: final_arguments_hash,
              parent_ids: parent_ids,
              queue_name: step_record.queue,
              repository: repo # Inject repository for parent_outputs
            )

            final_arguments_hash_for_perform = final_arguments_hash.transform_keys(&:to_sym) rescue {}
            puts "DEBUG: [AJ::StepJob] Arguments being passed to perform: #{final_arguments_hash_for_perform.inspect}"

            result = user_step_instance.perform(**final_arguments_hash_for_perform)

            # --- 3a. Notify Orchestrator: Success ---
            orchestrator.step_succeeded(yantra_step_id, result)

          rescue StandardError => e
            # --- 3b. Handle Failure via RetryHandler ---
            puts "ERROR: [AJ::StepJob] Job #{yantra_step_id} failed on attempt #{self.executions}. Delegating to RetryHandler."
            unless step_record
              puts "FATAL: [AJ::StepJob] Cannot handle error for job #{yantra_step_id} - step_record not loaded."
              raise e # Re-raise original error if step_record is missing
            end
            # Ensure user_step_klass is defined, fallback to base Step class if loading failed earlier
            user_klass_for_handler = user_step_klass || Yantra::Step

            # Get the configured or default RetryHandler class
            retry_handler_class = Yantra.configuration.try(:retry_handler_class) || Yantra::Worker::RetryHandler

            # --- UPDATED: Instantiate RetryHandler with injected notifier ---
            handler = retry_handler_class.new(
              repository: repo,
              step_record: step_record,
              error: e,
              executions: self.executions,
              user_step_klass: user_klass_for_handler,
              notifier: notifier # Pass the notifier instance
            )
            # --- END UPDATE ---

            begin
              outcome = handler.handle_error!
              # If handler returns :failed, it means max attempts reached and handled
              if outcome == :failed
                puts "INFO: [AJ::StepJob] Notifying orchestrator job finished (failed permanently) for #{yantra_step_id}"
                # Fetch final state to ensure it's marked failed before calling step_finished
                final_record = repo.find_step(yantra_step_id)
                if final_record&.state&.to_sym == Yantra::Core::StateMachine::FAILED
                   orchestrator.step_finished(yantra_step_id)
                else
                   # Log warning if state isn't as expected after handler indicated permanent failure
                   puts "WARN: [AJ::StepJob] RetryHandler indicated permanent failure, but step state is not 'failed' in repo. State: #{final_record&.state}"
                end
              end
              # If handler didn't return :failed, it must have re-raised the error for retry
            rescue => retry_error
              # Ensure we propagate the *original* error if handler raises something different
              unless retry_error.equal?(e)
                 puts "WARN: [AJ::StepJob] RetryHandler raised a different error than expected. Propagating original error."
                 retry_error = e
              end
              puts "INFO: [AJ::StepJob] Retry allowed for job #{yantra_step_id}. Re-raising error for background job system to handle retry scheduling."
              raise retry_error # Re-raise for ActiveJob backend retry mechanism
            end # Inner begin/rescue for handler call

          end # Outer begin/rescue for perform
        end # end perform
      end # class StepJob
    end
  end
end

