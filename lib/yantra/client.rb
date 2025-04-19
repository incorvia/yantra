# lib/yantra/client.rb

require 'securerandom'
require_relative 'workflow'
require_relative 'step'
require_relative 'errors'
require_relative 'core/orchestrator'
require_relative 'core/state_machine'
require_relative 'core/workflow_retry_service'

module Yantra
  class Client
    def self.create_workflow(workflow_klass, *args, **kwargs)
      unless workflow_klass.is_a?(Class) && workflow_klass < Yantra::Workflow
        raise ArgumentError, "#{workflow_klass} must be a subclass of Yantra::Workflow"
      end

      wf_instance = workflow_klass.new(*args, **kwargs)
      repo = Yantra.repository

      unless repo.persist_workflow(wf_instance)
        raise Yantra::Errors::PersistenceError, "Failed to persist workflow record for ID: #{wf_instance.id}"
      end

      if wf_instance.steps.any?
        unless repo.persist_steps_bulk(wf_instance.steps)
          raise Yantra::Errors::PersistenceError, "Failed to persist job records for workflow ID: #{wf_instance.id}"
        end
      end

      if wf_instance.dependencies.any?
        links_array = wf_instance.dependencies.flat_map do |step_id, dep_ids|
          dep_ids.map { |dep_id| { step_id: step_id, depends_on_step_id: dep_id } }
        end

        unless repo.add_step_dependencies_bulk(links_array)
          raise Yantra::Errors::PersistenceError, "Failed to persist dependency links for workflow ID: #{wf_instance.id}"
        end
      end

      wf_instance.id
    end

    def self.start_workflow(workflow_id)
      Core::Orchestrator.new.start_workflow(workflow_id)
    end

    def self.find_workflow(workflow_id)
      Yantra.repository.find_workflow(workflow_id)
    end

    def self.find_step(step_id)
      Yantra.repository.find_step(step_id)
    end

    def self.get_workflow_steps(workflow_id, status: nil)
      Yantra.repository.get_workflow_steps(workflow_id, status: status)
    end

    def self.cancel_workflow(workflow_id)
      log = log_method_for("cancel_workflow")

      log.call(:info, "Attempting to cancel workflow #{workflow_id}...")
      repo     = Yantra.repository
      notifier = Yantra.notifier
      workflow = repo.find_workflow(workflow_id)
      finished_at_time = Time.current

      unless workflow
        log.call(:warn, "Workflow #{workflow_id} not found.")
        return false
      end

      current_state = workflow.state.to_sym
      if Core::StateMachine.terminal?(current_state) || current_state == Core::StateMachine::CANCELLED
        log.call(:warn, "Workflow already in terminal/cancelled state (#{current_state}).")
        return false
      end

      update_success = [Core::StateMachine::PENDING, Core::StateMachine::RUNNING].any? do |prev_state|
        repo.update_workflow_attributes(
          workflow_id,
          { state: Core::StateMachine::CANCELLED.to_s, finished_at: finished_at_time },
          expected_old_state: prev_state
        )
      end

      unless update_success
        latest_state = repo.find_workflow(workflow_id)&.state || 'unknown'
        log.call(:warn, "Failed to update state to cancelled. Current: #{latest_state}.")
        return false
      end

      log.call(:info, "Workflow #{workflow_id} marked as cancelled.")

      begin
        payload = {
          workflow_id: workflow_id,
          klass: workflow.klass,
          state: Core::StateMachine::CANCELLED,
          finished_at: finished_at_time
        }
        notifier&.publish('yantra.workflow.cancelled', payload)
        log.call(:info, "Published yantra.workflow.cancelled event.")
      rescue => e
        log.call(:error, "Failed to publish workflow cancelled event: #{e.message}")
      end

      steps_to_cancel = repo.get_workflow_steps(workflow_id)
                            .select { |j| [Core::StateMachine::PENDING.to_s, Core::StateMachine::ENQUEUED.to_s].include?(j.state.to_s) }

      step_ids = steps_to_cancel.map(&:id)
      log.call(:debug, "Steps to cancel: #{step_ids.inspect}")

      if step_ids.any?
        log.call(:info, "Cancelling #{step_ids.size} jobs in DB.")

        begin
          cancelled_count = repo.cancel_steps_bulk(step_ids)
          log.call(:info, "Cancelled #{cancelled_count} jobs in repository.")

          step_ids.each do |step_id|
            begin
              notifier&.publish('yantra.step.cancelled', { step_id: step_id, workflow_id: workflow_id })
            rescue => e
              log.call(:error, "Failed to publish step.cancelled for #{step_id}: #{e.message}")
            end
          end
        rescue Yantra::Errors::PersistenceError => e
          log.call(:error, "Failed during job cancellation: #{e.message}")
        end
      else
        log.call(:info, "No pending/enqueued steps found to cancel.")
      end

      true
    rescue => e
      log.call(:error, "Unexpected error: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      false
    end

    def self.retry_failed_steps(workflow_id)
      log = log_method_for("retry_failed_steps")

      repo     = Yantra.repository
      worker   = Yantra.worker_adapter
      notifier = Yantra.notifier
      workflow = repo.find_workflow(workflow_id)

      unless workflow
        log.call(:warn, "Workflow #{workflow_id} not found.")
        return false
      end

      unless workflow.state.to_sym == Core::StateMachine::FAILED
        log.call(:warn, "Workflow not in FAILED state (current: #{workflow.state}).")
        return false
      end

      log.call(:info, "Resetting workflow #{workflow_id} to RUNNING.")
      success = repo.update_workflow_attributes(
        workflow_id,
        {
          state: Core::StateMachine::RUNNING.to_s,
          has_failures: false,
          finished_at: nil
        },
        expected_old_state: Core::StateMachine::FAILED
      )

      unless success
        latest_state = repo.find_workflow(workflow_id)&.state || 'unknown'
        log.call(:error, "Failed to reset state. Current: #{latest_state}.")
        return false
      end

      retry_service = Core::WorkflowRetryService.new(
        workflow_id: workflow_id,
        repository: repo,
        notifier: notifier,
        worker_adapter: worker
      )
      count = retry_service.call
      log.call(:info, "Re-enqueued #{count} failed steps for workflow #{workflow_id}.")
      count
    rescue => e
      log.call(:error, "Unexpected error: #{e.class} - #{e.message}\n#{e.backtrace.take(5).join("\n")}")
      false
    end

    private

    def self.log_method_for(context)
      prefix = "[Client.#{context}]"
      if Yantra.logger
        ->(level, msg) { Yantra.logger.send(level, "#{prefix} #{msg}") }
      else
        ->(level, msg) { puts "#{level.upcase}: #{prefix} #{msg}" }
      end
    end
  end
end

