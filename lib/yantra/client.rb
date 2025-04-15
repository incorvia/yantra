# lib/yantra/client.rb

require 'securerandom'
require_relative 'workflow'
require_relative 'job'
require_relative 'errors'
require_relative 'core/orchestrator' # Require Orchestrator

module Yantra
  # Public interface for interacting with the Yantra workflow system.
  # Provides methods for creating, starting, and inspecting workflows and jobs.
  class Client

    # Creates and persists a new workflow instance along with its defined
    # jobs and their dependencies.
    # (Implementation remains the same as before)
    def self.create_workflow(workflow_klass, *args, **kwargs)
      # 1. Validate Input
      unless workflow_klass.is_a?(Class) && workflow_klass < Yantra::Workflow
        raise ArgumentError, "#{workflow_klass} must be a Class inheriting from Yantra::Workflow"
      end

      # 2. Instantiate Workflow & Build Graph & Calculate Terminal Status
      puts "INFO: Instantiating workflow #{workflow_klass}..."
      wf_instance = workflow_klass.new(*args, **kwargs)
      puts "INFO: Workflow instance created and processed with #{wf_instance.jobs.count} jobs and #{wf_instance.dependencies.keys.count} jobs having dependencies."

      # 3. Get the configured repository adapter instance
      repo = Yantra.repository

      # --- Persistence ---
      # TODO: Consider wrapping these steps in a transaction

      # 4. Persist the Workflow record itself
      puts "INFO: Persisting workflow record (ID: #{wf_instance.id})..."
      unless repo.persist_workflow(wf_instance) # Pass instance directly
         raise Yantra::Errors::PersistenceError, "Failed to persist workflow record for ID: #{wf_instance.id}"
      end
      puts "INFO: Workflow record persisted."

      # 5. Persist the Jobs (using bulk method)
      jobs_to_persist = wf_instance.jobs
      if jobs_to_persist.any?
        puts "INFO: Persisting #{jobs_to_persist.count} job records via bulk..."
        unless repo.persist_jobs_bulk(jobs_to_persist)
          raise Yantra::Errors::PersistenceError, "Failed to bulk persist job records for workflow ID: #{wf_instance.id}"
        end
        puts "INFO: Job records persisted."
      end

      # 6. Persist the Dependencies (using bulk method)
      dependencies_hash = wf_instance.dependencies
      if dependencies_hash.any?
        links_array = dependencies_hash.flat_map do |job_id, dep_ids|
          dep_ids.map { |dep_id| { job_id: job_id, depends_on_job_id: dep_id } }
        end
        puts "INFO: Persisting #{links_array.count} dependency links via bulk..."
        unless repo.add_job_dependencies_bulk(links_array)
           raise Yantra::Errors::PersistenceError, "Failed to persist dependency links for workflow ID: #{wf_instance.id}"
        end
        puts "INFO: Dependency links persisted."
      end

      # 7. Return the Workflow ID
      puts "INFO: Workflow #{wf_instance.id} created successfully."
      wf_instance.id
    end

    # --- Implemented Client Methods ---

    # Starts a previously created workflow.
    # This triggers the Orchestrator to find and enqueue initial jobs.
    # @param workflow_id [String] The UUID of the workflow to start.
    # @return [Boolean] true if the start process was initiated successfully, false otherwise.
    def self.start_workflow(workflow_id)
      puts "INFO: [Client] Requesting start for workflow #{workflow_id}..."
      # Instantiate orchestrator (uses configured repo/worker adapters)
      orchestrator = Core::Orchestrator.new
      # Delegate to orchestrator's start method
      orchestrator.start_workflow(workflow_id)
    end

    # Finds a workflow by its ID using the configured repository.
    # @param workflow_id [String] The UUID of the workflow.
    # @return [Object, nil] A representation of the workflow or nil if not found.
    def self.find_workflow(workflow_id)
      puts "INFO: [Client] Finding workflow #{workflow_id}..."
      Yantra.repository.find_workflow(workflow_id)
    end

    # Finds a job by its ID using the configured repository.
    # @param job_id [String] The UUID of the job.
    # @return [Object, nil] A representation of the job or nil if not found.
    def self.find_job(job_id)
      puts "INFO: [Client] Finding job #{job_id}..."
      Yantra.repository.find_job(job_id)
    end

    # Retrieves jobs associated with a workflow, optionally filtered by status.
    # @param workflow_id [String] The UUID of the workflow.
    # @param status [Symbol, String, nil] Filter jobs by this state if provided.
    # @return [Array<Object>] An array of job representations.
    def self.get_workflow_jobs(workflow_id, status: nil)
      puts "INFO: [Client] Getting jobs for workflow #{workflow_id} (status: #{status})..."
      Yantra.repository.get_workflow_jobs(workflow_id, status: status)
    end

    # --- Placeholder for Other Client Methods ---

    # def self.list_workflows(status: nil, limit: 50, offset: 0)
    #   puts "INFO: [Client] Listing workflows..."
    #   Yantra.repository.list_workflows(status: status, limit: limit, offset: offset)
    # end

    # def self.cancel_workflow(workflow_id)
    #   puts "INFO: [Client] Requesting cancellation for workflow #{workflow_id}..."
    #   # orchestrator = Core::Orchestrator.new
    #   # orchestrator.cancel_workflow(workflow_id) # Need cancel method on orchestrator
    #   raise NotImplementedError
    # end

    # def self.retry_failed_jobs(workflow_id)
    #   puts "INFO: [Client] Requesting retry for failed jobs in workflow #{workflow_id}..."
    #   # orchestrator = Core::Orchestrator.new
    #   # orchestrator.retry_failed_jobs(workflow_id) # Need retry method on orchestrator
    #   raise NotImplementedError
    # end

    # ... etc ...

  end
end

