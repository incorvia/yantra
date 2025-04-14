# lib/yantra/client.rb

require 'securerandom'
require_relative 'workflow'
require_relative 'job'
require_relative 'errors'
# Note: We interact with the repository via Yantra.repository,
# so no specific adapter require is needed here.

module Yantra
  # Public interface for interacting with the Yantra workflow system.
  # Provides methods for creating, starting, and inspecting workflows and jobs.
  class Client

    # Creates and persists a new workflow instance along with its defined
    # jobs and their dependencies.
    #
    # @param workflow_klass [Class] The user-defined class inheriting from Yantra::Workflow.
    # @param args [Array] Positional arguments to pass to the workflow's #initialize and #perform.
    # @param kwargs [Hash] Keyword arguments to pass to the workflow's #initialize and #perform.
    #   Note: :globals will be extracted if present and stored separately.
    # @return [String] The UUID of the newly created workflow.
    # @raise [ArgumentError] if workflow_klass is not a valid Yantra::Workflow subclass.
    # @raise [Yantra::Errors::PersistenceError] if saving any part fails.
    def self.create_workflow(workflow_klass, *args, **kwargs)
      # 1. Validate Input
      unless workflow_klass.is_a?(Class) && workflow_klass < Yantra::Workflow
        raise ArgumentError, "#{workflow_klass} must be a Class inheriting from Yantra::Workflow"
      end

      # 2. Instantiate Workflow & Build Graph & Calculate Terminal Status
      # The Workflow#initialize method now calls #perform and #calculate_terminal_status! internally
      puts "INFO: Instantiating workflow #{workflow_klass}..."
      wf_instance = workflow_klass.new(*args, **kwargs)
      puts "INFO: Workflow instance created and processed with #{wf_instance.jobs.count} jobs and #{wf_instance.dependencies.keys.count} jobs having dependencies."

      # 3. Get the configured repository adapter instance
      repo = Yantra.repository

      # --- Persistence ---
      # TODO: Consider wrapping these steps in a transaction for adapters that support it (like SQL).
      # repo.transaction do ... end

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
        # Use the bulk persistence method defined in the interface/adapter
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

    # --- Placeholder for Other Client Methods ---
    # These would be implemented based on Design Doc Section 6

    # def self.start_workflow(workflow_id)
    #   puts "INFO: Starting workflow #{workflow_id}..."
    #   # orchestrator = Core::Orchestrator.new(Yantra.repository) # Get orchestrator instance
    #   # orchestrator.start_workflow(workflow_id)
    #   raise NotImplementedError, "#{self.class.name}.start_workflow is not implemented"
    # end

    # def self.find_workflow(workflow_id)
    #   puts "INFO: Finding workflow #{workflow_id}..."
    #   Yantra.repository.find_workflow(workflow_id)
    # end

    # def self.find_job(job_id)
    #   puts "INFO: Finding job #{job_id}..."
    #   Yantra.repository.find_job(job_id)
    # end

    # def self.get_workflow_jobs(workflow_id, status: nil)
    #   puts "INFO: Getting jobs for workflow #{workflow_id} (status: #{status})..."
    #   Yantra.repository.get_workflow_jobs(workflow_id, status: status)
    # end

    # ... implement other methods like cancel, retry, recover, delete ...

  end
end

