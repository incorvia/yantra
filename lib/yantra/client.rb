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

      # 2. Instantiate Workflow & Build Graph
      # This runs the user's `perform` method via `initialize`, populating
      # the instance's @jobs and @dependencies attributes in memory.
      puts "INFO: Instantiating workflow #{workflow_klass}..."
      wf_instance = workflow_klass.new(*args, **kwargs)
      puts "INFO: Workflow instance created with #{wf_instance.jobs.count} jobs and #{wf_instance.dependencies.count} dependency links defined."

      # Get the configured repository adapter instance
      repo = Yantra.repository

      # --- Persistence ---
      # TODO: Consider wrapping these steps in a transaction for adapters that support it (like SQL).
      # This would require adding a `transaction` method to the RepositoryInterface.
      # repo.transaction do
      #   ... persistence steps ...
      # end

      # 3. Persist the Workflow record itself
      # Pass the workflow instance directly to the adapter's persist_workflow method.
      # The adapter implementation knows how to extract the necessary attributes.
      # It should also set the initial state to :pending internally or expect it.
      puts "INFO: Persisting workflow record (ID: #{wf_instance.id})..."
      unless repo.persist_workflow(wf_instance) # Pass the instance, not an attributes hash
         # persist_workflow should ideally raise on failure, but double-check interface contract
         raise Yantra::Errors::PersistenceError, "Failed to persist workflow record for ID: #{wf_instance.id}"
      end
      puts "INFO: Workflow record persisted."

      # 4. Persist the Jobs
      jobs_to_persist = wf_instance.jobs
      if jobs_to_persist.any?
        puts "INFO: Persisting #{jobs_to_persist.count} job records..."
        # --- Ideal: Bulk Persist Jobs ---
        # TODO: Define `persist_jobs_bulk` in RepositoryInterface and implement in adapters using insert_all.
        # if repo.respond_to?(:persist_jobs_bulk)
        #   # Assuming persist_jobs_bulk takes an array of job instances
        #   repo.persist_jobs_bulk(jobs_to_persist)
        # else
        # Fallback: Persist jobs individually (less efficient)
        jobs_to_persist.each do |job|
          # Assuming persist_job takes the job instance
          unless repo.persist_job(job)
             raise Yantra::Errors::PersistenceError, "Failed to persist job record for ID: #{job.id}"
          end
        end
        # end
        puts "INFO: Job records persisted."
      end

      # 5. Persist the Dependencies
      dependencies_hash = wf_instance.dependencies
      if dependencies_hash.any?
        # Transform the { job_id => [dep_id1, ...] } hash into the array format needed by bulk insert.
        links_array = dependencies_hash.flat_map do |job_id, dep_ids|
          dep_ids.map { |dep_id| { job_id: job_id, depends_on_job_id: dep_id } }
        end
        puts "INFO: Persisting #{links_array.count} dependency links..."
        unless repo.add_job_dependencies_bulk(links_array)
           raise Yantra::Errors::PersistenceError, "Failed to persist dependency links for workflow ID: #{wf_instance.id}"
        end
        puts "INFO: Dependency links persisted."
      end

      # 6. Return the Workflow ID
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

