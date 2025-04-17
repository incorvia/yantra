# lib/tasks/yantra_tasks.rake

require 'rake'
require 'active_support/time' # For durations like `days.ago`

namespace :yantra do
  namespace :cleanup do
    desc "Delete expired Yantra workflows that finished before a certain time."
    task :expired_workflows, [:days_ago] => :environment do |_task, args|
      # --- Configuration ---
      # Default to deleting workflows older than 30 days if no argument is provided
      days_ago = args[:days_ago]&.to_i || 30
      if days_ago <= 0

        exit(1) # Exit with an error code
      end

      # Calculate the cutoff timestamp
      cutoff_timestamp = days_ago.days.ago


      begin
        # --- Get Repository Adapter ---
        # Ensure Yantra is configured, especially the persistence adapter
        unless Yantra.config&.persistence_adapter
          raise "Yantra configuration or persistence adapter not found. Ensure Yantra is initialized."
        end
        repository = Yantra.config.persistence_adapter

        # --- Check if Adapter Supports Cleanup ---
        # Crucially, the adapter *must* respond to the method.
        # This relies on the method being defined in the RepositoryInterface
        # and implemented by the specific adapter (like ActiveRecordAdapter).
        unless repository.respond_to?(:delete_expired_workflows)
          raise "The configured Yantra persistence adapter (#{repository.class.name}) does not support `delete_expired_workflows`."
        end

        # --- Execute Cleanup ---
        start_time = Time.now
        deleted_count = repository.delete_expired_workflows(cutoff_timestamp)
        end_time = Time.now
        duration = end_time - start_time

        # --- Report Results ---



      rescue => e
        # --- Error Handling ---


        exit(1) # Exit with an error code
      end
    end
  end
end

