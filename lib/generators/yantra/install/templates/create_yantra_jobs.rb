# frozen_string_literal: true

# Migration template copied from the Yantra gem.
# Creates the table for storing individual job state within workflows.
class CreateYantraJobs < ActiveRecord::Migration[7.0] # Adjust [7.0] to your target Rails version
  def change
    create_table :yantra_jobs, id: :uuid do |t|
      # Foreign key to the workflow this job belongs to. Index is added below.
      t.uuid :workflow_id, null: false

      t.string :klass, null: false      # Fully qualified class name of the Yantra::Job subclass
      t.jsonb :arguments                # Arguments specific to this job instance
      t.string :state, null: false      # Job state (pending, enqueued, running, succeeded, etc.)
      t.string :queue                   # Target queue for the background worker
      t.integer :retries, default: 0, null: false # Current retry count
      t.jsonb :output                   # Stored output upon successful completion
      t.jsonb :error                    # Stored error details upon failure
      t.boolean :is_terminal, default: false, null: false # Indicates if this is a graph terminal node

      # Timestamps reflecting the job lifecycle
      t.timestamp :created_at, null: false  # Or use t.timestamps
      t.timestamp :updated_at, null: false  # Or use t.timestamps
      t.timestamp :enqueued_at              # When the job was sent to the background queue
      t.timestamp :started_at               # When the job's perform method began execution
      t.timestamp :finished_at              # When the job's perform method completed (success or failure)
    end

    # Foreign Key constraint
    add_foreign_key :yantra_jobs, :yantra_workflows, column: :workflow_id, primary_key: :id, on_delete: :cascade

    # --- Indexes ---
    # Crucial index for finding running jobs and checking workflow completion/status
    add_index :yantra_jobs, [:workflow_id, :state]

    # Optional indexes depending on query patterns
    add_index :yantra_jobs, :state # If querying jobs by state across all workflows
    # add_index :yantra_jobs, :queue # If querying jobs by queue
    # add_index :yantra_jobs, :klass # If querying jobs by class
  end
end
