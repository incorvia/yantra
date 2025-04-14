# frozen_string_literal: true

# Migration template copied from the Yantra gem.
# Creates the table for storing individual job state within workflows.
class CreateYantraJobs < ActiveRecord::Migration[7.0] # Adjust [7.0] to your target Rails version
  def change
    create_table :yantra_jobs, id: false do |t| # Use id: false
      # Explicitly define the UUID primary key as a string
      t.string :id, limit: 36, primary_key: true, null: false

      # Use :string, limit: 36 for UUID foreign keys
      t.string :workflow_id, limit: 36, null: false

      t.string :klass, null: false
      t.json :arguments # Use :json
      t.string :state, null: false
      t.string :queue
      t.integer :retries, default: 0, null: false
      t.json :output # Use :json
      t.json :error # Use :json
      t.boolean :is_terminal, default: false, null: false

      t.timestamp :created_at, null: false
      t.timestamp :updated_at, null: false
      t.timestamp :enqueued_at
      t.timestamp :started_at
      t.timestamp :finished_at
    end

    # Foreign Key constraint - Commented out for SQLite compatibility during schema load
    # SQLite doesn't enforce FKs by default anyway. Associations rely on AR logic.
    # Remove `type: :uuid` as PK is now string
    # add_foreign_key :yantra_jobs, :yantra_workflows, column: :workflow_id, primary_key: :id, on_delete: :cascade

    # --- Indexes ---
    add_index :yantra_jobs, [:workflow_id, :state] # Critical index
    add_index :yantra_jobs, :state
    # Add index for the foreign key column itself
    add_index :yantra_jobs, :workflow_id
  end
end

