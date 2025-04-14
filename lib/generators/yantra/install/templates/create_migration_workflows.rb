# frozen_string_literal: true

# Migration template copied from the Yantra gem.
# Creates the main table for storing workflow state.
class CreateYantraWorkflows < ActiveRecord::Migration[7.0] # Adjust [7.0] to your target Rails version
  def change
    # Use UUIDs for primary keys if your database supports it (recommended for Postgres)
    # Enable pgcrypto extension for Postgres if not already enabled:
    # enable_extension 'pgcrypto' unless extension_enabled?('pgcrypto')

    create_table :yantra_workflows, id: :uuid do |t|
      t.string :klass, null: false          # Fully qualified class name of the Yantra::Workflow subclass
      t.jsonb :arguments                    # Arguments used to start the workflow
      t.string :state, null: false          # Current state (pending, running, succeeded, failed, etc.)
      t.jsonb :globals                      # Optional shared data for the workflow
      t.boolean :has_failures, null: false, default: false # Flag set to true if any job fails

      t.timestamp :created_at, null: false  # Handled by default t.timestamps if preferred
      t.timestamp :updated_at, null: false  # Handled by default t.timestamps if preferred
      t.timestamp :started_at               # When the workflow transitioned to running
      t.timestamp :finished_at              # When the workflow transitioned to succeeded/failed/cancelled

      # t.timestamps null: false # Alternative shorter syntax for created_at/updated_at
    end

    # Index state for faster querying by status
    add_index :yantra_workflows, :state
    # Optional: Index klass if you query by workflow type often
    # add_index :yantra_workflows, :klass
  end
end
