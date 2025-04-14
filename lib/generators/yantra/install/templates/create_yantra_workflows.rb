# frozen_string_literal: true

# Migration template copied from the Yantra gem.
# Creates the main table for storing workflow state.
class CreateYantraWorkflows < ActiveRecord::Migration[7.0] # Adjust [7.0] to your target Rails version
  def change
    # Use string type for UUID primary key for cross-database compatibility.
    # enable_extension 'pgcrypto' # Still needed for default generation in Postgres if desired

    create_table :yantra_workflows, id: false do |t| # Use id: false
      # Explicitly define the UUID primary key as a string
      t.string :id, limit: 36, primary_key: true, null: false

      t.string :klass, null: false
      t.json :arguments # Use :json for cross-db compatibility
      t.string :state, null: false
      t.json :globals # Use :json for cross-db compatibility
      t.boolean :has_failures, null: false, default: false

      t.timestamp :created_at, null: false
      t.timestamp :updated_at, null: false
      t.timestamp :started_at
      t.timestamp :finished_at
    end

    add_index :yantra_workflows, :state
  end
end
