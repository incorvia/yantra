# frozen_string_literal: true

# Migration template copied from the Yantra gem.
# Creates the join table for tracking job dependencies (DAG edges).
class CreateYantraStepDependencies < ActiveRecord::Migration[7.0] # Adjust [7.0] to your target Rails version
  def change
    create_table :yantra_step_dependencies, id: false do |t|
      # Use :string, limit: 36 for UUID foreign keys
      t.string :step_id, limit: 36, null: false
      t.string :depends_on_step_id, limit: 36, null: false
    end

    # --- Indexes ---
    add_index :yantra_step_dependencies, [:step_id, :depends_on_step_id], unique: true, name: 'idx_step_dependencies_unique'
    add_index :yantra_step_dependencies, :depends_on_step_id, name: 'idx_step_dependencies_on_prereq'
    # Add index for the step_id column as well if needed for finding dependencies quickly
    # add_index :yantra_step_dependencies, :step_id

    # --- Foreign Keys ---
    # Commented out for SQLite compatibility during schema load.
    # Remove `type: :uuid` as PK is now string
    # add_foreign_key :yantra_step_dependencies, :yantra_steps, column: :step_id, primary_key: :id, on_delete: :cascade
    # add_foreign_key :yantra_step_dependencies, :yantra_steps, column: :depends_on_step_id, primary_key: :id, on_delete: :cascade
  end
end
