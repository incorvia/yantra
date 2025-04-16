# frozen_string_literal: true

class CreateYantraStepDependencies < ActiveRecord::Migration[<%= ActiveRecord::Migration.current_version %>]
  def change
    # Creates the join table to represent the dependency graph (DAG edges).
    # No separate primary key ('id: false') as the combination of the two foreign keys is unique.
    create_table :yantra_step_dependencies, id: false do |t|
      # Foreign key for the step that depends on another (the "downstream" step) (as string UUID).
      t.string :step_id, limit: 36, null: false

      # Foreign key for the step that must be completed first (the "upstream" prerequisite/dependency) (as string UUID).
      t.string :depends_on_step_id, limit: 36, null: false
    end

    # Add a unique composite index to enforce the one-way dependency relationship
    # and provide fast lookups based on the dependent step.
    add_index :yantra_step_dependencies, [:step_id, :depends_on_step_id], unique: true, name: 'index_yantra_step_dependencies_on_step_and_depends_on'

    # Add an index on the prerequisite step ID for efficiently finding all steps
    # that depend on a specific step (finding dependents).
    add_index :yantra_step_dependencies, [:depends_on_step_id], name: 'index_yantra_step_dependencies_on_depends_on_step_id'

    # Add foreign key constraints separately.
    # Ensures dependency records are removed if either linked step is deleted (cascade).
    # Specifies the primary_key type matches the string 'id' column on yantra_steps.
    add_foreign_key :yantra_step_dependencies, :yantra_steps, column: :step_id, primary_key: :id, on_delete: :cascade
    add_foreign_key :yantra_step_dependencies, :yantra_steps, column: :depends_on_step_id, primary_key: :id, on_delete: :cascade
  end
end
