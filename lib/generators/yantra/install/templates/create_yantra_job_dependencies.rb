# frozen_string_literal: true

# Migration template copied from the Yantra gem.
# Creates the join table for tracking job dependencies (DAG edges).
class CreateYantraJobDependencies < ActiveRecord::Migration[7.0] # Adjust [7.0] to your target Rails version
  def change
    # This table typically doesn't need its own surrogate primary key (id column).
    # The combination of job_id and depends_on_job_id should be unique.
    create_table :yantra_job_dependencies, id: false do |t|
      t.uuid :job_id, null: false             # The job that depends on another
      t.uuid :depends_on_job_id, null: false # The prerequisite job
    end

    # --- Indexes ---
    # Unique index to prevent duplicate dependency entries and act as primary key logic.
    add_index :yantra_job_dependencies, [:job_id, :depends_on_job_id], unique: true, name: 'idx_job_dependencies_unique'

    # Index for efficiently finding jobs that depend on a specific job (dependents).
    add_index :yantra_job_dependencies, :depends_on_job_id, name: 'idx_job_dependencies_on_prereq'

    # --- Foreign Keys ---
    # Ensure job_id points to a valid job
    add_foreign_key :yantra_job_dependencies, :yantra_jobs, column: :job_id, primary_key: :id, on_delete: :cascade

    # Ensure depends_on_job_id points to a valid job
    add_foreign_key :yantra_job_dependencies, :yantra_jobs, column: :depends_on_job_id, primary_key: :id, on_delete: :cascade
  end
end
