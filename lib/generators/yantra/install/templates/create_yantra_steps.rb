# frozen_string_literal: true

class CreateYantraSteps < ActiveRecord::Migration[<%= ActiveRecord::Migration.current_version %>]
  def change
    # Creates the table for storing individual step instances within workflows.
    # Uses id: false and defines the primary key explicitly as a string UUID.
    create_table :yantra_steps, id: false do |t|
      # Explicitly define the UUID primary key as a string
      t.string :id, limit: 36, primary_key: true, null: false

      # Foreign key linking to the yantra_workflows table (as string UUID).
      t.string :workflow_id, limit: 36, null: false, index: true # Indexed for lookups by workflow.

      # Basic step information
      t.string :klass, null: false        # Stores the class name of the user-defined step.
      t.json :arguments                   # Stores the arguments passed to the step's perform method (use :text if :json type not supported).
      t.string :state, null: false, index: true # Stores the current state of the step. Indexed.
      t.string :queue                     # Stores the name of the background queue this step should run on.
      t.integer :retries, default: 0, null: false # Counter for how many times this step has been retried.
      t.integer :max_attempts, default: 3, null: false # Stores the maximum number of attempts allowed for this step instance.
      t.json :output                    # Stores the return value of the step's perform method upon success (use :text if :json type not supported).
      t.json :error                     # Stores information about the last error if the step failed (use :text if :json type not supported).

      # Timestamps (using t.timestamp)
      t.timestamp :created_at, null: false # Timestamp for when the step record was created.
      t.timestamp :updated_at, null: false # Timestamp for the last update.
      t.timestamp :enqueued_at            # Timestamp for when the step was sent to the background queue.
      t.timestamp :started_at             # Timestamp for when the step execution began.
      t.timestamp :finished_at            # Timestamp for when the step reached a terminal state.
    end

    # Add the foreign key constraint separately.
    # Ensures that if a workflow is deleted, its associated steps are also deleted (cascade).
    # Specifies the primary_key type matches the string 'id' column on yantra_workflows.
    add_foreign_key :yantra_steps, :yantra_workflows, column: :workflow_id, primary_key: :id, on_delete: :cascade

    # Add any other desired indexes
    # Example: Composite index (optional, but potentially useful)
    # add_index :yantra_steps, [:workflow_id, :state]
  end
end
