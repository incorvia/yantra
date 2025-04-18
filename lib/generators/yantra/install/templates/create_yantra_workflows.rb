class CreateYantraWorkflows < ActiveRecord::Migration[<%= ActiveRecord::Migration.current_version %>]
  def change
    # Creates the main table for storing workflow instances.
    # Uses id: false and defines the primary key explicitly as a string UUID.
    create_table :yantra_workflows, id: false do |t|
      # Explicitly define the UUID primary key as a string
      t.string :id, limit: 36, primary_key: true, null: false

      # Basic workflow information
      t.string :klass, null: false       # Stores the class name of the user-defined workflow.
      t.jsonb :arguments                  # Stores the initial arguments passed to the workflow (use :text if :json type not supported).
      t.string :state, null: false       # Stores the current state (e.g., pending, running, succeeded, failed).
      t.jsonb :globals                    # Optional field for storing global data (use :text if :json type not supported).
      t.boolean :has_failures, null: false, default: false # Flag indicating if any step within the workflow has failed permanently.

      # Timestamps (using t.timestamp for cross-db compatibility)
      t.timestamp :created_at, null: false # Timestamp for when the workflow record was created.
      t.timestamp :updated_at, null: false # Timestamp for the last update (e.g., state change).
      t.timestamp :started_at             # Timestamp for when the workflow execution began.
      t.timestamp :finished_at            # Timestamp for when the workflow reached a terminal state.
    end

    # Add indexes for commonly queried columns
    add_index :yantra_workflows, :state
    add_index :yantra_workflows, :started_at
    add_index :yantra_workflows, :finished_at
    # Note: Primary key is automatically indexed.
  end
end
