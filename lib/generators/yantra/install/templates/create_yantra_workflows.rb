class CreateYantraWorkflows < ActiveRecord::Migration[<%= ActiveRecord::Migration.current_version %>]
  def change
    # Creates the main table for storing workflow instances.
    # Uses id: false and defines the primary key explicitly as a string UUID.
    create_table :yantra_workflows, id: false do |t|
      # Explicitly define the UUID primary key as a string
      t.string :id, limit: 36, primary_key: true, null: false

      # Basic workflow information
      t.string :klass, null: false       # Stores the class name of the user-defined workflow.
      t.jsonb :arguments                # Stores the initial arguments passed to the workflow (use :text if :json type not supported).
      t.string :state, null: false       # Stores the current state (e.g., pending, running, succeeded, failed).
      t.jsonb :globals                   # Optional field for storing global data (use :text if :json type not supported).
      t.boolean :has_failures, null: false, default: false # Flag indicating if any step within the workflow has failed permanently.

      # Timestamps (using t.timestamp for cross-db compatibility)
      # Explicit precision: nil ensures consistency if needed, otherwise defaults might vary.
      t.timestamp :created_at, precision: nil, null: false # Timestamp for when the workflow record was created.
      t.timestamp :updated_at, precision: nil, null: false # Timestamp for the last update (e.g., state change).
      t.timestamp :started_at, precision: nil             # Timestamp for when the workflow execution began.
      t.timestamp :finished_at, precision: nil            # Timestamp for when the workflow reached a terminal state.
    end

    add_index :yantra_workflows, :state       # Quickly find workflows by state
    add_index :yantra_workflows, :started_at  # Query by start time
    add_index :yantra_workflows, :finished_at # Query by finish time

    add_index :yantra_workflows, [:klass, :state], name: 'index_yantra_workflows_on_klass_and_state'

    add_index :yantra_workflows, :has_failures, name: 'index_yantra_workflows_on_has_failures'
  end
end
