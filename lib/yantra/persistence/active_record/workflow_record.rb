# lib/yantra/persistence/active_record/workflow_record.rb

# Ensure ActiveRecord is loaded, typically done via Bundler/environment setup.
# require 'active_record'

module Yantra
  module Persistence
    # Namespace for the ActiveRecord persistence adapter implementation.
    module ActiveRecord
      # Represents a row in the 'yantra_workflows' table using ActiveRecord.
      # This class is an internal implementation detail of the ActiveRecordAdapter
      # and is not directly used by the core Yantra logic.
      class WorkflowRecord < ::ActiveRecord::Base
        # Explicitly set the table name if it doesn't follow Rails conventions
        # or for clarity.
        self.table_name = 'yantra_workflows'

        # --- Associations ---

        # A workflow typically has many associated jobs.
        has_many :step_records,
                 # Specify the class name with full namespace.
                 class_name: "Yantra::Persistence::ActiveRecord::StepRecord",
                 # Specify the foreign key column in the 'yantra_steps' table
                 # that references this workflow's ID.
                 foreign_key: :workflow_id,
                 # Define the corresponding association name in StepRecord
                 # for bi-directional association optimization.
                 inverse_of: :workflow_record,
                 # If a workflow record is deleted from the database,
                 # also delete all associated job records.
                 dependent: :destroy

        # --- Scopes (Examples) ---
        # Provide convenient ways to query workflows by state.
        scope :with_state, ->(state) { where(state: state.to_s) }
        scope :running, -> { with_state('running') }
        scope :failed, -> { with_state('failed') }
        scope :succeeded, -> { with_state('succeeded') }
        scope :pending, -> { with_state('pending') }
        # ... add other state scopes as needed

        # --- Validations (Examples) ---
        # Add validations as needed to ensure data integrity at the DB level.
        # validates :id, presence: true, uniqueness: true # UUIDs should be present and unique
        # validates :klass, presence: true
        # validates :state, presence: true, inclusion: {
        #   in: %w[pending running succeeded failed cancelled],
        #   message: "%{value} is not a valid state"
        # }
        # validates :has_failures, inclusion: { in: [true, false] }

        # --- Serialization (If using JSON/JSONB) ---
        # Modern Rails versions often handle JSON/JSONB automatically.
        # If explicit serialization is needed:
        # serialize :arguments, JSON
        # serialize :globals, JSON # If you have a globals JSONB field

        # --- Instance Methods (Optional Helpers) ---

        # Example helper to convert AR record to a simple Workflow Status Object/DTO
        # This might be used by the ActiveRecordAdapter when implementing `find_workflow`.
        # def to_status_object
        #   {
        #     id: self.id,
        #     klass: self.klass,
        #     state: self.state,
        #     has_failures: self.has_failures,
        #     # arguments: self.arguments, # Usually not needed for status overview
        #     created_at: self.created_at,
        #     started_at: self.started_at,
        #     finished_at: self.finished_at
        #   }
        # end

      end
    end
  end
end

