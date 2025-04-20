# lib/yantra/persistence/active_record/workflow_record.rb

# Ensure ActiveRecord is loaded, typically done via Bundler/environment setup.
# require 'active_record'

module Yantra
  module Persistence
    # Namespace for the ActiveRecord persistence adapter implementation.
    module ActiveRecord
      # Represents a workflow instance persisted in the 'yantra_workflows' table.
      #
      # This class maps the core attributes of a Yantra workflow (like state, class)
      # and its relationship to its constituent steps (jobs) to a database record.
      # It is primarily an internal detail used by the ActiveRecordAdapter.
      class WorkflowRecord < ::ActiveRecord::Base
        # Explicitly set the table name for clarity and independence from conventions.
        self.table_name = 'yantra_workflows'

        # Defines the one-to-many relationship between a workflow and its steps.
        has_many :step_records,
                 class_name: 'Yantra::Persistence::ActiveRecord::StepRecord',
                 foreign_key: :workflow_id, # Column in 'yantra_steps' referencing this workflow
                 inverse_of: :workflow_record, # Corresponding belongs_to in StepRecord
                 dependent: :destroy # Deleting a workflow cascades to delete its associated steps

        # Scopes for querying workflows based on their state.
        scope :with_state, ->(state) { where(state: state.to_s) }
        scope :pending, -> { with_state('pending') }
        scope :running, -> { with_state('running') }
        scope :succeeded, -> { with_state('succeeded') }
        scope :failed, -> { with_state('failed') }
      end
    end
  end
end
