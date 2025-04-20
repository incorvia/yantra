# lib/yantra/persistence/active_record/step_record.rb

# Ensure ActiveRecord is loaded, typically done via Bundler/environment setup.
# require 'active_record'

module Yantra
  module Persistence
    # Namespace for the ActiveRecord persistence adapter implementation.
    module ActiveRecord
      # Represents a workflow step (or job) persisted in the 'yantra_steps' table.
      #
      # This class maps the core attributes of a Yantra step (like state, class, arguments)
      # and its relationships (workflow, dependencies) to a database record. It is
      # primarily an internal detail used by the ActiveRecordAdapter.
      class StepRecord < ::ActiveRecord::Base
        # Explicitly set the table name for clarity and independence from conventions.
        self.table_name = 'yantra_steps'

        # Establishes the link to the parent workflow record.
        belongs_to :workflow_record,
                   class_name: 'Yantra::Persistence::ActiveRecord::WorkflowRecord',
                   foreign_key: :workflow_id,
                   inverse_of: :step_records # Corresponds to has_many :step_records in WorkflowRecord

        # Defines the links in the join table representing steps this step depends on (prerequisites).
        # `step_id` in the join table refers to *this* step.
        has_many :dependency_links,
                 class_name: 'Yantra::Persistence::ActiveRecord::StepDependencyRecord',
                 foreign_key: :step_id,
                 inverse_of: :step, # Link back from StepDependencyRecord
                 dependent: :destroy # Deleting this step removes its requirement links

        # Defines the association to the actual prerequisite StepRecord models via the join table.
        has_many :dependencies,
                 through: :dependency_links,
                 source: :dependency # Use the :dependency association on StepDependencyRecord

        # Defines the links in the join table representing steps that depend on this step.
        # `depends_on_step_id` in the join table refers to *this* step.
        has_many :dependent_links,
                 class_name: 'Yantra::Persistence::ActiveRecord::StepDependencyRecord',
                 foreign_key: :depends_on_step_id,
                 inverse_of: :dependency, # Link back from StepDependencyRecord
                 dependent: :destroy # Deleting this step removes links where others depended on it

        # Defines the association to the actual dependent StepRecord models via the join table.
        has_many :dependents,
                 through: :dependent_links,
                 source: :step # Use the :step association on StepDependencyRecord

        # Scopes for querying steps based on their state.
        scope :with_state, ->(state) { where(state: state.to_s) }
        scope :pending, -> { with_state('pending') }
        scope :enqueued, -> { with_state('enqueued') }
        scope :running, -> { with_state('running') }
        scope :succeeded, -> { with_state('succeeded') }
        scope :failed, -> { with_state('failed') }
        scope :cancelled, -> { with_state('cancelled') }
      end
    end
  end
end
