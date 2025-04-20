# lib/yantra/persistence/active_record/step_dependency_record.rb

# Ensure ActiveRecord is loaded, typically done via Bundler/environment setup.
# require 'active_record'

module Yantra
  module Persistence
    # Namespace for the ActiveRecord persistence adapter implementation.
    module ActiveRecord
      # Represents a row in the 'yantra_step_dependencies' join table using ActiveRecord.
      # This table links jobs to their prerequisites (other jobs that must complete first).
      # For a given row:
      #   - `step_id` refers to the job that has the dependency.
      #   - `depends_on_step_id` refers to the job that must be completed first.
      # This class is an internal implementation detail of the ActiveRecordAdapter.
      class StepDependencyRecord < ::ActiveRecord::Base
        # Explicitly set the table name.
        self.table_name = 'yantra_step_dependencies'

        # --- Associations ---

        # Each record belongs to the job that has the dependency requirement.
        belongs_to :step,
                   class_name: "Yantra::Persistence::ActiveRecord::StepRecord",
                   foreign_key: :step_id,
                   # Corresponds to the `has_many :dependency_links` in StepRecord
                   inverse_of: :dependency_links

        # Each record belongs to the job that is the prerequisite dependency.
        belongs_to :dependency,
                   class_name: "Yantra::Persistence::ActiveRecord::StepRecord",
                   foreign_key: :depends_on_step_id,
                   # Corresponds to the `has_many :dependent_links` in StepRecord
                   inverse_of: :dependent_links

        # --- Validations ---

        # Ensure both foreign keys are always present.
        validates :step_id, presence: true
        validates :depends_on_step_id, presence: true

        # Ensure that a specific job cannot depend on the same prerequisite job more than once.
        # This requires a unique index on [step_id, depends_on_step_id] in the database.
        validates :depends_on_step_id,
                  uniqueness: {
                    scope: :step_id,
                    message: "dependency relationship already exists"
                  }

        # Prevent a job from depending on itself.
        validate :step_cannot_depend_on_itself

        private

        # Custom validation method to check if step_id and depends_on_step_id are the same.
        def step_cannot_depend_on_itself
          # Only run validation if both IDs are present (other validators handle presence)
          if step_id.present? && step_id == depends_on_step_id
            errors.add(:depends_on_step_id, "cannot be the same as step_id (a job cannot depend on itself)")
          end
        end

      end
    end
  end
end


