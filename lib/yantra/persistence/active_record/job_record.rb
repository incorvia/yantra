# lib/yantra/persistence/active_record/job_record.rb

# Ensure ActiveRecord is loaded, typically done via Bundler/environment setup.
# require 'active_record'

module Yantra
  module Persistence
    # Namespace for the ActiveRecord persistence adapter implementation.
    module ActiveRecord
      # Represents a row in the 'yantra_jobs' table using ActiveRecord.
      # This class is an internal implementation detail of the ActiveRecordAdapter
      # and is not directly used by the core Yantra logic.
      class JobRecord < ::ActiveRecord::Base
        # Explicitly set the table name if it doesn't follow Rails conventions
        # or for clarity.
        self.table_name = 'yantra_jobs'

        # --- Associations ---

        # Define the relationship to the WorkflowRecord model.
        belongs_to :workflow_record,
                   # Specify the class name with full namespace.
                   class_name: "Yantra::Persistence::ActiveRecord::WorkflowRecord",
                   # Specify the foreign key column in the 'yantra_jobs' table.
                   foreign_key: :workflow_id,
                   # Define the corresponding association name in WorkflowRecord
                   # for bi-directional association optimization.
                   inverse_of: :job_records

        # --- Dependencies Association ---
        # Defines the jobs that must complete *before* this job can start.
        # This uses the 'yantra_job_dependencies' join table, represented by
        # the JobDependencyRecord model.

        # Defines the link to the join model where this job's ID is in the 'job_id' column.
        has_many :dependency_links,
                 class_name: "Yantra::Persistence::ActiveRecord::JobDependencyRecord",
                 foreign_key: :job_id,
                 inverse_of: :job, # Assumes JobDependencyRecord has `belongs_to :job`
                 dependent: :destroy # If this job is deleted, remove its dependency links.

        # Defines the association to the actual prerequisite JobRecord models
        # through the dependency_links join records.
        has_many :dependencies,
                 through: :dependency_links,
                 # Specifies which association on JobDependencyRecord points to the prerequisite job.
                 # Assumes JobDependencyRecord has `belongs_to :dependency, class_name: 'JobRecord'`
                 source: :dependency

        # --- Dependents Association ---
        # Defines the jobs that depend on *this* job completing successfully.
        # This also uses the 'yantra_job_dependencies' join table.

        # Defines the link to the join model where this job's ID is in the 'depends_on_job_id' column.
        has_many :dependent_links,
                 class_name: "Yantra::Persistence::ActiveRecord::JobDependencyRecord",
                 foreign_key: :depends_on_job_id,
                 inverse_of: :dependency, # Assumes JobDependencyRecord has `belongs_to :dependency`
                 dependent: :destroy # If this job is deleted, remove links where other jobs depended on it.

        # Defines the association to the actual dependent JobRecord models
        # through the dependent_links join records.
        has_many :dependents,
                 through: :dependent_links,
                 # Specifies which association on JobDependencyRecord points back to the dependent job.
                 # Assumes JobDependencyRecord has `belongs_to :job, class_name: 'JobRecord'`
                 source: :job

        # --- Scopes (Examples) ---
        # Provide convenient ways to query jobs by state.
        scope :with_state, ->(state) { where(state: state.to_s) }
        scope :pending, -> { with_state('pending') }
        scope :enqueued, -> { with_state('enqueued') }
        scope :running, -> { with_state('running') }
        scope :succeeded, -> { with_state('succeeded') }
        scope :failed, -> { with_state('failed') }
        scope :cancelled, -> { with_state('cancelled') }

        # --- Validations (Examples) ---
        # Add validations as needed to ensure data integrity at the DB level.
        # validates :id, presence: true, uniqueness: true # UUIDs should be present and unique
        # validates :workflow_id, presence: true
        # validates :klass, presence: true
        # validates :state, presence: true, inclusion: {
        #   in: %w[pending enqueued running succeeded failed cancelled],
        #   message: "%{value} is not a valid state"
        # }

        # --- Serialization (If using JSON/JSONB and need explicit handling) ---
        # Modern Rails versions often handle JSON/JSONB automatically.
        # If explicit serialization is needed (e.g., for older Rails or specific behavior):
        # serialize :arguments, JSON
        # serialize :output, JSON
        # serialize :error, JSON

        # --- Instance Methods (Optional Helpers) ---

        # Example helper to convert AR record to a simple Job Status Object/DTO
        # This might be used by the ActiveRecordAdapter when implementing `find_job`
        # if returning a full AR object isn't desired for the public API.
        # def to_status_object
        #   {
        #     id: self.id,
        #     workflow_id: self.workflow_id,
        #     klass: self.klass,
        #     state: self.state,
        #     arguments: self.arguments, # Consider if arguments are needed in status
        #     output: self.output,
        #     error: self.error,
        #     retries: self.retries,
        #     created_at: self.created_at,
        #     enqueued_at: self.enqueued_at,
        #     started_at: self.started_at,
        #     finished_at: self.finished_at
        #   }
        # end

      end
    end
  end
end

