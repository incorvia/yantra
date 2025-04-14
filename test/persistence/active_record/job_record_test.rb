# test/persistence/active_record/job_record_test.rb

require "test_helper"

# Explicitly require the ActiveRecord models needed for these tests.
# This will raise LoadError if run without AR dev dependencies installed.
if AR_LOADED # Only require if AR itself was loaded successfully
  require "yantra/persistence/active_record/workflow_record"
  require "yantra/persistence/active_record/job_record"
  require "yantra/persistence/active_record/job_dependency_record"
end

# Define tests directly, assuming AR dependencies are present
# when these tests are intended to run. Will error if constants aren't loaded.
module Yantra
  module Persistence
    module ActiveRecord
      # Test class for the JobRecord model.
      # Inherits from the base class defined in test_helper.
      # Assumes YantraActiveRecordTestCase is defined if AR_LOADED is true.
      class JobRecordTest < YantraActiveRecordTestCase

        # Minitest setup method, runs before each test in this class
        def setup
          super # Run setup from YantraActiveRecordTestCase (includes skip check)
          # Create a workflow instance variable available to each test, providing an ID
          @workflow = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "running")
        end

        # Basic test to ensure creation and retrieval work
        def test_can_create_and_find_job
          # Arrange
          job_id = SecureRandom.uuid
          attributes = {
            id: job_id, # Provide the ID
            workflow_record: @workflow,
            klass: "MyTestJob",
            state: "enqueued",
            queue: "default"
          }

          # Act
          created_job = JobRecord.create!(attributes)

          # Assert Creation
          refute_nil created_job
          assert created_job.persisted?
          assert_equal job_id, created_job.id
          assert_equal @workflow.id, created_job.workflow_id

          # Act
          found_job = JobRecord.find(job_id)

          # Assert Find
          refute_nil found_job
          assert_equal "MyTestJob", found_job.klass
          assert_equal "enqueued", found_job.state
        end

        # Test associations (belongs_to, has_many :through)
        def test_associations
          # Arrange: Create related jobs, providing IDs
          job1_prereq = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job1", state: "succeeded")
          job2_main = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job2", state: "pending")
          job3_dependent = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job3", state: "pending")

          # Setup dependencies: job2 -> job1, job3 -> job2
          dep1 = JobDependencyRecord.create!(job: job2_main, dependency: job1_prereq)
          dep2 = JobDependencyRecord.create!(job: job3_dependent, dependency: job2_main)

          # Assert: belongs_to :workflow_record
          assert_equal @workflow, job2_main.workflow_record

          # Assert: has_many :dependencies (prerequisites)
          assert_equal [job1_prereq], job2_main.dependencies
          assert_equal [job2_main], job3_dependent.dependencies
          assert_empty job1_prereq.dependencies

          # Assert: has_many :dependents
          assert_includes job1_prereq.dependents.reload, job2_main
          assert_includes job2_main.dependents.reload, job3_dependent
          assert_empty job3_dependent.dependents
        end

        # Test scopes defined in JobRecord
        def test_state_scopes
          # Arrange: Create jobs with different states, providing IDs
          job_running1 = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobR", state: "running")
          job_running2 = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobR", state: "running")
          job_pending = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobP", state: "pending")
          job_failed = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobF", state: "failed")

          # Assert: Check scopes (use pluck + sort for reliable comparison)
          assert_equal [job_running1.id, job_running2.id].sort, JobRecord.running.pluck(:id).sort
          assert_equal [job_failed.id], JobRecord.failed.pluck(:id)
          assert_equal [job_pending.id], JobRecord.with_state(:pending).pluck(:id)
          assert_equal [job_running1.id, job_running2.id].sort, JobRecord.with_state("running").pluck(:id).sort
        end

        # --- Validation Tests (Examples - Add if validations uncommented in model) ---

        # def test_validation_requires_workflow
        #   # Assumes validates :workflow_id, presence: true is active
        #   job = JobRecord.new(id: SecureRandom.uuid, klass: "Job", state: "pending") # Provide ID
        #   refute job.valid?
        #   assert_includes job.errors[:workflow_id], "can't be blank"
        # end

        # def test_validation_requires_klass
        #   # Assumes validates :klass, presence: true is active
        #   job = JobRecord.new(id: SecureRandom.uuid, workflow_record: @workflow, state: "pending") # Provide ID
        #   refute job.valid?
        #   assert_includes job.errors[:klass], "can't be blank"
        # end

      end # class JobRecordTest
    end # module ActiveRecord
  end # module Persistence
end # module Yantra

