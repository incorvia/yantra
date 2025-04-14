# test/persistence/active_record/job_dependency_record_test.rb

require "test_helper"

# Explicitly require the ActiveRecord models needed for these tests.
require "yantra/persistence/active_record/workflow_record"
require "yantra/persistence/active_record/job_record"
require "yantra/persistence/active_record/job_dependency_record"

# Define tests directly, assuming AR dependencies are present
# when these tests are intended to run.
module Yantra
  module Persistence
    module ActiveRecord
      # Test class for the JobDependencyRecord model.
      # Inherits from the base class defined in test_helper.
      # Assumes YantraActiveRecordTestCase is defined if AR_LOADED is true.
      class JobDependencyRecordTest < YantraActiveRecordTestCase

        # Use setup for common test data
        def setup
          super # Run base class setup (skip checks, etc.)
          @workflow = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "running")
          @job1 = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job1", state: "pending")
          @job2 = JobRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job2", state: "pending")
        end

        # Test successful creation
        def test_is_valid_with_job_and_dependency
          record = JobDependencyRecord.new(job: @job1, dependency: @job2)
          assert record.valid?, "Record should be valid with job and dependency. Errors: #{record.errors.full_messages.join(', ')}"
          assert record.save
        end

        # Test validations defined in the JobDependencyRecord model
        def test_invalid_without_job
          record = JobDependencyRecord.new(dependency: @job2)
          refute record.valid?, "Record should be invalid without job"
          # Check the error on the foreign key attribute due to presence validation
          assert_includes record.errors[:job_id], "can't be blank"
          # The default belongs_to validation might also add an error to :job
          # assert_includes record.errors[:job], "must exist" # Optional check
        end

        def test_invalid_without_dependency
          record = JobDependencyRecord.new(job: @job1)
          refute record.valid?, "Record should be invalid without dependency"
          # Check the error on the foreign key attribute due to presence validation
          assert_includes record.errors[:depends_on_job_id], "can't be blank"
          # The default belongs_to validation might also add an error to :dependency
          # assert_includes record.errors[:dependency], "must exist" # Optional check
        end

        def test_invalid_with_duplicate_dependency
          # Arrange: Create the first valid link
          assert JobDependencyRecord.create(job: @job1, dependency: @job2), "First dependency should save"

          # Act: Try to create the same link again
          duplicate = JobDependencyRecord.new(job: @job1, dependency: @job2)

          # Assert
          refute duplicate.valid?, "Duplicate dependency should be invalid"
          # Check the uniqueness validation message on the attribute
          assert_includes duplicate.errors[:depends_on_job_id], "dependency relationship already exists"
        end

        def test_invalid_with_self_dependency
          # Act
          record = JobDependencyRecord.new(job: @job1, dependency: @job1)

          # Assert
          refute record.valid?, "Self-dependency should be invalid"
          # Use assert_match for regex comparison against the error message string
          assert_match(/cannot be the same as job_id/, record.errors[:depends_on_job_id].join)
        end

        # Test direct associations
        def test_associations
           # Arrange
           record = JobDependencyRecord.create!(job: @job1, dependency: @job2)

           # Assert
           assert_equal @job1, record.job
           assert_equal @job2, record.dependency
        end

      end # class JobDependencyRecordTest
    end # module ActiveRecord
  end # module Persistence
end # module Yantra

