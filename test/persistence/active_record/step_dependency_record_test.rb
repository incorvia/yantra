# test/persistence/active_record/step_dependency_record_test.rb

require "test_helper"

# Explicitly require the ActiveRecord models needed for these tests.
require "yantra/persistence/active_record/workflow_record"
require "yantra/persistence/active_record/step_record"
require "yantra/persistence/active_record/step_dependency_record"

# Define tests directly, assuming AR dependencies are present
# when these tests are intended to run.
module Yantra
  module Persistence
    module ActiveRecord
      # Test class for the StepDependencyRecord model.
      # Inherits from the base class defined in test_helper.
      # Assumes YantraActiveRecordTestCase is defined if AR_LOADED is true.
      class StepDependencyRecordTest < YantraActiveRecordTestCase

        # Use setup for common test data
        def setup
          super # Run base class setup (skip checks, etc.)
          @workflow = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "running")
          @step1 = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job1", state: "pending")
          @step2 = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job2", state: "pending")
        end

        # Test successful creation
        def test_is_valid_with_step_and_dependency
          record = StepDependencyRecord.new(step: @step1, dependency: @step2)
          assert record.valid?, "Record should be valid with job and dependency. Errors: #{record.errors.full_messages.join(', ')}"
          assert record.save
        end

        # Test validations defined in the StepDependencyRecord model
        def test_invalid_without_job
          record = StepDependencyRecord.new(dependency: @step2)
          refute record.valid?, "Record should be invalid without job"
          # Check the error on the foreign key attribute due to presence validation
          assert_includes record.errors[:step_id], "can't be blank"
          # The default belongs_to validation might also add an error to :step
          # assert_includes record.errors[:step], "must exist" # Optional check
        end

        def test_invalid_without_dependency
          record = StepDependencyRecord.new(step: @step1)
          refute record.valid?, "Record should be invalid without dependency"
          # Check the error on the foreign key attribute due to presence validation
          assert_includes record.errors[:depends_on_step_id], "can't be blank"
          # The default belongs_to validation might also add an error to :dependency
          # assert_includes record.errors[:dependency], "must exist" # Optional check
        end

        def test_invalid_with_duplicate_dependency
          # Arrange: Create the first valid link
          assert StepDependencyRecord.create(step: @step1, dependency: @step2), "First dependency should save"

          # Act: Try to create the same link again
          duplicate = StepDependencyRecord.new(step: @step1, dependency: @step2)

          # Assert
          refute duplicate.valid?, "Duplicate dependency should be invalid"
          # Check the uniqueness validation message on the attribute
          assert_includes duplicate.errors[:depends_on_step_id], "dependency relationship already exists"
        end

        def test_invalid_with_self_dependency
          # Act
          record = StepDependencyRecord.new(step: @step1, dependency: @step1)

          # Assert
          refute record.valid?, "Self-dependency should be invalid"
          # Use assert_match for regex comparison against the error message string
          assert_match(/cannot be the same as step_id/, record.errors[:depends_on_step_id].join)
        end

        # Test direct associations
        def test_associations
           # Arrange
           record = StepDependencyRecord.create!(step: @step1, dependency: @step2)

           # Assert
           assert_equal @step1, record.step
           assert_equal @step2, record.dependency
        end

      end # class StepDependencyRecordTest
    end # module ActiveRecord
  end # module Persistence
end # module Yantra

