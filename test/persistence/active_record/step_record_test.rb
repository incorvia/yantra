# test/persistence/active_record/step_record_test.rb

require "test_helper"

# Explicitly require the ActiveRecord models needed for these tests.
# This will raise LoadError if run without AR dev dependencies installed.
require "yantra/persistence/active_record/workflow_record"
require "yantra/persistence/active_record/step_record"
require "yantra/persistence/active_record/step_dependency_record"

# Define tests directly, assuming AR dependencies are present
# when these tests are intended to run. Will error if constants aren't loaded.
module Yantra
  module Persistence
    module ActiveRecord
      # Test class for the StepRecord model.
      # Inherits from the base class defined in test_helper.
      # Assumes YantraActiveRecordTestCase is defined if AR_LOADED is true.
      class StepRecordTest < YantraActiveRecordTestCase

        # Minitest setup method, runs before each test in this class
        def setup
          super # Run setup from YantraActiveRecordTestCase (includes skip check)
          # Create a workflow instance variable available to each test, providing an ID
          @workflow = WorkflowRecord.create!(id: SecureRandom.uuid, klass: "Wf", state: "running")
        end

        # Basic test to ensure creation and retrieval work
        def test_can_create_and_find_step
          # Arrange
          step_id = SecureRandom.uuid
          attributes = {
            id: step_id, # Provide the ID
            workflow_record: @workflow,
            klass: "MyTestStep",
            state: "enqueued",
            queue: "default"
          }

          # Act
          created_step = StepRecord.create!(attributes)

          # Assert Creation
          refute_nil created_step
          assert created_step.persisted?
          assert_equal step_id, created_step.id
          assert_equal @workflow.id, created_step.workflow_id

          # Act
          found_step = StepRecord.find(step_id)

          # Assert Find
          refute_nil found_step
          assert_equal "MyTestStep", found_step.klass
          assert_equal "enqueued", found_step.state
        end

        # Test associations (belongs_to, has_many :through)
        def test_associations
          # Arrange: Create related jobs, providing IDs
          job1_prereq = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job1", state: "succeeded")
          job2_main = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job2", state: "pending")
          job3_dependent = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "Job3", state: "pending")

          # Setup dependencies: job2 -> job1, job3 -> job2
          StepDependencyRecord.create!(step: job2_main, dependency: job1_prereq)
          StepDependencyRecord.create!(step: job3_dependent, dependency: job2_main)

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

        # Test scopes defined in StepRecord
        def test_state_scopes
          # Arrange: Create jobs with different states, providing IDs
          step_running1 = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobR", state: "running")
          step_running2 = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobR", state: "running")
          step_pending = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "JobP", state: "pending")
          step_failed = StepRecord.create!(id: SecureRandom.uuid, workflow_record: @workflow, klass: "StepF", state: "failed")

          # Assert: Check scopes (use pluck + sort for reliable comparison)
          assert_equal [step_running1.id, step_running2.id].sort, StepRecord.running.pluck(:id).sort
          assert_equal [step_failed.id], StepRecord.failed.pluck(:id)
          assert_equal [step_pending.id], StepRecord.with_state(:pending).pluck(:id)
          assert_equal [step_running1.id, step_running2.id].sort, StepRecord.with_state("running").pluck(:id).sort
        end

        # --- Validation Tests (Examples - Add if validations uncommented in model) ---

        # def test_validation_requires_workflow
        #   # Assumes validates :workflow_id, presence: true is active
        #   job = StepRecord.new(id: SecureRandom.uuid, klass: "Job", state: "pending") # Provide ID
        #   refute step.valid?
        #   assert_includes step.errors[:workflow_id], "can't be blank"
        # end

        # def test_validation_requires_klass
        #   # Assumes validates :klass, presence: true is active
        #   job = StepRecord.new(id: SecureRandom.uuid, workflow_record: @workflow, state: "pending") # Provide ID
        #   refute step.valid?
        #   assert_includes step.errors[:klass], "can't be blank"
        # end

      end # class StepRecordTest
    end # module ActiveRecord
  end # module Persistence
end # module Yantra

