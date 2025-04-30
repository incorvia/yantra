# test/worker/sidekiq/adapter_test.rb

require "test_helper"
require "mocha/minitest"
require "securerandom"

# Conditionally load Sidekiq and adapter if available
begin
  require 'sidekiq'
  require 'sidekiq/testing'
  require 'yantra/worker/sidekiq/adapter'
  require 'yantra/worker/sidekiq/step_job' # Use step_job.rb
  SIDEKIQ_LOADED = true
rescue LoadError => e
  puts "WARN: Skipping Yantra::Worker::Sidekiq::Adapter tests: #{e.message}"
  SIDEKIQ_LOADED = false
end

require 'yantra/worker/enqueuing_interface'
require 'yantra/errors'

# Run tests only if Sidekiq and the adapter could be loaded
if SIDEKIQ_LOADED

  module Yantra
    module Worker
      module Sidekiq
        class AdapterTest < Minitest::Test
          include Mocha::API

          def setup
            # <<< Enable fake mode FIRST >>>
            ::Sidekiq::Testing.fake!
            # <<< Clear queues AND worker jobs AFTER enabling fake mode >>>
            ::Sidekiq::Queues.clear_all
            Yantra::Worker::Sidekiq::StepJob.jobs.clear if defined?(Yantra::Worker::Sidekiq::StepJob.jobs)

            @adapter = Yantra::Worker::Sidekiq::Adapter.new
            @step_id = SecureRandom.uuid
            @workflow_id = SecureRandom.uuid
            @step_klass_name = "MyTestStep"
            @queue_name = "yantra_test_queue"
          end

          def teardown
            ::Sidekiq::Testing.disable!
            ::Sidekiq::Queues.clear_all
            Yantra::Worker::Sidekiq::StepJob.jobs.clear if defined?(Yantra::Worker::Sidekiq::StepJob.jobs)
            Mocha::Mockery.instance.teardown
          end

          # --- Interface Test ---
          def test_includes_interface_module
            assert_includes Yantra::Worker::Sidekiq::Adapter.included_modules,
                            Yantra::Worker::EnqueuingInterface
          end

          # --- Enqueue Tests (Using fake queue) ---
          def test_enqueue_success_pushes_job_to_correct_queue
            # Act
            result = @adapter.enqueue(@step_id, @workflow_id, @step_klass_name, @queue_name)

            # Assert queue content using BOTH methods for debugging
            # <<< Check worker class array >>>
            assert_equal 1, Yantra::Worker::Sidekiq::StepJob.jobs.size, "Job should be in StepJob.jobs"
            # <<< Check specific queue hash >>>
            assert_equal 1, ::Sidekiq::Queues[@queue_name].size, "Job should be in Sidekiq::Queues"

            job = Yantra::Worker::Sidekiq::StepJob.jobs.first
            refute_nil job
            assert_equal Yantra::Worker::Sidekiq::StepJob.name, job['class']
            assert_equal [@step_id, @workflow_id, @step_klass_name], job['args']
            assert_equal @queue_name, job['queue']
            assert_equal 25, job['retry']
            assert result, "Enqueue should return true on success"
          end

          def test_enqueue_returns_false_on_standard_error
            assert defined?(Yantra::Worker::Sidekiq::StepJob), "StepJob class not defined"
            mock_chain = mock('sidekiq_chain')
            expected_options = { 'queue' => @queue_name.to_s }
            Yantra::Worker::Sidekiq::StepJob.stubs(:set).with(expected_options).returns(mock_chain)
            mock_chain.stubs(:perform_async).raises(StandardError, "Redis down")

            # Act
            result = @adapter.enqueue(@step_id, @workflow_id, @step_klass_name, @queue_name)

            # Assert
            refute result, "Enqueue should return false on StandardError"
            assert_equal 0, ::Sidekiq::Queues[@queue_name].size
            assert_equal 0, Yantra::Worker::Sidekiq::StepJob.jobs.size
          end

           def test_enqueue_in_schedules_job_with_positive_delay
          delay = 300 # 5 minutes
          mock_configured_job = mock('ConfiguredJob')
          # Sidekiq adapter uses string keys for options hash passed to set
          expected_options = { 'queue' => @queue_name.to_s }
          expected_args = [@step_id, @workflow_id, @step_klass_name]

          # Expect StepJob.set().perform_in() chain
          StepJob.expects(:set).with(expected_options).returns(mock_configured_job)
          mock_configured_job.expects(:perform_in).with(delay, *expected_args).returns("jid123") # perform_in returns JID

          # Act
          result = @adapter.enqueue_in(delay, @step_id, @workflow_id, @step_klass_name, @queue_name)

          # Assert
          assert result, "enqueue_in should return true on success"
        end

        def test_enqueue_in_schedules_job_without_queue
          delay = 60
          mock_configured_job = mock('ConfiguredJob')
          expected_options = {} # No queue option if queue_name is nil
          expected_args = [@step_id, @workflow_id, @step_klass_name]

          # Expect StepJob.set().perform_in() chain
          StepJob.expects(:set).with(expected_options).returns(mock_configured_job)
          mock_configured_job.expects(:perform_in).with(delay, *expected_args).returns("jid456")

          # Act
          result = @adapter.enqueue_in(delay, @step_id, @workflow_id, @step_klass_name, nil) # Pass nil queue_name

          # Assert
          assert result, "enqueue_in should return true on success without queue"
        end

        def test_enqueue_in_calls_enqueue_for_zero_delay
          delay = 0
          # Expect the immediate enqueue method to be called
          # Stubbing the instance method on the object under test
          @adapter.expects(:enqueue).with(@step_id, @workflow_id, @step_klass_name, @queue_name).returns(true)

          # Act
          result = @adapter.enqueue_in(delay, @step_id, @workflow_id, @step_klass_name, @queue_name)

          # Assert
          assert result, "enqueue_in should return true when delegating to enqueue"
        end

         def test_enqueue_in_calls_enqueue_for_nil_delay
          delay = nil
          # Expect the immediate enqueue method to be called
          @adapter.expects(:enqueue).with(@step_id, @workflow_id, @step_klass_name, @queue_name).returns(true)

          # Act
          result = @adapter.enqueue_in(delay, @step_id, @workflow_id, @step_klass_name, @queue_name)

          # Assert
          assert result, "enqueue_in should return true when delegating to enqueue"
        end

        def test_enqueue_in_handles_error_and_returns_false
          delay = 300
          mock_configured_job = mock('ConfiguredJob')
          expected_options = { 'queue' => @queue_name.to_s }
          expected_args = [@step_id, @workflow_id, @step_klass_name]

          StepJob.expects(:set).with(expected_options).returns(mock_configured_job)
          # Simulate perform_in raising an error
          mock_configured_job.expects(:perform_in).with(delay, *expected_args).raises(StandardError, "Redis down")

          # Expect error log
          Yantra.logger.expects(:error)

          # Act
          result = @adapter.enqueue_in(delay, @step_id, @workflow_id, @step_klass_name, @queue_name)

          # Assert
          refute result, "enqueue_in should return false on error"
        end
        end # class AdapterTest
      end # module Sidekiq
    end # module Worker
  end # module Yantra

end # if SIDEKIQ_LOADED

