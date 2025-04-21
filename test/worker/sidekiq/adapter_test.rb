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

            Yantra.stubs(:logger).returns(nil)
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
            assert_equal true, job['retry']
            assert result, "Enqueue should return true on success"
          end

          def test_enqueue_returns_false_on_standard_error
            assert defined?(Yantra::Worker::Sidekiq::StepJob), "StepJob class not defined"
            mock_chain = mock('sidekiq_chain')
            Yantra::Worker::Sidekiq::StepJob.stubs(:set).with(queue: @queue_name).returns(mock_chain)
            mock_chain.stubs(:perform_async).raises(StandardError, "Redis down")

            # Act
            result = @adapter.enqueue(@step_id, @workflow_id, @step_klass_name, @queue_name)

            # Assert
            refute result, "Enqueue should return false on StandardError"
            assert_equal 0, ::Sidekiq::Queues[@queue_name].size
            assert_equal 0, Yantra::Worker::Sidekiq::StepJob.jobs.size
          end
        end # class AdapterTest
      end # module Sidekiq
    end # module Worker
  end # module Yantra

end # if SIDEKIQ_LOADED

