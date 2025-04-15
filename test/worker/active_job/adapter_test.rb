# test/worker/active_job/adapter_test.rb
require "test_helper"

# Conditionally load ActiveJob related files
if AR_LOADED # Using AR_LOADED as proxy for full dev env
  require "yantra/worker/active_job/adapter"
  require "yantra/worker/enqueuing_interface" # For include test
  require "yantra/errors"
  # Attempt to load the job class, but don't fail here if it's missing
  begin
    require "yantra/worker/active_job/async_job"
  rescue LoadError
  end
end

module Yantra
  module Worker
    module ActiveJob
      # Run tests only if ActiveJob components could be loaded/defined
      if AR_LOADED && defined?(Yantra::Worker::ActiveJob::Adapter) && defined?(Yantra::Worker::ActiveJob::AsyncJob)

        class AdapterTest < Minitest::Test # Use standard Minitest::Test

          def setup
            # No database needed for these tests
            @adapter = Adapter.new
            @job_id = SecureRandom.uuid
            @workflow_id = SecureRandom.uuid
            @job_klass_name = "MySpecificYantraJob"
            @queue_name = "critical"

            # --- Define the structure of the Mock Class ---
            # We create a new instance of this structure in each test
            @mock_async_job_class_structure = Class.new do
              @_mock_set_options = nil
              @_mock_perform_later_args = nil
              @_raise_error_on_perform_later = false
              class << self
                attr_accessor :_mock_set_options, :_mock_perform_later_args, :_raise_error_on_perform_later
                def set(options); @_mock_set_options = options; self; end
                def perform_later(*args); @_mock_perform_later_args = args; raise StandardError, "Queue backend connection failed" if @_raise_error_on_perform_later; true; end
                def captured_set_options; @_mock_set_options; end
                def captured_perform_later_args; @_mock_perform_later_args; end
                def should_raise=(val); @_raise_error_on_perform_later = val; end
                def reset_mock!; @_mock_set_options=nil; @_mock_perform_later_args=nil; @_raise_error_on_perform_later=false; end
              end
            end
            # --- End Mock Class Structure Definition ---
          end

          # Teardown is no longer needed for restoring constants stubbed in setup
          # def teardown; super; end

          def test_implements_enqueuing_interface
            assert_includes Adapter.ancestors, Yantra::Worker::EnqueuingInterface
          end

          def test_enqueue_calls_set_and_perform_later_with_correct_args
            # Arrange: Create a fresh instance of the mock class structure
            mock_job_class = @mock_async_job_class_structure.dup # Use dup to get a clean class object
            mock_job_class.reset_mock!

            # Act: Stub the constant only for this block
            with_stubbed_const(Yantra::Worker::ActiveJob, :AsyncJob, mock_job_class) do
               @adapter.enqueue(@job_id, @workflow_id, @job_klass_name, @queue_name)
            end

            # Assert: Check values stored on the mock class by its methods
            assert_equal({ queue: @queue_name }, mock_job_class.captured_set_options)
            assert_equal [@job_id, @workflow_id, @job_klass_name], mock_job_class.captured_perform_later_args
          end

          def test_enqueue_raises_worker_error_on_perform_later_failure
            # Arrange: Create and configure mock class to raise error
            mock_job_class = @mock_async_job_class_structure.dup
            mock_job_class.reset_mock!
            mock_job_class.should_raise = true

            # Act & Assert: Stub the constant only for this block
            with_stubbed_const(Yantra::Worker::ActiveJob, :AsyncJob, mock_job_class) do
              error = assert_raises(Yantra::Errors::WorkerError) do
                @adapter.enqueue(@job_id, @workflow_id, @job_klass_name, @queue_name)
              end
              # Check wrapped error message
              assert_match /ActiveJob enqueuing failed: Queue backend connection failed/, error.message
            end
            # Verify perform_later was still called (and raised the error) by checking captured args
            assert_equal [@job_id, @workflow_id, @job_klass_name], mock_job_class.captured_perform_later_args
          end

          def test_enqueue_raises_config_error_if_job_class_undefined
             # Arrange: Use helper to temporarily remove the constant
             with_stubbed_const(Yantra::Worker::ActiveJob, :AsyncJob, nil) do # Pass nil to remove
               # Act & Assert
               assert_raises(Yantra::Errors::ConfigurationError, /AsyncJob class failed to load/) do
                  @adapter.enqueue(@job_id, @workflow_id, @job_klass_name, @queue_name)
               end
             end
          end

          private

          # Helper to safely stub/restore constants for the duration of a block
          # Stores original value and definition status to restore accurately
          def with_stubbed_const(mod, const_sym, temp_value)
              original_value = nil
              # Check if constant is defined anywhere in the ancestor chain
              original_defined = mod.const_defined?(const_sym, false) # false = check only mod, not ancestors

              # Get original value only if defined directly on the module/class
              original_value = mod.const_get(const_sym) if original_defined

              # Remove existing constant (only if defined directly on mod)
              mod.send(:remove_const, const_sym) if original_defined

              # Set the temporary value if it's not nil
              mod.const_set(const_sym, temp_value) unless temp_value.nil?

              yield # Execute the test block with the stubbed constant

          ensure
              # Restore original state in ensure block
              # Remove the temporary constant if it was set
              mod.send(:remove_const, const_sym) if mod.const_defined?(const_sym, false)

              # Restore the original constant only if it was originally defined directly on mod
              mod.const_set(const_sym, original_value) if original_defined
          end

          # Removed stub_const and restore_const helpers previously used in setup/teardown

        end

      else
         # Placeholder test if ActiveJob components didn't load
         class AdapterTestPlaceholder < Minitest::Test
            def test_skipping_active_job_adapter_tests
               skip "Skipping ActiveJob::Adapter tests because ActiveJob components not loaded."
            end
         end
      end # if defined?
    end
  end
end

