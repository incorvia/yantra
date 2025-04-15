Yantra: A Pluggable Workflow Engine for RubyYantra is a robust, DAG-based workflow orchestration engine for Ruby applications, designed with flexibility, maintainability, and observability in mind. It allows you to define complex workflows with dependent jobs and execute them reliably using common background worker systems.It aims to address limitations found in previous systems by providing:Backend Agnosticism: Choose your persistence layer (e.g., Redis, PostgreSQL via ActiveRecord) without changing core logic.Simplified Core: Clear state management and object lookups using consistent identifiers.Robust Observability: Deeply integrated event emission for monitoring and alerting.Maintainability & Extensibility: Modern design patterns for easier development and testing.Core ConceptsWorkflow: A Directed Acyclic Graph (DAG) defined by inheriting Yantra::Workflow. Contains jobs and their dependencies.Job: A unit of work defined by inheriting Yantra::Job and implementing perform. Belongs to a Workflow.Dependency: An edge in the DAG; Job B runs only after Job A succeeds.State Machine: Workflows and Jobs have well-defined states (pending, enqueued, running, succeeded, failed, cancelled) with enforced transitions, managed internally by Yantra::Core::StateMachine.Repository: An abstraction (Yantra::Persistence::RepositoryInterface) hiding persistence details. Implemented by Adapters.Adapters (Persistence): Concrete implementations for specific backends (e.g., Yantra::Persistence::Redis::Adapter, Yantra::Persistence::ActiveRecord::Adapter).Adapters (Worker): Concrete implementations (Yantra::Worker::SidekiqAdapter, etc.) conforming to Yantra::Worker::EnqueuingInterface to abstract background job systems.Client: The main public API (Yantra::Client) for creating, starting, and interacting with workflows.Orchestrator: Internal component (Yantra::Core::Orchestrator) that drives workflow execution based on job states and dependencies.Event: Notifications (yantra.workflow.started, yantra.job.failed, etc.) emitted for observability.ArchitectureYantra uses a layered architecture:User App -> Yantra::Client -> Yantra Core Logic (Orchestrator, StateMachine) -> RepositoryInterface -> Persistence Adapter -> DBUser App -> Yantra::Client -> Yantra Core Logic (Orchestrator) -> EnqueuingInterface -> Worker Adapter -> Background QueueThis decouples application logic, core orchestration, persistence, and background job enqueuing.InstallationAdd this line to your application's Gemfile:gem 'yantra'
# Add adapter dependencies if needed, e.g.:
# gem 'redis'
# gem 'pg' or gem 'sqlite3'
# gem 'sidekiq' or gem 'resque'
And then execute:$ bundle install
ConfigurationConfigure Yantra in an initializer (e.g., config/initializers/yantra.rb):Yantra.configure do |config|
  # Choose your persistence backend adapter
  config.persistence_adapter = :active_record # or :redis

  # Configure adapter-specific settings
  # For Redis:
  # config.redis_url = ENV['YANTRA_REDIS_URL'] || 'redis://localhost:6379/1'
  # config.redis_namespace = 'yantra_myapp'
  # config.redis_options = { ssl_params: { verify_mode: OpenSSL::SSL::VERIFY_NONE } } # Example

  # For ActiveRecord: No specific config needed if using Rails' default connection.
  # Yantra models will use the connection configured via database.yml / connects_to.

  # Choose your background worker adapter
  config.worker_adapter = :sidekiq # or :resque, :active_job, etc.

  # Configure logging (optional)
  # config.logger = Rails.logger # Or your custom logger
  # config.logger.level = Logger::DEBUG

  # Configure event notification backend (optional, defaults to ActiveSupport::Notifications if available)
  # config.notification_backend = MyCustomNotifier
end
Basic Usage1. Define Jobs:# app/workflows/jobs/fetch_user_data_job.rb
class FetchUserDataJob < Yantra::Job
  def perform(user_id:)
    puts "Fetching data for user #{user_id}"
    # ... fetch logic ...
    { user_name: "User #{user_id}", fetched_at: Time.now } # Return output hash
  end
end

# app/workflows/jobs/generate_report_job.rb
class GenerateReportJob < Yantra::Job
  # Override default queue if needed
  # def self.queue_name; :reports; end

  def perform(user_data:, report_format:)
    puts "Generating #{report_format} report for #{user_data[:user_name]}"
    # ... report generation ...
    "report_url_for_#{user_data[:user_name]}.#{report_format}"
  end
end
2. Define a Workflow:# app/workflows/user_report_workflow.rb
class UserReportWorkflow < Yantra::Workflow
  # Arguments passed to Client.create_workflow are received here
  def perform(user_id:, format: 'pdf', priority: 'low')
    # Use the run DSL to define jobs and dependencies
    fetch_job = run FetchUserDataJob, name: :fetch, params: { user_id: user_id }

    # GenerateReportJob runs after fetch_job completes.
    # Its params use the output from the fetch_job (assuming output is passed automatically - TBD design)
    # or could use data passed during workflow creation.
    run GenerateReportJob, name: :generate, params: { user_data: fetch_job.output, report_format: format }, after: :fetch # Pass output (Requires output passing mechanism)
    # Alternative: Pass initial args
    # run GenerateReportJob, name: :generate, params: { user_id: user_id, report_format: format }, after: :fetch
  end
end
(Note: Automatic output passing from dependencies to dependents is a potential feature not fully detailed in the current design).3. Create and Start the Workflow:# Somewhere in your application code (e.g., a controller, service, background job)

user_id_to_process = 456

# Create and persist the workflow definition
workflow_id = Yantra::Client.create_workflow(
  UserReportWorkflow,
  user_id: user_id_to_process, # Keyword arg for perform
  format: 'csv',               # Optional keyword arg for perform
  globals: { company_id: 'xyz' } # Optional global data
)

puts "Created workflow with ID: #{workflow_id}"

# Start the workflow (enqueues initial jobs)
Yantra::Client.start_workflow(workflow_id)

puts "Workflow #{workflow_id} started."
Persistence Setup (ActiveRecord)If using the :active_record persistence adapter in a Rails application:Run the Installer: This copies the necessary migration files into your application.rails generate yantra:install
Run Migrations: Create the Yantra tables in your database.rake db:migrate
This will create the yantra_workflows, yantra_jobs, and yantra_job_dependencies tables.Development & TestingClone the repository.Run bundle install.For ActiveRecord Tests:Set up the dummy Rails app: cd test/dummy && bundle install --binstubs && cd ../..Generate the test schema: cd test/dummy && bin/rails g yantra:install && bin/rake db:migrate RAILS_ENV=test && bin/rake db:schema:dump RAILS_ENV=test && cd ../..Copy the schema: mkdir -p test/support/db && cp test/dummy/db/schema.rb test/support/db/schema.rbRun tests: bundle exec rake testContributing[Placeholder - Add contribution guidelines, code of conduct]LicenseThe gem is available as open source under the terms of the MIT License.
