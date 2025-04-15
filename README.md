Yantra: A Pluggable Workflow Engine for RubyYantra is a robust, DAG-based workflow orchestration engine for Ruby applications, designed with flexibility, maintainability, and observability in mind. It allows you to define complex workflows with dependent jobs and execute them reliably using common background worker systems.It aims to address limitations found in previous systems by providing:Backend Agnosticism: Choose your persistence layer (e.g., Redis, PostgreSQL via ActiveRecord) without changing core logic.Simplified Core: Clear state management and object lookups using consistent identifiers.Robust Observability: Deeply integrated event emission for monitoring and alerting.Maintainability & Extensibility: Modern design patterns for easier development and testing.Core ConceptsWorkflow: A Directed Acyclic Graph (DAG) defined by inheriting Yantra::Workflow. Contains jobs and their dependencies.Job: A unit of work defined by inheriting Yantra::Job and implementing perform. Belongs to a Workflow.Dependency: An edge in the DAG; Job B runs only after Job A succeeds.State Machine: Workflows and Jobs have well-defined states (pending, enqueued, running, succeeded, failed, cancelled) with enforced transitions, managed internally by Yantra::Core::StateMachine.Repository: An abstraction (Yantra::Persistence::RepositoryInterface) hiding persistence details. Implemented by Adapters.Adapters (Persistence): Concrete implementations for specific backends (e.g., Yantra::Persistence::Redis::Adapter, Yantra::Persistence::ActiveRecord::Adapter).Adapters (Worker): Concrete implementations (Yantra::Worker::SidekiqAdapter, etc.) conforming to Yantra::Worker::EnqueuingInterface to abstract background job systems.Client: The main public API (Yantra::Client) for creating, starting, and interacting with workflows.Orchestrator: Internal component (Yantra::Core::Orchestrator) that drives workflow execution based on job states and dependencies.Event: Notifications (yantra.workflow.started, yantra.job.failed, etc.) emitted for observability.ArchitectureYantra uses a layered architecture:User App -> Yantra::Client -> Yantra Core Logic (Orchestrator, StateMachine) -> RepositoryInterface -> Persistence Adapter -> DB
User App -> Yantra::Client -> Yantra Core Logic (Orchestrator) -> EnqueuingInterface -> Worker Adapter -> Background Queue
This decouples application logic, core orchestration, persistence, and background job enqueuing.InstallationAdd this line to your application's Gemfile:gem 'yantra'

# Add adapter dependencies if needed, e.g.:
# gem 'redis'
# gem 'pg' or gem 'sqlite3'
# gem 'sidekiq' or gem 'resque'
And then execute:bundle install
ConfigurationConfigure Yantra in an initializer (e.g., config/initializers/yantra.rb):Yantra.configure do |config|
  # Choose your persistence backend adapter
  config.persistence_adapter = :active_record # or :redis

  # --- Configure adapter-specific settings ---
  # Example for Redis:
  # config.redis_url = ENV['YANTRA_REDIS_URL'] || 'redis://localhost:6379/1'
  # config.redis_namespace = 'yantra_myapp'
  # config.redis_options = { ssl_params: { verify_mode: OpenSSL::SSL::VERIFY_NONE } }

  # Example for ActiveRecord:
  # No specific config needed by default if using Rails' connection management.
  # Yantra models will use the connection configured via database.yml / connects_to.

  # --- Choose your background worker adapter ---
  config.worker_adapter = :sidekiq # or :resque, :active_job, etc.
  # TODO: Add worker-specific config examples if needed

  # --- Configure logging (optional) ---
  # config.logger = Rails.logger # Or your custom logger
  # config.logger.level = Logger::DEBUG

  # --- Configure event notification backend (optional) ---
  # Defaults to ActiveSupport::Notifications if available
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
  # def queue_name; :reports; end # Use instance method

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
    # TODO: Define how job outputs are accessed by dependents.
    # Example assuming output is accessible via job instance (requires enhancement):
    # run GenerateReportJob, name: :generate, params: { user_data: fetch_job.output, report_format: format }, after: :fetch
    # Alternative: Pass initial args if output passing isn't implemented
    run GenerateReportJob, name: :generate, params: { user_id: user_id, report_format: format }, after: :fetch
  end
end
(Note: Automatic output passing from dependencies to dependents is a potential feature requiring further design).3. Create and Start the Workflow:# Somewhere in your application code (e.g., a controller, service, background job)

user_id_to_process = 456

# Create and persist the workflow definition
workflow_id = Yantra::Client.create_workflow(
  UserReportWorkflow,
  user_id: user_id_to_process, # Keyword arg for perform
  format: 'csv',               # Optional keyword arg for perform
  globals: { company_id: 'xyz' } # Optional global data
)

puts "Created workflow with ID: #{workflow_id}"

# Start the workflow (enqueues initial jobs via Orchestrator)
# Ensure Yantra::Client.start_workflow is implemented
# Yantra::Client.start_workflow(workflow_id)

# puts "Workflow #{workflow_id} started."
Persistence Setup (ActiveRecord)If using the :active_record persistence adapter in a Rails application:Run the Installer: This copies the necessary migration files into your application.rails generate yantra:install
Run Migrations: Create the Yantra tables in your database.rake db:migrate
This will create the yantra_workflows, yantra_jobs, and yantra_job_dependencies tables.Development & TestingClone the repository.Run bundle install (installs runtime and development dependencies).For ActiveRecord Tests:Ensure necessary development gems are installed (rails, activerecord, sqlite3).Set up the dummy Rails app located in test/dummy/:cd test/dummy
bundle install --binstubs
cd ../..
Generate the test schema using the dummy app:cd test/dummy
bin/rails generate yantra:install
bin/rake db:migrate RAILS_ENV=test
bin/rake db:schema:dump RAILS_ENV=test
cd ../..
Copy the generated schema to the test support directory (adjust path if needed):mkdir -p test/support/db
cp test/dummy/db/schema.rb test/support/db/schema.rb
Run tests:bundle exec rake test
Contributing[Placeholder - Add contribution guidelines, code of conduct, setup instructions]LicenseThe gem is available as open source under the terms of the MIT License.
