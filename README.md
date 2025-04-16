# Yantra: A Pluggable Workflow Engine for Ruby

[![Gem Version](https://badge.fury.io/rb/yantra.svg)](https://badge.fury.io/rb/yantra)
[![Build Status](https://github.com/your_org/yantra/actions/workflows/ci.yml/badge.svg)](https://github.com/your_org/yantra/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Yantra is a robust, DAG-based workflow orchestration engine for Ruby applications, designed with flexibility, maintainability, and observability in mind. It allows you to define complex workflows with dependent jobs and execute them reliably using common background worker systems.

It aims to address limitations found in previous systems by providing:

* **Backend Agnosticism:** Choose your persistence layer (e.g., Redis, PostgreSQL via ActiveRecord) without changing core logic.
* **Simplified Core:** Clear state management and object lookups using consistent identifiers.
* **Robust Observability:** Deeply integrated event emission for monitoring and alerting.
* **Maintainability & Extensibility:** Modern design patterns for easier development and testing.

## Core Concepts

* **Workflow:** A Directed Acyclic Graph (DAG) defined by inheriting `Yantra::Workflow`. Contains jobs and their dependencies.
* **Job:** A unit of work defined by inheriting `Yantra::Step` and implementing `perform`. Belongs to a Workflow.
* **Dependency:** An edge in the DAG; Job B runs only after Job A succeeds.
* **State Machine:** Workflows and Jobs have well-defined states (`pending`, `enqueued`, `running`, `succeeded`, `failed`, `cancelled`) with enforced transitions, managed internally by `Yantra::Core::StateMachine`.
* **Repository:** An abstraction (`Yantra::Persistence::RepositoryInterface`) hiding persistence details. Implemented by Adapters.
* **Adapters (Persistence):** Concrete implementations for specific backends (e.g., `Yantra::Persistence::Redis::Adapter`, `Yantra::Persistence::ActiveRecord::Adapter`).
* **Adapters (Worker):** Concrete implementations (`Yantra::Worker::SidekiqAdapter`, etc.) conforming to `Yantra::Worker::EnqueuingInterface` to abstract background job systems.
* **Client:** The main public API (`Yantra::Client`) for creating, starting, and interacting with workflows.
* **Orchestrator:** Internal component (`Yantra::Core::Orchestrator`) that drives workflow execution based on job states and dependencies.
* **Event:** Notifications (`yantra.workflow.started`, `yantra.job.failed`, etc.) emitted for observability.

## Architecture

Yantra uses a layered architecture:

```text
User App -> Yantra::Client -> Yantra Core Logic (Orchestrator, StateMachine) -> RepositoryInterface -> Persistence Adapter -> DB
User App -> Yantra::Client -> Yantra Core Logic (Orchestrator) -> EnqueuingInterface -> Worker Adapter -> Background Queue
```

This decouples application logic, core orchestration, persistence, and background job enqueuing.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'yantra'

# Add adapter dependencies if needed, e.g.:
# gem 'redis'
# gem 'pg' or gem 'sqlite3'
# gem 'sidekiq' or gem 'resque'
# gem 'activejob' (usually included with Rails)
```

And then execute:

```bash
bundle install
```

## Configuration

Configure Yantra in an initializer (e.g., `config/initializers/yantra.rb`):

```ruby
Yantra.configure do |config|
  # --- Persistence Adapter ---
  # Choose your persistence backend (:active_record, :redis)
  config.persistence_adapter = :active_record # Default

  # Example for Redis (only needed if using :redis adapter):
  # config.redis_url = ENV['YANTRA_REDIS_URL'] || 'redis://localhost:6379/1'
  # config.redis_namespace = 'yantra_myapp'
  # config.redis_options = { ssl_params: { verify_mode: OpenSSL::SSL::VERIFY_NONE } }

  # Example for ActiveRecord:
  # No specific config needed by default if using Rails' connection management.
  # Yantra models will use the connection configured via database.yml / connects_to.

  # --- Worker Adapter ---
  # Choose your background worker integration (:active_job, :sidekiq, :resque)
  config.worker_adapter = :active_job # Default
  # TODO: Add worker-specific config examples if needed

  # --- Retry Behavior ---
  # Sets the default maximum number of attempts for jobs (including the first run).
  # Can be overridden per job class by defining `self.yantra_max_attempts`.
  config.default_max_step_attempts = 3 # Default is 3

  # --- General Settings ---
  # Configure logging (optional)
  # config.logger = Rails.logger # Or your custom logger
  # config.logger.level = Logger::DEBUG

  # Configure event notification backend (optional)
  # Defaults to ActiveSupport::Notifications if available
  # config.notification_backend = MyCustomNotifier
end
```

## Basic Usage

**1. Define Jobs:**

```ruby
# app/workflows/jobs/fetch_user_data_job.rb
class FetchUserDataJob < Yantra::Step
  # Optional: Override max attempts for this specific job type
  # def self.yantra_max_attempts; 5; end

  def perform(user_id:)
    puts "Fetching data for user #{user_id}"
    # Simulate potential transient error
    # raise Net::TimeoutError if rand(3) == 0
    # ... fetch logic ...
    { user_name: "User #{user_id}", fetched_at: Time.now } # Return output hash
  end
end

# app/workflows/jobs/generate_report_job.rb
class GenerateReportJob < Yantra::Step
  # Override default queue if needed
  # def queue_name; :reports; end # Use instance method

  def perform(user_data:, report_format:)
    puts "Generating #{report_format} report for #{user_data[:user_name]}"
    # ... report generation ...
    "report_url_for_#{user_data[:user_name]}.#{report_format}"
  end
end
```

**2. Define a Workflow:**

```ruby
# app/workflows/user_report_workflow.rb
class UserReportWorkflow < Yantra::Workflow
  # Arguments passed to Client.create_workflow are received here
  def perform(user_id:, format: 'pdf', priority: 'low')
    # Use the run DSL to define jobs and dependencies
    fetch_step_ref = run FetchUserDataJob, name: :fetch, params: { user_id: user_id }

    # GenerateReportJob runs after fetch_job completes.
    # TODO: Define how job outputs are accessed by dependents.
    # Alternative: Pass initial args if output passing isn't implemented
    run GenerateReportJob, name: :generate, params: { user_id: user_id, report_format: format }, after: fetch_step_ref
  end
end
```

*(Note: Automatic output passing from dependencies to dependents is a potential feature requiring further design).*

**3. Create and Start the Workflow:**

```ruby
# Somewhere in your application code (e.g., a controller, service, background job)

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
```

## Persistence Setup (ActiveRecord)

If using the `:active_record` persistence adapter in a Rails application:

1.  **Run the Installer:** This copies the necessary migration files into your application.

    ```bash
    rails generate yantra:install
    ```

2.  **Run Migrations:** Create the Yantra tables in your database.

    ```bash
    rake db:migrate
    ```

    This will create the `yantra_workflows`, `yantra_steps`, and `yantra_step_dependencies` tables.

## Development & Testing

1.  Clone the repository.

2.  Run `bundle install` (installs runtime and development dependencies).

3.  **For ActiveRecord Tests:**

    * Ensure necessary development gems are installed (`rails`, `activerecord`, `sqlite3`, `database_cleaner-active_record`).
    * Set up the dummy Rails app located in `test/dummy/`:
        ```bash
        cd test/dummy
        bundle install --binstubs
        cd ../..
        ```
    * Generate the test schema using the dummy app:
        ```bash
        cd test/dummy
        bin/rails generate yantra:install
        bin/rake db:migrate RAILS_ENV=test
        bin/rake db:schema:dump RAILS_ENV=test
        cd ../..
        ```
    * Copy the generated schema to the test support directory (adjust path if needed):
        ```bash
        mkdir -p test/support/db
        cp test/dummy/db/schema.rb test/support/db/schema.rb
        ```

4.  Run tests:

    ```bash
    bundle exec rake test
    ```

## Contributing

[Placeholder - Add contribution guidelines, code of conduct, setup instructions]

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).


