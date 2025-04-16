# Yantra: A Pluggable Workflow Engine for Ruby

[![Gem Version](https://badge.fury.io/rb/yantra.svg)](https://badge.fury.io/rb/yantra)
[![Build Status](https://github.com/your_org/yantra/actions/workflows/ci.yml/badge.svg)](https://github.com/your_org/yantra/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Maintainability](https://api.codeclimate.com/v1/badges/YOUR_BADGE_ID/maintainability)](https://codeclimate.com/github/your_org/yantra/maintainability) [![Test Coverage](https://api.codeclimate.com/v1/badges/YOUR_BADGE_ID/test_coverage)](https://codeclimate.com/github/your_org/yantra/test_coverage) Yantra is a robust, DAG-based workflow orchestration engine for Ruby applications, designed with flexibility, maintainability, and observability in mind. It allows you to define complex workflows with dependent steps and execute them reliably using common background worker systems.

It aims to address limitations found in previous systems by providing:

* **Backend Agnosticism:** Choose your persistence layer (e.g., PostgreSQL via ActiveRecord, Redis planned) without changing core logic, thanks to the Repository Pattern.
* **Simplified Core:** Clear state management and object lookups using consistent UUID identifiers.
* **Robust Observability:** (Planned) Deeply integrated event emission for monitoring and alerting.
* **Maintainability & Extensibility:** Modern design patterns (Repository, State Machines, Adapters) for easier development and testing.

## Core Concepts

* **Workflow:** A Directed Acyclic Graph (DAG) defined by inheriting `Yantra::Workflow` and implementing a `define` method. Contains steps and their dependencies. Assigned a unique UUID.
* **Step:** A unit of work defined by inheriting `Yantra::Step` and implementing a `perform` method. Belongs to a Workflow. Assigned a unique UUID. Contains state, arguments, output, error info, etc.
* **Dependency:** An edge in the DAG; Step B runs only after Step A succeeds. Defined in the `define` method using the `after:` option.
* **State Machine:** Workflows and Steps have well-defined states (`pending`, `enqueued`, `running`, `succeeded`, `failed`, `cancelled`) with enforced transitions, managed internally by `Yantra::Core::StateMachine`.
* **Repository:** An abstraction (`Yantra::Persistence::RepositoryInterface`) hiding persistence details. Implemented by Persistence Adapters.
* **Persistence Adapter:** Concrete implementation for a specific backend (e.g., `Yantra::Persistence::ActiveRecord::Adapter`, `Yantra::Persistence::Redis::Adapter` planned). Chosen via configuration.
* **Worker Adapter:** Concrete implementation (e.g., `Yantra::Worker::ActiveJob::Adapter`, `Yantra::Worker::SidekiqAdapter` planned) conforming to `Yantra::Worker::EnqueuingInterface` to abstract background job systems. Chosen via configuration.
* **Client:** The main public API (`Yantra::Client`) for creating, starting, monitoring, and interacting with workflows and steps.
* **Orchestrator:** Internal component (`Yantra::Core::Orchestrator`) that drives workflow execution based on step states and dependencies, using the Repository and Worker Adapter.
* **Event:** (Planned) Notifications (e.g., `yantra.workflow.started`, `yantra.step.succeeded`, `yantra.step.failed`) emitted for observability.

## Architecture

Yantra uses a layered architecture to decouple concerns:

```text
+-------------------+      +---------------------+      +--------------------+
| User Application  | ---> | Yantra::Client      | ---> | Yantra Core Logic  |
| (Defines Wf/Steps)|      | (Public API)        |      | (Orchestrator, State)|
+-------------------+      +---------------------+      +--------------------+
        |                                                  |           |
        | Uses Client API                                  | Uses Interfaces |
        V                                                  V           V
+----------------------------+      +----------------------------+<----+
| Yantra::Persistence::Repo  | <--- | Yantra::Worker             |
| (Interface)                |      | (Enqueuing Interface)      |
+----------------------------+      +----------------------------+
  | Implemented By                    | Implemented By
  V                                   V
+----------------------------------+  +---------------------------------------+
| ActiveRecord Adapter | Redis Adapter | | ActiveJob Adapter | Sidekiq Adapter | ... |
+----------------------------------+  +---------------------------------------+
  |                    |               |                 |
  V                    V               V                 V
+----------+         +-------+       +-----------+     +---------+
| Database |         | Redis |       | ActiveJob |     | Sidekiq | ...
+----------+         +-------+       +-----------+     +---------+

```

This decouples application logic, core orchestration, persistence, and background step enqueuing.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'yantra'

# Add adapter dependencies if needed, e.g.:
# gem 'redis' # If using Redis adapter (when available)
# gem 'pg' or gem 'sqlite3' # If using ActiveRecord adapter
# gem 'sidekiq' or gem 'resque' # If using those worker adapters (when available)
# gem 'activejob' (usually included with Rails)
```

And then execute:

```bash
bundle install
```

Or install it yourself as:

```bash
gem install yantra
```

Then, run the installation generator (needed for ActiveRecord persistence):

```bash
rails generate yantra:install
```

This will:
1. Create an initializer file at `config/initializers/yantra.rb`.
2. Generate database migrations for the necessary tables (`yantra_workflows`, `yantra_steps`, `yantra_step_dependencies`).

Run the migrations:
```bash
rails db:migrate
```

## Configuration

Configure Yantra in an initializer (e.g., `config/initializers/yantra.rb`):

```ruby
# config/initializers/yantra.rb
Yantra.configure do |config|
  # --- Persistence Adapter ---
  # Choose your persistence backend (:active_record, :redis)
  config.persistence_adapter = :active_record # Default

  # Example for Redis (only needed if using :redis adapter - planned):
  # config.redis_url = ENV['YANTRA_REDIS_URL'] || 'redis://localhost:6379/1'
  # config.redis_namespace = 'yantra_myapp'
  # config.redis_options = { ssl_params: { verify_mode: OpenSSL::SSL::VERIFY_NONE } }

  # Example for ActiveRecord:
  # No specific config needed by default if using Rails' connection management.
  # Yantra models will use the connection configured via database.yml / connects_to.

  # --- Worker Adapter ---
  # Choose your background worker integration (:active_job, :sidekiq, :resque)
  config.worker_adapter = :active_job # Default
  # TODO: Add worker-specific config examples if needed (e.g., Sidekiq options)

  # --- Default Step Options ---
  # Configure default options applied to all steps unless overridden in the Step class.
  config.default_step_options = {
    queue: :default, # Default queue name for background jobs
    retries: 3       # Default number of retries (total attempts = retries + 1)
  }
  # Individual steps can override these using `yantra_options queue: '...', retries: ...`

  # --- General Settings ---
  # Configure logging (optional)
  # config.logger = Rails.logger # Or your custom logger
  # config.logger.level = Logger::DEBUG

  # Configure event notification backend (optional - planned feature)
  # Defaults to ActiveSupport::Notifications if available
  # config.notification_backend = MyCustomNotifier
end
```

## Basic Usage

**1. Define Steps:**
Inherit from `Yantra::Step` and implement `perform`.

```ruby
# app/workflows/steps/fetch_user_data_step.rb
class FetchUserDataStep < Yantra::Step
  # Optional: Override default options for this specific step type
  # yantra_options queue: 'api_intensive', retries: 5

  def perform(user_id:)
    puts "Fetching data for user #{user_id}"
    # Simulate potential transient error
    # raise Net::TimeoutError if rand(3) == 0 # Example of forcing a retry
    # ... fetch logic ...
    result = { user_name: "User #{user_id}", fetched_at: Time.now }
    puts "Fetched: #{result}"
    result # Return output hash (must be JSON-serializable)
  end
end

# app/workflows/steps/generate_report_step.rb
class GenerateReportStep < Yantra::Step
  # yantra_options queue: 'reports' # Example override

  # Arguments include params passed in `run` and output from dependencies.
  # The dependency's output is available via `dependency_step_instance.output`.
  def perform(user_data:, report_format:)
    user_name = user_data[:user_name] # Access output from FetchUserDataStep
    puts "Generating #{report_format} report for #{user_name}"
    # ... report generation ...
    report_url = "report_url_for_#{user_name}.#{report_format}"
    puts "Generated: #{report_url}"
    { report_url: report_url } # Return output
  end
end

# app/workflows/steps/notify_admin_step.rb
class NotifyAdminStep < Yantra::Step
  def perform(report_info:, user_id:)
    puts "Notifying admin about report for user #{user_id}: #{report_info[:report_url]}"
    # ... notification logic ...
    { notified_at: Time.now }
  end
end
```

**2. Define a Workflow:**
Inherit from `Yantra::Workflow` and use the `define` DSL.

```ruby
# app/workflows/user_report_workflow.rb
class UserReportWorkflow < Yantra::Workflow
  # Arguments passed to Client.create_workflow are received here
  attr_reader :user_id, :format, :priority

  def initialize(user_id:, format: 'pdf', priority: 'low')
    @user_id = user_id
    @format = format
    @priority = priority
    super() # Important: Call super to initialize workflow state
  end

  # Use the `run` DSL to define steps and dependencies
  def define
    # Step 1: Fetch data
    fetch_step = run FetchUserDataStep,
                       params: { user_id: user_id }

    # Step 2: Generate report, runs after fetch_step succeeds.
    # Access fetch_step's output using the instance variable.
    generate_step = run GenerateReportStep,
                        params: { user_data: fetch_step.output, report_format: format },
                        after: [fetch_step] # Specify dependency

    # Step 3: Notify admin, runs after generate_step succeeds.
    run NotifyAdminStep,
        params: { report_info: generate_step.output, user_id: user_id },
        after: [generate_step]
  end
end
```

**DSL `run` Method:**

* `run StepClass, params: {}, after: [], name: nil`: Defines a step.
    * `StepClass`: The class inheriting from `Yantra::Step`.
    * `params`: Hash of arguments passed to the step's `perform` method. Can include `step_instance.output` from dependency steps to pass results.
    * `after`: An array of step instances (returned by previous `run` calls) that must complete successfully before this step can start.
    * `name`: (Optional) A unique symbolic name for the step within the workflow definition (rarely needed if using instance variables).

**3. Create and Start the Workflow:**
Use `Yantra::Client`.

```ruby
# Somewhere in your application code (e.g., a controller, service, background job)

user_id_to_process = 456

# Create and persist the workflow definition and its steps (in 'pending' state)
workflow_id = Yantra::Client.create_workflow(
  UserReportWorkflow,           # The workflow class
  user_id: user_id_to_process,  # Keyword args for the workflow's initialize method
  format: 'csv'
  # globals: { company_id: 'xyz' } # Optional global data accessible later (planned feature)
)

puts "Created workflow with ID: #{workflow_id}"

# Start the workflow (enqueues initial steps with no dependencies via Orchestrator)
Yantra::Client.start_workflow(workflow_id)

puts "Workflow #{workflow_id} started."
```

**4. Monitor Workflows and Steps:**

```ruby
# Find a workflow's status
workflow_status = Yantra::Client.find_workflow(workflow_id)
# => #<Yantra::Workflow status="running" id="..." ...>
puts workflow_status.state # => :pending, :running, :succeeded, :failed, :cancelled
puts workflow_status.created_at
puts workflow_status.finished_at

# Get all steps for a workflow
steps = Yantra::Client.get_workflow_steps(workflow_id)
steps.each do |step_status|
  puts "#{step_status.klass_name} (#{step_status.id}): #{step_status.state}"
  # => "FetchUserDataStep (uuid...): succeeded"
  # => "GenerateReportStep (uuid...): running"
  # => "NotifyAdminStep (uuid...): enqueued"
  puts "  Output: #{step_status.output}" if step_status.output
  puts "  Error: #{step_status.error}" if step_status.error
  puts "  Retries: #{step_status.retries}"
end

# Find a specific step by its UUID
step_status = Yantra::Client.find_step(a_specific_step_id)
# => #<Yantra::Step status="succeeded" id="..." ...>
```

**5. Handling Failures and Retries:**

* **Automatic Retries:** If a step's `perform` method raises an error, the `Yantra::Worker::RetryHandler` (used by the background step job) catches it. If the step has remaining retry attempts (based on `config.default_step_options[:retries]` or `StepClass.yantra_options[:retries]`), the background job system (e.g., ActiveJob) is instructed to retry the job, usually with exponential backoff. The step's state remains `running` or `enqueued` during retries.
* **Permanent Failure:** If a step fails and exhausts its retries, the `RetryHandler` marks the step's state as `failed` via the Repository and records the final error. The Orchestrator logic then marks the entire workflow as `failed`.
* **Manual Retries:**
    * `Yantra::Client.retry_step(step_id)`: Finds a specific step currently in the `failed` state and re-enqueues it if it has attempts remaining (or potentially resets attempts - TBD).
    * `Yantra::Client.retry_failed_steps(workflow_id)`: Finds all steps currently in the `failed` state for the workflow and re-enqueues them.

**6. Cancellation:**

```ruby
# Cancel a running workflow
Yantra::Client.cancel_workflow(workflow_id)
```
This marks the workflow as `cancelled`. The Orchestrator will stop enqueuing any further `pending` or `enqueued` steps for this workflow. Steps already `running` will generally continue to completion unless they implement specific cancellation checks.

## Persistence Setup (ActiveRecord)

If using the `:active_record` persistence adapter in a Rails application:

1.  **Run the Installer:** This copies the necessary migration files into your `db/migrate/` directory.
    ```bash
    rails generate yantra:install
    ```
2.  **Review Migrations:** Check the generated files (e.g., `*_create_yantra_workflows.rb`, `*_create_yantra_steps.rb`, `*_create_yantra_step_dependencies.rb`).
3.  **Run Migrations:** Create the Yantra tables in your database.
    ```bash
    rake db:migrate
    ```
    This will create the `yantra_workflows`, `yantra_steps`, and `yantra_step_dependencies` tables.

## Development & Testing

1.  Clone the repository.
2.  Run `bundle install` (installs runtime and development dependencies).
3.  **For ActiveRecord Tests:**
    * Ensure necessary development gems are installed (`rails`, `activerecord`, `sqlite3` or `pg`, `database_cleaner-active_record`).
    * Set up the dummy Rails app located in `test/dummy/`:
        ```bash
        # From the gem's root directory
        cd test/dummy
        bundle install --binstubs
        cd ../..
        ```
    * Generate the test schema using the dummy app:
        ```bash
        # From the gem's root directory
        cd test/dummy
        # Ensure the generator copies migrations if not already present
        bin/rails generate yantra:install --force
        # Create/migrate the test database
        bin/rake db:create RAILS_ENV=test
        bin/rake db:migrate RAILS_ENV=test
        # Dump the schema
        bin/rake db:schema:dump RAILS_ENV=test
        cd ../..
        ```
    * Copy the generated schema to the test support directory (used by `test_helper.rb`):
        ```bash
        mkdir -p test/support/db
        cp test/dummy/db/schema.rb test/support/db/schema.rb
        ```
4.  Run tests:
    ```bash
    bundle exec rake test
    ```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/your_org/yantra. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [code of conduct](CODE_OF_CONDUCT.md).

[Placeholder - Add more detailed contribution guidelines]

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Yantra project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](CODE_OF_CONDUCT.md).

