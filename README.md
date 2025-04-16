# Yantra: A Pluggable Workflow Engine for Ruby

[![Gem Version](https://badge.fury.io/rb/yantra.svg)](https://badge.fury.io/rb/yantra)
[![Build Status](https://github.com/your_org/yantra/actions/workflows/ci.yml/badge.svg)](https://github.com/your_org/yantra/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Maintainability](https://api.codeclimate.com/v1/badges/YOUR_BADGE_ID/maintainability)](https://codeclimate.com/github/your_org/yantra/maintainability) [![Test Coverage](https://api.codeclimate.com/v1/badges/YOUR_BADGE_ID/test_coverage)](https://codeclimate.com/github/your_org/yantra/test_coverage)

Yantra is a robust, DAG-based workflow orchestration engine for Ruby applications, designed with flexibility, maintainability, and observability in mind. It allows you to define complex workflows with dependent steps and execute them reliably using common background worker systems.

It aims to address limitations found in previous systems by providing:

* **Backend Agnosticism:** Choose your persistence layer (e.g., PostgreSQL via ActiveRecord, Redis planned) without changing core logic, thanks to the Repository Pattern.
* **Simplified Core:** Clear state management and object lookups using consistent UUID identifiers.
* **Robust Observability:** (Planned) Deeply integrated event emission for monitoring and alerting.
* **Maintainability & Extensibility:** Modern design patterns (Repository, State Machines, Adapters) for easier development and testing.

## Core Concepts

* **Workflow:** A Directed Acyclic Graph (DAG) defined by inheriting `Yantra::Workflow` and implementing a `define` method. Contains steps and their dependencies. Assigned a unique UUID.
* **Step:** A unit of work defined by inheriting `Yantra::Step` and implementing a `perform` method. Belongs to a Workflow. Assigned a unique UUID. Contains state, arguments, output, error info, etc. (Note: Runtime state like status, retries, output is primarily stored in the persistence layer, not the Step instance itself).
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
1.  Create an initializer file at `config/initializers/yantra.rb`.
2.  Generate database migrations for the necessary tables (`yantra_workflows`, `yantra_steps`, `yantra_step_dependencies`).

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
    # queue: :default, # Default queue name is now derived from Step class name
    retries: 3       # Default number of retries (total attempts = retries + 1)
  }
  # Individual steps can override these using `yantra_options queue: '...', retries: ...`

  # --- General Settings ---
  # Configure logging (optional - Implementation TBD)
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
  # You can define a class method `yantra_max_attempts` to override retries
  def self.yantra_max_attempts; 5; end # Total attempts = 5

  # You can override the default queue name
  def queue_name; 'api_intensive'; end

  def perform(user_id:)
    puts "Fetching data for user #{user_id}"
    # ... fetch logic ...
    result = { user_name: "User #{user_id}", fetched_at: Time.now }
    puts "Fetched: #{result}"
    result # Return output hash (must be JSON-serializable)
  end
end

# app/workflows/steps/generate_report_step.rb
class GenerateReportStep < Yantra::Step
  def queue_name; 'reports'; end # Example override

  # Arguments passed via `params:` in the workflow `run` call are available here.
  # Output from parent steps needs to be accessed via `parent_outputs` (see Pipelining section).
  def perform(user_name:, report_format:) # Changed: Expect user_name directly
    puts "Generating #{report_format} report for #{user_name}"
    # ... report generation ...
    report_url = "report_url_for_#{user_name}.#{report_format}"
    puts "Generated: #{report_url}"
    { report_url: report_url } # Return output
  end
end

# app/workflows/steps/notify_admin_step.rb
class NotifyAdminStep < Yantra::Step
  def perform(report_url:, user_id:) # Changed: Expect report_url directly
    puts "Notifying admin about report for user #{user_id}: #{report_url}"
    # ... notification logic ...
    { notified_at: Time.now }
  end
end
```

**2. Define a Workflow:**
Inherit from `Yantra::Workflow` and use the `define` DSL inside the `perform` method.

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
  def perform
    # Step 1: Fetch data
    # The object returned by `run` is a lightweight representation, mainly used
    # for dependency tracking (`after:`). It doesn't hold the step's output directly.
    fetch_step_ref = run FetchUserDataStep,
                         name: :fetcher, # Optional symbolic name
                         params: { user_id: user_id }

    # Step 2: Generate report, runs after fetch_step_ref completes.
    # We pass necessary data via `params`. For accessing output dynamically, see Pipelining.
    # NOTE: This example passes data via params. See Pipelining section for alternative.
    generate_step_ref = run GenerateReportStep,
                            name: :generator,
                            params: { user_name: "User_#{user_id}", report_format: format }, # Example: Construct needed data
                            after: [fetch_step_ref] # Specify dependency using the reference

    # Step 3: Notify admin, runs after generate_step_ref completes.
    run NotifyAdminStep,
        name: :notifier,
        params: { report_url: "some_generated_url", user_id: user_id }, # Example: Construct needed data
        after: [generate_step_ref]
  end
end
```

**DSL `run` Method:**

* `run StepClass, name: nil, params: {}, after: []`: Defines a step within the workflow's `perform` method.
    * `StepClass`: The class inheriting from `Yantra::Step`.
    * `name`: (Optional) A unique symbolic name for this step instance within the workflow definition. Useful for clarity but not strictly required.
    * `params`: Hash of arguments passed to the step's `perform` method when it executes. These must be JSON-serializable.
    * `after`: An array of references (returned by previous `run` calls) that must complete successfully before this step can start.

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
)

puts "Created workflow with ID: #{workflow_id}"

# Start the workflow (enqueues initial steps with no dependencies via Orchestrator)
Yantra::Client.start_workflow(workflow_id)

puts "Workflow #{workflow_id} started."
```

**4. Monitor Workflows and Steps:**

```ruby
# Find a workflow's status object
workflow_record = Yantra::Client.find_workflow(workflow_id)
# => #<Yantra::Persistence::ActiveRecord::WorkflowRecord id: "...", klass: "...", state: "running", ...>
if workflow_record
  puts workflow_record.state # => "pending", "running", "succeeded", "failed", "cancelled"
  puts workflow_record.created_at
  puts workflow_record.finished_at
end

# Get all step records for a workflow
step_records = Yantra::Client.get_workflow_steps(workflow_id)
step_records.each do |step_record|
  puts "#{step_record.klass} (#{step_record.id}): #{step_record.state}"
  # => "FetchUserDataStep (uuid...): succeeded"
  # => "GenerateReportStep (uuid...): running"
  # => "NotifyAdminStep (uuid...): enqueued"
  puts "  Output: #{step_record.output}" if step_record.output
  puts "  Error: #{step_record.error}" if step_record.error
  puts "  Retries: #{step_record.retries}"
end

# Find a specific step record by its UUID
step_record = Yantra::Client.find_step(a_specific_step_id)
# => #<Yantra::Persistence::ActiveRecord::StepRecord id: "..." ...>
```

**5. Handling Failures and Retries:**

* **Automatic Retries:** If a step's `perform` method raises an error, the `Yantra::Worker::RetryHandler` (used by the background step job) catches it. If the step has remaining retry attempts (based on `StepClass.yantra_max_attempts` or `Yantra.configuration.default_max_step_attempts`), the `RetryHandler` updates the persisted retry count and error, then re-raises the error. The underlying background job system (e.g., ActiveJob with Sidekiq/Resque backend) catches this re-raised error and schedules the retry according to its own logic (e.g., with exponential backoff). The step's state remains `running` during retry attempts.
* **Permanent Failure:** If a step fails and exhausts its retries, the `RetryHandler` marks the step's state as `failed` via the Repository and records the final error. The Orchestrator logic then marks the entire workflow as `failed` and cancels downstream steps.
* **Manual Retries:**
    * `Yantra::Client.retry_failed_steps(workflow_id)`: Finds all steps currently in the `failed` state for the given workflow, resets the workflow state to `running`, resets the failed steps' state to `enqueued`, and enqueues them for execution. Returns the number of steps re-enqueued or `false` if the workflow wasn't found or wasn't in a failed state.

**6. Cancellation:**

```ruby
# Cancel a running or pending workflow
Yantra::Client.cancel_workflow(workflow_id)
```
This marks the workflow as `cancelled`. The Orchestrator will stop enqueuing any further `pending` steps for this workflow and will bulk-cancel any steps currently in the `enqueued` state. Steps already `running` will generally continue to completion unless they implement specific cancellation checks.

## Pipelining (Accessing Parent Step Output)

Steps can access the output returned by their direct parent steps within their `perform` method using the `parent_outputs` helper. This allows you to chain steps together based on their results without explicitly passing IDs or fetching records manually.

The `parent_outputs` method performs a lazy-loaded query to fetch the outputs of all immediate parent steps that have successfully completed.

* **Return Value:** It returns a Hash where keys are the parent step IDs (UUID strings) and values are the corresponding output payloads that were returned by the parent's `perform` method and persisted.
* **Caching:** The result is cached within the step instance, so multiple calls to `parent_outputs` within the same `perform` execution will not trigger multiple database queries.
* **No Parents / No Output:** If a step has no parents, or if parent steps haven't completed or didn't return output, the corresponding entries might be missing or have `nil` values in the hash. If no parents have output or the step has no parents, it returns an empty hash `{}`.

**Example:**

```ruby
# app/workflows/steps/step_one.rb
class StepOne < Yantra::Step
  def perform(input:)
    # ... does work ...
    { step_one_result: "Processed_#{input}" } # Return output hash
  end
end

# app/workflows/steps/step_two.rb
class StepTwo < Yantra::Step
  def perform # Takes no direct arguments from workflow params
    # Access the outputs from parent steps
    outputs = parent_outputs # Returns hash like: {"uuid-of-step-one" => {"step_one_result"=>"Processed_value123"}}
    puts "DEBUG: Received parent outputs: #{outputs.inspect}"

    # Assuming StepOne is the only parent
    # Parent IDs are available via the @parent_ids instance variable if needed
    parent_id = @parent_ids.first
    parent_output_data = outputs[parent_id] # Access using the parent's ID key

    # IMPORTANT: Keys in the output hash might be strings if loaded from JSON/JSONB.
    # Access using strings for safety unless you handle symbolization.
    if parent_output_data && parent_output_data['step_one_result']
      result_from_one = parent_output_data['step_one_result']
      puts "StepTwo using data from StepOne: #{result_from_one}"
      # ... use result_from_one ...
      { final_output: "Used_#{result_from_one}" }
    else
      puts "WARN: StepTwo did not find expected output key 'step_one_result' from StepOne. Output received: #{parent_output_data.inspect}"
      { final_output: "Default output" }
    end
  end
end

# app/workflows/my_pipeline_workflow.rb
class MyPipelineWorkflow < Yantra::Workflow
  def perform # Changed from define to perform for consistency
    step1_ref = run StepOne, params: { input: "value123" }
    run StepTwo, after: [step1_ref] # StepTwo depends on StepOne
  end
end
```

**Note on Keys:** When retrieving outputs that were stored as JSON/JSONB in the database (common for the ActiveRecord adapter), the keys within the returned output hash might be strings even if you returned symbols from the parent step's `perform` method. Access the hash using string keys (e.g., `parent_output['my_key']`) for reliable retrieval, or use `Hash#with_indifferent_access` if using ActiveSupport.

## Maintenance Tasks

Yantra provides Rake tasks for common maintenance operations.

### Cleaning Expired Workflows

Over time, the persistence layer can accumulate records for completed workflows. You can clean these up using the provided Rake task.

**Purpose:** Deletes workflow records (and potentially associated step/dependency records, depending on adapter implementation and database constraints like cascading deletes) that finished before a specified time cutoff.

**Command:**
```bash
bundle exec rake yantra:cleanup:expired_workflows[days_ago]
```

* `days_ago`: (Optional) An integer specifying the number of days ago to set the cutoff. Workflows that finished *before* this time will be deleted. Defaults to `30` if not provided.

**Example:** Delete workflows that finished more than 60 days ago:
```bash
bundle exec rake yantra:cleanup:expired_workflows[60]
```

**Prerequisite:** The configured persistence adapter (e.g., `Yantra::Persistence::ActiveRecord::Adapter`) must implement the `delete_expired_workflows(cutoff_timestamp)` method. The ActiveRecord adapter included with Yantra supports this. Ensure your database schema uses cascading deletes or your adapter handles deleting associated step/dependency records appropriately if you require full cleanup.

## Persistence Setup (ActiveRecord)

If using the `:active_record` persistence adapter in a Rails application:

1.  **Run the Installer:** This copies the necessary migration files into your `db/migrate/` directory.
    ```bash
    rails generate yantra:install
    ```
2.  **Review Migrations:** Check the generated files (e.g., `*_create_yantra_workflows.rb`, `*_create_yantra_steps.rb`, `*_create_yantra_step_dependencies.rb`). Ensure column types like `output`, `arguments`, `error` are suitable (e.g., `jsonb` recommended for PostgreSQL).
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

