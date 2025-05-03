# Yantra

[![Gem Version](https://badge.fury.io/rb/yantra.svg)](https://badge.fury.io/rb/yantra)
[![Build Status](https://github.com/incorvia/yantra/actions/workflows/ci.yml/badge.svg)](https://github.com/incorvia/yantra/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Yantra is a robust, backend-agnostic workflow engine for Ruby applications. Define complex workflows as Directed Acyclic Graphs (DAGs) of individual steps (jobs), run them reliably via background workers, and gain insight through a built-in event system.

Yantra focuses on:

* **Reliability:** Clear state management and robust error handling with configurable retries.
* **Flexibility:** Choose your own backend for persistence (e.g., ActiveRecord, Redis\*) and background job processing (e.g., Active Job, Sidekiq\*, Resque\*) via adapters. *(Adapters marked with \* are planned but may not be implemented yet).*
* **Observability:** Emit events at key lifecycle points for monitoring and tracing.
* **Maintainability:** A clean, modern codebase with **zero external runtime dependencies** in its core.

## Features

* Define complex workflows with dependencies between steps.
* Backend-agnostic persistence via a Repository pattern (ActiveRecord adapter included).
* Background job processing via adapters (Active Job adapter included).
* Configurable automatic retries for failed steps.
* **Delayed step execution:** Schedule steps to run after a specified delay.
* Workflow and step cancellation.
* Simple data pipelining between dependent steps.
* Event-driven notifications for observability (adapters for Null, Logger, ActiveSupport::Notifications included).
* Zero external runtime gem dependencies required for the core library.

## Architecture Overview

Yantra employs a layered architecture to separate concerns (core logic, persistence, background job queuing) and allow for flexibility through adapters.

```text
+-------------------+     +---------------------+     +--------------------+
| User Application  | --->| Yantra::Client      | --->| Yantra Core Logic  |
| (Defines Wf/Jobs) |     | (Public API)        |     | (Orchestration, State) |
+-------------------+     +---------------------+     +--------------------+
                                                      |
                                                      | Uses Interfaces & Services
                                                      V
+---------------------------------+  +---------------------------------+
| Yantra::Core::DependentProcessor|  | Yantra::Core::StepEnqueuer      |
| (Handles Success/Failure Logic) |  | (Handles Job System Handoff)    |
+---------------------------------+  +---------------------------------+
              |                                      |
              V Uses                                 V Uses
+---------------------------------+  +---------------------------------+
| Yantra::Core::StateTransitionSvc|  | Yantra::Persistence::Repo       |
| (Ensures Valid State Changes)   |  | (Interface)                     |
+---------------------------------+  +---------------------------------+
                                     | Implemented By
                                     V
+----------------------------------+ +----------------------------+
| ActiveRecord Adapter | Redis Adpt*| | ActiveJob Adapter | Sidekiq Adpt*| ...
+----------------------------------+ +----------------------------+
  | Uses                           | Uses
  V                              V
+----------+ +-------+           +-----------+ +---------+
| Postgres | | Redis |           | ActiveJob | | Sidekiq | ...
+----------+ +-------+           +-----------+ +---------+
```

**Recent Architectural Refinements:**

* **Service Objects:** Core logic for handling step completion outcomes (`DependentProcessor`) and ensuring valid state transitions (`StateTransitionService`) has been extracted into dedicated service objects. This improves separation of concerns and testability within the core engine.
* **State Machine:** The state lifecycle has been refined. The `ENQUEUED` state has been removed. Steps now transition from `PENDING` -> `AWAITING_EXECUTION` (during the attempt to hand off to the job backend) -> `RUNNING` (when the worker picks it up). The `enqueued_at` timestamp on the step record indicates successful handoff to the job backend. `POST_PROCESSING` is used internally after successful `perform` before dependents are processed.
* **Repository Interface:** The persistence interface (`RepositoryInterface`) has been refactored for better consistency (see notes below).

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'yantra'
```

And then execute:

```bash
$ bundle install
```

**Adapter Dependencies:**

Yantra core has no runtime dependencies. However, you will need to add gems for the specific adapters you choose to use:

* For `persistence_adapter: :active_record`: Add `gem 'activerecord'` (and likely `gem 'pg'` or `gem 'sqlite3'`).
* For `worker_adapter: :active_job`: Add `gem 'activejob'` (and likely `gem 'activesupport'`).
* For `notification_adapter: :active_support_notifications`: Add `gem 'activesupport'`.
* *(Other adapters like Redis, Sidekiq, etc., will require their respective gems when implemented).*

## Configuration

Create an initializer file (e.g., `config/initializers/yantra.rb`) to configure Yantra:

```ruby
# config/initializers/yantra.rb

Yantra.configure do |config|
  # --- Persistence Adapter ---
  # Choose where workflow and step state is stored.
  # Built-in: :active_record
  # Custom: Pass an object/class that implements Yantra::Persistence::RepositoryInterface
  config.persistence_adapter = :active_record
  # Optional hash passed to the adapter's initializer (if it accepts one)
  # config.persistence_options = {}

  # --- Worker Adapter ---
  # Choose how background jobs are enqueued.
  # Built-in: :active_job
  # Custom: Pass an object/class that implements Yantra::Worker::EnqueuingInterface
  config.worker_adapter = :active_job
  # Optional hash passed to the adapter's initializer
  # config.worker_adapter_options = {}

  # --- Notification Adapter ---
  # Choose how lifecycle events are published.
  # Built-in: :null (default), :logger, :active_support_notifications
  # Custom: Pass an object/class that implements Yantra::Events::NotifierInterface
  config.notification_adapter = :logger # Example: Log events
  # Optional hash passed to the adapter's initializer
  # config.notification_adapter_options = {}

  # --- Logger ---
  # Configure the logger instance Yantra uses. Defaults to Rails.logger or STDOUT logger.
  # config.logger = MyCustomLogger.new

  # --- Default Step Options ---
  # Configure default retry attempts for steps.
  # `retries: 3` means a total of 4 attempts (initial + 3 retries).
  # Default is 0 retries (1 attempt) if not set.
  config.default_step_options[:retries] = 3
  # You can also set default_max_step_attempts directly (overrides retries calculation if present)
  # config.default_max_step_attempts = 4
end
```

**Adapter Loading:**

* **Built-in Adapters:** When you configure a built-in adapter using its symbol (e.g., `:active_record`, `:active_job`, `:logger`), Yantra automatically requires the necessary internal file. You just need to ensure the underlying gem (like `activerecord`) is in your `Gemfile`.
* **Custom Adapters:** If you build your own adapter class (e.g., `MyCustomNotifier`), you need to:
    1.  Ensure the file defining your class is loaded *before* Yantra is configured (e.g., using `require 'path/to/my_custom_notifier'` in your initializer).
    2.  Configure Yantra using either the class constant (`config.notification_adapter = MyCustomNotifier`) or a symbol (`config.notification_adapter = :my_custom_notifier`, assuming your class is defined as `Yantra::Events::MyCustomNotifier::Adapter`).

## Database Setup (ActiveRecord)

If you are using the `:active_record` persistence adapter, you need to generate and run the database migrations:

1.  **Generate Initial Migrations:**
    ```bash
    $ rails g yantra:install
    ```
    This will copy the necessary migration files (`create_yantra_workflows`, `create_yantra_steps`, `create_yantra_step_dependencies`) into your application's `db/migrate/` directory.
2.  **Run Migrations:**
    ```bash
    $ rails db:migrate
    ```

## Usage

### Defining a Workflow

Workflows orchestrate steps. Define a workflow by inheriting from `Yantra::Workflow` and implementing a `perform` method. Use the `run` DSL method inside `perform` to define the steps and their dependencies.

```ruby
# app/workflows/order_processing_workflow.rb
require 'active_support/core_ext/numeric/time' # For 5.minutes etc.

class OrderProcessingWorkflow < Yantra::Workflow
  def perform(order_id:, user_id:)
    # Define steps using the `run` DSL
    # run StepClass, name: :symbolic_name (optional), params: {..}, after: [dependency_ref, ...], delay: duration

    charge_step = run ChargeCreditCardStep, name: :charge, params: { order_id: order_id, amount: 100.00 }

    # This step runs immediately after charge_step succeeds
    update_step = run UpdateInventoryStep, name: :inventory, params: { order_id: order_id }, after: charge_step

    # This step runs 5 minutes after charge_step succeeds
    email_step = run SendConfirmationEmailStep, name: :email, params: { order_id: order_id, user_id: user_id }, after: charge_step, delay: 5.minutes

    # This step runs after both inventory and email steps succeed (email might be delayed)
    run ArchiveOrderStep, name: :archive, params: { order_id: order_id }, after: [update_step, email_step]
  end
end
```

* `run StepClass`: Specifies the `Yantra::Step` subclass to execute.
* `name:`: An optional symbolic name for the step within the workflow definition (used for dependencies). If omitted, a default name is generated.
* `params:`: A hash of arguments passed to the step's `perform` method. Must be JSON-serializable.
* `after:`: Specifies dependencies. Can be a single step reference variable (like `charge_step`) or an array of references (`[update_step, email_step]`). A step only runs after all its dependencies have successfully completed.
* `delay:`: **(New)** An optional delay before the step is enqueued *after* its dependencies are met. Accepts a non-negative **Numeric value representing seconds** (integer or float). If `activesupport` is loaded in your application, you can also conveniently use `ActiveSupport::Duration` objects (e.g., `10.seconds`, `1.hour`). The underlying job backend handles the scheduled execution.

### Defining a Step

Steps represent individual units of work. Define a step by inheriting from `Yantra::Step` and implementing a `perform` method.

```ruby
# app/steps/charge_credit_card_step.rb
class ChargeCreditCardStep < Yantra::Step
  # Optional: Override default retries just for this step
  # def self.yantra_max_attempts; 5; end # Total 5 attempts

  def perform(order_id:, amount:)
    Yantra.logger.info {"Attempting to charge order #{order_id} for #{amount}"}
    payment_service = PaymentGateway.new
    result = payment_service.charge(order_id: order_id, amount: amount)

    unless result.success?
      # Raise an error to mark the step as failed (will trigger retries if configured)
      raise StandardError, "Payment failed: #{result.error_message}"
    end

    # Return value is stored as step output (must be JSON-serializable)
    { transaction_id: result.transaction_id, charged_amount: amount }
  end
end
```

* The `perform` method receives arguments defined in the workflow's `run` call (`params:`). Use keyword arguments.
* Raise an exception within `perform` to indicate failure. Yantra will catch it, record the error, and handle retries based on configuration.
* The return value of `perform` is saved as the step's output (must be JSON-serializable).

### Creating & Starting a Workflow

Use the `Yantra::Client` to interact with workflows.

```ruby
# Create the workflow instance and persist its structure
workflow_id = Yantra::Client.create_workflow(OrderProcessingWorkflow, order_id: 123, user_id: 456)

# Start the workflow (enqueues initial steps)
Yantra::Client.start_workflow(workflow_id)
```

### Data Pipelining

A step can access the output of its direct parent(s) using the `parent_outputs` helper method within its `perform` method.

```ruby
# app/steps/send_confirmation_email_step.rb
class SendConfirmationEmailStep < Yantra::Step
  def perform(order_id:, user_id:)
    # parent_outputs returns a hash: { parent_step_id => parent_output_hash }
    # Note: Output hash keys might be strings if deserialized from JSON via AR/SQLite
    charge_step_output = parent_outputs.values.first # Assuming only one parent

    transaction_id = charge_step_output['transaction_id'] if charge_step_output
    amount = charge_step_output['charged_amount'] if charge_step_output

    unless transaction_id && amount
      raise "Could not find required charge details in parent output: #{parent_outputs.inspect}"
    end

    Yantra.logger.info {"Sending confirmation for order #{order_id}, transaction #{transaction_id}"}
    Mailer.send_confirmation(user_id: user_id, order_id: order_id, transaction_id: transaction_id, amount: amount)

    { email_sent: true }
  end
end
```

* `parent_outputs`: Returns a hash where keys are the IDs of direct parent steps and values are their corresponding output hashes (deserialized from JSON).
* Be aware that hash keys within the output might be strings if using ActiveRecord with certain database backends (like SQLite) unless specific model serialization (`serialize :output, JSON`) is used. Access them accordingly (e.g., `output['key']`).

### Monitoring and Management

Use the `Yantra::Client` API or query the persistence layer directly (if using ActiveRecord) to monitor workflows.

```ruby
# Find workflow status
workflow = Yantra::Client.find_workflow(workflow_id)
puts "Workflow state: #{workflow&.state}"
puts "Workflow has failures: #{workflow&.has_failures}"

# Find a specific step
step = Yantra::Client.find_step(step_id)
puts "Step state: #{step&.state}" # e.g., pending, awaiting_execution, running, succeeded, failed, cancelled
# Access output/error (may be string or hash depending on persistence/serialization)
puts "Step output: #{step&.output.inspect}"
puts "Step error: #{step&.error.inspect}"
# Access delay info
puts "Step Delay Specified (seconds): #{step&.delay_seconds}"
puts "Step Earliest Execution Time: #{step&.earliest_execution_time}"
puts "Step Enqueued At (Timestamp): #{step&.enqueued_at}" # Timestamp when successfully handed off

# Get all steps for a workflow
steps = Yantra::Client.list_steps(workflow_id: workflow_id)

# Get steps with a specific status
failed_steps = Yantra::Client.list_steps(workflow_id: workflow_id, status: :failed)
# Find steps waiting for worker/timer (successfully handed off)
scheduled_steps = Yantra::Client.list_steps(workflow_id: workflow_id, status: :awaiting_execution).select { |s| s.enqueued_at.present? }
# Find steps stuck during awaiting_execution (handoff failed)
# These are steps in awaiting_execution that were not successfully handed off to the job system
# (e.g. due to transient enqueue failure, like Redis outage)
stuck_steps = Yantra::Client.list_steps(workflow_id: workflow_id, status: :awaiting_execution).select { |s| s.enqueued_at.nil? }

# Cancel a running or pending workflow
Yantra::Client.cancel_workflow(workflow_id)

# Retry all failed steps in a failed workflow
# (Resets workflow state to running, re-enqueues FAILED steps and steps stuck in AWAITING_EXECUTION)
Yantra::Client.retry_failed_steps(workflow_id)
```

## Events / Observability

Yantra publishes events at key lifecycle points using the configured `notification_adapter`.

**Key Events:**

* `yantra.workflow.started`
* `yantra.workflow.succeeded`
* `yantra.workflow.failed`
* `yantra.workflow.cancelled`
* `yantra.step.bulk_enqueued` (Published when steps are successfully handed off to the job backend, includes immediately processed and delayed steps)
* `yantra.step.started`
* `yantra.step.succeeded`
* `yantra.step.failed` (Published on permanent failure after retries or critical error)
* `yantra.step.cancelled`

**Payloads:** Event payloads are hashes containing relevant IDs, class names, state, timestamps, and potentially output or error information. The `yantra.step.bulk_enqueued` payload includes an `enqueued_ids` array containing IDs for steps successfully processed in that batch.

**Subscribing (Example using ActiveSupport::Notifications):**

If you configure `config.notification_adapter = :active_support_notifications`, you can subscribe using standard `ActiveSupport::Notifications` methods:

```ruby
# config/initializers/yantra_events.rb

ActiveSupport::Notifications.subscribe(/yantra\..*/) do |name, start, finish, id, payload|
  Rails.logger.info "[YANTRA EVENT] #{name} | Duration: #{finish - start}s | Payload: #{payload.inspect}"
  # Send to monitoring, trigger alerts, etc.
  case name
  when 'yantra.workflow.failed'
    MonitoringService.alert("Workflow Failed: #{payload[:workflow_id]}")
  when 'yantra.step.failed'
    MonitoringService.track_step_failure(payload)
  end
end
```

## Error Handling and Retries

Yantra provides configurable automatic retries for steps that fail during execution. It employs a cooperative strategy with the underlying job runner (configured via `worker_adapter`) to handle the retry process.

**Division of Responsibility:**

* **Yantra (`RetryHandler`)**: Determines *if* a step should be retried based on its configured number of attempts (`default_step_options[:retries]` or step-specific `yantra_max_attempts`) and the current execution count. It manages Yantra's internal state related to retries.
* **Job Runner Backend (Sidekiq, ActiveJob Adapter, etc.)**: Handles the *mechanism* of retrying a job instance, including awaiting_execution the next attempt using its built-in delay/backoff algorithms. Yantra leverages this backend capability.

**Signaling Mechanism:**

Yantra signals its decision to the job runner backend as follows:

1.  **Retry Needed:** If a step fails but Yantra determines it has retries remaining, the `StepExecutor` will **raise the original error**. The job runner backend catches this error and triggers its own retry process.
2.  **Stop Processing (Success or Terminal Failure):** If a step succeeds, OR if it fails but has reached its maximum retry attempts according to Yantra, the `StepExecutor` will **complete normally without raising an error**. The job runner backend sees this as a successful job completion and will *not* attempt further retries.

**Backend Retry Configuration (Handled by Yantra Defaults):**

For this cooperative system to work correctly, the underlying job runner must execute Yantra's internal StepJob classes with **retries enabled** and a **sufficient attempt limit**.

**Yantra provides sensible defaults for this out-of-the-box:**

* **Internal Defaults:** Yantra's built-in adapter job classes (`Yantra::Worker::ActiveJob::StepJob` and `Yantra::Worker::Sidekiq::StepJob` [if available]) include internal configurations that enable backend retries with a default limit (typically **25 attempts**, aligning with Sidekiq's standard). This uses ActiveJob's `retry_on` or Sidekiq's `sidekiq_options` internally within the adapter code.
* **User Configuration:** In most scenarios, **no additional configuration is required** by the user specifically for Yantra's retry mechanism. The built-in defaults ensure the backend retry functionality is available for Yantra to leverage.

**Important Considerations:**

* **DO NOT Disable Backend Retries:** It is crucial that you **do not** globally disable retries in your job backend or specifically configure Yantra's job classes (e.g., via global ActiveJob settings or monkey-patching) with `retry: false`, `attempts: 0`, or equivalent. Disabling backend retries will prevent Yantra's retry logic from functioning as intended.
* **Sufficient Attempts:** The default backend attempt limit provided by Yantra's adapters (e.g., 25) is designed to be sufficient for most use cases. If you configure a specific Yantra step to require *more* retries than this backend limit (e.g., `retries: 30` in Yantra), the backend runner would stop retrying prematurely. This is generally an edge case; exceptionally high retry counts might warrant rethinking the step's design.
* **Potential Overrides (ActiveJob):** If using the `:active_job` adapter, be aware that global settings in your `ApplicationJob` (from which Yantra's `ActiveJob::StepJob` likely inherits) could potentially influence retry behavior. However, the specific `retry_on` configuration within Yantra's `StepJob` should generally take precedence for the errors it covers.
* **Customizing Attempts (Advanced):** If you have a specific need to change the number of backend retry attempts for Yantra jobs (e.g., increase it beyond the default 25), you may need to investigate advanced techniques like subclassing Yantra's adapter job or checking if Yantra offers configuration overrides via `config.worker_adapter_options`.

**In summary:** Yantra is designed to work with standard backend retry mechanisms enabled, and it includes internal defaults to facilitate this. Ensure you don't disable backend retries, and Yantra's retry system should work as expected.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/incorvia/yantra. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [code of conduct](link/to/your/CODE_OF_CONDUCT.md).

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Yantra project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](link/to/your/CODE_OF_CONDUCT.md).

