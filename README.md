# Yantra

[![Gem Version](https://badge.fury.io/rb/yantra.svg)](https://badge.fury.io/rb/yantra)
[![Build Status](https://github.com/incorvia/yantra/actions/workflows/ci.yml/badge.svg)](https://github.com/your_github/yantra/actions/workflows/ci.yml)
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
* Workflow and step cancellation.
* Simple data pipelining between dependent steps.
* Event-driven notifications for observability (adapters for Null, Logger, ActiveSupport::Notifications included).
* Zero external runtime gem dependencies required for the core library.

## Architecture Overview

Yantra employs a layered architecture to separate concerns (core logic, persistence, background job queuing) and allow for flexibility through adapters.

```text
+-------------------+      +---------------------+      +--------------------+
| User Application  | ---> | Yantra::Client      | ---> | Yantra Core Logic  |
| (Defines Wf/Jobs) |      | (Public API)        |      | (Orchestration, State) |
+-------------------+      +---------------------+      +--------------------+
                                                       |
                                                       | Uses Interfaces
                                                       V
+----------------------------+      +----------------------------+
| Yantra::Persistence::Repo  |      | Yantra::Worker::Adapter    |
| (Interface)                |      | (Interface)                |
+----------------------------+      +----------------------------+
  | Implemented By                    | Implemented By
  V                                   V
+----------------------------------+  +----------------------------+
| ActiveRecord Adapter | Redis Adpt*| | ActiveJob Adapter | Sidekiq Adpt*| ...
+----------------------------------+  +----------------------------+
  | Uses                              | Uses
  V                                   V
+----------+  +-------+            +-----------+  +---------+
| Postgres |  | Redis |            | ActiveJob |  | Sidekiq | ...
+----------+  +-------+            +-----------+  +---------+
```

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
* For `worker_adapter: :active_job`: Add `gem 'activejob'`.
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

1.  **Generate Migrations:**
    ```bash
    $ rails g yantra:install
    ```
    This will copy the necessary migration files into your application's `db/migrate/` directory.
2.  **Run Migrations:**
    ```bash
    $ rails db:migrate
    ```

## Usage

### Defining a Workflow

Workflows orchestrate steps. Define a workflow by inheriting from `Yantra::Workflow` and implementing a `perform` method. Use the `run` DSL method inside `perform` to define the steps and their dependencies.

```ruby
# app/workflows/order_processing_workflow.rb
class OrderProcessingWorkflow < Yantra::Workflow
  def perform(order_id:, user_id:)
    # Define steps using the `run` DSL
    # run StepClass, name: :symbolic_name (optional), params: {..}, after: [dependency_ref, ...]

    charge_step = run ChargeCreditCardStep, name: :charge, params: { order_id: order_id, amount: 100.00 }

    update_step = run UpdateInventoryStep, name: :inventory, params: { order_id: order_id }, after: charge_step

    # This step runs after charge_step succeeds
    email_step = run SendConfirmationEmailStep, name: :email, params: { order_id: order_id, user_id: user_id }, after: charge_step

    # This step runs after both inventory and email steps succeed
    run ArchiveOrderStep, name: :archive, params: { order_id: order_id }, after: [update_step, email_step]
  end
end
```

* `run StepClass`: Specifies the `Yantra::Step` subclass to execute.
* `name:`: An optional symbolic name for the step within the workflow definition (used for dependencies). If omitted, a default name is generated.
* `params:`: A hash of arguments passed to the step's `perform` method. Must be JSON-serializable.
* `after:`: Specifies dependencies. Can be a single step reference variable (like `charge_step`) or an array of references (`[update_step, email_step]`). A step only runs after all its dependencies have successfully completed.

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

```ruby
# Find workflow status
workflow = Yantra::Client.find_workflow(workflow_id)
puts "Workflow state: #{workflow&.state}"
puts "Workflow has failures: #{workflow&.has_failures}"

# Find a specific step
step = Yantra::Client.find_step(step_id)
puts "Step state: #{step&.state}"
# Access output/error (may be string or hash depending on persistence/serialization)
puts "Step output: #{step&.output.inspect}"
puts "Step error: #{step&.error.inspect}" # Use .inspect for better hash/string visibility

# Get all steps for a workflow
steps = Yantra::Client.get_workflow_steps(workflow_id)

# Get steps with a specific status
failed_steps = Yantra::Client.get_workflow_steps(workflow_id, status: :failed)

# Cancel a running or pending workflow
Yantra::Client.cancel_workflow(workflow_id)

# Retry all failed steps in a failed workflow
# (Resets workflow state to running, re-enqueues failed steps)
Yantra::Client.retry_failed_steps(workflow_id)
```

## Events / Observability

Yantra publishes events at key lifecycle points using the configured `notification_adapter`.

**Key Events:**

* `yantra.workflow.started`
* `yantra.workflow.succeeded`
* `yantra.workflow.failed`
* `yantra.workflow.cancelled`
* `yantra.step.enqueued`
* `yantra.step.started`
* `yantra.step.succeeded`
* `yantra.step.failed` (Published on permanent failure after retries)
* `yantra.step.cancelled`

**Payloads:** Event payloads are hashes containing relevant IDs, class names, state, timestamps, and potentially output or error information.

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

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/your_github/yantra. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [code of conduct](link/to/your/CODE_OF_CONDUCT.md).

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Yantra project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](link/to/your/CODE_OF_CONDUCT.md).

