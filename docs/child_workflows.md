# Child Workflows

Yantra supports parent-child workflow relationships, enabling you to create hierarchical workflow structures with idempotency support. This is particularly useful for batch processing scenarios where you need to process large datasets while maintaining the ability to retry safely.

## Overview

Child workflows provide:
- **Parent-child relationships**: Link workflows in a hierarchical structure
- **Idempotency**: Prevent duplicate processing using unique keys
- **Batch processing**: Process large datasets efficiently
- **Observability**: Track parent and child workflow relationships

## Database Schema

The `yantra_workflows` table includes two new columns:
- `parent_workflow_id`: References the parent workflow (nullable)
- `idempotency_key`: Unique key for idempotent operations (nullable, unique)

## Creating Child Workflows

### Basic Usage

```ruby
# Create a parent workflow
parent_id = Yantra.create_workflow(ProcessBatchWorkflow, batch_id: 123)

# Create a child workflow
child_id = Yantra.create_workflow(
  ProcessIndividualItemWorkflow,
  parent_workflow_id: parent_id,
  idempotency_key: "item-456-ABC123",
  item_id: 456,
  sku: "ABC123"
)
```

### With Idempotency

```ruby
# Generate a unique key for idempotency
idempotency_key = "meal-short-#{order_id}-#{sku}"

# Create child workflow with idempotency
child_id = Yantra.create_workflow(
  ProcessMealShortWorkflow,
  parent_workflow_id: parent_workflow_id,
  idempotency_key: idempotency_key,
  order_id: order_id,
  sku: sku
)
```

## Finding Child Workflows

### Get All Children of a Parent

```ruby
# Find all child workflows for a parent
child_workflows = Yantra.find_child_workflows(parent_workflow_id)

child_workflows.each do |child|
  puts "Child ID: #{child.id}, State: #{child.state}"
end
```

### Check for Existing Idempotency Keys

```ruby
# Generate potential keys from your data
potential_keys = csv_rows.map { |row| "meal-short-#{row[:order_id]}-#{row[:sku]}" }

# Find which keys already exist
existing_keys = Yantra.find_existing_idempotency_keys(parent_workflow_id, potential_keys)

# Use a Set for efficient lookups
existing_keys_set = Set.new(existing_keys)

# Only create workflows for new keys
csv_rows.each do |row|
  key = "meal-short-#{row[:order_id]}-#{row[:sku]}"
  
  unless existing_keys_set.include?(key)
    Yantra.create_workflow(
      ProcessMealShortWorkflow,
      parent_workflow_id: parent_workflow_id,
      idempotency_key: key,
      **row
    )
  end
end
```

## ActiveRecord Associations

The `WorkflowRecord` model includes associations for easy querying:

```ruby
# Find parent workflow
parent = WorkflowRecord.find(parent_id)

# Get all children
children = parent.child_workflows

# Find child workflow
child = WorkflowRecord.find(child_id)

# Get parent
parent = child.parent_workflow
```

## Example: Batch CSV Processing

Here's a complete example of how to use child workflows for batch CSV processing:

```ruby
class ProcessMealShortsCsvWorkflow < Yantra::Workflow
  def perform(s3_url:)
    # This parent workflow has only one step
    run CreateChildWorkflowsFromCsvStep, 
        name: :process_csv, 
        params: { 
          s3_url: s3_url,
          parent_workflow_id: id, # Use this workflow's ID as parent
          child_workflow_class: ProcessMealShortWorkflow,
          idempotency_key_generator: ->(row) { "meal-short-#{row[:order_id]}-#{row[:sku]}" }
        }
  end
end

class ProcessMealShortWorkflow < Yantra::Workflow
  def perform(order_id:, sku:)
    # This child workflow processes a single meal short
    run ValidateOrderStep, params: { order_id: order_id }
    run ProcessMealShortStep, params: { order_id: order_id, sku: sku }, after: :validate_order
    run SendConfirmationStep, params: { order_id: order_id }, after: :process_meal_short
  end
end

# Usage
parent_id = Yantra.create_workflow(
  ProcessMealShortsCsvWorkflow, 
  s3_url: "https://s3.amazonaws.com/bucket/meal-shorts.csv"
)

Yantra.start_workflow(parent_id)
```

## Benefits

### 1. True Replayability
Parent workflows can be retried safely. On re-run, they will automatically skip already-created children and only process failed or new rows.

### 2. Enhanced Observability
You can build admin views that show the relationship between parent and child workflows, making it easy to see which rows succeeded or failed.

### 3. Atomic Granularity
Each row gets its own isolated child workflow with its own complex, multi-step process and perfect observability into its state.

### 4. Efficient Processing
The `find_existing_idempotency_keys` method allows you to check for existing workflows in a single database query, making batch processing efficient.

## Migration

To add child workflow support to your existing Yantra installation, run:

```bash
rails generate yantra:install:add_parent_child_and_idempotency_to_yantra_workflows
rails db:migrate
```

This will add the necessary database columns and indexes for parent-child relationships and idempotency keys.

## Best Practices

1. **Use descriptive idempotency keys**: Make keys unique and meaningful (e.g., `"meal-short-123-ABC456"`)

2. **Handle errors gracefully**: When creating child workflows in batches, continue processing other rows even if some fail

3. **Monitor parent-child relationships**: Use the provided methods to track the relationship between parent and child workflows

4. **Consider batch sizes**: For very large datasets, consider processing in smaller batches to avoid memory issues

5. **Use appropriate indexes**: The migration includes indexes for efficient querying of parent-child relationships 