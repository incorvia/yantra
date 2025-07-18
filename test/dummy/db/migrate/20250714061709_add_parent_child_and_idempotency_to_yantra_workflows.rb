class AddParentChildAndIdempotencyToYantraWorkflows < ActiveRecord::Migration[7.2]
  def change
    # Add parent-child relationship support
    add_column :yantra_workflows, :parent_workflow_id, :string, limit: 36, null: true
    
    # Add idempotency key support
    add_column :yantra_workflows, :idempotency_key, :string, null: true
    
    # Add indexes for efficient querying
    add_index :yantra_workflows, :parent_workflow_id, name: 'index_yantra_workflows_on_parent_workflow_id'
    add_index :yantra_workflows, :idempotency_key, name: 'index_yantra_workflows_on_idempotency_key', unique: true
    
    # Add composite index for finding existing workflows by parent and idempotency key
    add_index :yantra_workflows, [:parent_workflow_id, :idempotency_key], 
              name: 'index_yantra_workflows_on_parent_and_idempotency_key'
  end
end 