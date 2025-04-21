# frozen_string_literal: true

class CreateYantraSteps < ActiveRecord::Migration[<%= ActiveRecord::Migration.current_version %>]
  def change
    create_table :yantra_steps, id: false do |t|
      t.string :id, limit: 36, primary_key: true, null: false
      t.string :workflow_id, limit: 36, null: false
      t.string :klass, null: false
      t.jsonb :arguments
      t.string :state, null: false
      t.string :queue
      t.integer :retries, default: 0, null: false
      t.integer :max_attempts, default: 3, null: false
      t.jsonb :output
      t.jsonb :error
      t.timestamp :created_at, precision: nil, null: false
      t.timestamp :updated_at, precision: nil, null: false
      t.timestamp :enqueued_at, precision: nil
      t.timestamp :started_at, precision: nil
      t.timestamp :finished_at, precision: nil
    end

    add_foreign_key :yantra_steps, :yantra_workflows, column: :workflow_id, primary_key: :id, on_delete: :cascade

    add_index :yantra_steps, [:workflow_id, :state], name: 'index_yantra_steps_on_workflow_id_and_state'
    add_index :yantra_steps, [:state, :updated_at], name: 'index_yantra_steps_on_state_and_updated_at'
    add_index :yantra_steps, [:klass, :state], name: 'index_yantra_steps_on_klass_and_state'
    # add_index :yantra_steps, :queue, name: 'index_yantra_steps_on_queue' # Optional
  end
end

