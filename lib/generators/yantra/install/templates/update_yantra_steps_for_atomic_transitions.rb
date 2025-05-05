# frozen_string_literal: true

class UpdateYantraStepsForAtomicTransitions < ActiveRecord::Migration[<%= ActiveRecord::Migration.current_version %>]
  def change
    # Remove the old execution time delay column (no longer used)
    remove_column :yantra_steps, :earliest_execution_time, :datetime

    # Add a new column used to mark steps during atomic bulk transitions
    add_column :yantra_steps, :transition_batch_token, :string, null: true

    # Index the token for fast lookup after update
    add_index :yantra_steps, :transition_batch_token, name: 'index_yantra_steps_on_transition_batch_token'
  end
end

