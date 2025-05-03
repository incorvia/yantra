# frozen_string_literal: true

class AddPerformedAtToYantraSteps < ActiveRecord::Migration[<%= ActiveRecord::Migration.current_version %>]
  def change
    add_column :yantra_steps, :performed_at, :datetime, precision: nil, null: true

    if column_exists?(:yantra_steps, :delayed_until)
      rename_column :yantra_steps, :delayed_until, :earliest_execution_time
    end

    add_index :yantra_steps, :performed_at
  end
end
