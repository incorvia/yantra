# frozen_string_literal: true

class AddPerformedAtToYantraSteps < ActiveRecord::Migration[<%= ActiveRecord::Migration.current_version %>]
  def change
    add_column :yantra_steps, :performed_at, :datetime, precision: nil, null: true

    # Optional: Index might be useful for querying steps that performed but haven't finished
    # add_index :yantra_steps, :performed_at
  end
end
