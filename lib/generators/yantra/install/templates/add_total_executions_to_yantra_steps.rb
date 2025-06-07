# frozen_string_literal: true

class AddTotalExecutionsToYantraSteps < ActiveRecord::Migration[<%= ActiveRecord::Migration.current_version %>]
  def change
    # Add a new integer column to store the total number of execution attempts.
    # This counter will be incremented by the StepExecutor at the beginning of each
    # execution and should never be reset, providing a lifetime audit history.
    add_column :yantra_steps, :total_executions, :integer, default: 0, null: false
  end
end
