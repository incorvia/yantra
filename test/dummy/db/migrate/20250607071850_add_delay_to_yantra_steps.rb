# db/migrate/YYYYMMDDHHMMSS_add_delay_to_yantra_steps.rb
# Note: Replace YYYYMMDDHHMMSS with the actual timestamp when generating.
# frozen_string_literal: true

# This migration adds columns to store the optional execution delay
# and the calculated time until which a delayed step should wait.
class AddDelayToYantraSteps < ActiveRecord::Migration[7.2]
  def change
    # Add an integer column to store the user-specified delay in seconds.
    # This preserves the original delay requested in the workflow definition.
    # Allowing NULL signifies no delay was specified.
    add_column :yantra_steps, :delay_seconds, :integer, null: true

    # Add a datetime column to store the calculated timestamp until which
    # the step should be delayed. This is set when the step is handed off
    # to the worker adapter with a delay. NULL indicates the step was
    # enqueued immediately or hasn't been processed for delayed enqueue yet.
    add_column :yantra_steps, :delayed_until, :datetime, precision: nil, null: true

    # Optional: Add an index on delayed_until if you need to efficiently query
    # for steps scheduled to run within a specific time window.
    # add_index :yantra_steps, :delayed_until
  end
end
