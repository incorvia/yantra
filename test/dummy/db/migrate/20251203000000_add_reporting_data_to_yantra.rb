class AddReportingDataToYantra < ActiveRecord::Migration[7.2]
  def change
    add_column :yantra_workflows, :reporting_data, :jsonb, default: {}
    add_column :yantra_steps, :reporting_data, :jsonb, default: {}

    add_index :yantra_workflows, :reporting_data, using: :gin
    add_index :yantra_steps, :reporting_data, using: :gin
  end
end
