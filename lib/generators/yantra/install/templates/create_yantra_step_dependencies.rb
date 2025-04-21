# frozen_string_literal: true

class CreateYantraStepDependencies < ActiveRecord::Migration[<%= ActiveRecord::Migration.current_version %>]
  def change
    create_table :yantra_step_dependencies, id: false do |t|
      t.string :step_id, limit: 36, null: false
      t.string :depends_on_step_id, limit: 36, null: false
    end

    add_index :yantra_step_dependencies, [:step_id, :depends_on_step_id], unique: true, name: 'index_yantra_step_dependencies_on_step_and_depends_on'
    add_index :yantra_step_dependencies, [:depends_on_step_id], name: 'index_yantra_step_dependencies_on_depends_on_step_id'

    add_foreign_key :yantra_step_dependencies, :yantra_steps, column: :step_id, primary_key: :id, on_delete: :cascade
    add_foreign_key :yantra_step_dependencies, :yantra_steps, column: :depends_on_step_id, primary_key: :id, on_delete: :cascade
  end
end

