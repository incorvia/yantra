# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[7.2].define(version: 2025_06_07_071946) do
  # These are extensions that must be enabled in order to support this database
  enable_extension "plpgsql"

  create_table "yantra_step_dependencies", id: false, force: :cascade do |t|
    t.string "step_id", limit: 36, null: false
    t.string "depends_on_step_id", limit: 36, null: false
    t.index ["depends_on_step_id"], name: "index_yantra_step_dependencies_on_depends_on_step_id"
    t.index ["step_id", "depends_on_step_id"], name: "index_yantra_step_dependencies_on_step_and_depends_on", unique: true
  end

  create_table "yantra_steps", id: { type: :string, limit: 36 }, force: :cascade do |t|
    t.string "workflow_id", limit: 36, null: false
    t.string "klass", null: false
    t.jsonb "arguments"
    t.string "state", null: false
    t.string "queue"
    t.integer "retries", default: 0, null: false
    t.integer "max_attempts", default: 3, null: false
    t.jsonb "output"
    t.jsonb "error"
    t.datetime "created_at", precision: nil, null: false
    t.datetime "updated_at", precision: nil, null: false
    t.datetime "enqueued_at", precision: nil
    t.datetime "started_at", precision: nil
    t.datetime "finished_at", precision: nil
    t.integer "delay_seconds"
    t.datetime "performed_at", precision: nil
    t.string "transition_batch_token"
    t.integer "total_executions", default: 0, null: false
    t.index ["klass", "state"], name: "index_yantra_steps_on_klass_and_state"
    t.index ["performed_at"], name: "index_yantra_steps_on_performed_at"
    t.index ["state", "updated_at"], name: "index_yantra_steps_on_state_and_updated_at"
    t.index ["transition_batch_token"], name: "index_yantra_steps_on_transition_batch_token"
    t.index ["workflow_id", "state"], name: "index_yantra_steps_on_workflow_id_and_state"
  end

  create_table "yantra_workflows", id: { type: :string, limit: 36 }, force: :cascade do |t|
    t.string "klass", null: false
    t.jsonb "arguments"
    t.string "state", null: false
    t.jsonb "globals"
    t.boolean "has_failures", default: false, null: false
    t.datetime "created_at", precision: nil, null: false
    t.datetime "updated_at", precision: nil, null: false
    t.datetime "started_at", precision: nil
    t.datetime "finished_at", precision: nil
    t.index ["finished_at"], name: "index_yantra_workflows_on_finished_at"
    t.index ["has_failures"], name: "index_yantra_workflows_on_has_failures"
    t.index ["klass", "state"], name: "index_yantra_workflows_on_klass_and_state"
    t.index ["started_at"], name: "index_yantra_workflows_on_started_at"
    t.index ["state"], name: "index_yantra_workflows_on_state"
  end

  add_foreign_key "yantra_step_dependencies", "yantra_steps", column: "depends_on_step_id", on_delete: :cascade
  add_foreign_key "yantra_step_dependencies", "yantra_steps", column: "step_id", on_delete: :cascade
  add_foreign_key "yantra_steps", "yantra_workflows", column: "workflow_id", on_delete: :cascade
end
