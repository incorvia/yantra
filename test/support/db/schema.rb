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

ActiveRecord::Schema[7.2].define(version: 2025_04_16_031219) do
  create_table "yantra_step_dependencies", id: false, force: :cascade do |t|
    t.string "step_id", limit: 36, null: false
    t.string "depends_on_step_id", limit: 36, null: false
    t.index ["depends_on_step_id"], name: "idx_step_dependencies_on_prereq"
    t.index ["step_id", "depends_on_step_id"], name: "idx_step_dependencies_unique", unique: true
  end

  create_table "yantra_steps", id: { type: :string, limit: 36 }, force: :cascade do |t|
    t.string "workflow_id", limit: 36, null: false
    t.string "klass", null: false
    t.json "arguments"
    t.string "state", null: false
    t.string "queue"
    t.integer "retries", default: 0, null: false
    t.json "output"
    t.json "error"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.datetime "enqueued_at"
    t.datetime "started_at"
    t.datetime "finished_at"
    t.index ["state"], name: "index_yantra_steps_on_state"
    t.index ["workflow_id", "state"], name: "index_yantra_steps_on_workflow_id_and_state"
    t.index ["workflow_id"], name: "index_yantra_steps_on_workflow_id"
  end

  create_table "yantra_workflows", id: { type: :string, limit: 36 }, force: :cascade do |t|
    t.string "klass", null: false
    t.json "arguments"
    t.string "state", null: false
    t.json "globals"
    t.boolean "has_failures", default: false, null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.datetime "started_at"
    t.datetime "finished_at"
    t.index ["state"], name: "index_yantra_workflows_on_state"
  end
end
