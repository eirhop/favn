defmodule Favn.Storage.SQLite.Migrations.CreateRuns do
  @moduledoc false
  use Ecto.Migration

  def change do
    create table(:runs, primary_key: false) do
      add(:id, :text, primary_key: true)
      add(:status, :text, null: false)
      add(:started_at, :utc_datetime)
      add(:finished_at, :utc_datetime)
      add(:inserted_at_us, :bigint, null: false)
      add(:updated_at_us, :bigint, null: false)
      add(:updated_seq, :bigint, null: false)
      add(:run_blob, :binary, null: false)
    end

    create(index(:runs, [:status]))
    create(index(:runs, [:updated_seq, :updated_at_us, :id]))

    create table(:favn_counters, primary_key: false) do
      add(:name, :text, primary_key: true)
      add(:value, :bigint, null: false)
    end

    create table(:run_node_results, primary_key: false) do
      add(:run_id, references(:runs, column: :id, type: :text, on_delete: :delete_all),
        null: false
      )

      add(:ref_module, :text, null: false)
      add(:ref_name, :text, null: false)
      add(:window_key, :text, null: false)
      add(:status, :text, null: false)
      add(:started_at, :utc_datetime)
      add(:finished_at, :utc_datetime)
      add(:attempt_count, :integer, null: false)
      add(:max_attempts, :integer, null: false)
      add(:result_blob, :binary, null: false)
    end

    create(index(:run_node_results, [:run_id]))
    create(unique_index(:run_node_results, [:run_id, :ref_module, :ref_name, :window_key]))

    create table(:window_latest_results, primary_key: false) do
      add(:ref_module, :text, null: false)
      add(:ref_name, :text, null: false)
      add(:window_key, :text, null: false)
      add(:status, :text, null: false)
      add(:last_run_id, :text, null: false)
      add(:finished_at, :utc_datetime)
      add(:updated_at_us, :bigint, null: false)
    end

    create(unique_index(:window_latest_results, [:ref_module, :ref_name, :window_key]))

    create table(:scheduler_states, primary_key: false) do
      add(:pipeline_module, :text, primary_key: true)
      add(:schedule_id, :text, primary_key: true)
      add(:schedule_fingerprint, :text)
      add(:last_evaluated_at, :utc_datetime)
      add(:last_due_at, :utc_datetime)
      add(:last_submitted_due_at, :utc_datetime)
      add(:in_flight_run_id, :text)
      add(:queued_due_at, :utc_datetime)
      add(:updated_at, :utc_datetime)
    end

    execute("""
    INSERT INTO favn_counters (name, value)
    VALUES ('run_write_order', 0)
    ON CONFLICT(name) DO NOTHING
    """)
  end
end
