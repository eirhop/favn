defmodule Favn.Storage.Postgres.Migrations.CreateFoundation do
  @moduledoc false
  use Ecto.Migration

  def up do
    execute("CREATE SEQUENCE IF NOT EXISTS favn_run_write_seq")

    create table(:favn_runs, primary_key: false) do
      add(:id, :text, primary_key: true)
      add(:status, :text, null: false)
      add(:submit_kind, :text, null: false)
      add(:replay_mode, :text, null: false)
      add(:event_seq, :bigint, null: false)
      add(:write_seq, :bigint, null: false)
      add(:started_at, :utc_datetime_usec, null: false)
      add(:finished_at, :utc_datetime_usec)
      add(:max_concurrency, :integer, null: false)
      add(:timeout_ms, :integer)
      add(:rerun_of_run_id, :text)
      add(:parent_run_id, :text)
      add(:root_run_id, :text)
      add(:lineage_depth, :integer, null: false, default: 0)
      add(:target_refs_json, :map, null: false)
      add(:submit_ref_json, :map)
      add(:params_json, :map, null: false)
      add(:retry_policy_json, :map, null: false)
      add(:pipeline_json, :map)
      add(:pipeline_context_json, :map)
      add(:plan_json, :map)
      add(:backfill_json, :map)
      add(:operator_reason_json, :map)
      add(:error_json, :map)
      add(:terminal_reason_json, :map)
      add(:snapshot_version, :integer, null: false)
      add(:snapshot_hash, :text, null: false)
      add(:snapshot_json, :map, null: false)
      timestamps(type: :utc_datetime_usec)
    end

    execute(
      "ALTER TABLE favn_runs ADD CONSTRAINT favn_runs_status_check CHECK (status IN ('running', 'ok', 'error', 'cancelled', 'timed_out'))"
    )

    execute(
      "ALTER TABLE favn_runs ADD CONSTRAINT favn_runs_submit_kind_check CHECK (submit_kind IN ('asset', 'pipeline', 'backfill_asset', 'backfill_pipeline', 'rerun'))"
    )

    execute(
      "ALTER TABLE favn_runs ADD CONSTRAINT favn_runs_replay_mode_check CHECK (replay_mode IN ('none', 'resume_from_failure', 'exact_replay'))"
    )

    create(unique_index(:favn_runs, [:write_seq]))
    create(index(:favn_runs, [:status, :write_seq]))
    create(index(:favn_runs, [:root_run_id, :write_seq]))
    create(index(:favn_runs, [:parent_run_id, :write_seq]))
    create(index(:favn_runs, [:rerun_of_run_id, :write_seq]))
    create(index(:favn_runs, [:started_at]))
    create(index(:favn_runs, [:finished_at]))

    create table(:favn_run_nodes) do
      add(:run_id, references(:favn_runs, column: :id, type: :text, on_delete: :delete_all),
        null: false
      )

      add(:ref_module, :text, null: false)
      add(:ref_name, :text, null: false)
      add(:window_key_text, :text, null: false)
      add(:window_key_json, :map)
      add(:stage, :integer, null: false)
      add(:status, :text, null: false)
      add(:attempt_count, :integer, null: false)
      add(:max_attempts, :integer, null: false)
      add(:next_retry_at, :utc_datetime_usec)
      add(:started_at, :utc_datetime_usec)
      add(:finished_at, :utc_datetime_usec)
      add(:duration_ms, :bigint)
      add(:meta_json, :map, null: false)
      add(:error_json, :map)
      add(:attempts_json, :map, null: false)
      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:favn_run_nodes, [:run_id, :ref_module, :ref_name, :window_key_text]))
    create(index(:favn_run_nodes, [:run_id, :stage, :ref_module, :ref_name, :window_key_text]))
    create(index(:favn_run_nodes, [:ref_module, :ref_name, :window_key_text, :finished_at]))
    create(index(:favn_run_nodes, [:status, :finished_at]))

    execute(
      "CREATE INDEX favn_run_nodes_success_idx ON favn_run_nodes (ref_module, ref_name, window_key_text, finished_at DESC) WHERE status = 'ok'"
    )

    create table(:favn_asset_window_latest, primary_key: false) do
      add(:ref_module, :text, primary_key: true)
      add(:ref_name, :text, primary_key: true)
      add(:window_key_text, :text, primary_key: true)
      add(:window_key_json, :map)

      add(:last_run_id, references(:favn_runs, column: :id, type: :text, on_delete: :nothing),
        null: false
      )

      add(:last_run_node_id, references(:favn_run_nodes, on_delete: :nothing), null: false)
      add(:last_finished_at, :utc_datetime_usec, null: false)
      add(:last_write_seq, :bigint, null: false)
      add(:updated_at, :utc_datetime_usec, null: false)
    end

    create(index(:favn_asset_window_latest, [:last_finished_at]))
    create(index(:favn_asset_window_latest, [:last_run_id]))

    create table(:favn_scheduler_cursors, primary_key: false) do
      add(:pipeline_module, :text, primary_key: true)
      add(:schedule_id, :text)
      add(:schedule_fingerprint, :text)
      add(:last_evaluated_at, :utc_datetime_usec)
      add(:last_due_at, :utc_datetime_usec)
      add(:last_submitted_due_at, :utc_datetime_usec)
      add(:in_flight_run_id, :text)
      add(:queued_due_at, :utc_datetime_usec)
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :utc_datetime_usec)
    end

    create(index(:favn_scheduler_cursors, [:schedule_fingerprint]))
    create(index(:favn_scheduler_cursors, [:in_flight_run_id]))
  end

  def down do
    drop(table(:favn_scheduler_cursors))
    drop(table(:favn_asset_window_latest))
    drop(table(:favn_run_nodes))
    drop(table(:favn_runs))
    execute("DROP SEQUENCE IF EXISTS favn_run_write_seq")
  end
end
