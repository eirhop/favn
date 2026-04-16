defmodule FavnStorageSqlite.Migrations.CreateFoundation do
  @moduledoc false

  use Ecto.Migration

  def change do
    create table(:favn_manifest_versions, primary_key: false) do
      add(:manifest_version_id, :string, primary_key: true)
      add(:content_hash, :string, null: false)
      add(:schema_version, :integer, null: false)
      add(:runner_contract_version, :integer, null: false)
      add(:serialization_format, :string, null: false)
      add(:manifest_json, :text, null: false)
      add(:inserted_at, :utc_datetime_usec)
    end

    create(unique_index(:favn_manifest_versions, [:content_hash]))

    create table(:favn_runtime_settings, primary_key: false) do
      add(:key, :string, primary_key: true)
      add(:value_text, :text)
      add(:updated_at, :utc_datetime_usec)
    end

    create table(:favn_runs, primary_key: false) do
      add(:run_id, :string, primary_key: true)
      add(:manifest_version_id, :string, null: false)
      add(:manifest_content_hash, :string, null: false)
      add(:status, :string, null: false)
      add(:event_seq, :integer, null: false)
      add(:snapshot_hash, :string, null: false)
      add(:updated_seq, :integer, null: false)
      add(:inserted_at, :utc_datetime_usec)
      add(:updated_at, :utc_datetime_usec)
      add(:run_blob, :binary, null: false)
    end

    create(index(:favn_runs, [:status, :updated_seq]))

    create table(:favn_run_events, primary_key: false) do
      add(:run_id, :string, null: false)
      add(:sequence, :integer, null: false)
      add(:occurred_at, :utc_datetime_usec)
      add(:event_blob, :binary, null: false)
    end

    create(unique_index(:favn_run_events, [:run_id, :sequence]))

    create table(:favn_scheduler_cursors, primary_key: false) do
      add(:pipeline_module, :string, null: false)
      add(:schedule_id, :string, null: false)
      add(:version, :integer, null: false)
      add(:updated_at, :utc_datetime_usec)
      add(:state_blob, :binary, null: false)
    end

    create(unique_index(:favn_scheduler_cursors, [:pipeline_module, :schedule_id]))

    create table(:favn_counters, primary_key: false) do
      add(:name, :string, primary_key: true)
      add(:value, :integer, null: false)
    end
  end
end
