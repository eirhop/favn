defmodule FavnStorageSqlite.Migrations.RebuildBackfillReadModelsForDtos do
  @moduledoc false

  use Ecto.Migration

  def up do
    drop_if_exists(table(:favn_asset_window_states))
    drop_if_exists(table(:favn_backfill_windows))
    drop_if_exists(table(:favn_pipeline_coverage_baselines))

    create table(:favn_pipeline_coverage_baselines, primary_key: false) do
      add(:baseline_id, :string, primary_key: true)
      add(:pipeline_module, :string, null: false)
      add(:source_key, :string, null: false)
      add(:segment_key_hash, :string, null: false)
      add(:segment_key_redacted, :string)
      add(:window_kind, :string, null: false)
      add(:timezone, :string, null: false)
      add(:coverage_start_at, :text)
      add(:coverage_until, :text, null: false)
      add(:created_by_run_id, :string, null: false)
      add(:manifest_version_id, :string, null: false)
      add(:status, :string, null: false)
      add(:record_payload, :text, null: false)
      add(:created_at, :text, null: false)
      add(:updated_at, :text, null: false)
    end

    create(index(:favn_pipeline_coverage_baselines, [:pipeline_module, :status]))
    create(index(:favn_pipeline_coverage_baselines, [:source_key, :segment_key_hash]))

    create table(:favn_backfill_windows, primary_key: false) do
      add(:backfill_run_id, :string, null: false)
      add(:child_run_id, :string)
      add(:pipeline_module, :string, null: false)
      add(:manifest_version_id, :string, null: false)
      add(:coverage_baseline_id, :string)
      add(:window_kind, :string, null: false)
      add(:window_start_at, :text, null: false)
      add(:window_end_at, :text, null: false)
      add(:timezone, :string, null: false)
      add(:window_key, :string, null: false)
      add(:status, :string, null: false)
      add(:attempt_count, :integer, null: false)
      add(:latest_attempt_run_id, :string)
      add(:last_success_run_id, :string)
      add(:record_payload, :text, null: false)
      add(:started_at, :text)
      add(:finished_at, :text)
      add(:created_at, :text)
      add(:updated_at, :text, null: false)
    end

    create(
      unique_index(:favn_backfill_windows, [:backfill_run_id, :pipeline_module, :window_key])
    )

    create(index(:favn_backfill_windows, [:pipeline_module, :window_key]))
    create(index(:favn_backfill_windows, [:status, :updated_at]))

    create table(:favn_asset_window_states, primary_key: false) do
      add(:asset_ref_module, :string, null: false)
      add(:asset_ref_name, :string, null: false)
      add(:pipeline_module, :string, null: false)
      add(:manifest_version_id, :string, null: false)
      add(:window_kind, :string, null: false)
      add(:window_start_at, :text, null: false)
      add(:window_end_at, :text, null: false)
      add(:timezone, :string, null: false)
      add(:window_key, :string, null: false)
      add(:status, :string, null: false)
      add(:latest_run_id, :string, null: false)
      add(:latest_parent_run_id, :string)
      add(:latest_success_run_id, :string)
      add(:rows_written, :integer)
      add(:record_payload, :text, null: false)
      add(:updated_at, :text, null: false)
    end

    create(
      unique_index(:favn_asset_window_states, [:asset_ref_module, :asset_ref_name, :window_key])
    )

    create(index(:favn_asset_window_states, [:pipeline_module, :window_key]))
    create(index(:favn_asset_window_states, [:status, :updated_at]))
  end

  def down do
    drop_if_exists(table(:favn_asset_window_states))
    drop_if_exists(table(:favn_backfill_windows))
    drop_if_exists(table(:favn_pipeline_coverage_baselines))
  end
end
