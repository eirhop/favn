defmodule FavnStoragePostgres.Migrations.OptimizeRunFlowReadsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"
  @disable_ddl_transaction true
  @disable_migration_lock true

  def up do
    alter table(:asset_attempt_overviews, prefix: @prefix) do
      add(:target_id, :text)
      add(:window_kind, :text)
      add(:window_start_at, :timestamptz)
      add(:window_end_at, :timestamptz)
      add(:window_timezone, :text)
      add(:failure_summary, :text)
    end

    drop(
      constraint(:asset_attempt_overviews, :asset_attempt_overviews_values_valid, prefix: @prefix)
    )

    create(
      constraint(:asset_attempt_overviews, :asset_attempt_overviews_values_valid,
        prefix: @prefix,
        validate: false,
        check:
          "status IN ('planned', 'queued', 'running', 'retrying', 'ok', 'error', 'timed_out', 'cancelled', 'skipped_fresh', 'blocked') AND (stage IS NULL OR stage >= 0) AND (attempt_number IS NULL OR attempt_number > 0) AND (duration_ms IS NULL OR duration_ms >= 0) AND (target_id IS NULL OR octet_length(target_id) BETWEEN 1 AND 255) AND (window_kind IS NULL OR window_kind IN ('hour', 'day', 'month', 'year')) AND (window_timezone IS NULL OR octet_length(window_timezone) BETWEEN 1 AND 255) AND (failure_summary IS NULL OR octet_length(failure_summary) <= 1024) AND octet_length(workspace_id) BETWEEN 1 AND 255 AND octet_length(root_run_id) BETWEEN 1 AND 255 AND octet_length(run_id) BETWEEN 1 AND 255 AND octet_length(asset_step_id) BETWEEN 1 AND 255 AND octet_length(asset_ref) BETWEEN 1 AND 1024 AND octet_length(window_identity) BETWEEN 1 AND 1024"
      )
    )

    create(
      index(
        :asset_attempt_overviews,
        ["workspace_id", "run_id", ~s|asset_ref COLLATE "C"|, ~s|asset_step_id COLLATE "C"|],
        prefix: @prefix,
        name: :asset_attempt_overviews_flow_page_idx,
        concurrently: true
      )
    )

    create(
      index(
        :asset_attempt_overviews,
        [:workspace_id, :run_id, :status],
        prefix: @prefix,
        name: :asset_attempt_overviews_flow_counts_idx,
        concurrently: true
      )
    )

    create(
      index(
        :asset_attempt_overviews,
        [:workspace_id, :run_id, :asset_step_id, :source_publication_id],
        prefix: @prefix,
        name: :asset_attempt_overviews_flow_delta_idx,
        concurrently: true
      )
    )

    execute(
      "ALTER TABLE #{@prefix}.asset_attempt_overviews VALIDATE CONSTRAINT asset_attempt_overviews_values_valid"
    )
  end

  def down do
    drop(
      index(:asset_attempt_overviews,
        prefix: @prefix,
        name: :asset_attempt_overviews_flow_delta_idx,
        concurrently: true
      )
    )

    drop(
      index(:asset_attempt_overviews,
        prefix: @prefix,
        name: :asset_attempt_overviews_flow_counts_idx,
        concurrently: true
      )
    )

    drop(
      index(:asset_attempt_overviews,
        prefix: @prefix,
        name: :asset_attempt_overviews_flow_page_idx,
        concurrently: true
      )
    )

    drop(
      constraint(:asset_attempt_overviews, :asset_attempt_overviews_values_valid, prefix: @prefix)
    )

    alter table(:asset_attempt_overviews, prefix: @prefix) do
      remove(:failure_summary)
      remove(:window_timezone)
      remove(:window_end_at)
      remove(:window_start_at)
      remove(:window_kind)
      remove(:target_id)
    end

    create(
      constraint(:asset_attempt_overviews, :asset_attempt_overviews_values_valid,
        prefix: @prefix,
        check:
          "status IN ('queued', 'running', 'retrying', 'ok', 'error', 'timed_out', 'cancelled', 'skipped_fresh', 'blocked') AND (stage IS NULL OR stage >= 0) AND (attempt_number IS NULL OR attempt_number > 0) AND (duration_ms IS NULL OR duration_ms >= 0) AND octet_length(workspace_id) BETWEEN 1 AND 255 AND octet_length(root_run_id) BETWEEN 1 AND 255 AND octet_length(run_id) BETWEEN 1 AND 255 AND octet_length(asset_step_id) BETWEEN 1 AND 255 AND octet_length(asset_ref) BETWEEN 1 AND 1024 AND octet_length(window_identity) BETWEEN 1 AND 1024"
      )
    )
  end
end
