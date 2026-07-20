defmodule FavnStoragePostgres.Migrations.AddAssetAttemptOverviewsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:asset_attempt_overviews, primary_key: false, prefix: @prefix) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:root_run_id, :text, null: false, primary_key: true)
      add(:run_id, :text, null: false, primary_key: true)
      add(:asset_step_id, :text, null: false, primary_key: true)
      add(:asset_ref, :text, null: false)
      add(:window_identity, :text, null: false)
      add(:window, :map)
      add(:status, :text, null: false)
      add(:stage, :integer)
      add(:attempt_number, :integer)
      add(:execution_pool, :text)
      add(:queue_reason, :text)
      add(:started_at, :timestamptz)
      add(:finished_at, :timestamptz)
      add(:duration_ms, :bigint)
      add(:error, :map)
      add(:output_metadata, :map)
      add(:source_publication_id, :bigint, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      constraint(:asset_attempt_overviews, :asset_attempt_overviews_values_valid,
        prefix: @prefix,
        check:
          "status IN ('queued', 'running', 'retrying', 'ok', 'error', 'timed_out', 'cancelled', 'skipped_fresh', 'blocked') AND (stage IS NULL OR stage >= 0) AND (attempt_number IS NULL OR attempt_number > 0) AND (duration_ms IS NULL OR duration_ms >= 0) AND octet_length(workspace_id) BETWEEN 1 AND 255 AND octet_length(root_run_id) BETWEEN 1 AND 255 AND octet_length(run_id) BETWEEN 1 AND 255 AND octet_length(asset_step_id) BETWEEN 1 AND 255 AND octet_length(asset_ref) BETWEEN 1 AND 1024 AND octet_length(window_identity) BETWEEN 1 AND 1024"
      )
    )

    create(
      constraint(:asset_attempt_overviews, :asset_attempt_overviews_identifier_lengths_v2,
        prefix: @prefix,
        check:
          "octet_length(workspace_id) BETWEEN 1 AND 255 AND octet_length(root_run_id) BETWEEN 1 AND 255 AND octet_length(run_id) BETWEEN 1 AND 255 AND octet_length(asset_step_id) BETWEEN 1 AND 255 AND octet_length(asset_ref) BETWEEN 1 AND 1024 AND octet_length(window_identity) BETWEEN 1 AND 1024"
      )
    )

    create(
      constraint(:asset_attempt_overviews, :asset_attempt_overviews_payload_bounds_v2,
        prefix: @prefix,
        check:
          ~s|("window" IS NULL OR octet_length("window"::text) <= 65536) AND (error IS NULL OR octet_length(error::text) <= 65536) AND (output_metadata IS NULL OR octet_length(output_metadata::text) <= 262144)|
      )
    )

    create(
      index(
        :asset_attempt_overviews,
        [:workspace_id, :root_run_id, :window_identity, :asset_ref, :run_id, :asset_step_id],
        prefix: @prefix,
        name: :asset_attempt_overviews_group_idx
      )
    )

    create(
      index(
        :asset_attempt_overviews,
        [:workspace_id, :run_id, :asset_step_id],
        prefix: @prefix,
        name: :asset_attempt_overviews_run_idx
      )
    )
  end

  def down do
    drop(table(:asset_attempt_overviews, prefix: @prefix))
  end
end
