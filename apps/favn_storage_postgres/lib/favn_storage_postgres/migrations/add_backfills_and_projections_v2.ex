defmodule FavnStoragePostgres.Migrations.AddBackfillsAndProjectionsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:coverage_baselines, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:baseline_id, :text, null: false, primary_key: true)
      add(:deployment_id, :text, null: false)
      add(:manifest_version_id, :text, null: false)
      add(:target_kind, :text, null: false)
      add(:target_id, :text, null: false)
      add(:coverage_start, :timestamptz, null: false)
      add(:coverage_end, :timestamptz, null: false)
      add(:evidence, :map, null: false)
      add(:evidence_hash, :binary, null: false)
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.coverage_baselines
    ADD CONSTRAINT coverage_baselines_target_fk
    FOREIGN KEY (workspace_id, deployment_id, target_kind, target_id)
    REFERENCES #{@prefix}.workspace_deployment_targets(workspace_id, deployment_id, target_kind, target_id)
    ON DELETE RESTRICT
    """)

    create(
      index(
        :coverage_baselines,
        [:workspace_id, :manifest_version_id, :target_kind, :target_id, {:desc, :coverage_end}],
        prefix: @prefix,
        name: :coverage_baselines_target_idx
      )
    )

    create(
      constraint(:coverage_baselines, :coverage_baselines_values_valid,
        prefix: @prefix,
        check:
          "target_kind IN ('asset', 'pipeline') AND coverage_start < coverage_end AND version > 0"
      )
    )

    create table(:backfills, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:backfill_id, :text, null: false, primary_key: true)
      add(:root_run_id, :text, null: false)
      add(:start_command_id, :text, null: false)
      add(:last_command_id, :text)
      add(:request_hash, :binary, null: false)
      add(:deployment_id, :text, null: false)
      add(:manifest_version_id, :text, null: false)
      add(:target_kind, :text, null: false)
      add(:target_id, :text, null: false)
      add(:range_start, :timestamptz, null: false)
      add(:range_end, :timestamptz, null: false)
      add(:status, :text, null: false, default: "planning")
      add(:expected_window_count, :integer, null: false)
      add(:expected_batch_count, :integer, null: false)
      add(:appended_window_count, :integer, null: false, default: 0)
      add(:appended_batch_count, :integer, null: false, default: 0)
      add(:plan_hash, :binary, null: false)
      add(:metadata, :map, null: false)
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.backfills
    ADD CONSTRAINT backfills_deployment_manifest_fk
    FOREIGN KEY (workspace_id, deployment_id, manifest_version_id)
    REFERENCES #{@prefix}.workspace_deployments(workspace_id, deployment_id, manifest_version_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.backfills
    ADD CONSTRAINT backfills_root_run_fk
    FOREIGN KEY (workspace_id, root_run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.backfills
    ADD CONSTRAINT backfills_target_fk
    FOREIGN KEY (workspace_id, deployment_id, target_kind, target_id)
    REFERENCES #{@prefix}.workspace_deployment_targets(workspace_id, deployment_id, target_kind, target_id)
    ON DELETE RESTRICT
    """)

    create(
      unique_index(:backfills, [:workspace_id, :start_command_id],
        prefix: @prefix,
        name: :backfills_start_command_uidx
      )
    )

    create(
      index(
        :backfills,
        [:workspace_id, :manifest_version_id, :target_kind, :target_id, {:desc, :inserted_at}],
        prefix: @prefix,
        name: :backfills_target_idx
      )
    )

    create(
      index(:backfills, [:workspace_id, :root_run_id, :backfill_id],
        prefix: @prefix,
        name: :backfills_root_run_idx
      )
    )

    create(
      constraint(:backfills, :backfills_values_valid,
        prefix: @prefix,
        check:
          "target_kind IN ('asset', 'pipeline') AND range_start < range_end " <>
            "AND status IN ('planning', 'ready', 'running', 'completed', 'failed', 'cancelled') " <>
            "AND expected_window_count BETWEEN 0 AND 100000 " <>
            "AND expected_batch_count BETWEEN 0 AND 1000 " <>
            "AND ((expected_window_count = 0 AND expected_batch_count = 0) OR " <>
            "(expected_window_count > 0 AND expected_batch_count > 0 " <>
            "AND expected_batch_count <= expected_window_count " <>
            "AND expected_window_count <= expected_batch_count * 500)) " <>
            "AND appended_window_count BETWEEN 0 AND expected_window_count " <>
            "AND appended_batch_count BETWEEN 0 AND expected_batch_count AND version > 0"
      )
    )

    create table(:backfill_plan_batches, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:backfill_id, :text, null: false, primary_key: true)
      add(:batch_index, :integer, null: false, primary_key: true)
      add(:command_id, :text, null: false)
      add(:batch_hash, :binary, null: false)
      add(:window_count, :integer, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.backfill_plan_batches
    ADD CONSTRAINT backfill_plan_batches_backfill_fk
    FOREIGN KEY (workspace_id, backfill_id)
    REFERENCES #{@prefix}.backfills(workspace_id, backfill_id)
    ON DELETE RESTRICT
    """)

    create(
      unique_index(:backfill_plan_batches, [:workspace_id, :command_id],
        prefix: @prefix,
        name: :backfill_plan_batches_command_uidx
      )
    )

    create(
      constraint(:backfill_plan_batches, :backfill_plan_batches_values_valid,
        prefix: @prefix,
        check: "batch_index BETWEEN 0 AND 999 AND window_count BETWEEN 1 AND 500"
      )
    )

    create table(:backfill_windows, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:backfill_id, :text, null: false, primary_key: true)
      add(:window_id, :text, null: false, primary_key: true)
      add(:batch_index, :integer, null: false)
      add(:window_key, :text, null: false)
      add(:window_start, :timestamptz, null: false)
      add(:window_end, :timestamptz, null: false)
      add(:status, :text, null: false, default: "planned")
      add(:claim_owner, :text)
      add(:fencing_token, :bigint, null: false, default: 0)
      add(:claim_command_id, :text)
      add(:last_command_id, :text)
      add(:claim_expires_at, :timestamptz)
      add(:run_id, :text)
      add(:attempt_count, :integer, null: false, default: 0)
      add(:last_error, :map)
      add(:payload, :map, null: false)
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    create(
      unique_index(:backfill_windows, [:workspace_id, :backfill_id, :window_key],
        prefix: @prefix,
        name: :backfill_windows_key_uidx
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.backfill_windows
    ADD CONSTRAINT backfill_windows_batch_fk
    FOREIGN KEY (workspace_id, backfill_id, batch_index)
    REFERENCES #{@prefix}.backfill_plan_batches(workspace_id, backfill_id, batch_index)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.backfill_windows
    ADD CONSTRAINT backfill_windows_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    create(
      index(:backfill_windows, [:workspace_id, :backfill_id, :status, :window_key, :window_id],
        prefix: @prefix,
        name: :backfill_windows_page_idx
      )
    )

    create(
      index(:backfill_windows, [:workspace_id, :window_start, :window_id],
        prefix: @prefix,
        name: :backfill_windows_claim_idx,
        include: [:backfill_id, :status, :claim_expires_at],
        where: "status IN ('ready', 'claimed', 'running')"
      )
    )

    create(
      index(
        :backfill_windows,
        [:workspace_id, :backfill_id, :window_start, :window_id],
        prefix: @prefix,
        name: :backfill_windows_backfill_claim_idx,
        include: [:status, :claim_expires_at],
        where: "status IN ('ready', 'claimed', 'running')"
      )
    )

    create(
      constraint(:backfill_windows, :backfill_windows_values_valid,
        prefix: @prefix,
        check:
          "window_start < window_end AND batch_index >= 0 AND fencing_token >= 0 " <>
            "AND attempt_count >= 0 AND version > 0 " <>
            "AND status IN ('planned', 'ready', 'claimed', 'running', 'succeeded', 'failed', 'cancelled')"
      )
    )

    create table(:projection_cursors, prefix: @prefix, primary_key: false) do
      add(:projector_name, :text, null: false, primary_key: true)
      add(:shard_id, :integer, null: false, primary_key: true)
      add(:last_publication_id, :bigint, null: false, default: 0)
      add(:owner_id, :text)
      add(:fencing_token, :bigint, null: false, default: 0)
      add(:claim_expires_at, :timestamptz)
      add(:version, :bigint, null: false, default: 1)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      constraint(:projection_cursors, :projection_cursors_values_valid,
        prefix: @prefix,
        check: "shard_id >= 0 AND last_publication_id >= 0 AND fencing_token >= 0 AND version > 0"
      )
    )

    execute("""
    INSERT INTO #{@prefix}.projection_cursors
      (projector_name, shard_id, last_publication_id, fencing_token, version, updated_at)
    VALUES ('control_plane_v1', 0, 0, 0, 1, clock_timestamp())
    """)

    create table(:projection_failures, prefix: @prefix, primary_key: false) do
      add(:failure_id, :bigint, primary_key: true, generated: "BY DEFAULT AS IDENTITY")
      add(:projector_name, :text, null: false)
      add(:shard_id, :integer, null: false)
      add(:publication_id, :bigint, null: false)
      add(:workspace_id, :text, null: false)
      add(:event_kind, :text, null: false)
      add(:error_kind, :text, null: false)
      add(:error_detail, :map, null: false)
      add(:attempt_count, :integer, null: false)
      timestamps(type: :timestamptz)
    end

    create(
      unique_index(:projection_failures, [:projector_name, :shard_id, :publication_id],
        prefix: @prefix,
        name: :projection_failures_event_uidx
      )
    )

    create table(:execution_group_overviews, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:root_run_id, :text, null: false, primary_key: true)
      add(:status, :text, null: false)
      add(:run_count, :integer, null: false)
      add(:pending_count, :integer, null: false)
      add(:running_count, :integer, null: false)
      add(:succeeded_count, :integer, null: false)
      add(:failed_count, :integer, null: false)
      add(:latest_event_id, :bigint, null: false)
      add(:source_publication_id, :bigint, null: false)
      add(:inserted_at, :timestamptz, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      index(:execution_group_overviews, [:workspace_id, {:desc, :latest_event_id}, :root_run_id],
        prefix: @prefix,
        name: :execution_group_overviews_recent_idx
      )
    )

    create(
      index(
        :execution_group_overviews,
        [{:desc, :latest_event_id}, :workspace_id, :root_run_id],
        prefix: @prefix,
        name: :execution_group_overviews_platform_recent_idx
      )
    )

    create table(:backfill_overviews, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:backfill_id, :text, null: false, primary_key: true)
      add(:status, :text, null: false)
      add(:total_count, :integer, null: false)
      add(:planned_count, :integer, null: false)
      add(:ready_count, :integer, null: false)
      add(:active_count, :integer, null: false)
      add(:succeeded_count, :integer, null: false)
      add(:failed_count, :integer, null: false)
      add(:cancelled_count, :integer, null: false)
      add(:source_publication_id, :bigint, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      index(:backfill_overviews, [:workspace_id, :status, :backfill_id],
        prefix: @prefix,
        name: :backfill_overviews_status_idx
      )
    )

    create table(:target_statuses, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:deployment_id, :text, null: false, primary_key: true)
      add(:target_kind, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false, primary_key: true)
      add(:status, :text, null: false)
      add(:run_id, :text)
      add(:event_id, :bigint)
      add(:source_publication_id, :bigint, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      index(:target_statuses, [:workspace_id, :deployment_id, :target_kind, :status, :target_id],
        prefix: @prefix,
        name: :target_statuses_status_idx
      )
    )

    create table(:asset_window_states, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:manifest_version_id, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false, primary_key: true)
      add(:window_key, :text, null: false, primary_key: true)
      add(:window_start, :timestamptz, null: false)
      add(:window_end, :timestamptz, null: false)
      add(:status, :text, null: false)
      add(:run_id, :text)
      add(:materialization_id, :text)
      add(:payload, :map, null: false)
      add(:source_publication_id, :bigint, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      index(
        :asset_window_states,
        [:workspace_id, :manifest_version_id, :target_id, {:desc, :window_start}, :window_key],
        prefix: @prefix,
        name: :asset_window_states_history_idx
      )
    )

    create table(:asset_freshness_states, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:deployment_id, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false, primary_key: true)
      add(:freshness_key, :text, null: false, primary_key: true)
      add(:latest_attempt_materialization_id, :text)
      add(:latest_success_materialization_id, :text)
      add(:latest_success_node_key_hash, :binary)
      add(:input_fingerprint, :binary)
      add(:status, :text, null: false)
      add(:payload, :map, null: false)
      add(:source_publication_id, :bigint, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      index(:asset_freshness_states, [:workspace_id, :latest_success_node_key_hash],
        prefix: @prefix,
        name: :asset_freshness_states_node_idx,
        where: "latest_success_node_key_hash IS NOT NULL"
      )
    )
  end

  def down do
    drop(table(:asset_freshness_states, prefix: @prefix))
    drop(table(:asset_window_states, prefix: @prefix))
    drop(table(:target_statuses, prefix: @prefix))
    drop(table(:backfill_overviews, prefix: @prefix))
    drop(table(:execution_group_overviews, prefix: @prefix))
    drop(table(:projection_failures, prefix: @prefix))
    drop(table(:projection_cursors, prefix: @prefix))
    drop(table(:backfill_windows, prefix: @prefix))
    drop(table(:backfill_plan_batches, prefix: @prefix))
    drop(table(:backfills, prefix: @prefix))
    drop(table(:coverage_baselines, prefix: @prefix))
  end
end
