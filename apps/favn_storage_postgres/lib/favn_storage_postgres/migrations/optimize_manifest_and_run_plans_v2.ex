defmodule FavnStoragePostgres.Migrations.OptimizeManifestAndRunPlansV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:manifest_versions, prefix: @prefix) do
      add(:asset_count, :integer)
      add(:pipeline_count, :integer)
      add(:schedule_count, :integer)
      add(:atom_strings, {:array, :text})
    end

    execute("""
    UPDATE #{@prefix}.manifest_versions
    SET asset_count = COALESCE(jsonb_array_length(manifest->'assets'), 0),
        pipeline_count = COALESCE(jsonb_array_length(manifest->'pipelines'), 0),
        schedule_count = COALESCE(jsonb_array_length(manifest->'schedules'), 0)
    """)

    execute("""
    ALTER TABLE #{@prefix}.manifest_versions
      ALTER COLUMN asset_count SET NOT NULL,
      ALTER COLUMN pipeline_count SET NOT NULL,
      ALTER COLUMN schedule_count SET NOT NULL
    """)

    create(
      constraint(:manifest_versions, :manifest_versions_summary_valid,
        prefix: @prefix,
        check:
          "asset_count >= 0 AND pipeline_count >= 0 AND schedule_count >= 0 " <>
            "AND (atom_strings IS NULL OR cardinality(atom_strings) <= 100000)"
      )
    )

    create table(:run_plans, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, primary_key: true)
      add(:run_id, :text, primary_key: true)
      add(:manifest_version_id, :text, null: false)
      add(:plan_version, :smallint, null: false)
      add(:plan_hash, :binary, null: false)
      add(:plan, :map, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    create(
      constraint(:run_plans, :run_plans_values_valid,
        prefix: @prefix,
        check:
          "plan_version > 0 AND octet_length(plan_hash) = 32 " <>
            "AND jsonb_typeof(plan) = 'object' AND octet_length(plan::text) <= 67108864"
      )
    )

    create(
      constraint(:run_plans, :run_plans_identifier_lengths_v2,
        prefix: @prefix,
        check:
          "octet_length(workspace_id) BETWEEN 1 AND 255 " <>
            "AND octet_length(run_id) BETWEEN 1 AND 255 " <>
            "AND octet_length(manifest_version_id) BETWEEN 1 AND 255"
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.run_plans
    ADD CONSTRAINT run_plans_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    execute("""
    ALTER TABLE #{@prefix}.run_plans
    ADD CONSTRAINT run_plans_manifest_fk
    FOREIGN KEY (manifest_version_id)
    REFERENCES #{@prefix}.manifest_versions(manifest_version_id)
    ON DELETE RESTRICT
    """)
  end

  def down do
    drop(table(:run_plans, prefix: @prefix))

    drop(constraint(:manifest_versions, :manifest_versions_summary_valid, prefix: @prefix))

    alter table(:manifest_versions, prefix: @prefix) do
      remove(:atom_strings)
      remove(:schedule_count)
      remove(:pipeline_count)
      remove(:asset_count)
    end
  end
end
