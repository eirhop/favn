defmodule FavnStoragePostgres.Migrations.AddAssetEvidenceBindingsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:asset_evidence_bindings, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false, primary_key: true)
      add(:evidence_generation_id, :text, null: false)
      add(:initial_manifest_id, :text, null: false)
      add(:created_at, :timestamptz, null: false)
    end

    create(
      constraint(:asset_evidence_bindings, :asset_evidence_bindings_values_valid,
        prefix: @prefix,
        check:
          "octet_length(workspace_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(target_id) BETWEEN 1 AND 255 AND " <>
            "evidence_generation_id ~ '^ag_[0-9a-f]{64}$' AND " <>
            "octet_length(initial_manifest_id) BETWEEN 1 AND 255"
      )
    )

    create(
      constraint(
        :asset_evidence_bindings,
        :asset_evidence_bindings_identifier_lengths_v2,
        prefix: @prefix,
        check:
          "octet_length(workspace_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(target_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(evidence_generation_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(initial_manifest_id) BETWEEN 1 AND 255"
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.asset_evidence_bindings
    ADD CONSTRAINT asset_evidence_bindings_workspace_fk
    FOREIGN KEY (workspace_id)
    REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.asset_evidence_bindings
    ADD CONSTRAINT asset_evidence_bindings_manifest_fk
    FOREIGN KEY (initial_manifest_id)
    REFERENCES #{@prefix}.manifest_versions(manifest_version_id)
    ON DELETE RESTRICT
    """)

    execute("""
    INSERT INTO #{@prefix}.asset_evidence_bindings
      (workspace_id, target_id, evidence_generation_id, initial_manifest_id, created_at)
    SELECT runtime.workspace_id,
           target.target_id,
           asset.value ->> 'semantic_generation_id',
           deployment.manifest_version_id,
           clock_timestamp()
    FROM #{@prefix}.workspace_runtime_state runtime
    JOIN #{@prefix}.workspace_deployments deployment
      ON deployment.workspace_id = runtime.workspace_id
     AND deployment.deployment_id = runtime.active_deployment_id
    JOIN #{@prefix}.manifest_versions manifest
      ON manifest.manifest_version_id = deployment.manifest_version_id
    CROSS JOIN LATERAL jsonb_array_elements(manifest.manifest -> 'assets') AS asset(value)
    JOIN #{@prefix}.workspace_deployment_targets target
      ON target.workspace_id = deployment.workspace_id
     AND target.deployment_id = deployment.deployment_id
     AND target.target_kind = 'asset'
     AND target.target_id =
       'asset:' || (asset.value -> 'ref' ->> 0) || ':' || (asset.value -> 'ref' ->> 1)
    WHERE COALESCE(jsonb_typeof(asset.value -> 'target_descriptor'), 'null') = 'null'
      AND asset.value ->> 'semantic_generation_id' ~ '^ag_[0-9a-f]{64}$'
    ON CONFLICT (workspace_id, target_id) DO NOTHING
    """)
  end

  def down do
    drop(table(:asset_evidence_bindings, prefix: @prefix))
  end
end
