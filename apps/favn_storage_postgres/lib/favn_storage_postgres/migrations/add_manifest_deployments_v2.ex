defmodule FavnStoragePostgres.Migrations.AddManifestDeploymentsV2 do
  @moduledoc false
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:manifest_deployment_operations, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, primary_key: true)
      add(:operation_id, :text, primary_key: true)
      add(:archive_sha256, :binary, null: false)
      add(:request_fingerprint, :binary, null: false)
      add(:service_identity, :text, null: false)
      add(:manifest_version_id, :text, null: false)
      add(:manifest_content_hash, :binary, null: false)
      add(:runner_releases, :map, null: false)
      add(:state, :text, null: false)
      add(:deployment_id, :text)
      add(:failure_class, :text)
      add(:activation_diagnostics, :map)
      add(:claim_owner, :text)
      add(:claim_fence, :bigint, null: false, default: 0)
      add(:claim_expires_at, :timestamptz)
      add(:inspection_total, :integer, null: false, default: 0)
      add(:inspection_completed, :integer, null: false, default: 0)
      add(:accepted_at, :timestamptz, null: false)
      add(:activating_at, :timestamptz)
      add(:terminal_at, :timestamptz)
      add(:inserted_at, :timestamptz, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      index(:manifest_deployment_operations, [:state, :claim_expires_at, :accepted_at],
        prefix: @prefix,
        name: :manifest_deployment_operations_recovery_idx
      )
    )

    create table(:manifest_deployment_upload_leases, prefix: @prefix, primary_key: false) do
      add(:lease_id, :text, primary_key: true)
      add(:workspace_id, :text, null: false)
      add(:service_identity, :text, null: false)
      add(:expires_at, :timestamptz, null: false)
      add(:inserted_at, :timestamptz, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      index(:manifest_deployment_upload_leases, [:expires_at],
        prefix: @prefix,
        name: :manifest_deployment_upload_leases_expiry_idx
      )
    )

    create table(:manifest_activation_leases, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, primary_key: true)
      add(:operation_id, :text, null: false)
      add(:owner, :text, null: false)
      add(:fencing_token, :bigint, null: false)
      add(:expires_at, :timestamptz, null: false)
      add(:inserted_at, :timestamptz, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      index(:manifest_activation_leases, [:expires_at],
        prefix: @prefix,
        name: :manifest_activation_leases_expiry_idx
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.manifest_deployment_operations
      ADD CONSTRAINT manifest_deployment_operations_workspace_fk
        FOREIGN KEY (workspace_id) REFERENCES #{@prefix}.workspaces(workspace_id) ON DELETE RESTRICT,
      ADD CONSTRAINT manifest_deployment_operations_manifest_fk
        FOREIGN KEY (manifest_version_id) REFERENCES #{@prefix}.manifest_versions(manifest_version_id) ON DELETE RESTRICT,
      ADD CONSTRAINT manifest_deployment_operations_values_valid CHECK (
        octet_length(workspace_id) BETWEEN 1 AND 255 AND
        octet_length(operation_id) BETWEEN 1 AND 128 AND
        operation_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]*$' AND
        octet_length(archive_sha256) = 32 AND
        octet_length(request_fingerprint) = 32 AND
        octet_length(service_identity) BETWEEN 1 AND 128 AND
        octet_length(manifest_version_id) BETWEEN 1 AND 255 AND
        octet_length(manifest_content_hash) = 32 AND
        jsonb_typeof(runner_releases) = 'object' AND
        state IN ('accepted', 'activating', 'succeeded', 'needs_attention', 'failed', 'unknown') AND
        (deployment_id IS NULL OR octet_length(deployment_id) BETWEEN 1 AND 255) AND
        (failure_class IS NULL OR octet_length(failure_class) BETWEEN 1 AND 255) AND
        (activation_diagnostics IS NULL OR octet_length(activation_diagnostics::text) <= 65536) AND
        claim_fence >= 0 AND
        inspection_total >= 0 AND
        inspection_completed BETWEEN 0 AND inspection_total AND
        ((claim_owner IS NULL AND claim_expires_at IS NULL) OR
         (octet_length(claim_owner) BETWEEN 1 AND 255 AND claim_expires_at IS NOT NULL)) AND
        ((state IN ('succeeded', 'needs_attention') AND deployment_id IS NOT NULL AND terminal_at IS NOT NULL) OR
         (state IN ('failed', 'unknown') AND failure_class IS NOT NULL AND terminal_at IS NOT NULL) OR
         (state IN ('accepted', 'activating') AND terminal_at IS NULL))
      )
    """)

    execute("""
    ALTER TABLE #{@prefix}.manifest_deployment_upload_leases
      ADD CONSTRAINT manifest_deployment_upload_leases_workspace_fk
        FOREIGN KEY (workspace_id) REFERENCES #{@prefix}.workspaces(workspace_id) ON DELETE CASCADE,
      ADD CONSTRAINT manifest_deployment_upload_leases_values_valid CHECK (
        octet_length(lease_id) BETWEEN 1 AND 255 AND
        octet_length(workspace_id) BETWEEN 1 AND 255 AND
        octet_length(service_identity) BETWEEN 1 AND 128
      )
    """)

    execute("""
    ALTER TABLE #{@prefix}.manifest_activation_leases
      ADD CONSTRAINT manifest_activation_leases_workspace_fk
        FOREIGN KEY (workspace_id) REFERENCES #{@prefix}.workspaces(workspace_id) ON DELETE CASCADE,
      ADD CONSTRAINT manifest_activation_leases_values_valid CHECK (
        octet_length(workspace_id) BETWEEN 1 AND 255 AND
        octet_length(operation_id) BETWEEN 1 AND 255 AND
        octet_length(owner) BETWEEN 1 AND 255 AND
        fencing_token > 0
      )
    """)
  end

  def down do
    drop(table(:manifest_activation_leases, prefix: @prefix))
    drop(table(:manifest_deployment_upload_leases, prefix: @prefix))
    drop(table(:manifest_deployment_operations, prefix: @prefix))
  end
end
