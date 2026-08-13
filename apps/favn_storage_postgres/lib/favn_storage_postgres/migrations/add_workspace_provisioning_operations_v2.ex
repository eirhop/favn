defmodule FavnStoragePostgres.Migrations.AddWorkspaceProvisioningOperationsV2 do
  @moduledoc false
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:workspace_provisioning_operations, prefix: @prefix, primary_key: false) do
      add(:operation_id, :text, primary_key: true)
      add(:workspace_id, :text, null: false)
      add(:request_fingerprint, :binary, null: false)
      add(:actor_id, :text, null: false)
      add(:username, :text, null: false)
      add(:authentication_mode, :text, null: false)
      add(:tenant_fingerprint, :binary)
      add(:object_fingerprint, :binary)
      add(:status, :text, null: false)
      add(:inserted_at, :timestamptz, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      unique_index(:workspace_provisioning_operations, [:workspace_id],
        prefix: @prefix,
        name: :workspace_provisioning_operations_workspace_uidx
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.workspace_provisioning_operations
    ADD CONSTRAINT workspace_provisioning_operations_workspace_fk
    FOREIGN KEY (workspace_id) REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.workspace_provisioning_operations
    ADD CONSTRAINT workspace_provisioning_operations_actor_fk
    FOREIGN KEY (actor_id) REFERENCES #{@prefix}.auth_actors(actor_id)
    ON DELETE RESTRICT
    """)

    create(
      constraint(
        :workspace_provisioning_operations,
        :workspace_provisioning_operations_values_valid,
        prefix: @prefix,
        check:
          "octet_length(operation_id) BETWEEN 1 AND 255 " <>
            "AND octet_length(workspace_id) BETWEEN 1 AND 255 " <>
            "AND octet_length(request_fingerprint) = 32 " <>
            "AND octet_length(actor_id) BETWEEN 1 AND 255 " <>
            "AND octet_length(username) BETWEEN 1 AND 128 " <>
            "AND authentication_mode IN ('entra', 'password') " <>
            "AND ((authentication_mode = 'entra' " <>
            "AND octet_length(tenant_fingerprint) = 32 " <>
            "AND octet_length(object_fingerprint) = 32) " <>
            "OR (authentication_mode = 'password' " <>
            "AND tenant_fingerprint IS NULL AND object_fingerprint IS NULL)) " <>
            "AND status = 'ready'"
      )
    )
  end

  def down do
    drop(table(:workspace_provisioning_operations, prefix: @prefix))
  end
end
