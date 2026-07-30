defmodule FavnStoragePostgres.Migrations.AddExternalIdentitiesV2 do
  @moduledoc false
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:auth_external_identities, prefix: @prefix, primary_key: false) do
      add(:provider, :text, primary_key: true)
      add(:tenant_id, :text, primary_key: true)
      add(:subject_id, :text, primary_key: true)
      add(:actor_id, :text, null: false)
      add(:linked_at, :timestamptz, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.auth_external_identities
    ADD CONSTRAINT auth_external_identities_actor_fk
    FOREIGN KEY (actor_id) REFERENCES #{@prefix}.auth_actors(actor_id)
    ON DELETE RESTRICT
    """)

    create(
      unique_index(:auth_external_identities, [:provider, :tenant_id, :actor_id],
        prefix: @prefix,
        name: :auth_external_identities_actor_uidx
      )
    )

    create(
      constraint(:auth_external_identities, :auth_external_identities_values_valid,
        prefix: @prefix,
        check:
          "provider = 'azure_container_apps_entra' " <>
            "AND tenant_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' " <>
            "AND subject_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'"
      )
    )

    alter table(:auth_sessions, prefix: @prefix) do
      add(:external_tenant_id, :text)
      add(:external_subject_id, :text)
    end

    drop(constraint(:auth_sessions, :auth_sessions_values_valid, prefix: @prefix))

    create(
      constraint(:auth_sessions, :auth_sessions_values_valid,
        prefix: @prefix,
        check:
          "status IN ('active', 'revoked', 'expired') " <>
            "AND provider IN ('password_local', 'trusted_local_dev', 'azure_container_apps_entra') " <>
            "AND ((provider = 'azure_container_apps_entra' " <>
            "AND external_tenant_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' " <>
            "AND external_subject_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') " <>
            "OR (provider <> 'azure_container_apps_entra' " <>
            "AND external_tenant_id IS NULL AND external_subject_id IS NULL)) " <>
            "AND octet_length(token_hash) >= 32"
      )
    )
  end

  def down do
    drop(constraint(:auth_sessions, :auth_sessions_values_valid, prefix: @prefix))

    execute("""
    DELETE FROM #{@prefix}.auth_operator_commands command
    USING #{@prefix}.auth_sessions session
    WHERE command.workspace_id = session.workspace_id
      AND command.actor_id = session.actor_id
      AND command.session_id = session.session_id
      AND session.provider = 'azure_container_apps_entra'
    """)

    execute("""
    DELETE FROM #{@prefix}.auth_sessions
    WHERE provider = 'azure_container_apps_entra'
    """)

    alter table(:auth_sessions, prefix: @prefix) do
      remove(:external_tenant_id)
      remove(:external_subject_id)
    end

    create(
      constraint(:auth_sessions, :auth_sessions_values_valid,
        prefix: @prefix,
        check:
          "status IN ('active', 'revoked', 'expired') " <>
            "AND provider IN ('password_local', 'trusted_local_dev') " <>
            "AND octet_length(token_hash) >= 32"
      )
    )

    drop(table(:auth_external_identities, prefix: @prefix))
  end
end
