defmodule FavnStoragePostgres.Migrations.AddOperatorCommandIntentsV2 do
  @moduledoc false
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create(
      unique_index(:auth_sessions, [:workspace_id, :actor_id, :session_id],
        prefix: @prefix,
        name: :auth_sessions_workspace_actor_session_uidx
      )
    )

    create table(:auth_operator_commands, prefix: @prefix, primary_key: false) do
      add(:intent_id, :text, primary_key: true)
      add(:workspace_id, :text, null: false)
      add(:actor_id, :text, null: false)
      add(:session_id, :text, null: false)
      add(:operation, :text, null: false)
      add(:resource_type, :text, null: false)
      add(:resource_id, :text, null: false)
      add(:key_hash, :text, null: false)
      add(:request_fingerprint, :text, null: false)
      add(:status, :text, null: false)
      add(:result_resource_type, :text)
      add(:result_resource_id, :text)
      add(:result_detail, :map)
      add(:expires_at, :timestamptz, null: false)
      add(:terminal_at, :timestamptz)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.auth_operator_commands
    ADD CONSTRAINT auth_operator_commands_workspace_fk
    FOREIGN KEY (workspace_id) REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.auth_operator_commands
    ADD CONSTRAINT auth_operator_commands_actor_fk
    FOREIGN KEY (actor_id) REFERENCES #{@prefix}.auth_actors(actor_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.auth_operator_commands
    ADD CONSTRAINT auth_operator_commands_session_authority_fk
    FOREIGN KEY (workspace_id, actor_id, session_id)
    REFERENCES #{@prefix}.auth_sessions(workspace_id, actor_id, session_id)
    ON DELETE RESTRICT
    """)

    create(
      unique_index(:auth_operator_commands, [:workspace_id, :operation, :actor_id, :key_hash],
        prefix: @prefix,
        name: :auth_operator_commands_key_uidx
      )
    )

    create(
      unique_index(
        :auth_operator_commands,
        [:workspace_id, :operation, :resource_type, :resource_id],
        prefix: @prefix,
        name: :auth_operator_commands_pending_request_uidx,
        where: "status IN ('pending', 'unknown')"
      )
    )

    create(
      index(:auth_operator_commands, [:workspace_id, :session_id, :status, :expires_at],
        prefix: @prefix,
        name: :auth_operator_commands_session_pending_idx
      )
    )

    create(
      constraint(:auth_operator_commands, :auth_operator_commands_values_valid,
        prefix: @prefix,
        check:
          "status IN ('pending', 'accepted', 'partial', 'rejected', 'unknown') " <>
            "AND octet_length(operation) BETWEEN 1 AND 128 " <>
            "AND octet_length(key_hash) BETWEEN 16 AND 128 " <>
            "AND octet_length(request_fingerprint) BETWEEN 16 AND 128 " <>
            "AND ((status = 'pending' AND terminal_at IS NULL) OR " <>
            "(status <> 'pending' AND terminal_at IS NOT NULL))"
      )
    )
  end

  def down do
    drop(table(:auth_operator_commands, prefix: @prefix))

    drop(
      index(:auth_sessions, [:workspace_id, :actor_id, :session_id],
        prefix: @prefix,
        name: :auth_sessions_workspace_actor_session_uidx
      )
    )
  end
end
