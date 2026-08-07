defmodule FavnStoragePostgres.Migrations.GeneralizeOperatorCommandPrincipalsV2 do
  @moduledoc false
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:auth_operator_commands, prefix: @prefix) do
      add(:principal_kind, :text)
      add(:principal_id, :text)
      modify(:actor_id, :text, null: true)
      modify(:session_id, :text, null: true)
    end

    execute("""
    UPDATE #{@prefix}.auth_operator_commands
    SET principal_kind = 'actor', principal_id = actor_id
    WHERE principal_kind IS NULL
    """)

    alter table(:auth_operator_commands, prefix: @prefix) do
      modify(:principal_kind, :text, null: false)
      modify(:principal_id, :text, null: false)
    end

    drop(
      index(:auth_operator_commands, [:workspace_id, :operation, :actor_id, :key_hash],
        prefix: @prefix,
        name: :auth_operator_commands_key_uidx
      )
    )

    drop(
      index(:auth_operator_commands, [:workspace_id, :session_id, :status, :expires_at],
        prefix: @prefix,
        name: :auth_operator_commands_session_pending_idx
      )
    )

    drop(
      constraint(:auth_operator_commands, :auth_operator_commands_values_valid, prefix: @prefix)
    )

    create(
      unique_index(
        :auth_operator_commands,
        [:workspace_id, :operation, :principal_kind, :principal_id, :key_hash],
        prefix: @prefix,
        name: :auth_operator_commands_key_uidx
      )
    )

    create(
      index(
        :auth_operator_commands,
        [:workspace_id, :principal_kind, :principal_id, :status, :expires_at],
        prefix: @prefix,
        name: :auth_operator_commands_session_pending_idx
      )
    )

    create(
      constraint(:auth_operator_commands, :auth_operator_commands_values_valid,
        prefix: @prefix,
        check:
          "status IN ('pending', 'accepted', 'partial', 'rejected', 'unknown') " <>
            "AND principal_kind IN ('actor', 'service') " <>
            "AND octet_length(principal_id) BETWEEN 1 AND 255 " <>
            "AND ((principal_kind = 'actor' AND actor_id = principal_id " <>
            "AND session_id IS NOT NULL) OR " <>
            "(principal_kind = 'service' AND actor_id IS NULL AND session_id IS NULL)) " <>
            "AND octet_length(operation) BETWEEN 1 AND 128 " <>
            "AND octet_length(key_hash) BETWEEN 16 AND 128 " <>
            "AND octet_length(request_fingerprint) BETWEEN 16 AND 128 " <>
            "AND ((status = 'pending' AND terminal_at IS NULL) OR " <>
            "(status <> 'pending' AND terminal_at IS NOT NULL))"
      )
    )
  end

  def down do
    raise "operator command service principals cannot be losslessly downgraded"
  end
end
