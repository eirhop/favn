defmodule FavnStoragePostgres.Migrations.BindAuthSessionsToWorkspacesV2 do
  @moduledoc false
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:auth_sessions, prefix: @prefix) do
      add(:workspace_id, :text)
    end

    # Existing tokens predate workspace binding and cannot be assigned to a
    # tenant without guessing. Invalidate them so every active session created
    # after this migration has an explicit workspace authority.
    execute("""
    UPDATE #{@prefix}.auth_sessions
    SET status = 'revoked',
        revoked_at = COALESCE(revoked_at, NOW()),
        updated_at = NOW()
    WHERE status = 'active'
    """)

    execute("""
    ALTER TABLE #{@prefix}.auth_sessions
    ADD CONSTRAINT auth_sessions_workspace_fk
    FOREIGN KEY (workspace_id) REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    create(
      index(:auth_sessions, [:workspace_id, :inserted_at, :session_id],
        prefix: @prefix,
        name: :auth_sessions_workspace_page_idx
      )
    )

    create(
      constraint(:auth_sessions, :auth_sessions_workspace_bound,
        prefix: @prefix,
        check: "status <> 'active' OR workspace_id IS NOT NULL"
      )
    )
  end

  def down do
    drop(constraint(:auth_sessions, :auth_sessions_workspace_bound, prefix: @prefix))

    drop(
      index(:auth_sessions, [:workspace_id],
        prefix: @prefix,
        name: :auth_sessions_workspace_page_idx
      )
    )

    execute("ALTER TABLE #{@prefix}.auth_sessions DROP CONSTRAINT auth_sessions_workspace_fk")

    alter table(:auth_sessions, prefix: @prefix) do
      remove(:workspace_id)
    end
  end
end
