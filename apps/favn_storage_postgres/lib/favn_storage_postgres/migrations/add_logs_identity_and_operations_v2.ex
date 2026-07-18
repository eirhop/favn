defmodule FavnStoragePostgres.Migrations.AddLogsIdentityAndOperationsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:log_batches, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:batch_id, :text, null: false, primary_key: true)
      add(:command_id, :text, null: false)
      add(:batch_hash, :binary, null: false)
      add(:entry_count, :integer, null: false)
      add(:outbox_event_id, :bigint, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    create(
      unique_index(:log_batches, [:workspace_id, :command_id],
        prefix: @prefix,
        name: :log_batches_command_uidx
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.log_batches
    ADD CONSTRAINT log_batches_outbox_fk
    FOREIGN KEY (workspace_id, outbox_event_id)
    REFERENCES #{@prefix}.outbox_events(workspace_id, outbox_event_id)
    ON DELETE RESTRICT
    """)

    create(
      constraint(:log_batches, :log_batches_count_valid,
        prefix: @prefix,
        check: "entry_count BETWEEN 1 AND 1000"
      )
    )

    create table(:log_entries, prefix: @prefix, primary_key: false) do
      add(:log_id, :bigint, primary_key: true, generated: "BY DEFAULT AS IDENTITY")
      add(:workspace_id, :text, null: false)
      add(:batch_id, :text, null: false)
      add(:position, :integer, null: false)
      add(:run_id, :text)
      add(:source, :text, null: false)
      add(:level, :text, null: false)
      add(:message, :text, null: false)
      add(:metadata, :map, null: false)
      add(:occurred_at, :timestamptz, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.log_entries
    ADD CONSTRAINT log_entries_batch_fk
    FOREIGN KEY (workspace_id, batch_id)
    REFERENCES #{@prefix}.log_batches(workspace_id, batch_id)
    ON DELETE RESTRICT
    """)

    create(
      unique_index(:log_entries, [:workspace_id, :batch_id, :position],
        prefix: @prefix,
        name: :log_entries_batch_position_uidx
      )
    )

    create(
      index(:log_entries, [:workspace_id, {:desc, :occurred_at}, {:desc, :log_id}],
        prefix: @prefix,
        name: :log_entries_recent_idx
      )
    )

    create(
      index(:log_entries, [:workspace_id, :run_id, {:desc, :occurred_at}, {:desc, :log_id}],
        prefix: @prefix,
        name: :log_entries_run_idx,
        where: "run_id IS NOT NULL"
      )
    )

    create(
      index(:log_entries, [:workspace_id, :level, {:desc, :occurred_at}, {:desc, :log_id}],
        prefix: @prefix,
        name: :log_entries_level_idx
      )
    )

    create(
      constraint(:log_entries, :log_entries_values_valid,
        prefix: @prefix,
        check:
          "position >= 0 AND level IN ('debug', 'info', 'warning', 'error') " <>
            "AND octet_length(message) BETWEEN 1 AND 8192 " <>
            "AND octet_length(source) BETWEEN 1 AND 255"
      )
    )

    create table(:auth_actors, prefix: @prefix, primary_key: false) do
      add(:actor_id, :text, null: false, primary_key: true)
      add(:username, :text, null: false)
      add(:normalized_username, :text, null: false)
      add(:display_name, :text, null: false)
      add(:creation_command_id, :text, null: false)
      add(:creation_hash, :binary, null: false)
      add(:status, :text, null: false, default: "active")
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    create(
      unique_index(:auth_actors, [:normalized_username],
        prefix: @prefix,
        name: :auth_actors_username_uidx
      )
    )

    create(
      unique_index(:auth_actors, [:creation_command_id],
        prefix: @prefix,
        name: :auth_actors_creation_command_uidx
      )
    )

    create(
      constraint(:auth_actors, :auth_actors_values_valid,
        prefix: @prefix,
        check:
          "status IN ('active', 'suspended', 'retired') AND version > 0 " <>
            "AND octet_length(actor_id) BETWEEN 1 AND 255 " <>
            "AND octet_length(normalized_username) BETWEEN 1 AND 255"
      )
    )

    create table(:auth_credentials, prefix: @prefix, primary_key: false) do
      add(:actor_id, :text, null: false, primary_key: true)
      add(:password_hash, :text, null: false)
      add(:algorithm, :text, null: false)
      add(:version, :bigint, null: false, default: 1)
      add(:changed_at, :timestamptz, null: false)
      add(:inserted_at, :timestamptz, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.auth_credentials
    ADD CONSTRAINT auth_credentials_actor_fk
    FOREIGN KEY (actor_id) REFERENCES #{@prefix}.auth_actors(actor_id)
    ON DELETE RESTRICT
    """)

    create(
      constraint(:auth_credentials, :auth_credentials_values_valid,
        prefix: @prefix,
        check:
          "version > 0 AND algorithm IN ('argon2id') AND octet_length(password_hash) BETWEEN 20 AND 1024"
      )
    )

    create table(:auth_sessions, prefix: @prefix, primary_key: false) do
      add(:session_id, :text, null: false, primary_key: true)
      add(:actor_id, :text, null: false)
      add(:creation_command_id, :text, null: false)
      add(:token_hash, :binary, null: false)
      add(:provider, :text, null: false)
      add(:status, :text, null: false, default: "active")
      add(:expires_at, :timestamptz, null: false)
      add(:revoked_at, :timestamptz)
      add(:last_seen_at, :timestamptz)
      add(:inserted_at, :timestamptz, null: false)
      add(:updated_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.auth_sessions
    ADD CONSTRAINT auth_sessions_actor_fk
    FOREIGN KEY (actor_id) REFERENCES #{@prefix}.auth_actors(actor_id)
    ON DELETE RESTRICT
    """)

    create(
      unique_index(:auth_sessions, [:token_hash],
        prefix: @prefix,
        name: :auth_sessions_token_uidx
      )
    )

    create(
      unique_index(:auth_sessions, [:creation_command_id],
        prefix: @prefix,
        name: :auth_sessions_creation_command_uidx
      )
    )

    create(
      index(:auth_sessions, [:actor_id, :status, :expires_at, :session_id],
        prefix: @prefix,
        name: :auth_sessions_actor_active_idx,
        where: "status = 'active'"
      )
    )

    create(
      constraint(:auth_sessions, :auth_sessions_values_valid,
        prefix: @prefix,
        check:
          "status IN ('active', 'revoked', 'expired') AND provider IN ('password_local', 'trusted_local_dev') AND octet_length(token_hash) >= 32"
      )
    )

    create table(:auth_workspace_memberships, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:actor_id, :text, null: false, primary_key: true)
      add(:roles, {:array, :text}, null: false)
      add(:status, :text, null: false, default: "active")
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.auth_workspace_memberships
    ADD CONSTRAINT auth_workspace_memberships_workspace_fk
    FOREIGN KEY (workspace_id) REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.auth_workspace_memberships
    ADD CONSTRAINT auth_workspace_memberships_actor_fk
    FOREIGN KEY (actor_id) REFERENCES #{@prefix}.auth_actors(actor_id)
    ON DELETE RESTRICT
    """)

    create(
      index(:auth_workspace_memberships, [:workspace_id, :status, :actor_id],
        prefix: @prefix,
        name: :auth_workspace_memberships_page_idx
      )
    )

    create(
      constraint(:auth_workspace_memberships, :auth_workspace_memberships_values_valid,
        prefix: @prefix,
        check:
          "status IN ('active', 'suspended', 'revoked') AND cardinality(roles) BETWEEN 1 AND 16 AND version > 0"
      )
    )

    create table(:auth_platform_grants, prefix: @prefix, primary_key: false) do
      add(:actor_id, :text, null: false, primary_key: true)
      add(:roles, {:array, :text}, null: false)
      add(:status, :text, null: false, default: "active")
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.auth_platform_grants
    ADD CONSTRAINT auth_platform_grants_actor_fk
    FOREIGN KEY (actor_id) REFERENCES #{@prefix}.auth_actors(actor_id)
    ON DELETE RESTRICT
    """)

    create(
      constraint(:auth_platform_grants, :auth_platform_grants_values_valid,
        prefix: @prefix,
        check:
          "status IN ('active', 'suspended', 'revoked') AND cardinality(roles) BETWEEN 1 AND 16 AND version > 0"
      )
    )

    create table(:auth_audit_entries, prefix: @prefix, primary_key: false) do
      add(:audit_id, :bigint, primary_key: true, generated: "BY DEFAULT AS IDENTITY")
      add(:workspace_id, :text, null: false)
      add(:command_id, :text, null: false)
      add(:principal_id, :text, null: false)
      add(:action, :text, null: false)
      add(:subject_kind, :text, null: false)
      add(:subject_id, :text, null: false)
      add(:detail, :map, null: false)
      add(:occurred_at, :timestamptz, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    create(
      unique_index(:auth_audit_entries, [:workspace_id, :command_id, :action],
        prefix: @prefix,
        name: :auth_audit_entries_command_uidx
      )
    )

    create(
      index(:auth_audit_entries, [:workspace_id, {:desc, :audit_id}],
        prefix: @prefix,
        name: :auth_audit_entries_page_idx
      )
    )

    create table(:auth_platform_audit_entries, prefix: @prefix, primary_key: false) do
      add(:audit_id, :bigint, primary_key: true, generated: "BY DEFAULT AS IDENTITY")
      add(:command_id, :text, null: false)
      add(:principal_id, :text, null: false)
      add(:action, :text, null: false)
      add(:subject_kind, :text, null: false)
      add(:subject_id, :text, null: false)
      add(:detail, :map, null: false)
      add(:occurred_at, :timestamptz, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    create(
      unique_index(:auth_platform_audit_entries, [:command_id, :action],
        prefix: @prefix,
        name: :auth_platform_audit_entries_command_uidx
      )
    )

    create(
      index(:auth_platform_audit_entries, [{:desc, :audit_id}],
        prefix: @prefix,
        name: :auth_platform_audit_entries_page_idx
      )
    )

    create table(:idempotency_records, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:operation, :text, null: false, primary_key: true)
      add(:principal_kind, :text, null: false, primary_key: true)
      add(:principal_id, :text, null: false, primary_key: true)
      add(:key_hash, :binary, null: false, primary_key: true)
      add(:request_fingerprint, :binary, null: false)
      add(:status, :text, null: false)
      add(:response, :map)
      add(:response_status, :integer)
      add(:expires_at, :timestamptz, null: false)
      timestamps(type: :timestamptz)
    end

    create(
      index(:idempotency_records, [:expires_at, :workspace_id, :operation],
        prefix: @prefix,
        name: :idempotency_records_expiry_idx
      )
    )

    create(
      constraint(:idempotency_records, :idempotency_records_values_valid,
        prefix: @prefix,
        check: "status IN ('started', 'committed') AND principal_kind IN ('actor', 'service')"
      )
    )

    create table(:maintenance_jobs, prefix: @prefix, primary_key: false) do
      add(:job_id, :text, null: false, primary_key: true)
      add(:job_kind, :text, null: false)
      add(:scope_kind, :text, null: false)
      add(:workspace_id, :text)
      add(:status, :text, null: false)
      add(:cursor, :map)
      add(:configuration, :map, null: false)
      add(:owner_id, :text)
      add(:fencing_token, :bigint, null: false, default: 0)
      add(:claim_expires_at, :timestamptz)
      add(:processed_count, :bigint, null: false, default: 0)
      add(:last_error, :map)
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    create(
      index(:maintenance_jobs, [:job_kind, :status, :updated_at, :job_id],
        prefix: @prefix,
        name: :maintenance_jobs_queue_idx
      )
    )

    create(
      constraint(:maintenance_jobs, :maintenance_jobs_values_valid,
        prefix: @prefix,
        check:
          "job_kind IN ('projection_missing_row_backfill', 'reconcile', 'purge') " <>
            "AND scope_kind IN ('platform', 'workspace') " <>
            "AND ((scope_kind = 'platform' AND workspace_id IS NULL) OR " <>
            "     (scope_kind = 'workspace' AND workspace_id IS NOT NULL)) " <>
            "AND status IN ('pending', 'running', 'completed', 'failed') " <>
            "AND fencing_token >= 0 AND processed_count >= 0 AND version > 0"
      )
    )
  end

  def down do
    drop(table(:maintenance_jobs, prefix: @prefix))
    drop(table(:idempotency_records, prefix: @prefix))
    drop(table(:auth_platform_audit_entries, prefix: @prefix))
    drop(table(:auth_audit_entries, prefix: @prefix))
    drop(table(:auth_platform_grants, prefix: @prefix))
    drop(table(:auth_workspace_memberships, prefix: @prefix))
    drop(table(:auth_sessions, prefix: @prefix))
    drop(table(:auth_credentials, prefix: @prefix))
    drop(table(:auth_actors, prefix: @prefix))
    drop(table(:log_entries, prefix: @prefix))
    drop(table(:log_batches, prefix: @prefix))
  end
end
