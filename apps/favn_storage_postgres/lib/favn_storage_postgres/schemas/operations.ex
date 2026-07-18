defmodule FavnStoragePostgres.Schemas.LogBatch do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "log_batches" do
    field(:workspace_id, :string, primary_key: true)
    field(:batch_id, :string, primary_key: true)
    field(:command_id, :string)
    field(:batch_hash, :binary)
    field(:entry_count, :integer)
    field(:outbox_event_id, :integer)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.LogEntry do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:log_id, :id, autogenerate: true}
  @schema_prefix "favn_control"
  schema "log_entries" do
    field(:workspace_id, :string)
    field(:batch_id, :string)
    field(:position, :integer)
    field(:run_id, :string)
    field(:asset_step_id, :string)
    field(:runner_execution_id, :string)
    field(:node_key_hash, :binary)
    field(:asset_ref_hash, :binary)
    field(:stream, :string)
    field(:source, :string)
    field(:level, :string)
    field(:message, :string)
    field(:metadata, :map)
    field(:occurred_at, :utc_datetime_usec)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AuthActor do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:actor_id, :string, autogenerate: false}
  @schema_prefix "favn_control"
  schema "auth_actors" do
    field(:username, :string)
    field(:normalized_username, :string)
    field(:display_name, :string)
    field(:creation_command_id, :string)
    field(:creation_hash, :binary)
    field(:status, :string)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AuthCredential do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:actor_id, :string, autogenerate: false}
  @schema_prefix "favn_control"
  schema "auth_credentials" do
    field(:password_hash, :string)
    field(:algorithm, :string)
    field(:version, :integer)
    field(:changed_at, :utc_datetime_usec)
    field(:inserted_at, :utc_datetime_usec)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AuthSession do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:session_id, :string, autogenerate: false}
  @schema_prefix "favn_control"
  schema "auth_sessions" do
    field(:actor_id, :string)
    field(:creation_command_id, :string)
    field(:token_hash, :binary)
    field(:provider, :string)
    field(:status, :string)
    field(:expires_at, :utc_datetime_usec)
    field(:revoked_at, :utc_datetime_usec)
    field(:last_seen_at, :utc_datetime_usec)
    field(:inserted_at, :utc_datetime_usec)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AuthWorkspaceMembership do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "auth_workspace_memberships" do
    field(:workspace_id, :string, primary_key: true)
    field(:actor_id, :string, primary_key: true)
    field(:roles, {:array, :string})
    field(:status, :string)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AuthPlatformGrant do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:actor_id, :string, autogenerate: false}
  @schema_prefix "favn_control"
  schema "auth_platform_grants" do
    field(:roles, {:array, :string})
    field(:status, :string)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AuthAuditEntry do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:audit_id, :id, autogenerate: true}
  @schema_prefix "favn_control"
  schema "auth_audit_entries" do
    field(:workspace_id, :string)
    field(:command_id, :string)
    field(:principal_id, :string)
    field(:action, :string)
    field(:subject_kind, :string)
    field(:subject_id, :string)
    field(:detail, :map)
    field(:occurred_at, :utc_datetime_usec)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AuthPlatformAuditEntry do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:audit_id, :id, autogenerate: true}
  @schema_prefix "favn_control"
  schema "auth_platform_audit_entries" do
    field(:command_id, :string)
    field(:principal_id, :string)
    field(:action, :string)
    field(:subject_kind, :string)
    field(:subject_id, :string)
    field(:detail, :map)
    field(:occurred_at, :utc_datetime_usec)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.IdempotencyRecord do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "idempotency_records" do
    field(:workspace_id, :string, primary_key: true)
    field(:operation, :string, primary_key: true)
    field(:principal_kind, :string, primary_key: true)
    field(:principal_id, :string, primary_key: true)
    field(:key_hash, :binary, primary_key: true)
    field(:request_fingerprint, :binary)
    field(:status, :string)
    field(:reservation_generation, :integer)
    field(:response, :map)
    field(:response_status, :integer)
    field(:resource_kind, :string)
    field(:resource_id, :string)
    field(:expires_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.MaintenanceJob do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:job_id, :string, autogenerate: false}
  @schema_prefix "favn_control"
  schema "maintenance_jobs" do
    field(:job_kind, :string)
    field(:scope_kind, :string)
    field(:workspace_id, :string)
    field(:status, :string)
    field(:cursor, :map)
    field(:configuration, :map)
    field(:owner_id, :string)
    field(:fencing_token, :integer)
    field(:claim_expires_at, :utc_datetime_usec)
    field(:processed_count, :integer)
    field(:last_error, :map)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end
