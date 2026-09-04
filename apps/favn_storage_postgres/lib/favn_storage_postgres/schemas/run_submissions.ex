defmodule FavnStoragePostgres.Schemas.RunSubmission do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "run_submissions" do
    field(:workspace_id, :string, primary_key: true)
    field(:submission_id, :string, primary_key: true)
    field(:source, :string)
    field(:idempotency_key, :string)
    field(:request_hash, :binary)
    field(:authority, :map)
    field(:deployment_id, :string)
    field(:manifest_version_id, :string)
    field(:target_kind, :string)
    field(:target_id, :string)
    field(:run_id, :string)
    field(:cancellation_owner_run_id, :string)
    field(:intent, :map)
    field(:status, :string)
    field(:attempt, :integer)
    field(:claim_owner, :string)
    field(:claim_generation, :integer)
    field(:claim_expires_at, :utc_datetime_usec)
    field(:preparation, :map)
    field(:outcome, :map)
    field(:error, :map)
    field(:failure_kind, :string)
    field(:cancellation_requested_at, :utc_datetime_usec)
    field(:cancellation_reason, :string)
    field(:retry_root_id, :string)
    field(:retry_of_submission_id, :string)
    field(:retry_command_id, :string)
    field(:superseded_by_submission_id, :string)
    field(:enqueued_at, :utc_datetime_usec)
    field(:available_at, :utc_datetime_usec)
    field(:preparing_at, :utc_datetime_usec)
    field(:admitting_at, :utc_datetime_usec)
    field(:terminal_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RunSubmissionCommand do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "run_submission_commands" do
    field(:workspace_id, :string, primary_key: true)
    field(:command_id, :string, primary_key: true)
    field(:submission_id, :string)
    field(:command_kind, :string)
    field(:request_hash, :binary)
    field(:result, :map)
    field(:inserted_at, :utc_datetime_usec)
  end
end
