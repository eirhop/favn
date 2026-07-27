defmodule FavnStoragePostgres.Schemas.RunnerTask do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "runner_tasks" do
    field(:workspace_id, :string, primary_key: true)
    field(:task_id, :string, primary_key: true)
    field(:domain_identity, :string)
    field(:task_kind, :string)
    field(:run_id, :string)
    field(:operation_id, :string)
    field(:asset_step_id, :string)
    field(:runner_pool, :string)
    field(:required_runner_release_id, :string)
    field(:required_capability, :string)
    field(:retry_class, :string)
    field(:status, :string)
    field(:enqueued_at, :utc_datetime_usec)
    field(:deadline_at, :utc_datetime_usec)
    field(:payload_version, :integer)
    field(:payload, :map)
    field(:payload_hash, :binary)
    field(:orchestration_context, :map)
    field(:assigned_runner_instance_id, :string)
    field(:assigned_runner_session_generation, :integer)
    field(:assignment_generation, :integer)
    field(:assignment_expires_at, :utc_datetime_usec)
    field(:cancellation_requested_at, :utc_datetime_usec)
    field(:cancellation_acknowledged_at, :utc_datetime_usec)
    field(:runtime_input_resolution_id, :string)
    field(:runtime_input_resolution_status, :string)
    field(:runtime_input_payload_fingerprint, :binary)
    field(:runtime_input_error, :map)
    field(:runtime_inputs_resolved_at, :utc_datetime_usec)
    field(:last_command_id, :string)
    field(:result_version, :integer)
    field(:result, :map)
    field(:error, :map)
    field(:terminal_at, :utc_datetime_usec)
    field(:inserted_at, :utc_datetime_usec)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RunnerTaskCommand do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "runner_task_commands" do
    field(:scope_id, :string, primary_key: true)
    field(:command_id, :string, primary_key: true)
    field(:operation, :string)
    field(:request_hash, :binary)
    field(:result, :map)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RunnerTaskLogBatch do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "runner_task_log_batches" do
    field(:workspace_id, :string, primary_key: true)
    field(:task_id, :string, primary_key: true)
    field(:batch_id, :string, primary_key: true)
    field(:assignment_generation, :integer)
    field(:sequence, :integer)
    field(:entries, {:array, :map})
    field(:payload_hash, :binary)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.RunnerCapacityDemand do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "runner_capacity_demands" do
    field(:runner_pool, :string, primary_key: true)
    field(:required_runner_release_id, :string, primary_key: true)
    field(:outstanding_count, :integer)
    field(:queued_count, :integer)
    field(:active_count, :integer)
    field(:oldest_queued_at, :utc_datetime_usec)
    field(:version, :integer)
    field(:healthy, :boolean, source: :healthy)
    field(:updated_at, :utc_datetime_usec)
  end
end
