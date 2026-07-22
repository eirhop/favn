defmodule FavnStoragePostgres.Schemas.RunnerExecution do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "runner_executions" do
    field(:workspace_id, :string, primary_key: true)
    field(:runner_execution_id, :string, primary_key: true)
    field(:run_id, :string)
    field(:dispatch_id, :string)
    field(:last_command_id, :string)
    field(:owner_id, :string)
    field(:run_fencing_token, :integer)
    field(:status, :string)
    field(:version, :integer)
    field(:dispatch_payload, :map)
    field(:result, :map)
    field(:error, :map)
    field(:dispatched_at, :utc_datetime_usec)
    field(:terminal_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ScheduleCursor do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "schedule_cursors" do
    field(:workspace_id, :string, primary_key: true)
    field(:deployment_id, :string, primary_key: true)
    field(:target_kind, :string)
    field(:pipeline_target_id, :string, primary_key: true)
    field(:schedule_id, :string, primary_key: true)
    field(:schedule_fingerprint, :string)
    field(:definition, :map)
    field(:next_due_at, :utc_datetime_usec)
    field(:cursor, :map)
    field(:version, :integer)
    field(:claim_owner, :string)
    field(:claim_generation, :integer)
    field(:claim_command_id, :string)
    field(:last_command_id, :string)
    field(:claim_expires_at, :utc_datetime_usec)
    field(:updated_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ScheduleOccurrence do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "schedule_occurrences" do
    field(:workspace_id, :string, primary_key: true)
    field(:occurrence_id, :string, primary_key: true)
    field(:occurrence_key, :binary)
    field(:evaluation_command_id, :string)
    field(:deployment_id, :string)
    field(:pipeline_target_id, :string)
    field(:schedule_id, :string)
    field(:due_at, :utc_datetime_usec)
    field(:payload, :map)
    field(:status, :string)
    field(:claim_owner, :string)
    field(:claim_generation, :integer)
    field(:claim_command_id, :string)
    field(:last_command_id, :string)
    field(:claim_expires_at, :utc_datetime_usec)
    field(:run_id, :string)
    field(:attempt_count, :integer)
    field(:last_error, :map)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.CapacityScope do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:scope_id, :string, autogenerate: false}
  @schema_prefix "favn_control"
  schema "capacity_scopes" do
    field(:workspace_id, :string)
    field(:scope_kind, :string)
    field(:scope_key, :string)
    field(:capacity_limit, :integer)
    field(:active_count, :integer)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ExecutionLease do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "execution_leases" do
    field(:workspace_id, :string, primary_key: true)
    field(:lease_id, :string, primary_key: true)
    field(:run_id, :string)
    field(:step_id, :string)
    field(:command_id, :string)
    field(:request_hash, :binary)
    field(:owner_id, :string)
    field(:owner_generation, :integer)
    field(:last_renewal_id, :string)
    field(:status, :string)
    field(:expires_at, :utc_datetime_usec)
    field(:released_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ExecutionLeaseScope do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "execution_lease_scopes" do
    field(:workspace_id, :string, primary_key: true)
    field(:lease_id, :string, primary_key: true)
    field(:scope_id, :string, primary_key: true)
    field(:units, :integer)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.AdmissionWaiter do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "admission_waiters" do
    field(:workspace_id, :string, primary_key: true)
    field(:waiter_id, :string, primary_key: true)
    field(:run_id, :string)
    field(:step_id, :string)
    field(:command_id, :string)
    field(:request_hash, :binary)
    field(:requested_scopes, {:array, :map})
    field(:blocking_scope_id, :string)
    field(:priority, :integer)
    field(:status, :string)
    field(:available_at, :utc_datetime_usec)
    field(:expires_at, :utc_datetime_usec)
    field(:claim_owner, :string)
    field(:claim_generation, :integer)
    field(:claim_command_id, :string)
    field(:claim_expires_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.MaterializationClaim do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "materialization_claims" do
    field(:workspace_id, :string, primary_key: true)
    field(:claim_key, :string, primary_key: true)
    field(:deployment_id, :string)
    field(:target_kind, :string)
    field(:target_id, :string)
    field(:target_generation_id, Ecto.UUID)
    field(:evidence_generation_id, :string)
    field(:partition_key, :string)
    field(:run_id, :string)
    field(:claim_command_id, :string)
    field(:claim_request_hash, :binary)
    field(:owner_id, :string)
    field(:fencing_token, :integer)
    field(:last_renewal_id, :string)
    field(:last_finish_command_id, :string)
    field(:finish_hash, :binary)
    field(:status, :string)
    field(:expires_at, :utc_datetime_usec)
    field(:completed_at, :utc_datetime_usec)
    field(:result, :map)
    field(:error, :map)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.Materialization do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "materializations" do
    field(:workspace_id, :string, primary_key: true)
    field(:materialization_id, :string, primary_key: true)
    field(:claim_key, :string)
    field(:deployment_id, :string)
    field(:target_kind, :string)
    field(:target_id, :string)
    field(:target_generation_id, Ecto.UUID)
    field(:evidence_generation_id, :string)
    field(:partition_key, :string)
    field(:run_id, :string)
    field(:payload, :map)
    field(:payload_hash, :binary)
    field(:outbox_event_id, :integer)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ResourceCircuit do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "resource_circuits" do
    field(:workspace_id, :string, primary_key: true)
    field(:resource_kind, :string, primary_key: true)
    field(:resource_name, :string, primary_key: true)
    field(:state, :string)
    field(:consecutive_failures, :integer)
    field(:failure_threshold, :integer)
    field(:probe_after_ms, :integer)
    field(:opened_at, :utc_datetime_usec)
    field(:next_probe_at, :utc_datetime_usec)
    field(:probe_owner_id, :string)
    field(:probe_expires_at, :utc_datetime_usec)
    field(:last_category, :string)
    field(:last_outcome_at, :utc_datetime_usec)
    field(:version, :integer)
    timestamps(type: :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ResourceCircuitOutcome do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "resource_circuit_outcomes" do
    field(:workspace_id, :string, primary_key: true)
    field(:outcome_id, :string, primary_key: true)
    field(:resource_kind, :string)
    field(:resource_name, :string)
    field(:run_id, :string)
    field(:asset_step_id, :string)
    field(:attempt, :integer)
    field(:status, :string)
    field(:category, :string)
    field(:occurred_at, :utc_datetime_usec)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule FavnStoragePostgres.Schemas.ResourceRecoveryCandidate do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "resource_recovery_candidates" do
    field(:workspace_id, :string, primary_key: true)
    field(:candidate_id, :string, primary_key: true)
    field(:source_run_id, :string)
    field(:node_key, :string)
    field(:resource_kind, :string)
    field(:resource_name, :string)
    field(:reason, :string)
    field(:status, :string)
    field(:expires_at, :utc_datetime_usec)
    field(:claim_owner, :string)
    field(:claim_expires_at, :utc_datetime_usec)
    field(:recovery_run_id, :string)
    timestamps(type: :utc_datetime_usec)
  end
end
