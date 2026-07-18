defmodule FavnOrchestrator.Persistence.Commands.ClaimRun do
  @moduledoc "Claims or takes over one available run and returns a new fencing generation."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :command_id, :run_id, :owner_id, :lease_duration_ms]
  defstruct [:workspace_context, :command_id, :run_id, :owner_id, :lease_duration_ms]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          run_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimRecoveryBatch do
  @moduledoc "Claims a bounded batch of active runs whose ownership is available in one workspace."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :batch_id, :owner_id, :lease_duration_ms]
  defstruct [:workspace_context, :batch_id, :owner_id, :lease_duration_ms, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          batch_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer(),
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RenewRunOwnership do
  @moduledoc "Renews only one matching unexpired run ownership generation."
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :renewal_id,
    :run_id,
    :owner_id,
    :fencing_token,
    :lease_duration_ms
  ]
  defstruct [
    :workspace_context,
    :renewal_id,
    :run_id,
    :owner_id,
    :fencing_token,
    :lease_duration_ms
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          renewal_id: String.t(),
          run_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          lease_duration_ms: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ReleaseRunOwnership do
  @moduledoc "Idempotently releases one matching run ownership generation."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :run_id, :owner_id, :fencing_token]
  defstruct [:workspace_context, :run_id, :owner_id, :fencing_token]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RecordRunnerDispatch do
  @moduledoc "Persists runner dispatch intent before the external runner call."
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :run_id,
    :runner_execution_id,
    :dispatch_id,
    :owner_id,
    :fencing_token,
    :payload,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :run_id,
    :runner_execution_id,
    :dispatch_id,
    :owner_id,
    :fencing_token,
    :payload,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          run_id: String.t(),
          runner_execution_id: String.t(),
          dispatch_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          payload: map(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.AdvanceRunnerExecution do
  @moduledoc "Applies one fenced, version-guarded runner-execution lifecycle transition."
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :run_id,
    :runner_execution_id,
    :owner_id,
    :fencing_token,
    :expected_version,
    :status,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :run_id,
    :runner_execution_id,
    :owner_id,
    :fencing_token,
    :expected_version,
    :status,
    :result,
    :error,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          run_id: String.t(),
          runner_execution_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          expected_version: pos_integer(),
          status: atom(),
          result: map() | nil,
          error: map() | nil,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageRunnerExecutions do
  @moduledoc """
  Bounded runner-execution query for recovery and diagnostics.

  Historical pages (`active_only?: false`) require an exact `run_id`; broad
  workspace or owner history is intentionally not part of the storage contract.
  """
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context]
  defstruct [:workspace_context, :run_id, :owner_id, :after, active_only?: true, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t() | nil,
          owner_id: String.t() | nil,
          after: %{runner_execution_id: String.t()} | nil,
          active_only?: boolean(),
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunOwnership do
  @moduledoc "Current fenced run ownership authority."
  @enforce_keys [:workspace_id, :run_id, :owner_id, :fencing_token, :expires_at]
  defstruct [:workspace_id, :run_id, :owner_id, :fencing_token, :expires_at]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          run_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          expires_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunnerExecution do
  @moduledoc "Durable runner dispatch and execution state returned across persistence."
  @enforce_keys [:workspace_id, :runner_execution_id, :run_id, :status, :version]
  defstruct [
    :workspace_id,
    :runner_execution_id,
    :run_id,
    :dispatch_id,
    :owner_id,
    :fencing_token,
    :status,
    :version,
    :payload,
    :result,
    :error,
    :dispatched_at,
    :terminal_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          runner_execution_id: String.t(),
          run_id: String.t(),
          dispatch_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          status: atom(),
          version: pos_integer(),
          payload: map(),
          result: map() | nil,
          error: map() | nil,
          dispatched_at: DateTime.t() | nil,
          terminal_at: DateTime.t() | nil
        }
end
