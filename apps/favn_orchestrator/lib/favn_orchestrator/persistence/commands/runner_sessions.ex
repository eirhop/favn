defmodule FavnOrchestrator.Persistence.Commands.OpenRunnerSession do
  @moduledoc """
  Opens one durable runner session row for an accepted registration.

  The session id is minted once per accepted registration and is the
  idempotency token: retrying the same open cannot double-open. The write
  also closes any other open row for the same runner instance as presumed
  dead, because its close was evidently lost.
  """
  @enforce_keys [
    :platform_context,
    :session_id,
    :runner_instance_id,
    :runner_boot_id,
    :session_generation,
    :control_plane_boot_id,
    :runner_pool,
    :required_runner_release_id,
    :beam_node,
    :protocol_version,
    :lifecycle_mode,
    :registered_at
  ]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.CloseRunnerSession do
  @moduledoc """
  Closes one runner session row when its process exit is observed.

  Close-only-if-still-open: a row already closed keeps its recorded reason.
  """
  @enforce_keys [:platform_context, :session_id, :ended_at, :end_reason]
  defstruct @enforce_keys ++
              [busy_at_exit: false, interrupted_task_workspace_id: nil, interrupted_task_id: nil]

  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.ReconcileRunnerSessions do
  @moduledoc """
  Closes rows opened by earlier control-plane boots as presumed dead.

  Rows opened by the current boot are never touched, so reconciliation
  cannot race a registration accepted by the current boot.
  """
  @enforce_keys [:platform_context, :control_plane_boot_id, :ended_at]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.PruneRunnerSessions do
  @moduledoc "Deletes a bounded batch of closed runner sessions older than the cutoff."
  @enforce_keys [:platform_context, :older_than]
  defstruct @enforce_keys ++ [limit: 10_000]
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Results.RunnerSession do
  @moduledoc "Durable runner session state returned by persistence."
  defstruct [
    :session_id,
    :runner_instance_id,
    :runner_boot_id,
    :session_generation,
    :control_plane_boot_id,
    :runner_pool,
    :required_runner_release_id,
    :beam_node,
    :protocol_version,
    :lifecycle_mode,
    :registered_at,
    :ended_at,
    :end_reason,
    :busy_at_exit,
    :interrupted_task_workspace_id,
    :interrupted_task_id,
    :inserted_at,
    task_counts: %{}
  ]

  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Results.RunnerSessionWindowTotals do
  @moduledoc """
  Aggregate awake and busy time over one window.

  Busy time sums completed final assignments (tasks whose `assigned_at`
  survived to the terminal state) clamped to the window; work performed
  under an earlier expired assignment is not counted.
  """
  @enforce_keys [:session_count, :awake_ms, :busy_ms]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          session_count: non_neg_integer(),
          awake_ms: non_neg_integer(),
          busy_ms: non_neg_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Results.WorkspaceRunnerTaskStats do
  @moduledoc "Bounded workspace task counters for the runners page header."
  @enforce_keys [:queued_count, :active_count, :failed_count, :oldest_queued_at]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          queued_count: non_neg_integer(),
          active_count: non_neg_integer(),
          failed_count: non_neg_integer(),
          oldest_queued_at: DateTime.t() | nil
        }
end
