defmodule FavnOrchestrator.Persistence.Commands.BackfillPlanWindow do
  @moduledoc "One deterministic window in a resumable backfill plan."

  @enforce_keys [:window_id, :window_key, :window_start, :window_end]
  defstruct [:window_id, :window_key, :window_start, :window_end, payload: %{}]

  @type t :: %__MODULE__{
          window_id: String.t(),
          window_key: String.t(),
          window_start: DateTime.t(),
          window_end: DateTime.t(),
          payload: map()
        }
end

defmodule FavnOrchestrator.Persistence.BackfillPlan do
  @moduledoc "Stable canonical hashing helpers for resumable backfill planning."

  alias FavnOrchestrator.Persistence.Commands.BackfillPlanWindow

  @doc "Returns the SHA-256 hash of one exact ordered plan batch."
  @spec batch_hash([BackfillPlanWindow.t()]) :: binary()
  def batch_hash(windows) when is_list(windows) do
    windows
    |> Enum.map(fn %BackfillPlanWindow{} = window ->
      %{
        "window_id" => window.window_id,
        "window_key" => window.window_key,
        "window_start" => DateTime.to_iso8601(window.window_start),
        "window_end" => DateTime.to_iso8601(window.window_end),
        "payload" => window.payload
      }
    end)
    |> canonical_hash()
  end

  @doc "Returns the SHA-256 hash of the exact ordered list of batch hashes."
  @spec plan_hash([binary()]) :: binary()
  def plan_hash(batch_hashes) when is_list(batch_hashes) do
    batch_hashes
    |> Enum.map(&Base.url_encode64(&1, padding: false))
    |> canonical_hash()
  end

  defp canonical_hash(value) do
    value
    |> ordered()
    |> Jason.encode!()
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp ordered(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp ordered(%_{} = struct), do: struct |> Map.from_struct() |> ordered()

  defp ordered(map) when is_map(map) do
    map
    |> Enum.map(fn {key, value} -> {to_string(key), ordered(value)} end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Jason.OrderedObject.new()
  end

  defp ordered(values) when is_list(values), do: Enum.map(values, &ordered/1)
  defp ordered(value) when is_atom(value) and not is_nil(value), do: Atom.to_string(value)
  defp ordered(value), do: value
end

defmodule FavnOrchestrator.Persistence.Commands.StartBackfillPlan do
  @moduledoc "Creates an idempotent resumable backfill planning header."

  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :backfill_id,
    :root_run_id,
    :deployment_id,
    :manifest_version_id,
    :target_kind,
    :target_id,
    :range_start,
    :range_end,
    :expected_window_count,
    :expected_batch_count,
    :plan_hash,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :backfill_id,
    :root_run_id,
    :deployment_id,
    :manifest_version_id,
    :target_kind,
    :target_id,
    :range_start,
    :range_end,
    :expected_window_count,
    :expected_batch_count,
    :plan_hash,
    :occurred_at,
    :idempotency,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          backfill_id: String.t(),
          root_run_id: String.t(),
          deployment_id: String.t(),
          manifest_version_id: String.t(),
          target_kind: :asset | :pipeline,
          target_id: String.t(),
          range_start: DateTime.t(),
          range_end: DateTime.t(),
          expected_window_count: non_neg_integer(),
          expected_batch_count: non_neg_integer(),
          plan_hash: binary(),
          occurred_at: DateTime.t(),
          metadata: map(),
          idempotency: CommandIdempotency.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Commands.AppendBackfillPlanBatch do
  @moduledoc "Appends one immutable deterministic batch and receipt to a backfill plan."

  alias FavnOrchestrator.Persistence.Commands.BackfillPlanWindow
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :backfill_id,
    :batch_index,
    :batch_hash,
    :windows,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :backfill_id,
    :batch_index,
    :batch_hash,
    :windows,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          backfill_id: String.t(),
          batch_index: non_neg_integer(),
          batch_hash: binary(),
          windows: [BackfillPlanWindow.t()],
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ActivateBackfillPlan do
  @moduledoc "Verifies every backfill plan receipt and makes its windows dispatchable."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :command_id, :backfill_id, :expected_version, :occurred_at]
  defstruct [:workspace_context, :command_id, :backfill_id, :expected_version, :occurred_at]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          backfill_id: String.t(),
          expected_version: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimBackfillWindows do
  @moduledoc "Claims a bounded queue of dispatchable backfill windows."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :batch_id, :owner_id, :lease_duration_ms]
  defstruct [
    :workspace_context,
    :batch_id,
    :owner_id,
    :lease_duration_ms,
    :backfill_id,
    limit: 100
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          batch_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer(),
          backfill_id: String.t() | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Commands.TransitionBackfillWindow do
  @moduledoc "Applies one fenced version-guarded backfill-window transition."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :backfill_id,
    :window_id,
    :owner_id,
    :fencing_token,
    :expected_version,
    :status,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :backfill_id,
    :window_id,
    :owner_id,
    :fencing_token,
    :expected_version,
    :status,
    :run_id,
    :error,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          backfill_id: String.t(),
          window_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          expected_version: pos_integer(),
          status: :running | :succeeded | :failed | :cancelled,
          run_id: String.t() | nil,
          error: map() | nil,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetBackfill do
  @moduledoc "Fetches one authoritative backfill header and projected progress."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :backfill_id]
  defstruct [:workspace_context, :backfill_id]

  @type t :: %__MODULE__{workspace_context: WorkspaceContext.t(), backfill_id: String.t()}
end

defmodule FavnOrchestrator.Persistence.Queries.PageBackfillWindows do
  @moduledoc "Keyset-pages windows for one backfill and optional status."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :backfill_id]
  defstruct [:workspace_context, :backfill_id, :status, :after, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          backfill_id: String.t(),
          status: atom() | nil,
          after: %{window_key: String.t(), window_id: String.t()} | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageAssetWindows do
  @moduledoc "Keyset-pages exact manifest/asset/window history."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :manifest_version_id, :target_id]
  defstruct [
    :workspace_context,
    :manifest_version_id,
    :target_id,
    :after,
    limit: 100
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          manifest_version_id: String.t(),
          target_id: String.t(),
          after: %{window_start: DateTime.t(), window_id: String.t()} | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Results.Backfill do
  @moduledoc "Authoritative resumable backfill header with compact projected progress."

  @enforce_keys [
    :workspace_id,
    :backfill_id,
    :root_run_id,
    :deployment_id,
    :manifest_version_id,
    :target_kind,
    :target_id,
    :status,
    :expected_window_count,
    :expected_batch_count,
    :version
  ]
  defstruct [
    :workspace_id,
    :backfill_id,
    :root_run_id,
    :deployment_id,
    :manifest_version_id,
    :target_kind,
    :target_id,
    :range_start,
    :range_end,
    :status,
    :expected_window_count,
    :expected_batch_count,
    :appended_window_count,
    :appended_batch_count,
    :version,
    :metadata,
    :progress
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          backfill_id: String.t(),
          root_run_id: String.t(),
          deployment_id: String.t(),
          manifest_version_id: String.t(),
          target_kind: :asset | :pipeline,
          target_id: String.t(),
          range_start: DateTime.t(),
          range_end: DateTime.t(),
          status: atom(),
          expected_window_count: non_neg_integer(),
          expected_batch_count: non_neg_integer(),
          appended_window_count: non_neg_integer(),
          appended_batch_count: non_neg_integer(),
          version: pos_integer(),
          metadata: map(),
          progress: map() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Results.BackfillWindow do
  @moduledoc "Authoritative fenced backfill window."

  @enforce_keys [
    :workspace_id,
    :backfill_id,
    :window_id,
    :window_key,
    :status,
    :version
  ]
  defstruct [
    :workspace_id,
    :backfill_id,
    :window_id,
    :window_key,
    :window_start,
    :window_end,
    :status,
    :claim_owner,
    :fencing_token,
    :claim_expires_at,
    :run_id,
    :attempt_count,
    :last_error,
    :payload,
    :version
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          backfill_id: String.t(),
          window_id: String.t(),
          window_key: String.t(),
          window_start: DateTime.t(),
          window_end: DateTime.t(),
          status: atom(),
          claim_owner: String.t() | nil,
          fencing_token: non_neg_integer(),
          claim_expires_at: DateTime.t() | nil,
          run_id: String.t() | nil,
          attempt_count: non_neg_integer(),
          last_error: map() | nil,
          payload: map(),
          version: pos_integer()
        }
end
