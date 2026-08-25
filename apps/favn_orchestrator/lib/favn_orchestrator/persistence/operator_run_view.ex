defmodule FavnOrchestrator.Persistence.Queries.GetRunFlow do
  @moduledoc "Reads one bounded Flow snapshot for an exact run."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :run_id]
  defstruct [:workspace_context, :run_id, limit: 1_000]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          limit: 1..1_000
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetRunHeader do
  @moduledoc "Reads one lean exact-run header with aggregate counts."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :run_id]
  defstruct @enforce_keys

  @type t :: %__MODULE__{workspace_context: WorkspaceContext.t(), run_id: String.t()}
end

defmodule FavnOrchestrator.Persistence.Queries.ListRunWindows do
  @moduledoc "Lists lean window-run choices related to one exact run."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :run_id]
  defstruct [:workspace_context, :run_id, limit: 1_000]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          limit: 1..1_000
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetRunAssetAttempt do
  @moduledoc "Reads one exact asset attempt and its detail payloads."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :run_id, :asset_step_id]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          asset_step_id: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.ListRunEventSummaries do
  @moduledoc "Lists bounded display fields for one exact run's events."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :run_id]
  defstruct [:workspace_context, :run_id, limit: 200]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          limit: 1..200
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunViewHeader do
  @moduledoc "Lean exact-run header and aggregate counts."

  @enforce_keys [:run_id, :root_run_id, :status, :started_at, :updated_at, :counts]
  defstruct [
    :run_id,
    :root_run_id,
    :parent_run_id,
    :rerun_of_run_id,
    :manifest_version_id,
    :status,
    :submit_kind,
    :trigger_type,
    :event_sequence,
    :started_at,
    :updated_at,
    :finished_at,
    :target_id,
    :target_label,
    :window_start_at,
    :window_end_at,
    :counts
  ]

  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Results.RunFlowCandidate do
  @moduledoc "Minimal observed Flow row after bounded storage selection."

  @enforce_keys [:run_id, :asset_step_id, :asset_ref, :status]
  defstruct @enforce_keys ++ [:window_identity, :started_at, :finished_at, :stage]

  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Results.RunFlowSnapshot do
  @moduledoc """
  Atomic exact-run header plus bounded observed candidates.

  Candidates are the run's asset attempts. Steps the plan anticipates but that
  execution has not admitted yet are absent by design: an attempt row exists
  from the moment a step is queued, and reading the persisted plan to show the
  rest cost a full parse of it on every refresh.
  """

  alias FavnOrchestrator.Persistence.Results.RunFlowCandidate
  alias FavnOrchestrator.Persistence.Results.RunViewHeader

  @enforce_keys [:header, :observed, :overflow?]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          header: RunViewHeader.t(),
          observed: [RunFlowCandidate.t()],
          overflow?: boolean()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunWindowChoice do
  @moduledoc """
  Lean navigation choice for one persisted window run.

  `kind` and `timezone` are recovered from the stored window key. A key that
  predates the current encoding yields `nil` for both; callers must label such a
  choice by its timestamps rather than by its calendar position.
  """

  alias Favn.Window.Key, as: WindowKey

  @enforce_keys [:run_id, :window_start_at, :window_end_at]
  defstruct @enforce_keys ++ [:status, :kind, :timezone]

  @type t :: %__MODULE__{
          run_id: String.t(),
          window_start_at: DateTime.t(),
          window_end_at: DateTime.t(),
          status: atom() | nil,
          kind: WindowKey.kind() | nil,
          timezone: String.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunWindowChoices do
  @moduledoc """
  Bounded window choices with an explicit overflow marker.

  `backfill_status` is the owning backfill's own status, not an aggregate of the
  returned choices. A non-terminal backfill is still producing window runs, so
  the choice list is incomplete by design rather than by truncation.
  """

  alias FavnOrchestrator.Persistence.Results.RunWindowChoice

  @enforce_keys [:items, :overflow?]
  defstruct @enforce_keys ++ [:backfill_status]

  @type t :: %__MODULE__{
          items: [RunWindowChoice.t()],
          overflow?: boolean(),
          backfill_status: atom() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunAssetAttempt do
  @moduledoc "One exact asset attempt with full detail payloads."

  @enforce_keys [:run_id, :asset_step_id, :asset_ref, :status]
  defstruct [
    :run_id,
    :asset_step_id,
    :asset_ref,
    :status,
    :started_at,
    :finished_at,
    :duration_ms,
    :attempt_number,
    :stage,
    :execution_pool,
    :queue_reason,
    :window,
    :error,
    :output_metadata
  ]

  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Results.RunEventSummary do
  @moduledoc "Only the fields rendered by the selected-run Events screen."

  @enforce_keys [:run_id, :sequence, :occurred_at, :event_type]
  defstruct [:run_id, :sequence, :occurred_at, :event_type, :status, :asset_ref, :summary]

  @type t :: %__MODULE__{}
end
