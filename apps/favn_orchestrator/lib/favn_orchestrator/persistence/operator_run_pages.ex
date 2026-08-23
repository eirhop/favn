defmodule FavnOrchestrator.Persistence.Queries.PageRunWindows do
  @moduledoc "Keyset-pages lean window-run summaries for one execution group."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :run_id]
  defstruct [:workspace_context, :run_id, :after, limit: 50]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          after: %{window_start_at: DateTime.t(), window_id: String.t()} | nil,
          limit: 1..50
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageRunEventSummaries do
  @moduledoc "Keyset-pages payload-free event summaries for one execution group."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :run_id]
  defstruct [:workspace_context, :run_id, :after_event_id, limit: 50]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          after_event_id: pos_integer() | nil,
          limit: 1..50
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunWindowSummary do
  @moduledoc "Lean window-run row with exact asset outcome counts."
  @enforce_keys [:window_id, :status, :counts]
  defstruct [
    :window_id,
    :run_id,
    :status,
    :window_start_at,
    :window_end_at,
    :asset_ref,
    :duration_ms,
    :counts
  ]

  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Results.RunEventSummary do
  @moduledoc "Payload-free safe summary of one durable run event."
  @enforce_keys [:event_id, :run_id, :sequence, :occurred_at, :event_type]
  defstruct [
    :event_id,
    :run_id,
    :sequence,
    :occurred_at,
    :event_type,
    :asset_step_id,
    :status,
    :stage,
    :summary
  ]

  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Results.RunSummaryPage do
  @moduledoc "One bounded keyset page and its exact total."
  @enforce_keys [:items, :total, :has_more?]
  defstruct [:items, :total, :has_more?, :next_cursor, projection_cursor: 0]

  @type t(item) :: %__MODULE__{
          items: [item],
          total: non_neg_integer(),
          has_more?: boolean(),
          next_cursor: map() | integer() | nil,
          projection_cursor: non_neg_integer()
        }
end
