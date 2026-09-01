defmodule FavnOrchestrator.Persistence.Queries.PageRunnerSessions do
  @moduledoc """
  Pages durable runner sessions, newest registration first.

  `overlapping_after` keeps every session whose lifetime overlaps the window
  starting there (still-open sessions always overlap). `states` filters on
  `:connected` (still open), `:shut_down`, `:crashed`, or `:presumed_dead`.
  """
  @enforce_keys [:platform_context]
  defstruct @enforce_keys ++
              [overlapping_after: nil, states: :all, limit: 50, include_task_counts: true]

  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Queries.GetRunnerSessionWindowTotals do
  @moduledoc "Aggregates awake and busy time over one bounded window."
  @enforce_keys [:platform_context, :window_start, :window_end]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Queries.PageRunnerSessionTasks do
  @moduledoc """
  Pages one workspace's durable tasks attributed to one runner session.

  Attribution joins the assigned runner instance and session generation and
  clamps to the session interval, so a resumed generation cannot leak tasks
  across session rows.
  """
  @enforce_keys [:workspace_context, :runner_instance_id, :session_generation, :registered_at]
  defstruct @enforce_keys ++ [ended_at: nil, statuses: [:failed, :unknown], limit: 20]
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Queries.GetWorkspaceRunnerTaskStats do
  @moduledoc "Reads bounded workspace task counters for the runners page header."
  @enforce_keys [:workspace_context, :failed_since]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end
