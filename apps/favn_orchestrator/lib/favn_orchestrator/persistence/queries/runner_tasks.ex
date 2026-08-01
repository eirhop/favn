defmodule FavnOrchestrator.Persistence.Queries.GetRunnerTask do
  @moduledoc "Reads one task by its workspace-scoped identity."
  @enforce_keys [:workspace_context, :task_id]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Queries.PageRunRunnerTasks do
  @moduledoc "Pages active or historical runner tasks for one run."
  @enforce_keys [:workspace_context, :run_id]
  defstruct @enforce_keys ++ [statuses: :all, limit: 100, cursor: nil]
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Queries.PageWorkspaceRunnerTasks do
  @moduledoc "Pages the most recent durable runner tasks for one workspace."
  @enforce_keys [:workspace_context]
  defstruct @enforce_keys ++ [statuses: :all, limit: 50, cursor: nil]
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Queries.GetRunnerCapacityDemand do
  @moduledoc "Reads exact platform-global O(1) demand for one pool and release."
  @enforce_keys [:platform_context, :runner_pool, :required_runner_release_id]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Queries.ListRunnerCapacityDemands do
  @moduledoc "Lists a bounded platform-global capacity snapshot for diagnostics."
  @enforce_keys [:platform_context]
  defstruct @enforce_keys ++ [limit: 256]
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Queries.GetRunnerReleaseDrain do
  @moduledoc """
  Reads durable drain blockers for one exact pool/release partition.

  This includes non-terminal runner tasks plus durable domain continuations
  that may enqueue another task after a temporary zero-demand gap.
  """
  @enforce_keys [:platform_context, :runner_pool, :required_runner_release_id]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Queries.GetRunnerCapacityHealth do
  @moduledoc "Aggregates health over every durable runner capacity partition."
  @enforce_keys [:platform_context]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Queries.ListRunnerReleaseDrains do
  @moduledoc "Lists a bounded durable drain snapshot for known pool/release partitions."
  @enforce_keys [:platform_context]
  defstruct @enforce_keys ++ [limit: 256]
  @type t :: %__MODULE__{}
end
