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

defmodule FavnOrchestrator.Persistence.Queries.GetRunnerCapacityDemand do
  @moduledoc "Reads exact platform-global O(1) demand for one pool and release."
  @enforce_keys [:platform_context, :runner_pool, :required_runner_release_id]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end
