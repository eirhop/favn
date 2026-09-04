defmodule FavnOrchestrator.Persistence.Results.CancellationScope do
  @moduledoc "The submitted operation targeted by the operator's Cancel action."
  @enforce_keys [:run_id, :kind, :cancellable?]
  defstruct [:run_id, :kind, :status, :cancellable?, :label]

  @type t :: %__MODULE__{
          run_id: String.t(),
          kind: :run | :backfill,
          status: nil | :cancelling | :cancelled | :needs_attention,
          cancellable?: boolean(),
          label: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageCancellingOperations do
  @moduledoc "One bounded page of operations whose cancellation needs reconciliation."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :kind]
  defstruct [:workspace_context, :kind, :after, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          kind: :run | :backfill,
          after: String.t() | nil,
          limit: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Results.CancellationWork do
  @moduledoc "Bounded remaining run and task identities; all local cleanup is already committed."
  defstruct run_ids: [], task_ids: []
  @type t :: %__MODULE__{run_ids: [String.t()], task_ids: [String.t()]}
end
