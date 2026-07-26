defmodule FavnOrchestrator.Persistence.Queries.GetRunSubmission do
  @moduledoc "Loads one workspace-scoped durable run submission."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :submission_id]
  defstruct [:workspace_context, :submission_id]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          submission_id: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageRunSubmissions do
  @moduledoc "Keyset-pages durable run submissions newest first."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context]
  defstruct [:workspace_context, :status, :after, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          status: FavnOrchestrator.Persistence.Results.RunSubmission.status() | nil,
          after: %{inserted_at: DateTime.t(), submission_id: String.t()} | nil,
          limit: 1..200
        }
end
