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

defmodule FavnOrchestrator.Persistence.Queries.GetRunSubmissionByRunId do
  @moduledoc "Fetches one durable submission by its workspace-unique run identity."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :run_id]
  defstruct [:workspace_context, :run_id]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetRunSubmissionStats do
  @moduledoc """
  Reads bounded operator diagnostics for one workspace's submission lifecycle.

  The result is an aggregate only: it does not expose intent or authority
  payloads and is not used on the runner capacity path.
  """

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context]
  defstruct [:workspace_context]

  @type t :: %__MODULE__{workspace_context: WorkspaceContext.t()}
end

defmodule FavnOrchestrator.Persistence.Queries.PageClaimableRunSubmissionWorkspaces do
  @moduledoc """
  Keyset-pages workspaces that currently have queued or lease-expired submissions.

  Workspace ordering is deliberately stable and unweighted. The process-local
  coordinator advances and wraps this cursor to provide non-starving
  round-robin selection without making PostgreSQL cluster membership state.
  """

  alias FavnOrchestrator.Persistence.PlatformContext

  @enforce_keys [:platform_context]
  defstruct [:platform_context, :after, limit: 100]

  @type t :: %__MODULE__{
          platform_context: PlatformContext.t(),
          after: String.t() | nil,
          limit: 1..200
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunSubmissionWorkspacePage do
  @moduledoc "Bounded keyset page of workspaces with claimable run submissions."

  @enforce_keys [:workspace_ids, :has_more?]
  defstruct [:next, :has_more?, workspace_ids: []]

  @type t :: %__MODULE__{
          workspace_ids: [String.t()],
          has_more?: boolean(),
          next: String.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunSubmissionStats do
  @moduledoc "Aggregate operator visibility for one workspace's durable submission lifecycle."

  @enforce_keys [
    :total,
    :counts,
    :failure_counts,
    :queued_depth,
    :active_depth,
    :retrying_depth,
    :cancellation_requested_depth,
    :observed_at
  ]
  defstruct [
    :oldest_queued_at,
    :oldest_queued_age_ms,
    :observed_at,
    total: 0,
    counts: %{},
    failure_counts: %{},
    queued_depth: 0,
    active_depth: 0,
    retrying_depth: 0,
    cancellation_requested_depth: 0
  ]

  @type t :: %__MODULE__{
          total: non_neg_integer(),
          counts: %{
            optional(FavnOrchestrator.Persistence.Results.RunSubmission.status()) =>
              non_neg_integer()
          },
          failure_counts: %{optional(:safe | :permanent | :unknown) => non_neg_integer()},
          queued_depth: non_neg_integer(),
          active_depth: non_neg_integer(),
          retrying_depth: non_neg_integer(),
          cancellation_requested_depth: non_neg_integer(),
          oldest_queued_at: DateTime.t() | nil,
          oldest_queued_age_ms: non_neg_integer() | nil,
          observed_at: DateTime.t()
        }
end
