defmodule FavnOrchestrator.Persistence.Commands.RunTarget do
  @moduledoc "One normalized target membership pinned to a run."

  @enforce_keys [:target_kind, :target_id, :target_module, :is_primary]
  defstruct [:target_kind, :target_id, :target_module, :target_name, :is_primary]

  @type t :: %__MODULE__{
          target_kind: :asset | :pipeline,
          target_id: String.t(),
          target_module: String.t(),
          target_name: String.t() | nil,
          is_primary: boolean()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.CreateRun do
  @moduledoc "Atomically creates a run snapshot, first event, targets, ownership root, and outbox event."

  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Commands.RunTarget
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunState

  @enforce_keys [
    :workspace_context,
    :command_id,
    :deployment_id,
    :run,
    :targets,
    :event
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :deployment_id,
    :run,
    :targets,
    :event,
    :idempotency
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          deployment_id: String.t(),
          run: RunState.t(),
          targets: [RunTarget.t()],
          event: map(),
          idempotency: CommandIdempotency.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Commands.CommitRunTransition do
  @moduledoc "Commits one guarded run snapshot/event mutation and its durable outbox record."

  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunState

  @enforce_keys [:workspace_context, :command_id, :expected_sequence, :run, :event]
  defstruct [
    :workspace_context,
    :command_id,
    :expected_sequence,
    :owner_id,
    :fencing_token,
    :run,
    :event,
    :idempotency
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          expected_sequence: pos_integer(),
          owner_id: String.t() | nil,
          fencing_token: pos_integer() | nil,
          run: RunState.t(),
          event: map(),
          idempotency: CommandIdempotency.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RequestRunCancellation do
  @moduledoc "Atomically records an operator cancellation request and its outbox event."

  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :command_id, :run_id, :reason, :occurred_at]
  defstruct [:workspace_context, :command_id, :run_id, :reason, :occurred_at, :idempotency]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          run_id: String.t(),
          reason: map(),
          occurred_at: DateTime.t(),
          idempotency: CommandIdempotency.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Commands.PinRuntimeInputs do
  @moduledoc "Atomically persists immutable, encrypted runtime-input pins for one run."

  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :command_id, :run_id, :pins]
  defstruct [:workspace_context, :command_id, :run_id, :pins]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          run_id: String.t(),
          pins: [Pin.t()]
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetRun do
  @moduledoc "Fetches one run under a workspace authority boundary."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :run_id]
  defstruct [:workspace_context, :run_id]
  @type t :: %__MODULE__{workspace_context: WorkspaceContext.t(), run_id: String.t()}
end

defmodule FavnOrchestrator.Persistence.Queries.PageRuns do
  @moduledoc "Bounded keyset query for workspace or explicitly authorized platform run history."

  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:scope]
  defstruct [
    :scope,
    :after,
    :manifest_version_id,
    :root_execution_group_id,
    :status,
    limit: 50
  ]

  @type cursor :: %{required(:latest_event_id) => pos_integer(), required(:run_id) => String.t()}
  @type t :: %__MODULE__{
          scope: WorkspaceContext.t() | PlatformContext.t(),
          after: cursor() | nil,
          manifest_version_id: String.t() | nil,
          root_execution_group_id: String.t() | nil,
          status: atom() | nil,
          limit: 1..200
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageRunEvents do
  @moduledoc "Bounded canonical event query for one run or execution group."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context]
  defstruct [
    :workspace_context,
    :run_id,
    :root_execution_group_id,
    :after_sequence,
    :after_event_id,
    :before_event_id,
    :order,
    :event_types,
    limit: 50
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t() | nil,
          root_execution_group_id: String.t() | nil,
          after_sequence: non_neg_integer() | nil,
          after_event_id: non_neg_integer() | nil,
          before_event_id: non_neg_integer() | nil,
          order: :asc | :desc | nil,
          event_types: [String.t() | atom()] | nil,
          limit: 1..200
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PagePublishedRunEvents do
  @moduledoc """
  Bounded publication-ordered run-event query for durable change feeds.

  The publication cursor is assigned only after commit by the outbox sequencer;
  PostgreSQL identity values are never accepted as replay watermarks.
  """

  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:scope]
  defstruct [:scope, :after_publication_id, :event_types, limit: 50]

  @type t :: %__MODULE__{
          scope: WorkspaceContext.t() | PlatformContext.t(),
          after_publication_id: non_neg_integer() | nil,
          event_types: [String.t() | atom()] | nil,
          limit: 1..200
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetRuntimeInputs do
  @moduledoc "Fetches a bounded set of runtime-input pins for one workspace run."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :run_id]
  defstruct [:workspace_context, :run_id, :node_keys]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          node_keys: [Favn.Plan.node_key()] | nil
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunCommitted do
  @moduledoc "Complete result of one committed or exactly replayed run write."

  alias FavnOrchestrator.RunState

  @enforce_keys [:run, :event, :event_id, :outbox_event_id, :replayed?]
  defstruct [:run, :event, :event_id, :outbox_event_id, :replayed?]

  @type t :: %__MODULE__{
          run: RunState.t(),
          event: map(),
          event_id: pos_integer(),
          outbox_event_id: pos_integer(),
          replayed?: boolean()
        }
end

defmodule FavnOrchestrator.Persistence.Results.CursorPage do
  @moduledoc "Bounded keyset page used by Persistence V2 reads."

  @enforce_keys [:items, :limit, :has_more?]
  defstruct [:items, :limit, :has_more?, :next_cursor]

  @type t(item) :: %__MODULE__{
          items: [item],
          limit: pos_integer(),
          has_more?: boolean(),
          next_cursor: map() | nil
        }
end
