defmodule FavnOrchestrator.Persistence.Commands.LogEntry do
  @moduledoc "One untrusted operational log input normalized at the persistence boundary."

  @enforce_keys [:source, :level, :message, :occurred_at]
  defstruct [:source, :level, :message, :occurred_at, :run_id, metadata: %{}]

  @type t :: %__MODULE__{
          source: String.t(),
          level: :debug | :info | :warning | :error,
          message: String.t(),
          occurred_at: DateTime.t(),
          run_id: String.t() | nil,
          metadata: map()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.AppendLogBatch do
  @moduledoc "Appends one bounded, redacted, deduplicated log batch."

  alias FavnOrchestrator.Persistence.Commands.LogEntry
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :command_id, :batch_id, :entries, :occurred_at]
  defstruct [:workspace_context, :command_id, :batch_id, :entries, :occurred_at]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          batch_id: String.t(),
          entries: [LogEntry.t()],
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageLogs do
  @moduledoc "Keyset-pages one explicit indexed operational-log filter."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :filter]
  defstruct [
    :workspace_context,
    :filter,
    :after,
    direction: :older,
    limit: 100
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          filter: map(),
          after:
            %{occurred_at: DateTime.t(), log_id: pos_integer()}
            | %{publication_id: non_neg_integer(), batch_offset: non_neg_integer()}
            | nil,
          direction: :older | :newer,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Commands.PurgeLogs do
  @moduledoc "Deletes one bounded retention batch before a cutoff."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :cutoff]
  defstruct [:workspace_context, :cutoff, limit: 1_000]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          cutoff: DateTime.t(),
          limit: 1..5_000
        }
end

defmodule FavnOrchestrator.Persistence.Results.LogEntry do
  @moduledoc "Redacted committed operational log entry."
  @enforce_keys [
    :log_id,
    :workspace_id,
    :batch_id,
    :position,
    :publication_id,
    :source,
    :level,
    :message,
    :occurred_at
  ]
  defstruct [
    :log_id,
    :workspace_id,
    :batch_id,
    :position,
    :publication_id,
    :run_id,
    :source,
    :level,
    :message,
    :metadata,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          log_id: pos_integer(),
          workspace_id: String.t(),
          batch_id: String.t(),
          position: non_neg_integer(),
          publication_id: pos_integer() | nil,
          run_id: String.t() | nil,
          source: String.t(),
          level: :debug | :info | :warning | :error,
          message: String.t(),
          metadata: map(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.PurgeResult do
  @moduledoc "One bounded retention deletion outcome."
  @enforce_keys [:deleted_count]
  defstruct [:deleted_count, :last_id]

  @type t :: %__MODULE__{deleted_count: non_neg_integer(), last_id: pos_integer() | nil}
end
