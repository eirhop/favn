defmodule FavnOrchestrator.Persistence.Commands.ClaimDueSchedules do
  @moduledoc "Claims a bounded due-schedule batch for one workspace."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :batch_id, :owner_id, :lease_duration_ms]
  defstruct [:workspace_context, :batch_id, :owner_id, :lease_duration_ms, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          batch_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer(),
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageSchedules do
  @moduledoc "Keyset-pages active-deployment schedule definitions and cursors."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context]
  defstruct [:workspace_context, :pipeline_target_id, :schedule_id, :after, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          pipeline_target_id: String.t() | nil,
          schedule_id: String.t() | nil,
          after: %{pipeline_target_id: String.t(), schedule_id: String.t()} | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageScheduleOccurrences do
  @moduledoc "Keyset-pages occurrence history for one exact active-deployment schedule."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :pipeline_target_id, :schedule_id]
  defstruct [:workspace_context, :pipeline_target_id, :schedule_id, :after, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          pipeline_target_id: String.t(),
          schedule_id: String.t(),
          after: %{due_at: DateTime.t(), occurrence_id: String.t()} | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ScheduleOccurrenceIntent do
  @moduledoc "One deterministic schedule occurrence persisted before dispatch."
  @enforce_keys [:occurrence_id, :due_at, :payload]
  defstruct [:occurrence_id, :due_at, :payload]
  @type t :: %__MODULE__{occurrence_id: String.t(), due_at: DateTime.t(), payload: map()}
end

defmodule FavnOrchestrator.Persistence.Commands.CommitScheduleEvaluation do
  @moduledoc "Atomically persists deterministic occurrence intents and advances one claimed cursor."
  alias FavnOrchestrator.Persistence.Commands.ScheduleOccurrenceIntent
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :deployment_id,
    :pipeline_target_id,
    :schedule_id,
    :owner_id,
    :claim_generation,
    :expected_version,
    :next_due_at,
    :cursor,
    :occurrences,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :deployment_id,
    :pipeline_target_id,
    :schedule_id,
    :owner_id,
    :claim_generation,
    :expected_version,
    :next_due_at,
    :cursor,
    :occurrences,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          deployment_id: String.t(),
          pipeline_target_id: String.t(),
          schedule_id: String.t(),
          owner_id: String.t(),
          claim_generation: pos_integer(),
          expected_version: pos_integer(),
          next_due_at: DateTime.t(),
          cursor: map(),
          occurrences: [ScheduleOccurrenceIntent.t()],
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimScheduleOccurrences do
  @moduledoc "Claims a bounded queue of undispatched schedule occurrences."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :batch_id, :owner_id, :lease_duration_ms]
  defstruct [:workspace_context, :batch_id, :owner_id, :lease_duration_ms, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          batch_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer(),
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Commands.CompleteScheduleOccurrence do
  @moduledoc "Completes one fenced occurrence by linking its run or recording a bounded failure."
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :occurrence_id,
    :owner_id,
    :claim_generation,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :occurrence_id,
    :owner_id,
    :claim_generation,
    :run_id,
    :error,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          occurrence_id: String.t(),
          owner_id: String.t(),
          claim_generation: pos_integer(),
          run_id: String.t() | nil,
          error: map() | nil,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.ScheduleClaim do
  @moduledoc "Fenced claim over one due schedule cursor."
  @enforce_keys [
    :workspace_id,
    :deployment_id,
    :pipeline_target_id,
    :schedule_id,
    :next_due_at,
    :cursor,
    :version,
    :owner_id,
    :claim_generation,
    :claim_expires_at
  ]
  defstruct [
    :workspace_id,
    :deployment_id,
    :pipeline_target_id,
    :schedule_id,
    :next_due_at,
    :cursor,
    :version,
    :owner_id,
    :claim_generation,
    :claim_expires_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          deployment_id: String.t(),
          pipeline_target_id: String.t(),
          schedule_id: String.t(),
          next_due_at: DateTime.t(),
          cursor: map(),
          version: pos_integer(),
          owner_id: String.t(),
          claim_generation: pos_integer(),
          claim_expires_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.Schedule do
  @moduledoc "Immutable deployed schedule definition with its mutable evaluation cursor."
  @enforce_keys [
    :workspace_id,
    :deployment_id,
    :pipeline_target_id,
    :schedule_id,
    :schedule_fingerprint,
    :definition,
    :next_due_at,
    :cursor,
    :version,
    :updated_at
  ]
  defstruct [
    :workspace_id,
    :deployment_id,
    :pipeline_target_id,
    :schedule_id,
    :schedule_fingerprint,
    :definition,
    :next_due_at,
    :cursor,
    :version,
    :claim_owner,
    :claim_expires_at,
    :updated_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          deployment_id: String.t(),
          pipeline_target_id: String.t(),
          schedule_id: String.t(),
          schedule_fingerprint: String.t(),
          definition: map(),
          next_due_at: DateTime.t(),
          cursor: map(),
          version: pos_integer(),
          claim_owner: String.t() | nil,
          claim_expires_at: DateTime.t() | nil,
          updated_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.ScheduleOccurrence do
  @moduledoc "Durable deterministic schedule occurrence intent."
  @enforce_keys [
    :workspace_id,
    :occurrence_id,
    :deployment_id,
    :pipeline_target_id,
    :schedule_id,
    :due_at,
    :payload,
    :status,
    :claim_generation
  ]
  defstruct [
    :workspace_id,
    :occurrence_id,
    :deployment_id,
    :pipeline_target_id,
    :schedule_id,
    :due_at,
    :payload,
    :status,
    :claim_owner,
    :claim_generation,
    :claim_expires_at,
    :run_id,
    :attempt_count,
    :last_error
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          occurrence_id: String.t(),
          deployment_id: String.t(),
          pipeline_target_id: String.t(),
          schedule_id: String.t(),
          due_at: DateTime.t(),
          payload: map(),
          status: atom(),
          claim_owner: String.t() | nil,
          claim_generation: non_neg_integer(),
          claim_expires_at: DateTime.t() | nil,
          run_id: String.t() | nil,
          attempt_count: non_neg_integer(),
          last_error: map() | nil
        }
end
