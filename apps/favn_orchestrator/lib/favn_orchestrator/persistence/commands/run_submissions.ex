defmodule FavnOrchestrator.Persistence.Commands.EnqueueRunSubmission do
  @moduledoc "Persists one normalized, redacted run-submission intent before asynchronous work."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :submission_id,
    :source,
    :idempotency_key,
    :request_hash,
    :deployment_id,
    :manifest_version_id,
    :target_kind,
    :target_id,
    :run_id,
    :intent,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :submission_id,
    :source,
    :idempotency_key,
    :request_hash,
    :deployment_id,
    :manifest_version_id,
    :target_kind,
    :target_id,
    :run_id,
    :intent,
    :occurred_at,
    :available_at
  ]

  @type source :: :api | :operator | :scheduler | :backfill | :rebuild | :recovery | :child_run
  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          submission_id: String.t(),
          source: source(),
          idempotency_key: String.t(),
          request_hash: <<_::256>>,
          deployment_id: String.t(),
          manifest_version_id: String.t(),
          target_kind: String.t(),
          target_id: String.t(),
          run_id: String.t(),
          intent: map(),
          occurred_at: DateTime.t(),
          available_at: DateTime.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimRunSubmissions do
  @moduledoc "Claims a bounded FIFO batch of available queued submissions for one workspace."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :owner_id,
    :lease_duration_ms,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :owner_id,
    :lease_duration_ms,
    :occurred_at,
    limit: 1
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer(),
          occurred_at: DateTime.t(),
          limit: 1..50
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimStaleRunSubmissions do
  @moduledoc """
  Fences a bounded batch of expired preparing/admitting submissions for reconciliation.

  The command preserves each submission's current status. Recovery policy must
  reconcile admitting work before deciding whether requeue is safe.
  """

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :owner_id,
    :lease_duration_ms,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :owner_id,
    :lease_duration_ms,
    :occurred_at,
    limit: 1
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer(),
          occurred_at: DateTime.t(),
          limit: 1..50
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RenewRunSubmissionClaim do
  @moduledoc "Renews one exact live run-submission claim."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :lease_duration_ms,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :lease_duration_ms,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          submission_id: String.t(),
          owner_id: String.t(),
          claim_generation: pos_integer(),
          lease_duration_ms: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.MarkRunSubmissionAdmitting do
  @moduledoc "Moves one fenced prepared submission into final run admission."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :preparation,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :preparation,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          submission_id: String.t(),
          owner_id: String.t(),
          claim_generation: pos_integer(),
          preparation: map(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.MarkRunSubmissionSubmitted do
  @moduledoc "Links one fenced admitting submission to its durably created run."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :run_id,
    :outcome,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :run_id,
    :outcome,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          submission_id: String.t(),
          owner_id: String.t(),
          claim_generation: pos_integer(),
          run_id: String.t(),
          outcome: map(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.MarkRunSubmissionFailed do
  @moduledoc "Terminates one fenced submission with an explicit retry-safety classification."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :failure_kind,
    :error,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :failure_kind,
    :error,
    :occurred_at
  ]

  @type failure_kind :: :safe | :permanent | :unknown
  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          submission_id: String.t(),
          owner_id: String.t(),
          claim_generation: pos_integer(),
          failure_kind: failure_kind(),
          error: map(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RequeueRunSubmission do
  @moduledoc "Requeues one fenced submission only after policy proves retry is safe."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :reason,
    :available_at,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :reason,
    :available_at,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          submission_id: String.t(),
          owner_id: String.t(),
          claim_generation: pos_integer(),
          reason: map(),
          available_at: DateTime.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RequestRunSubmissionCancellation do
  @moduledoc "Cancels queued work immediately or records a request for the current fenced worker."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :submission_id,
    :reason,
    :occurred_at
  ]
  defstruct [:workspace_context, :command_id, :submission_id, :reason, :occurred_at]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          submission_id: String.t(),
          reason: String.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.AcknowledgeRunSubmissionCancellation do
  @moduledoc "Terminates one claimed submission after its worker acknowledges cancellation."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :submission_id,
    :owner_id,
    :claim_generation,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          submission_id: String.t(),
          owner_id: String.t(),
          claim_generation: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.SupersedeRunSubmission do
  @moduledoc "Terminates queued intent after a replacement submission is durable."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :submission_id,
    :replacement_submission_id,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :submission_id,
    :replacement_submission_id,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          submission_id: String.t(),
          replacement_submission_id: String.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RetryFailedRunSubmission do
  @moduledoc """
  Creates one new linked queued submission from a terminal safe failure.

  `command_id` is the retry-command idempotency key. Repeating it returns the
  same linked submission; a distinct deliberate command may create another
  child with a new run identity, and the failed source row remains immutable.
  """

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :failed_submission_id,
    :submission_id,
    :idempotency_key,
    :run_id,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :failed_submission_id,
    :submission_id,
    :idempotency_key,
    :run_id,
    :occurred_at,
    :available_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          failed_submission_id: String.t(),
          submission_id: String.t(),
          idempotency_key: String.t(),
          run_id: String.t(),
          occurred_at: DateTime.t(),
          available_at: DateTime.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunSubmission do
  @moduledoc "Durable redacted state for one run-submission request."

  alias FavnOrchestrator.Persistence.RunSubmissionAuthority

  @enforce_keys [
    :workspace_id,
    :submission_id,
    :source,
    :idempotency_key,
    :request_hash,
    :authority,
    :deployment_id,
    :manifest_version_id,
    :target_kind,
    :target_id,
    :run_id,
    :intent,
    :status,
    :attempt,
    :claim_generation,
    :retry_root_id,
    :enqueued_at,
    :available_at,
    :inserted_at,
    :updated_at
  ]
  defstruct [
    :workspace_id,
    :submission_id,
    :source,
    :idempotency_key,
    :request_hash,
    :authority,
    :deployment_id,
    :manifest_version_id,
    :target_kind,
    :target_id,
    :run_id,
    :intent,
    :status,
    :attempt,
    :claim_owner,
    :claim_generation,
    :claim_expires_at,
    :preparation,
    :outcome,
    :error,
    :failure_kind,
    :cancellation_requested_at,
    :cancellation_reason,
    :retry_root_id,
    :retry_of_submission_id,
    :retry_command_id,
    :superseded_by_submission_id,
    :enqueued_at,
    :available_at,
    :preparing_at,
    :admitting_at,
    :terminal_at,
    :inserted_at,
    :updated_at
  ]

  @type status ::
          :queued | :preparing | :admitting | :submitted | :failed | :cancelled | :superseded
  @type t :: %__MODULE__{
          workspace_id: String.t(),
          submission_id: String.t(),
          source: atom(),
          idempotency_key: String.t(),
          request_hash: binary(),
          authority: RunSubmissionAuthority.t(),
          deployment_id: String.t(),
          manifest_version_id: String.t(),
          target_kind: String.t(),
          target_id: String.t(),
          run_id: String.t(),
          intent: map(),
          status: status(),
          attempt: non_neg_integer(),
          claim_owner: String.t() | nil,
          claim_generation: non_neg_integer(),
          claim_expires_at: DateTime.t() | nil,
          preparation: map() | nil,
          outcome: map() | nil,
          error: map() | nil,
          failure_kind: :safe | :permanent | :unknown | nil,
          cancellation_requested_at: DateTime.t() | nil,
          cancellation_reason: String.t() | nil,
          retry_root_id: String.t(),
          retry_of_submission_id: String.t() | nil,
          retry_command_id: String.t() | nil,
          superseded_by_submission_id: String.t() | nil,
          enqueued_at: DateTime.t(),
          available_at: DateTime.t(),
          preparing_at: DateTime.t() | nil,
          admitting_at: DateTime.t() | nil,
          terminal_at: DateTime.t() | nil,
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunSubmissionPage do
  @moduledoc "Bounded keyset page of durable run submissions."

  @enforce_keys [:items]
  defstruct [:next, items: []]

  @type t :: %__MODULE__{
          items: [FavnOrchestrator.Persistence.Results.RunSubmission.t()],
          next: %{inserted_at: DateTime.t(), submission_id: String.t()} | nil
        }
end
