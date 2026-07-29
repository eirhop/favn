defmodule FavnOrchestrator.Persistence.Commands.ClaimRun do
  @moduledoc "Claims or takes over one available run and returns a new fencing generation."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :command_id, :run_id, :owner_id, :lease_duration_ms]
  defstruct [:workspace_context, :command_id, :run_id, :owner_id, :lease_duration_ms]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          run_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimRecoveryBatch do
  @moduledoc """
  Claims a bounded batch of active runs whose ownership is available in one workspace.

  `unowned_grace_period_ms` keeps a newly persisted run out of recovery while
  normal submission hands it to its first RunServer. Never-claimed runs become
  recoverable after that bounded grace period.
  """
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :batch_id,
    :owner_id,
    :lease_duration_ms,
    :unowned_grace_period_ms
  ]
  defstruct [
    :workspace_context,
    :batch_id,
    :owner_id,
    :lease_duration_ms,
    :unowned_grace_period_ms,
    limit: 100
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          batch_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer(),
          unowned_grace_period_ms: non_neg_integer(),
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RenewRunOwnership do
  @moduledoc "Renews only one matching unexpired run ownership generation."
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :renewal_id,
    :run_id,
    :owner_id,
    :fencing_token,
    :lease_duration_ms
  ]
  defstruct [
    :workspace_context,
    :renewal_id,
    :run_id,
    :owner_id,
    :fencing_token,
    :lease_duration_ms
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          renewal_id: String.t(),
          run_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          lease_duration_ms: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ReleaseRunOwnership do
  @moduledoc "Idempotently releases one matching run ownership generation."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :run_id, :owner_id, :fencing_token]
  defstruct [:workspace_context, :run_id, :owner_id, :fencing_token]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RunOwnership do
  @moduledoc "Current fenced run ownership authority."
  @enforce_keys [:workspace_id, :run_id, :owner_id, :fencing_token, :expires_at]
  defstruct [:workspace_id, :run_id, :owner_id, :fencing_token, :expires_at]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          run_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          expires_at: DateTime.t()
        }
end
