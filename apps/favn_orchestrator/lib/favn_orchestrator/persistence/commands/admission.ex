defmodule FavnOrchestrator.Persistence.Commands.CapacityRequest do
  @moduledoc "One bounded capacity requirement for an execution lease."

  @enforce_keys [:scope_id]
  defstruct [:scope_id, units: 1]

  @type t :: %__MODULE__{scope_id: String.t(), units: pos_integer()}
end

defmodule FavnOrchestrator.Persistence.Commands.AdmitExecution do
  @moduledoc "Atomically admits execution or records its durable capacity waiter."

  alias FavnOrchestrator.Persistence.Commands.CapacityRequest
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :lease_id,
    :waiter_id,
    :run_id,
    :step_id,
    :owner_id,
    :owner_generation,
    :lease_duration_ms,
    :waiter_ttl_ms,
    :requests,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :lease_id,
    :waiter_id,
    :run_id,
    :step_id,
    :owner_id,
    :owner_generation,
    :lease_duration_ms,
    :waiter_ttl_ms,
    :requests,
    :occurred_at,
    priority: 0
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          lease_id: String.t(),
          waiter_id: String.t(),
          run_id: String.t(),
          step_id: String.t(),
          owner_id: String.t(),
          owner_generation: pos_integer(),
          lease_duration_ms: pos_integer(),
          waiter_ttl_ms: pos_integer(),
          requests: [CapacityRequest.t()],
          occurred_at: DateTime.t(),
          priority: integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RenewExecutionLease do
  @moduledoc "Renews one matching active execution lease generation."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :renewal_id,
    :lease_id,
    :owner_id,
    :owner_generation,
    :lease_duration_ms
  ]
  defstruct [
    :workspace_context,
    :renewal_id,
    :lease_id,
    :owner_id,
    :owner_generation,
    :lease_duration_ms
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          renewal_id: String.t(),
          lease_id: String.t(),
          owner_id: String.t(),
          owner_generation: pos_integer(),
          lease_duration_ms: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ReleaseExecutionLease do
  @moduledoc "Idempotently releases one matching execution lease generation."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :lease_id, :owner_id, :owner_generation]
  defstruct [:workspace_context, :lease_id, :owner_id, :owner_generation]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          lease_id: String.t(),
          owner_id: String.t(),
          owner_generation: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ReleaseRunLeases do
  @moduledoc "Releases a bounded batch of active execution leases for one run."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :run_id]
  defstruct [:workspace_context, :run_id, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          run_id: String.t(),
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimAdmissionWaiters do
  @moduledoc "Claims a bounded priority queue of waiters for one freed capacity scope."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :batch_id,
    :scope_id,
    :owner_id,
    :lease_duration_ms
  ]
  defstruct [
    :workspace_context,
    :batch_id,
    :scope_id,
    :owner_id,
    :lease_duration_ms,
    limit: 100
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          batch_id: String.t(),
          scope_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer(),
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ExpireAdmission do
  @moduledoc "Expires bounded batches of overdue leases and waiters."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :batch_id]
  defstruct [:workspace_context, :batch_id, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          batch_id: String.t(),
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Results.ExecutionLease do
  @moduledoc "Durable execution-capacity authority returned across persistence."

  @enforce_keys [
    :workspace_id,
    :lease_id,
    :run_id,
    :step_id,
    :owner_id,
    :owner_generation,
    :status,
    :expires_at,
    :scope_ids
  ]
  defstruct [
    :workspace_id,
    :lease_id,
    :run_id,
    :step_id,
    :owner_id,
    :owner_generation,
    :status,
    :expires_at,
    :released_at,
    :scope_ids
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          lease_id: String.t(),
          run_id: String.t(),
          step_id: String.t(),
          owner_id: String.t(),
          owner_generation: pos_integer(),
          status: :active | :released | :expired,
          expires_at: DateTime.t(),
          released_at: DateTime.t() | nil,
          scope_ids: [String.t()]
        }
end

defmodule FavnOrchestrator.Persistence.Results.AdmissionWaiter do
  @moduledoc "Durable blocked-execution queue item."

  @enforce_keys [
    :workspace_id,
    :waiter_id,
    :run_id,
    :step_id,
    :blocking_scope_id,
    :status,
    :priority,
    :expires_at,
    :requests
  ]
  defstruct [
    :workspace_id,
    :waiter_id,
    :run_id,
    :step_id,
    :blocking_scope_id,
    :status,
    :priority,
    :expires_at,
    :claim_owner,
    :claim_generation,
    :claim_expires_at,
    :requests
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          waiter_id: String.t(),
          run_id: String.t(),
          step_id: String.t(),
          blocking_scope_id: String.t(),
          status: :waiting | :claimed | :admitted | :expired | :cancelled,
          priority: integer(),
          expires_at: DateTime.t(),
          claim_owner: String.t() | nil,
          claim_generation: non_neg_integer(),
          claim_expires_at: DateTime.t() | nil,
          requests: [map()]
        }
end

defmodule FavnOrchestrator.Persistence.Results.Admission do
  @moduledoc "Admission decision containing either an execution lease or a waiter."

  alias FavnOrchestrator.Persistence.Results.AdmissionWaiter
  alias FavnOrchestrator.Persistence.Results.ExecutionLease

  @enforce_keys [:status]
  defstruct [:status, :lease, :waiter, :blocking_scope_id]

  @type t :: %__MODULE__{
          status: :admitted | :waiting,
          lease: ExecutionLease.t() | nil,
          waiter: AdmissionWaiter.t() | nil,
          blocking_scope_id: String.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Results.CapacityRelease do
  @moduledoc "Bounded capacity release or expiry outcome."

  @enforce_keys [:released_lease_ids, :expired_waiter_ids, :freed_scope_ids]
  defstruct [:released_lease_ids, :expired_waiter_ids, :freed_scope_ids]

  @type t :: %__MODULE__{
          released_lease_ids: [String.t()],
          expired_waiter_ids: [String.t()],
          freed_scope_ids: [String.t()]
        }
end
