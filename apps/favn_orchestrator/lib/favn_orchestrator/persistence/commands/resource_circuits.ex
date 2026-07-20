defmodule FavnOrchestrator.Persistence.Commands.ResourceCircuitRequest do
  @moduledoc "One configured resource participating in circuit admission."

  @enforce_keys [:resource, :policy]
  defstruct [:resource, :policy]

  @type t :: %__MODULE__{
          resource: Favn.Resource.Ref.t(),
          policy: Favn.CircuitBreaker.Policy.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.AcquireResourceCircuits do
  @moduledoc "Atomically checks resource circuits and reserves a half-open probe when due."

  @enforce_keys [
    :workspace_context,
    :command_id,
    :owner_id,
    :run_id,
    :asset_step_id,
    :requests,
    :probe_lease_ms,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :owner_id,
    :run_id,
    :asset_step_id,
    :requests,
    :probe_lease_ms,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: FavnOrchestrator.Persistence.WorkspaceContext.t(),
          command_id: String.t(),
          owner_id: String.t(),
          run_id: String.t(),
          asset_step_id: String.t(),
          requests: [FavnOrchestrator.Persistence.Commands.ResourceCircuitRequest.t()],
          probe_lease_ms: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RecordResourceOutcomes do
  @moduledoc "Idempotently records terminal resource health outcomes for one node attempt."

  @enforce_keys [
    :workspace_context,
    :command_id,
    :owner_id,
    :run_id,
    :asset_step_id,
    :attempt,
    :permits,
    :outcomes,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :owner_id,
    :run_id,
    :asset_step_id,
    :attempt,
    :permits,
    :outcomes,
    :occurred_at,
    recovery_candidates: []
  ]

  @type t :: %__MODULE__{
          workspace_context: FavnOrchestrator.Persistence.WorkspaceContext.t(),
          command_id: String.t(),
          owner_id: String.t(),
          run_id: String.t(),
          asset_step_id: String.t(),
          attempt: pos_integer(),
          permits: [FavnOrchestrator.Persistence.Results.ResourceCircuitPermit.t()],
          outcomes: [Favn.Contracts.ResourceOutcome.t()],
          recovery_candidates: [
            FavnOrchestrator.Persistence.Commands.RecordResourceRecoveryCandidate.t()
          ],
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ReleaseResourceCircuitPermits do
  @moduledoc "Releases a half-open probe that produced no resource-health outcome."

  @enforce_keys [:workspace_context, :owner_id, :permits, :occurred_at]
  defstruct [:workspace_context, :owner_id, :permits, :occurred_at]

  @type t :: %__MODULE__{
          workspace_context: FavnOrchestrator.Persistence.WorkspaceContext.t(),
          owner_id: String.t(),
          permits: [FavnOrchestrator.Persistence.Results.ResourceCircuitPermit.t()],
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RecordResourceRecoveryCandidate do
  @moduledoc "Records one blocked or explicitly repeat-safe node for optional linked recovery."

  @enforce_keys [
    :workspace_context,
    :candidate_id,
    :source_run_id,
    :node_key,
    :resource,
    :reason,
    :max_age_ms,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :candidate_id,
    :source_run_id,
    :node_key,
    :resource,
    :reason,
    :max_age_ms,
    :occurred_at
  ]

  @type reason :: :blocked | :safe_failure
  @type t :: %__MODULE__{
          workspace_context: FavnOrchestrator.Persistence.WorkspaceContext.t(),
          candidate_id: String.t(),
          source_run_id: String.t(),
          node_key: Favn.Plan.node_key(),
          resource: Favn.Resource.Ref.t(),
          reason: reason(),
          max_age_ms: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimResourceRecovery do
  @moduledoc "Claims pending recovery candidates after a resource circuit closes."

  @enforce_keys [
    :workspace_context,
    :command_id,
    :owner_id,
    :resource,
    :limit,
    :claim_lease_ms,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :owner_id,
    :resource,
    :limit,
    :claim_lease_ms,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: FavnOrchestrator.Persistence.WorkspaceContext.t(),
          command_id: String.t(),
          owner_id: String.t(),
          resource: Favn.Resource.Ref.t(),
          limit: pos_integer(),
          claim_lease_ms: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.CompleteResourceRecovery do
  @moduledoc "Marks claimed recovery candidates submitted or releases them for another attempt."

  @enforce_keys [:workspace_context, :owner_id, :candidate_ids, :status, :occurred_at]
  defstruct [
    :workspace_context,
    :owner_id,
    :candidate_ids,
    :status,
    :recovery_run_id,
    :occurred_at
  ]

  @type status :: :submitted | :pending
  @type t :: %__MODULE__{
          workspace_context: FavnOrchestrator.Persistence.WorkspaceContext.t(),
          owner_id: String.t(),
          candidate_ids: [String.t()],
          status: status(),
          recovery_run_id: String.t() | nil,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ListPendingResourceRecoveries do
  @moduledoc "Lists a bounded set of resource identities with claimable recovery work."

  @enforce_keys [:platform_context, :limit, :occurred_at]
  defstruct [:platform_context, :limit, :occurred_at]

  @type t :: %__MODULE__{
          platform_context: FavnOrchestrator.Persistence.PlatformContext.t(),
          limit: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.ResourceCircuitPermit do
  @moduledoc "Admission evidence for one resource circuit."

  @enforce_keys [:resource, :owner_id, :probe?]
  defstruct [:resource, :owner_id, :probe?]

  @type t :: %__MODULE__{
          resource: Favn.Resource.Ref.t(),
          owner_id: String.t(),
          probe?: boolean()
        }
end

defmodule FavnOrchestrator.Persistence.Results.ResourceCircuitBlocker do
  @moduledoc "Structured reason that one resource circuit denied admission."

  @enforce_keys [:resource, :state, :failure_threshold, :consecutive_failures]
  defstruct [
    :resource,
    :state,
    :failure_threshold,
    :consecutive_failures,
    :retry_at,
    :probe_owner_id
  ]

  @type t :: %__MODULE__{
          resource: Favn.Resource.Ref.t(),
          state: :open | :half_open,
          failure_threshold: pos_integer(),
          consecutive_failures: non_neg_integer(),
          retry_at: DateTime.t() | nil,
          probe_owner_id: String.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Results.ResourceCircuitAdmission do
  @moduledoc "Atomic resource circuit admission result."

  @enforce_keys [:status]
  defstruct status: :allowed, permits: [], blockers: []

  @type t :: %__MODULE__{
          status: :allowed | :blocked,
          permits: [FavnOrchestrator.Persistence.Results.ResourceCircuitPermit.t()],
          blockers: [FavnOrchestrator.Persistence.Results.ResourceCircuitBlocker.t()]
        }
end

defmodule FavnOrchestrator.Persistence.Results.ResourceCircuitUpdate do
  @moduledoc "Resources that changed from open or half-open to closed."

  defstruct closed_resources: []

  @type t :: %__MODULE__{closed_resources: [Favn.Resource.Ref.t()]}
end

defmodule FavnOrchestrator.Persistence.Results.ResourceRecoveryCandidate do
  @moduledoc "One claimed node eligible for a linked recovery run."

  @enforce_keys [:candidate_id, :source_run_id, :node_key, :resource, :reason]
  defstruct [:candidate_id, :source_run_id, :node_key, :resource, :reason]

  @type t :: %__MODULE__{
          candidate_id: String.t(),
          source_run_id: String.t(),
          node_key: Favn.Plan.node_key(),
          resource: Favn.Resource.Ref.t(),
          reason: :blocked | :safe_failure
        }
end

defmodule FavnOrchestrator.Persistence.Results.ResourceRecoveryBatch do
  @moduledoc "A bounded durable recovery claim."

  defstruct candidates: []

  @type t :: %__MODULE__{
          candidates: [FavnOrchestrator.Persistence.Results.ResourceRecoveryCandidate.t()]
        }
end

defmodule FavnOrchestrator.Persistence.Results.ResourceRecoveryWakeup do
  @moduledoc "One workspace/resource pair with pending durable recovery work."

  @enforce_keys [:workspace_id, :resource]
  defstruct [:workspace_id, :resource]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          resource: Favn.Resource.Ref.t()
        }
end
