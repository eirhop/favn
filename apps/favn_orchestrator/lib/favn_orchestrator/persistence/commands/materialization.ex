defmodule FavnOrchestrator.Persistence.Commands.ClaimMaterialization do
  @moduledoc "Claims one logical materialization identity with an expiring fence."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :claim_key,
    :deployment_id,
    :target_kind,
    :target_id,
    :evidence_generation_id,
    :partition_key,
    :run_id,
    :owner_id,
    :lease_duration_ms,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :claim_key,
    :deployment_id,
    :target_kind,
    :target_id,
    :target_generation_id,
    :evidence_generation_id,
    :partition_key,
    :run_id,
    :owner_id,
    :lease_duration_ms,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          claim_key: String.t(),
          deployment_id: String.t(),
          target_kind: :asset | :pipeline,
          target_id: String.t(),
          target_generation_id: String.t() | nil,
          evidence_generation_id: String.t(),
          partition_key: String.t(),
          run_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RenewMaterializationClaim do
  @moduledoc "Renews only one matching active materialization claim generation."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :renewal_id,
    :claim_key,
    :owner_id,
    :fencing_token,
    :lease_duration_ms
  ]
  defstruct [
    :workspace_context,
    :renewal_id,
    :claim_key,
    :owner_id,
    :fencing_token,
    :lease_duration_ms
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          renewal_id: String.t(),
          claim_key: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          lease_duration_ms: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.FinishMaterialization do
  @moduledoc "Fenced terminal outcome for a claimed logical materialization."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :claim_key,
    :owner_id,
    :fencing_token,
    :expected_version,
    :status,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :claim_key,
    :owner_id,
    :fencing_token,
    :expected_version,
    :status,
    :materialization_id,
    :payload,
    :error,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          claim_key: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          expected_version: pos_integer(),
          status: :succeeded | :failed,
          materialization_id: String.t() | nil,
          payload: map() | nil,
          error: map() | nil,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetMaterializations do
  @moduledoc "Batch-fetches exact logical materialization identities."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :claim_keys]
  defstruct [:workspace_context, :claim_keys]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          claim_keys: [String.t()]
        }
end

defmodule FavnOrchestrator.Persistence.Results.MaterializationClaim do
  @moduledoc "Current fenced materialization-claim authority."

  @enforce_keys [
    :workspace_id,
    :claim_key,
    :deployment_id,
    :target_kind,
    :target_id,
    :evidence_generation_id,
    :partition_key,
    :run_id,
    :owner_id,
    :fencing_token,
    :status,
    :expires_at,
    :version
  ]
  defstruct [
    :workspace_id,
    :claim_key,
    :deployment_id,
    :target_kind,
    :target_id,
    :target_generation_id,
    :evidence_generation_id,
    :partition_key,
    :run_id,
    :owner_id,
    :fencing_token,
    :status,
    :expires_at,
    :completed_at,
    :result,
    :error,
    :version
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          claim_key: String.t(),
          deployment_id: String.t(),
          target_kind: :asset | :pipeline,
          target_id: String.t(),
          target_generation_id: String.t() | nil,
          evidence_generation_id: String.t(),
          partition_key: String.t(),
          run_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          status: :claimed | :succeeded | :failed | :expired,
          expires_at: DateTime.t(),
          completed_at: DateTime.t() | nil,
          result: map() | nil,
          error: map() | nil,
          version: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Results.Materialization do
  @moduledoc "Immutable successful materialization ledger record."

  @enforce_keys [
    :workspace_id,
    :materialization_id,
    :claim_key,
    :deployment_id,
    :target_kind,
    :target_id,
    :target_generation_id,
    :evidence_generation_id,
    :partition_key,
    :run_id,
    :payload,
    :inserted_at
  ]
  defstruct [
    :workspace_id,
    :materialization_id,
    :claim_key,
    :deployment_id,
    :target_kind,
    :target_id,
    :target_generation_id,
    :evidence_generation_id,
    :partition_key,
    :run_id,
    :payload,
    :inserted_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          materialization_id: String.t(),
          claim_key: String.t(),
          deployment_id: String.t(),
          target_kind: :asset | :pipeline,
          target_id: String.t(),
          target_generation_id: String.t() | nil,
          evidence_generation_id: String.t(),
          partition_key: String.t(),
          run_id: String.t(),
          payload: map(),
          inserted_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.MaterializationDecision do
  @moduledoc "Materialization lookup or claim decision."

  alias FavnOrchestrator.Persistence.Results.Materialization
  alias FavnOrchestrator.Persistence.Results.MaterializationClaim

  @enforce_keys [:claim_key, :status]
  defstruct [:claim_key, :status, :claim, :materialization]

  @type t :: %__MODULE__{
          claim_key: String.t(),
          status: :claimed | :competing | :materialized | :failed | :missing,
          claim: MaterializationClaim.t() | nil,
          materialization: Materialization.t() | nil
        }
end
