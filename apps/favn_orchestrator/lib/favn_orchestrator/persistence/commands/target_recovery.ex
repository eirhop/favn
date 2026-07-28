defmodule FavnOrchestrator.Persistence.Queries.GetInitialTargetRecoveryCandidate do
  @moduledoc "Fetches exact durable evidence for one interrupted initial generation."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :target_id]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          target_id: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.CreateTargetRecoveryPlan do
  @moduledoc "Persists one immutable evidence-backed target-recovery plan."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :target_id,
    :recovery_kind,
    :desired_manifest_id,
    :source_manifest_id,
    :target_generation_id,
    :materialization_id,
    :plan_hash,
    :plan_payload,
    :actor_id,
    :reason,
    :idempotency_key,
    :expected_binding_version,
    :expected_physical_fingerprint,
    :evaluated_at,
    :occurred_at
  ]
  defstruct @enforce_keys ++ [:session_id]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          target_id: String.t(),
          recovery_kind: :reconcile_initial_generation,
          desired_manifest_id: String.t(),
          source_manifest_id: String.t(),
          target_generation_id: String.t(),
          materialization_id: String.t(),
          plan_hash: String.t(),
          plan_payload: map(),
          actor_id: String.t(),
          session_id: String.t() | nil,
          reason: String.t(),
          idempotency_key: String.t(),
          expected_binding_version: pos_integer(),
          expected_physical_fingerprint: String.t(),
          evaluated_at: DateTime.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.BeginTargetRecovery do
  @moduledoc "Persists recovery intent before any data-plane marker side effect."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :plan_hash,
    :expected_version,
    :recovery_token,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          plan_hash: String.t(),
          expected_version: pos_integer(),
          recovery_token: String.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ActivateRecoveredTargetGeneration do
  @moduledoc """
  Atomically activates one proven initial generation and completes its recovery.

  The target-operation fence, binding version, successful materialization, generation,
  physical fingerprint, and exact marker are all revalidated in the same transaction.
  """

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :expected_operation_version,
    :target_id,
    :target_generation_id,
    :materialization_id,
    :source_manifest_id,
    :expected_binding_version,
    :expected_desired_manifest_id,
    :expected_desired_descriptor_hash,
    :physical_schema_fingerprint,
    :expected_marker_operation_id,
    :data_plane_marker,
    :compatibility_status,
    :reason_code,
    :compatibility_diff,
    :lease_owner,
    :fencing_token,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          expected_operation_version: pos_integer(),
          target_id: String.t(),
          target_generation_id: String.t(),
          materialization_id: String.t(),
          source_manifest_id: String.t(),
          expected_binding_version: pos_integer(),
          expected_desired_manifest_id: String.t(),
          expected_desired_descriptor_hash: String.t(),
          physical_schema_fingerprint: String.t(),
          expected_marker_operation_id: String.t(),
          data_plane_marker: map() | nil,
          compatibility_status: atom(),
          reason_code: String.t(),
          compatibility_diff: map(),
          lease_owner: String.t(),
          fencing_token: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.MarkTargetRecoveryUnknown do
  @moduledoc "Persists an inconclusive data-plane marker outcome without retrying the side effect."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :expected_version,
    :unknown_outcome,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          expected_version: pos_integer(),
          unknown_outcome: map(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.FailTargetRecovery do
  @moduledoc "Persists a conclusive target-recovery failure."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :expected_version,
    :terminal_error,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          expected_version: pos_integer(),
          terminal_error: map(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetTargetRecovery do
  @moduledoc "Fetches one authoritative target-recovery operation."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [:workspace_context, :operation_id]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          operation_id: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.InitialTargetRecoveryCandidate do
  @moduledoc "Durable control-plane proof for an interrupted initial materialization."

  alias Favn.TargetGeneration
  alias FavnOrchestrator.Persistence.Results.TargetBinding

  @enforce_keys [:binding, :generation, :materialization_id]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          binding: TargetBinding.t(),
          generation: TargetGeneration.t(),
          materialization_id: String.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.TargetRecoveryOperation do
  @moduledoc "Authoritative durable state for one target-recovery operation."

  @enforce_keys [
    :workspace_id,
    :operation_id,
    :target_id,
    :recovery_kind,
    :desired_manifest_id,
    :source_manifest_id,
    :target_generation_id,
    :materialization_id,
    :plan_hash,
    :plan_version,
    :plan_payload,
    :state,
    :phase,
    :actor_id,
    :reason,
    :idempotency_key,
    :expected_binding_version,
    :expected_physical_fingerprint,
    :evaluated_at,
    :version,
    :inserted_at,
    :updated_at
  ]
  defstruct @enforce_keys ++
              [
                :session_id,
                :recovery_token,
                :result_marker,
                :compatibility_result,
                :unknown_outcome,
                :terminal_error,
                :started_at,
                :completed_at,
                idempotency_replay?: false
              ]

  @type state :: :planned | :applying | :outcome_unknown | :succeeded | :failed
  @type phase :: :planned | :marker_intent | :reconciling | :terminal

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          operation_id: String.t(),
          target_id: String.t(),
          recovery_kind: :reconcile_initial_generation,
          desired_manifest_id: String.t(),
          source_manifest_id: String.t(),
          target_generation_id: String.t(),
          materialization_id: String.t(),
          plan_hash: String.t(),
          plan_version: pos_integer(),
          plan_payload: map(),
          state: state(),
          phase: phase(),
          actor_id: String.t(),
          session_id: String.t() | nil,
          reason: String.t(),
          idempotency_key: String.t(),
          expected_binding_version: pos_integer(),
          expected_physical_fingerprint: String.t(),
          evaluated_at: DateTime.t(),
          recovery_token: String.t() | nil,
          result_marker: map() | nil,
          compatibility_result: map() | nil,
          unknown_outcome: map() | nil,
          terminal_error: map() | nil,
          version: pos_integer(),
          started_at: DateTime.t() | nil,
          completed_at: DateTime.t() | nil,
          inserted_at: DateTime.t(),
          updated_at: DateTime.t(),
          idempotency_replay?: boolean()
        }
end
