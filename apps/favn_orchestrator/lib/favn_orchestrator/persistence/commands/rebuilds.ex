defmodule FavnOrchestrator.Persistence.Commands.RebuildPlanAction do
  @moduledoc "One immutable topologically ordered rebuild-plan action."

  @enforce_keys [
    :target_id,
    :ordinal,
    :action,
    :reason,
    :upstream_impact,
    :pinned_input_generation_ids,
    :status
  ]
  defstruct [
    :target_id,
    :ordinal,
    :action,
    :reason,
    :upstream_impact,
    :mapping_proof,
    :pinned_input_generation_ids,
    :candidate_generation,
    :status
  ]

  @type t :: %__MODULE__{
          target_id: String.t(),
          ordinal: non_neg_integer(),
          action: :no_action | :backfill | :rebuild | :operator_decision,
          reason: map(),
          upstream_impact: map(),
          mapping_proof: map() | nil,
          pinned_input_generation_ids: [map()],
          candidate_generation: map() | nil,
          status: :planned
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RebuildPlanItem do
  @moduledoc "One frozen logical item in an immutable rebuild plan."

  @enforce_keys [
    :target_id,
    :item_id,
    :ordinal,
    :work_kind,
    :window_key,
    :candidate_generation_id
  ]
  defstruct [
    :target_id,
    :item_id,
    :ordinal,
    :work_kind,
    :window_key,
    :window_start,
    :window_end,
    :runtime_input_expectation,
    :candidate_generation_id
  ]

  @type t :: %__MODULE__{
          target_id: String.t(),
          item_id: String.t(),
          ordinal: non_neg_integer(),
          work_kind: :window | :full_load | :empty_generation,
          window_key: String.t(),
          window_start: DateTime.t() | nil,
          window_end: DateTime.t() | nil,
          runtime_input_expectation: map() | nil,
          candidate_generation_id: String.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Commands.CreateRebuildPlan do
  @moduledoc "Persists one immutable manual rebuild plan and its candidate generations."

  alias FavnOrchestrator.Persistence.Commands.RebuildPlanAction
  alias FavnOrchestrator.Persistence.Commands.RebuildPlanItem
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :root_target_id,
    :manifest_version_id,
    :candidate_generation_id,
    :plan_hash,
    :plan_payload,
    :actor_id,
    :reason,
    :idempotency_key,
    :evaluated_at,
    :actions,
    :items,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :operation_id,
    :root_target_id,
    :manifest_version_id,
    :active_generation_id,
    :candidate_generation_id,
    :plan_hash,
    :plan_payload,
    :actor_id,
    :session_id,
    :reason,
    :idempotency_key,
    :evaluated_at,
    :coverage_start,
    :coverage_end,
    :actions,
    :items,
    :occurred_at,
    plan_version: 1
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          root_target_id: String.t(),
          manifest_version_id: String.t(),
          active_generation_id: String.t() | nil,
          candidate_generation_id: String.t(),
          plan_hash: String.t(),
          plan_payload: map(),
          actor_id: String.t(),
          session_id: String.t() | nil,
          reason: String.t(),
          idempotency_key: String.t(),
          evaluated_at: DateTime.t(),
          coverage_start: DateTime.t() | nil,
          coverage_end: DateTime.t() | nil,
          actions: [RebuildPlanAction.t()],
          items: [RebuildPlanItem.t()],
          occurred_at: DateTime.t(),
          plan_version: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimRebuildOperation do
  @moduledoc "Claims one non-terminal rebuild operation for dispatch or recovery."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :command_id, :owner_id, :lease_duration_ms]
  defstruct [:workspace_context, :command_id, :owner_id, :lease_duration_ms, :operation_id]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer(),
          operation_id: String.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Commands.StartRebuildOperation do
  @moduledoc "Approves one exact immutable plan for execution."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :plan_hash,
    :expected_version,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          plan_hash: String.t(),
          expected_version: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RequestRebuildCancellation do
  @moduledoc "Persists an operator cancellation request without guessing activation outcome."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :command_id, :operation_id, :reason, :occurred_at]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          reason: String.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RetryRebuildOperation do
  @moduledoc "Requeues safe failed work on the same immutable rebuild plan."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :command_id, :operation_id, :plan_hash, :occurred_at]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          plan_hash: String.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.TransitionRebuildOperation do
  @moduledoc "Applies one fenced, versioned rebuild-operation transition."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :owner_id,
    :fencing_token,
    :expected_version,
    :expected_states,
    :state,
    :phase,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :operation_id,
    :owner_id,
    :fencing_token,
    :expected_version,
    :expected_states,
    :state,
    :phase,
    :activation_token,
    :result_marker,
    :unknown_outcome,
    :validation_result,
    :terminal_error,
    :cleanup_state,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          expected_version: pos_integer(),
          expected_states: [atom()],
          state: atom(),
          phase: atom(),
          activation_token: String.t() | nil,
          result_marker: map() | nil,
          unknown_outcome: map() | nil,
          validation_result: map() | nil,
          terminal_error: map() | nil,
          cleanup_state: atom() | nil,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimRebuildItems do
  @moduledoc "Claims a bounded ordinal batch of frozen rebuild items."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :batch_id,
    :operation_id,
    :target_id,
    :owner_id,
    :lease_duration_ms
  ]
  defstruct [
    :workspace_context,
    :batch_id,
    :operation_id,
    :target_id,
    :owner_id,
    :lease_duration_ms,
    limit: 100
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          batch_id: String.t(),
          operation_id: String.t(),
          target_id: String.t(),
          owner_id: String.t(),
          lease_duration_ms: pos_integer(),
          limit: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.TransitionRebuildItem do
  @moduledoc "Applies one fenced rebuild-item outcome."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :target_id,
    :item_id,
    :owner_id,
    :fencing_token,
    :expected_version,
    :status,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :operation_id,
    :target_id,
    :item_id,
    :owner_id,
    :fencing_token,
    :expected_version,
    :status,
    :child_run_id,
    :materialization_id,
    :row_count,
    :last_error,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          target_id: String.t(),
          item_id: String.t(),
          owner_id: String.t(),
          fencing_token: pos_integer(),
          expected_version: pos_integer(),
          status: atom(),
          child_run_id: String.t() | nil,
          materialization_id: String.t() | nil,
          row_count: non_neg_integer() | nil,
          last_error: map() | nil,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.TransitionRebuildAction do
  @moduledoc "Applies one versioned action checkpoint, validation, or activation intent."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :target_id,
    :owner_id,
    :operation_fencing_token,
    :expected_version,
    :expected_statuses,
    :status,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :operation_id,
    :target_id,
    :owner_id,
    :operation_fencing_token,
    :expected_version,
    :expected_statuses,
    :status,
    :child_operation_id,
    :child_run_id,
    :activation_intent,
    :validation_result,
    :terminal_error,
    :cleanup_state,
    :activated_at,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          target_id: String.t(),
          owner_id: String.t(),
          operation_fencing_token: pos_integer(),
          expected_version: pos_integer(),
          expected_statuses: [atom()],
          status: atom(),
          child_operation_id: String.t() | nil,
          child_run_id: String.t() | nil,
          activation_intent: map() | nil,
          validation_result: map() | nil,
          terminal_error: map() | nil,
          cleanup_state: atom() | nil,
          activated_at: DateTime.t() | nil,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ActivateRebuildGeneration do
  @moduledoc "Commits one observed data-plane activation to generation and binding authority."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :target_id,
    :owner_id,
    :operation_fencing_token,
    :previous_generation_id,
    :candidate_generation_id,
    :activation_token,
    :active_relation,
    :retired_relation,
    :data_plane_marker,
    :physical_schema_fingerprint,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          target_id: String.t(),
          owner_id: String.t(),
          operation_fencing_token: pos_integer(),
          previous_generation_id: String.t(),
          candidate_generation_id: String.t(),
          activation_token: String.t(),
          active_relation: map(),
          retired_relation: map(),
          data_plane_marker: map(),
          physical_schema_fingerprint: String.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.TransitionRebuildGeneration do
  @moduledoc "Fenced terminal status transition for one inactive rebuild candidate."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :target_id,
    :candidate_generation_id,
    :owner_id,
    :operation_fencing_token,
    :status,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          target_id: String.t(),
          candidate_generation_id: String.t(),
          owner_id: String.t(),
          operation_fencing_token: pos_integer(),
          status: :failed | :discarded,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.AcquireTargetOperationLocks do
  @moduledoc "Atomically acquires canonically ordered target-operation leases."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :target_ids,
    :operation_id,
    :operation_type,
    :lease_owner,
    :lease_duration_ms,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          target_ids: [String.t()],
          operation_id: String.t(),
          operation_type: :rebuild,
          lease_owner: String.t(),
          lease_duration_ms: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RenewTargetOperationLocks do
  @moduledoc "Renews every lock held by one exact operation fence set."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :lease_owner,
    :locks,
    :lease_duration_ms,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type lock_ref :: %{
          required(:target_id) => String.t(),
          required(:fencing_token) => pos_integer()
        }
  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          lease_owner: String.t(),
          locks: [lock_ref()],
          lease_duration_ms: pos_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ReleaseTargetOperationLocks do
  @moduledoc "Releases every matching lock held by one operation."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :operation_id,
    :lease_owner,
    :locks,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          operation_id: String.t(),
          lease_owner: String.t(),
          locks: [RenewTargetOperationLocks.lock_ref()],
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.GetRebuild do
  @moduledoc "Fetches one authoritative rebuild operation with ordered actions."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :operation_id]
  defstruct @enforce_keys

  @type t :: %__MODULE__{workspace_context: WorkspaceContext.t(), operation_id: String.t()}
end

defmodule FavnOrchestrator.Persistence.Queries.PageRebuildItems do
  @moduledoc "Pages rebuild items by immutable ordinal and item identity."
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :operation_id]
  defstruct [:workspace_context, :operation_id, :target_id, :status, :after, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          operation_id: String.t(),
          target_id: String.t() | nil,
          status: atom() | nil,
          after: map() | nil,
          limit: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RebuildOperation do
  @moduledoc "Authoritative rebuild operation and compact progress."
  defstruct [
    :workspace_id,
    :operation_id,
    :root_target_id,
    :manifest_version_id,
    :active_generation_id,
    :candidate_generation_id,
    :plan_hash,
    :plan_version,
    :plan_payload,
    :actor_id,
    :session_id,
    :reason,
    :idempotency_key,
    :evaluated_at,
    :coverage_start,
    :coverage_end,
    :action_count,
    :window_count,
    :state,
    :phase,
    :activation_token,
    :result_marker,
    :unknown_outcome,
    :validation_result,
    :terminal_error,
    :cleanup_state,
    :cancel_requested,
    :dispatcher_owner,
    :dispatcher_fencing_token,
    :dispatcher_expires_at,
    :version,
    :started_at,
    :completed_at,
    :cancelled_at,
    :inserted_at,
    :updated_at,
    actions: [],
    progress: %{}
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          operation_id: String.t(),
          root_target_id: String.t(),
          manifest_version_id: String.t(),
          active_generation_id: String.t() | nil,
          candidate_generation_id: String.t(),
          plan_hash: String.t(),
          plan_version: pos_integer(),
          plan_payload: map(),
          actor_id: String.t(),
          session_id: String.t() | nil,
          reason: String.t(),
          idempotency_key: String.t(),
          evaluated_at: DateTime.t(),
          coverage_start: DateTime.t() | nil,
          coverage_end: DateTime.t() | nil,
          action_count: pos_integer(),
          window_count: pos_integer(),
          state: atom(),
          phase: atom(),
          activation_token: String.t() | nil,
          result_marker: map() | nil,
          unknown_outcome: map() | nil,
          validation_result: map() | nil,
          terminal_error: map() | nil,
          cleanup_state: atom(),
          cancel_requested: boolean(),
          dispatcher_owner: String.t() | nil,
          dispatcher_fencing_token: non_neg_integer(),
          dispatcher_expires_at: DateTime.t() | nil,
          version: pos_integer(),
          started_at: DateTime.t() | nil,
          completed_at: DateTime.t() | nil,
          cancelled_at: DateTime.t() | nil,
          inserted_at: DateTime.t(),
          updated_at: DateTime.t(),
          actions: [FavnOrchestrator.Persistence.Results.RebuildAction.t()],
          progress: map()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RebuildAction do
  @moduledoc "One authoritative rebuild action checkpoint."
  defstruct [
    :workspace_id,
    :operation_id,
    :target_id,
    :ordinal,
    :action,
    :reason,
    :upstream_impact,
    :mapping_proof,
    :pinned_input_generation_ids,
    :candidate_generation_id,
    :status,
    :child_operation_id,
    :child_run_id,
    :activation_intent,
    :validation_result,
    :terminal_error,
    :cleanup_state,
    :activated_at,
    :version,
    :inserted_at,
    :updated_at,
    progress: %{}
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          operation_id: String.t(),
          target_id: String.t(),
          ordinal: non_neg_integer(),
          action: atom(),
          reason: map(),
          upstream_impact: map(),
          mapping_proof: map() | nil,
          pinned_input_generation_ids: [map()],
          candidate_generation_id: String.t() | nil,
          status: atom(),
          child_operation_id: String.t() | nil,
          child_run_id: String.t() | nil,
          activation_intent: map() | nil,
          validation_result: map() | nil,
          terminal_error: map() | nil,
          cleanup_state: atom(),
          activated_at: DateTime.t() | nil,
          version: pos_integer(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t(),
          progress: map()
        }
end

defmodule FavnOrchestrator.Persistence.Results.RebuildItem do
  @moduledoc "One authoritative frozen and fenced rebuild item."
  defstruct [
    :workspace_id,
    :operation_id,
    :target_id,
    :item_id,
    :ordinal,
    :work_kind,
    :window_key,
    :window_start,
    :window_end,
    :runtime_input_expectation,
    :status,
    :claim_owner,
    :fencing_token,
    :claim_expires_at,
    :child_run_id,
    :materialization_id,
    :attempt_count,
    :row_count,
    :last_error,
    :candidate_generation_id,
    :version,
    :inserted_at,
    :updated_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          operation_id: String.t(),
          target_id: String.t(),
          item_id: String.t(),
          ordinal: non_neg_integer(),
          work_kind: atom(),
          window_key: String.t(),
          window_start: DateTime.t() | nil,
          window_end: DateTime.t() | nil,
          runtime_input_expectation: map() | nil,
          status: atom(),
          claim_owner: String.t() | nil,
          fencing_token: non_neg_integer(),
          claim_expires_at: DateTime.t() | nil,
          child_run_id: String.t() | nil,
          materialization_id: String.t() | nil,
          attempt_count: non_neg_integer(),
          row_count: non_neg_integer() | nil,
          last_error: map() | nil,
          candidate_generation_id: String.t() | nil,
          version: pos_integer(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Results.TargetOperationLock do
  @moduledoc "One fenced target-operation lease."
  defstruct [
    :workspace_id,
    :target_id,
    :operation_id,
    :operation_type,
    :fencing_token,
    :lease_owner,
    :lease_expires_at,
    :version,
    :inserted_at,
    :updated_at
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          target_id: String.t(),
          operation_id: String.t(),
          operation_type: atom(),
          fencing_token: pos_integer(),
          lease_owner: String.t(),
          lease_expires_at: DateTime.t(),
          version: pos_integer(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }
end
