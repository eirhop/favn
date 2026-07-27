defmodule FavnOrchestrator.Persistence.Commands.EnqueueRunnerTask do
  @moduledoc "Idempotently admits one durable runner task."
  @enforce_keys [
    :workspace_context,
    :command_id,
    :task_id,
    :domain_identity,
    :task_kind,
    :runner_pool,
    :required_runner_release_id,
    :retry_class,
    :payload,
    :payload_hash,
    :occurred_at
  ]
  defstruct @enforce_keys ++
              [
                :run_id,
                :operation_id,
                :asset_step_id,
                :required_capability,
                :deadline_at,
                :orchestration_context
              ]

  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.ClaimRunnerTask do
  @moduledoc "Atomically claims the oldest compatible queued runner task."
  @enforce_keys [
    :platform_context,
    :command_id,
    :runner_instance_id,
    :runner_session_generation,
    :runner_pool,
    :required_runner_release_id,
    :supported_task_kinds,
    :capabilities,
    :lease_duration_ms,
    :occurred_at
  ]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.TransitionRunnerTask do
  @moduledoc "Fenced nonterminal runner-task transition."
  @enforce_keys [
    :workspace_context,
    :command_id,
    :task_id,
    :runner_instance_id,
    :runner_session_generation,
    :assignment_generation,
    :transition,
    :occurred_at
  ]
  defstruct @enforce_keys ++ [:lease_duration_ms]
  @type transition :: :preparing | :running | :renew
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.PersistRunnerTaskRuntimeInputs do
  @moduledoc """
  Persists the fenced acknowledgement metadata for a runtime-input resolution.

  Secret values remain in the encrypted runtime-input pin store. This command
  records only the stable resolution identity, outcome, and payload fingerprint.
  """

  @enforce_keys [
    :workspace_context,
    :command_id,
    :task_id,
    :runner_instance_id,
    :runner_session_generation,
    :assignment_generation,
    :resolution_id,
    :status,
    :occurred_at
  ]
  defstruct @enforce_keys ++ [:payload_fingerprint, :runtime_input_pin, :error]
  @type status :: :resolved | :failed
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.AppendRunnerTaskLogBatch do
  @moduledoc "Appends one fenced, deduplicated runner-task log batch."
  @enforce_keys [
    :workspace_context,
    :command_id,
    :task_id,
    :runner_instance_id,
    :runner_session_generation,
    :assignment_generation,
    :batch_id,
    :sequence,
    :entries,
    :payload_hash,
    :occurred_at
  ]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.CompleteRunnerTask do
  @moduledoc "Persists one fenced terminal runner-task result."
  @enforce_keys [
    :workspace_context,
    :command_id,
    :task_id,
    :runner_instance_id,
    :runner_session_generation,
    :assignment_generation,
    :result_version,
    :outcome,
    :retry_class,
    :result,
    :occurred_at
  ]
  defstruct @enforce_keys ++ [:error]
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.RequestRunnerTaskCancellation do
  @moduledoc "Durably requests cancellation of one runner task."
  @enforce_keys [:workspace_context, :command_id, :task_id, :reason, :occurred_at]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.AcknowledgeRunnerTaskCancellation do
  @moduledoc "Persists a fenced acknowledgement that a runner observed cancellation."
  @enforce_keys [
    :workspace_context,
    :command_id,
    :task_id,
    :runner_instance_id,
    :runner_session_generation,
    :assignment_generation,
    :occurred_at
  ]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.ReleaseRunnerTask do
  @moduledoc "Safely releases or marks unknown one fenced assignment."
  @enforce_keys [
    :workspace_context,
    :command_id,
    :task_id,
    :runner_instance_id,
    :runner_session_generation,
    :assignment_generation,
    :disposition,
    :reason,
    :occurred_at
  ]
  defstruct @enforce_keys
  @type disposition :: :requeue | :unknown | :cancelled
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.RetryRunnerTask do
  @moduledoc "Requeues one terminal task only when its persisted outcome proves retry safety."
  @enforce_keys [
    :workspace_context,
    :command_id,
    :task_id,
    :expected_assignment_generation,
    :expected_result_version,
    :occurred_at
  ]
  defstruct @enforce_keys
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.RecoverRunnerTasks do
  @moduledoc "Claims a bounded platform-global batch of expired assignments for recovery."
  @enforce_keys [:platform_context, :command_id, :owner_id, :occurred_at]
  defstruct @enforce_keys ++ [limit: 50, lease_duration_ms: 30_000]
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Commands.ReconcileRunnerCapacityDemand do
  @moduledoc "Audits or repairs one bounded exact capacity-demand projection."
  @enforce_keys [
    :platform_context,
    :command_id,
    :runner_pool,
    :required_runner_release_id,
    :occurred_at
  ]
  defstruct @enforce_keys ++ [mode: :audit]
  @type mode :: :audit | :repair
  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Results.RunnerTask do
  @moduledoc "Durable runner-task state returned by persistence."
  defstruct [
    :workspace_id,
    :task_id,
    :domain_identity,
    :task_kind,
    :run_id,
    :operation_id,
    :asset_step_id,
    :runner_pool,
    :required_runner_release_id,
    :required_capability,
    :retry_class,
    :status,
    :enqueued_at,
    :deadline_at,
    :payload_version,
    :payload,
    :payload_hash,
    :orchestration_context,
    :assigned_runner_instance_id,
    :assigned_runner_session_generation,
    :assignment_generation,
    :assignment_expires_at,
    :cancellation_requested_at,
    :cancellation_acknowledged_at,
    :runtime_input_resolution_id,
    :runtime_input_resolution_status,
    :runtime_input_payload_fingerprint,
    :runtime_input_error,
    :runtime_inputs_resolved_at,
    :result_version,
    :result,
    :error,
    :terminal_at,
    :inserted_at,
    :updated_at
  ]

  @type t :: %__MODULE__{}
end

defmodule FavnOrchestrator.Persistence.Results.RunnerCapacityDemand do
  @moduledoc "O(1) exact demand projection for one pool and release."
  defstruct [
    :runner_pool,
    :required_runner_release_id,
    :outstanding_count,
    :queued_count,
    :active_count,
    :oldest_queued_at,
    :version,
    :updated_at,
    healthy?: true
  ]

  @type t :: %__MODULE__{}
end
