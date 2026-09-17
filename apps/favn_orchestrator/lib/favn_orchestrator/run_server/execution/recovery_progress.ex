defmodule FavnOrchestrator.RunServer.Execution.RecoveryProgress do
  @moduledoc """
  Reconstructs compact execution facts from authoritative, ordered run events.

  Results remain in their owning records. This reducer retains event references
  for every planned node, not accumulated result payloads or process state. It
  neither admits work nor interprets a missing task as permission to execute.
  """

  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.RunState

  @enforce_keys [:run_id, :manifest_version_id, :manifest_content_hash, :nodes]
  defstruct @enforce_keys ++ [sequence: 0, steps: %{}, position: nil, failure: nil]

  @type step :: %{
          required(:node_key) => Favn.Plan.node_key(),
          required(:stage) => non_neg_integer(),
          required(:attempt) => non_neg_integer(),
          required(:phase) => :intended | :submitted | :outcome | :settled,
          required(:sequence) => pos_integer(),
          optional(:task_id) => String.t(),
          optional(:outcome_sequence) => pos_integer(),
          optional(:intent_sequence) => pos_integer(),
          optional(:status) => atom(),
          optional(:retry_allowed?) => boolean()
        }
  @type t :: %__MODULE__{
          run_id: String.t(),
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          nodes: map(),
          sequence: non_neg_integer(),
          steps: %{optional(String.t()) => step()},
          position: map() | nil,
          failure: map() | nil
        }

  @outcomes %{
    "step_finished" => :ok,
    "step_failed" => :error,
    "step_timed_out" => :timed_out,
    "step_cancelled" => :cancelled,
    "step_skipped_fresh" => :skipped_fresh,
    "step_blocked" => :blocked
  }
  @outcome_events Map.keys(@outcomes)
  @step_events @outcome_events ++
                 ["step_intended", "step_started", "step_retry_started", "step_settled"]

  @doc "Builds the identity index from the pinned plan, without creating atoms."
  @spec new(RunState.t()) :: t()
  def new(%RunState{} = run) do
    nodes =
      Map.new(run.plan.nodes, fn {key, node} ->
        {AssetStepIdentity.asset_step_id(run.id, key, node.ref),
         %{node_key: key, stage: node.stage}}
      end)

    %__MODULE__{
      run_id: run.id,
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      nodes: nodes
    }
  end

  @doc "Folds a contiguous event page, rejecting gaps and foreign identities."
  @spec fold(t(), [map()]) :: {:ok, t()} | {:error, term()}
  def fold(%__MODULE__{} = progress, events) when is_list(events) do
    Enum.reduce_while(events, {:ok, progress}, fn event, {:ok, acc} ->
      case apply_event(acc, event) do
        {:ok, next} -> {:cont, {:ok, next}}
        error -> {:halt, error}
      end
    end)
  end

  @doc "Applies one saved fact; a step outcome is not a settlement receipt."
  @spec apply_event(t(), map()) :: {:ok, t()} | {:error, term()}
  def apply_event(
        %__MODULE__{} = progress,
        %{
          run_id: run_id,
          manifest_version_id: manifest_id,
          manifest_content_hash: manifest_hash,
          sequence: sequence,
          event_type: kind
        } = event
      )
      when is_atom(kind) or is_binary(kind) do
    with true <- run_id == progress.run_id,
         true <- manifest_id == progress.manifest_version_id,
         true <- manifest_hash == progress.manifest_content_hash,
         true <- sequence == progress.sequence + 1,
         {:ok, next} <- reduce(progress, to_string(kind), event) do
      {:ok, %{next | sequence: sequence}}
    else
      false -> {:error, :invalid_recovery_event_identity_or_sequence}
      {:error, _} = error -> error
    end
  end

  def apply_event(%__MODULE__{}, _event),
    do: {:error, :invalid_recovery_event_identity_or_sequence}

  defp reduce(progress, "run_execution_position", event) do
    position = field(field(event, :data), :position)

    if valid_position?(position),
      do: {:ok, %{progress | position: position}},
      else: {:error, :invalid_recovery_position}
  end

  defp reduce(progress, kind, event) when kind in @step_events do
    data = field(event, :data)
    id = field(data, :asset_step_id)
    stage = field(data, :stage)
    attempt = field(data, :attempt, 0)

    with {:ok, %{node_key: key, stage: ^stage}} <- Map.fetch(progress.nodes, id),
         true <- is_integer(attempt) and attempt >= 0,
         true <- attempt > 0 or kind in ["step_skipped_fresh", "step_blocked"],
         true <- is_boolean(field(data, :retryable?, false)),
         true <- is_boolean(field(data, :retry_exhausted?, false)),
         {:ok, step} <-
           step(Map.get(progress.steps, id), key, stage, attempt, kind, event) do
      next = %{progress | steps: Map.put(progress.steps, id, step)}
      {:ok, remember_failure(next, kind, event, step)}
    else
      _invalid -> {:error, :invalid_recovery_step}
    end
  end

  defp reduce(progress, _kind, _event), do: {:ok, progress}

  defp step(previous, key, stage, attempt, kind, event) do
    previous_attempt = if previous, do: previous.attempt, else: 0

    cond do
      attempt < previous_attempt ->
        {:error, :recovery_attempt_regressed}

      invalid_restart?(previous, kind, attempt) ->
        {:error, :recovery_step_regressed}

      true ->
        base =
          if previous && attempt == previous_attempt,
            do: previous,
            else: %{node_key: key, stage: stage, attempt: attempt}

        base = Map.put(base, :sequence, event.sequence)
        update_step(base, kind, event)
    end
  end

  defp invalid_restart?(nil, _kind, _attempt), do: false

  defp invalid_restart?(previous, kind, attempt) do
    starts? = kind in ["step_intended", "step_started", "step_retry_started"]
    finished? = previous.phase in [:outcome, :settled]

    same_attempt? = attempt == previous.attempt

    (starts? and same_attempt? and (finished? or previous.phase == :submitted)) or
      (attempt > previous.attempt and previous[:retry_allowed?] != true) or
      (kind in @outcome_events and finished?) or
      (kind == "step_intended" and same_attempt?)
  end

  defp update_step(step, "step_intended", event),
    do: {:ok, Map.merge(step, %{phase: :intended, intent_sequence: event.sequence})}

  defp update_step(step, kind, event) when kind in ["step_started", "step_retry_started"] do
    case field(event.data, :runner_task_id) do
      id when is_binary(id) and byte_size(id) in 1..255 ->
        {:ok, Map.merge(step, %{phase: :submitted, task_id: id})}

      _invalid ->
        {:error, :invalid_recovery_task_reference}
    end
  end

  defp update_step(%{phase: :outcome} = step, "step_settled", _event),
    do: {:ok, %{step | phase: :settled}}

  defp update_step(_step, "step_settled", _event),
    do: {:error, :recovery_settlement_without_outcome}

  defp update_step(step, kind, event) do
    status = Map.fetch!(@outcomes, kind)
    phase = if status in [:skipped_fresh, :blocked], do: :settled, else: :outcome

    {:ok,
     Map.merge(step, %{
       phase: phase,
       status: status,
       retry_allowed?:
         status in [:error, :timed_out] and
           field(event.data, :retryable?, false) and
           not field(event.data, :retry_exhausted?, false),
       outcome_sequence: event.sequence
     })}
  end

  defp remember_failure(%{failure: nil} = progress, kind, event, step)
       when kind in ["step_failed", "step_timed_out", "step_cancelled", "step_blocked"] do
    if step.retry_allowed?,
      do: progress,
      else: %{
        progress
        | failure: %{
            sequence: event.sequence,
            status: if(step.status == :blocked, do: :error, else: step.status)
          }
      }
  end

  defp remember_failure(progress, _kind, _event, _step), do: progress

  defp valid_position?(position) when is_map(position) do
    map_size(position) == 5 and field(position, :version) == 1 and
      field(position, :mode) in ["pipeline", "sequential"] and
      field(position, :phase) in ["classify", "admit", "retry", "advance", "finish"] and
      is_integer(field(position, :index)) and field(position, :index) >= 0 and
      is_integer(field(position, :attempt)) and field(position, :attempt) > 0
  end

  defp valid_position?(_position), do: false

  defp field(map, key, default \\ nil)

  defp field(map, key, default) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp field(_invalid, _key, default), do: default
end
