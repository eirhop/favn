defmodule FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle do
  @moduledoc """
  Semantic contract for one orchestrated step attempt.

  This module centralizes runner status mapping, retryability, retry scheduling
  data, and runner work construction. Process mechanics remain owned by the run
  server and stage scheduler.
  """

  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias Favn.Plan.NodeIdentity
  alias Favn.Run.AssetResult
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.RunServer.Execution.ExecutionPool
  alias FavnOrchestrator.RunState

  @type node_key :: Favn.Plan.node_key()
  @type retry :: %{
          required(:node_key) => node_key(),
          required(:attempt) => pos_integer(),
          required(:next_attempt) => pos_integer(),
          required(:retry_after_ms) => non_neg_integer(),
          required(:asset_step_id) => String.t(),
          required(:asset_ref) => Favn.Ref.t(),
          optional(:window) => term(),
          optional(:execution_pool) => atom() | String.t() | nil
        }

  @type t :: %__MODULE__{
          run: RunState.t(),
          version: Version.t(),
          node_key: node_key(),
          asset_ref: Favn.Ref.t(),
          asset_step_id: String.t(),
          window: term(),
          stage: non_neg_integer(),
          attempt: pos_integer(),
          max_attempts: pos_integer(),
          execution_pool: atom() | String.t() | nil,
          work: RunnerWork.t() | nil
        }

  defstruct run: nil,
            version: nil,
            node_key: nil,
            asset_ref: nil,
            asset_step_id: nil,
            window: nil,
            stage: 0,
            attempt: 1,
            max_attempts: 1,
            execution_pool: nil,
            work: nil

  @doc "Creates lifecycle state for one planned step attempt."
  @spec new(RunState.t(), Version.t(), node_key(), non_neg_integer(), pos_integer()) :: t()
  def new(%RunState{} = run_state, %Version{} = version, node_key, stage, attempt)
      when is_integer(stage) and stage >= 0 and is_integer(attempt) and attempt > 0 do
    asset_ref = node_asset_ref(run_state, node_key)

    %__MODULE__{
      run: run_state,
      version: version,
      node_key: node_key,
      asset_ref: asset_ref,
      asset_step_id: AssetStepIdentity.asset_step_id(run_state.id, node_key, asset_ref),
      window: node_window(run_state, node_key),
      stage: stage,
      attempt: attempt,
      max_attempts: run_state.max_attempts,
      execution_pool: ExecutionPool.for_node(run_state, node_key)
    }
  end

  @doc "Builds runner work for this attempt."
  @spec build_work(t()) :: {:ok, t()} | {:error, term()}
  def build_work(%__MODULE__{} = lifecycle) do
    with {:ok, node_identity} <- node_identity(lifecycle) do
      work = %RunnerWork{
        run_id: lifecycle.run.id,
        manifest_version_id: node_identity.manifest_version_id,
        manifest_content_hash: lifecycle.version.content_hash,
        node_identity: node_identity,
        asset_ref: lifecycle.asset_ref,
        asset_refs: [lifecycle.asset_ref],
        planned_asset_refs: node_identity.planned_asset_refs,
        attempt: lifecycle.attempt,
        max_attempts: lifecycle.max_attempts,
        asset_step_id: lifecycle.asset_step_id,
        stage: lifecycle.stage,
        params: lifecycle.run.params,
        trigger:
          lifecycle.run.trigger
          |> Map.put(:window, node_identity.window)
          |> maybe_put_pipeline_trigger(
            Map.get(
              lifecycle.run.metadata,
              :pipeline_context,
              Map.get(lifecycle.run.metadata, "pipeline_context")
            )
          ),
        metadata: work_metadata(lifecycle.run.metadata)
      }

      {:ok, %{lifecycle | work: work}}
    end
  end

  @doc "Returns the persisted event type and base retryability for a run status."
  @spec step_outcome(RunState.status()) :: {atom(), boolean()}
  def step_outcome(:ok), do: {:step_finished, false}
  def step_outcome(:cancelled), do: {:step_cancelled, false}
  def step_outcome(:timed_out), do: {:step_timed_out, true}
  def step_outcome(:error), do: {:step_failed, true}
  def step_outcome(_other), do: {:step_failed, true}

  @doc "Maps runner result status into run status."
  @spec map_runner_status(term()) :: RunState.status()
  def map_runner_status(:ok), do: :ok
  def map_runner_status(:cancelled), do: :cancelled
  def map_runner_status(:timed_out), do: :timed_out
  def map_runner_status(_other), do: :error

  @doc "Returns true when a runner result can be retried."
  @spec runner_result_retryable?(RunnerResult.t() | term()) :: boolean()
  def runner_result_retryable?(%RunnerResult{error: error, asset_results: asset_results}) do
    runner_error_retryable?(error) and Enum.all?(asset_results || [], &asset_result_retryable?/1)
  end

  def runner_result_retryable?(_result), do: false

  @doc "Builds retry scheduling data, or says the attempt is terminal."
  @spec schedule_retry(t()) :: {:ok, retry()} | :terminal
  def schedule_retry(%__MODULE__{} = lifecycle) do
    if lifecycle.attempt < lifecycle.max_attempts do
      {:ok,
       %{
         node_key: lifecycle.node_key,
         asset_ref: lifecycle.asset_ref,
         asset_step_id: lifecycle.asset_step_id,
         window: lifecycle.window,
         stage: lifecycle.stage,
         attempt: lifecycle.attempt,
         max_attempts: lifecycle.max_attempts,
         next_attempt: lifecycle.attempt + 1,
         retry_after_ms: max(lifecycle.run.retry_backoff_ms, 0),
         execution_pool: lifecycle.execution_pool
       }}
    else
      :terminal
    end
  end

  @doc "Builds the event payload for a scheduled retry."
  @spec retry_event_payload(retry()) :: map()
  def retry_event_payload(retry) when is_map(retry) do
    %{
      asset_ref: retry.asset_ref,
      node_key: retry.node_key,
      asset_step_id: retry.asset_step_id,
      window: Map.get(retry, :window),
      stage: retry.stage,
      attempt: retry.attempt,
      max_attempts: retry.max_attempts,
      execution_pool: Map.get(retry, :execution_pool),
      next_attempt: retry.next_attempt,
      retry_backoff_ms: retry.retry_after_ms
    }
  end

  defp asset_result_retryable?(%RunnerAssetResult{error: error}),
    do: runner_error_retryable?(error)

  defp asset_result_retryable?(%AssetResult{error: error}), do: structured_retryable?(error)
  defp asset_result_retryable?(%{error: error}), do: structured_retryable?(error)
  defp asset_result_retryable?(%{"error" => error}), do: structured_retryable?(error)
  defp asset_result_retryable?(_result), do: false

  defp runner_error_retryable?(%RunnerError{retryable?: retryable?}), do: retryable?
  defp runner_error_retryable?(error), do: structured_retryable?(error)

  defp structured_retryable?(%{details: details}) when is_map(details),
    do: retryable_detail?(Map.get(details, :asset_retryable?))

  defp structured_retryable?(%{"details" => details}) when is_map(details),
    do: retryable_detail?(Map.get(details, "asset_retryable?"))

  defp structured_retryable?(_error), do: true

  defp retryable_detail?(false), do: false
  defp retryable_detail?("false"), do: false
  defp retryable_detail?(_other), do: true

  defp node_identity(%__MODULE__{
         run: %{plan: %Favn.Plan{} = plan},
         version: version,
         node_key: node_key
       }) do
    NodeIdentity.from_plan(version.manifest_version_id, plan, node_key)
  end

  defp node_identity(%__MODULE__{} = lifecycle) do
    {:ok,
     NodeIdentity.new!(%{
       manifest_version_id: lifecycle.version.manifest_version_id,
       node_key: lifecycle.node_key,
       target_refs: lifecycle.run.target_refs || [],
       planned_asset_refs: planned_asset_refs(lifecycle.run),
       window: nil,
       execution_pool: nil
     })}
  end

  defp node_asset_ref(%RunState{plan: %Favn.Plan{nodes: nodes}}, node_key) do
    case Map.fetch(nodes, node_key) do
      {:ok, node} -> node.ref
      :error -> elem(node_key, 0)
    end
  end

  defp node_asset_ref(%RunState{}, node_key), do: elem(node_key, 0)

  defp node_window(%RunState{plan: %Favn.Plan{nodes: nodes}}, node_key) do
    case Map.fetch(nodes, node_key) do
      {:ok, node} -> node.window
      :error -> nil
    end
  end

  defp node_window(%RunState{}, _node_key), do: nil

  defp planned_asset_refs(%RunState{target_refs: refs}) when is_list(refs) and refs != [],
    do: refs

  defp planned_asset_refs(%RunState{asset_ref: ref}) when is_tuple(ref), do: [ref]
  defp planned_asset_refs(%RunState{}), do: []

  defp maybe_put_pipeline_trigger(trigger, pipeline_context) when is_map(pipeline_context),
    do: Map.put(trigger, :pipeline, pipeline_context)

  defp maybe_put_pipeline_trigger(trigger, _pipeline_context), do: trigger

  defp work_metadata(metadata) when is_map(metadata) do
    metadata
    |> Map.delete(:runner_metadata)
    |> Map.delete("runner_metadata")
  end
end
