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
  alias Favn.Retry.Policy
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
          retry_policy: Policy.t() | nil,
          retry_policy_source: Favn.Plan.retry_policy_source() | nil,
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
            retry_policy: nil,
            retry_policy_source: nil,
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
      max_attempts: retry_policy(run_state, node_key).max_attempts,
      retry_policy: retry_policy(run_state, node_key),
      retry_policy_source: retry_policy_source(run_state, node_key),
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
        pipeline:
          lifecycle.run.metadata
          |> Map.get(:pipeline_context, Map.get(lifecycle.run.metadata, "pipeline_context"))
          |> Favn.Run.PipelineContext.from_map(),
        trigger: Map.put(lifecycle.run.trigger, :window, node_identity.window),
        metadata: work_metadata(lifecycle.run.metadata)
      }

      {:ok, %{lifecycle | work: work}}
    end
  end

  @doc "Attaches one absolute attempt deadline before any runner phase begins."
  @spec attach_deadline(RunnerWork.t(), RunState.t()) :: RunnerWork.t()
  def attach_deadline(%RunnerWork{} = work, %RunState{} = run) do
    deadline_at =
      case run.timeout_ms do
        timeout_ms when is_integer(timeout_ms) and timeout_ms > 0 ->
          DateTime.add(DateTime.utc_now(), timeout_ms, :millisecond)

        _timeout_ms ->
          nil
      end

    %{work | deadline_at: deadline_at}
  end

  @doc "Returns the absolute deadline already attached to runner work."
  @spec deadline_at(RunnerWork.t()) :: DateTime.t() | nil
  def deadline_at(%RunnerWork{deadline_at: deadline_at}), do: deadline_at

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
  @spec schedule_retry(t(), RunnerResult.t() | RunnerError.t() | term()) ::
          {:ok, retry()} | :terminal
  def schedule_retry(%__MODULE__{} = lifecycle, failure \\ nil) do
    if lifecycle.attempt < lifecycle.max_attempts do
      policy = lifecycle.retry_policy || legacy_retry_policy(lifecycle.run)

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
         retry_after_ms:
           Policy.delay_ms(policy, lifecycle.attempt, retry_after_ms(failure), :rand.uniform()),
         retry_policy: policy,
         retry_policy_source: lifecycle.retry_policy_source || :operator,
         execution_pool: lifecycle.execution_pool
       }}
    else
      :terminal
    end
  end

  @doc "Returns the policy frozen into one planned node."
  @spec retry_policy(RunState.t(), node_key()) :: Policy.t()
  def retry_policy(%RunState{plan: %Favn.Plan{nodes: nodes}} = run, node_key) do
    case get_in(nodes, [node_key, :retry_policy]) do
      %Policy{} = policy -> policy
      _missing -> legacy_retry_policy(run)
    end
  end

  def retry_policy(%RunState{} = run, _node_key), do: legacy_retry_policy(run)

  @doc "Returns whether another attempt remains for one planned node."
  @spec retry_allowed?(RunState.t(), node_key(), pos_integer()) :: boolean()
  def retry_allowed?(%RunState{} = run, node_key, attempt),
    do: attempt < retry_policy(run, node_key).max_attempts

  @doc "Calculates a node retry delay from the frozen policy."
  @spec retry_delay_ms(RunState.t(), node_key(), pos_integer(), term()) :: non_neg_integer()
  def retry_delay_ms(%RunState{} = run, node_key, attempt, failure \\ nil) do
    Policy.delay_ms(
      retry_policy(run, node_key),
      attempt,
      retry_after_ms(failure),
      :rand.uniform()
    )
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
      retry_backoff_ms: retry.retry_after_ms,
      retry_policy: retry.retry_policy,
      retry_policy_source: retry.retry_policy_source
    }
  end

  defp asset_result_retryable?(%RunnerAssetResult{error: error}),
    do: runner_error_retryable?(error)

  defp asset_result_retryable?(%AssetResult{error: error}), do: runner_error_retryable?(error)
  defp asset_result_retryable?(%{error: error}), do: runner_error_retryable?(error)
  defp asset_result_retryable?(%{"error" => error}), do: runner_error_retryable?(error)
  defp asset_result_retryable?(_result), do: false

  defp runner_error_retryable?(%RunnerError{retryable?: true, outcome: :safe_failure}), do: true
  defp runner_error_retryable?(%RunnerError{}), do: false
  defp runner_error_retryable?(error), do: structured_retryable?(error)

  defp structured_retryable?(%{details: details} = error) when is_map(details),
    do:
      retryable_detail?(Map.get(details, :asset_retryable?)) and
        safe_outcome?(Map.get(error, :outcome))

  defp structured_retryable?(%{"details" => details} = error) when is_map(details),
    do:
      retryable_detail?(Map.get(details, "asset_retryable?")) and
        safe_outcome?(Map.get(error, "outcome"))

  defp structured_retryable?(_error), do: false

  defp retryable_detail?(false), do: false
  defp retryable_detail?("false"), do: false
  defp retryable_detail?(true), do: true
  defp retryable_detail?("true"), do: true
  defp retryable_detail?(_other), do: false

  defp safe_outcome?(:safe_failure), do: true
  defp safe_outcome?("safe_failure"), do: true
  defp safe_outcome?(_outcome), do: false

  defp retry_after_ms(%RunnerResult{error: error}), do: retry_after_ms(error)
  defp retry_after_ms(%RunnerError{retry_after_ms: value}), do: value
  defp retry_after_ms(%{retry_after_ms: value}), do: value
  defp retry_after_ms(%{"retry_after_ms" => value}), do: value
  defp retry_after_ms(_failure), do: nil

  defp legacy_retry_policy(%RunState{} = run) do
    Policy.new!(max_attempts: run.max_attempts, backoff: run.retry_backoff_ms)
  end

  defp retry_policy_source(%RunState{plan: %Favn.Plan{nodes: nodes}}, node_key),
    do: get_in(nodes, [node_key, :retry_policy_source])

  defp retry_policy_source(%RunState{}, _node_key), do: :operator

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

  defp work_metadata(metadata) when is_map(metadata) do
    metadata
    |> Map.delete(:runner_metadata)
    |> Map.delete("runner_metadata")
    |> Map.delete(:pipeline_context)
    |> Map.delete("pipeline_context")
  end
end
