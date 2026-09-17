defmodule FavnOrchestrator.RunServer.Execution.StageClassifier do
  @moduledoc """
  Classifies and persists freshness decisions for one pipeline stage.

  Runnable nodes retain plan order. Fresh and blocked nodes receive durable node
  results before later stages are considered.
  """

  alias Favn.Freshness.Key
  alias Favn.Manifest.Version
  alias Favn.Run.NodeResult
  alias Favn.Retry.Policy, as: RetryPolicy
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.Freshness.Decider
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunServer.Execution.ExecutionPool
  alias FavnOrchestrator.RunServer.Execution.FreshnessContext
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RunnerPoolSelection

  @type terminal_failure :: %{required(:status) => RunState.status(), required(:error) => term()}

  @type result ::
          {:ok, RunState.t(), [Favn.Plan.node_key()], map(), FreshnessContext.t(),
           terminal_failure() | nil, [Favn.Plan.node_key()]}
          | {:error, RunState.t()}
          | {:persist_retry, PersistenceRetry.t(), term()}

  @max_batch_nodes 4
  @max_batch_ms 25

  @doc "Returns runnable nodes and persists non-running stage decisions."
  @spec classify(
          RunState.t(),
          Version.t(),
          non_neg_integer(),
          [Favn.Plan.node_key()],
          FreshnessContext.t(),
          terminal_failure() | nil
        ) :: result()
  def classify(
        %RunState{} = run_state,
        %Version{} = version,
        stage,
        node_keys,
        freshness_context,
        terminal_failure
      ) do
    {batch, remaining} = take_batch(node_keys)
    decisions = decisions(run_state, batch, freshness_context)

    classify_nodes(
      batch,
      {:ok, run_state, [], decisions, freshness_context, terminal_failure},
      version,
      stage,
      remaining
    )
  end

  defp classify_nodes([], result, _version, _stage, remaining),
    do: result |> restore_runnable_order() |> append_remaining(remaining)

  defp classify_nodes([node_key | rest], result, version, stage, remaining) do
    case classify_node(node_key, result, version, stage, rest ++ remaining) do
      {:cont, next} -> classify_nodes(rest, next, version, stage, remaining)
      {:halt, {:persist_retry, _, _} = retry} -> retry
      {:halt, other} -> other
    end
  end

  @doc false
  @spec resume_persisted(map(), RunState.t()) :: result()
  def resume_persisted(ctx, run) do
    {:cont, {:ok, next, runnable, decisions, context, failure}} = finish_decision(ctx, run)
    {:ok, next, Enum.reverse(runnable), decisions, context, failure, ctx.remaining_node_keys}
  end

  @doc false
  @spec decisions(RunState.t(), [Favn.Plan.node_key()], FreshnessContext.t(), keyword()) :: map()
  def decisions(run_state, node_keys, freshness_context, opts \\ []) do
    Decider.decide_many(run_state.plan, node_keys,
      assets_by_ref: freshness_context.assets_by_ref,
      refresh_policy: freshness_context.refresh_policy,
      forced_node_keys: forced_node_keys(run_state, freshness_context, opts),
      prior_states: freshness_context.prior_states,
      current_states: freshness_context.current_states,
      completed_node_keys: freshness_context.completed_node_keys,
      refreshed_node_keys: freshness_context.refreshed_node_keys,
      upstream_statuses: freshness_context.upstream_statuses,
      now: freshness_context.now
    )
  end

  defp classify_node(
         node_key,
         {:ok, current_run, runnable, decisions, current_context, current_failure},
         version,
         stage,
         remaining
       ) do
    decision = Map.fetch!(decisions, node_key)

    case decision.decision do
      :run ->
        {:cont,
         {:ok, current_run, [node_key | runnable], decisions, current_context, current_failure}}

      status when status in [:skipped_fresh, :blocked] ->
        persist_non_running_decision(%{
          run: current_run,
          version: version,
          node_key: node_key,
          stage: stage,
          status: status,
          decision: decision,
          runnable: runnable,
          decisions: decisions,
          context: current_context,
          terminal_failure: current_failure,
          remaining_node_keys: remaining
        })
    end
  end

  defp persist_non_running_decision(ctx) do
    retry =
      prepare_decision(ctx.run, ctx.version, ctx.node_key, ctx.stage, ctx.status, ctx.decision)

    retry = %{
      retry
      | resume: {:stage_classification, Map.put(ctx, :persisted_run, decision_result(retry))}
    }

    case PersistenceRetry.persist(retry) do
      :ok ->
        finish_decision(ctx, decision_result(retry))

      {:error, :external_cancel} ->
        {:halt, {:error, Snapshots.cancelled_snapshot(ctx.run)}}

      {:error, reason} ->
        if PersistenceRetry.replayable?(reason),
          do: {:halt, {:persist_retry, retry, reason}},
          else:
            {:halt, {:error, Snapshots.snapshot_update(ctx.run, status: :error, error: reason)}}
    end
  end

  defp finish_decision(ctx, next_run) do
    next_context = record_status(ctx.context, ctx.node_key, ctx.status)

    next_failure =
      if ctx.status == :blocked and is_nil(ctx.terminal_failure),
        do: %{status: :error, error: {:blocked, ctx.node_key, ctx.decision.reason}},
        else: ctx.terminal_failure

    {:cont, {:ok, next_run, ctx.runnable, ctx.decisions, next_context, next_failure}}
  end

  defp restore_runnable_order({:ok, run, runnable, decisions, context, failure}),
    do: {:ok, run, Enum.reverse(runnable), decisions, context, failure}

  defp append_remaining({:ok, run, runnable, decisions, context, failure}, remaining),
    do: {:ok, run, runnable, decisions, context, failure, remaining}

  defp take_batch(node_keys) do
    started_at = System.monotonic_time(:millisecond)

    Enum.reduce_while(node_keys, {[], node_keys, 0}, fn node_key, {batch, remaining, count} ->
      if count >= @max_batch_nodes or
           (count > 0 and System.monotonic_time(:millisecond) - started_at >= @max_batch_ms) do
        {:halt, {batch, remaining, count}}
      else
        {:cont, {[node_key | batch], tl(remaining), count + 1}}
      end
    end)
    |> then(fn {batch, remaining, _count} -> {Enum.reverse(batch), remaining} end)
  end

  defp forced_node_keys(run_state, freshness_context, opts) do
    case Keyword.fetch(opts, :forced_node_keys) do
      {:ok, forced} ->
        forced

      :error ->
        Map.get_lazy(freshness_context, :forced_node_keys, fn ->
          RefreshPolicy.expand_force_set(freshness_context.refresh_policy, run_state.plan)
        end)
    end
  end

  @doc "Persists one already-classified fresh or blocked node decision."
  @spec persist_decision(
          RunState.t(),
          Version.t(),
          Favn.Plan.node_key(),
          non_neg_integer(),
          :skipped_fresh | :blocked,
          map()
        ) :: {:ok, RunState.t()} | {:error, term()}
  def persist_decision(run, version, node_key, stage, status, decision) do
    retry = prepare_decision(run, version, node_key, stage, status, decision)
    with :ok <- PersistenceRetry.persist(retry), do: {:ok, decision_result(retry)}
  end

  @doc false
  @spec decision_result(PersistenceRetry.t()) :: RunState.t()
  def decision_result(retry),
    do: ResultBuilder.append_node_result(retry.run, retry.data.node_result)

  @doc false
  @spec prepare_decision(
          RunState.t(),
          Version.t(),
          Favn.Plan.node_key(),
          non_neg_integer(),
          atom(),
          map()
        ) :: PersistenceRetry.t()
  def prepare_decision(
        %RunState{} = run_state,
        %Version{} = _version,
        node_key,
        stage,
        status,
        decision
      ) do
    node = Map.fetch!(run_state.plan.nodes, node_key)
    now = DateTime.utc_now()
    freshness_key = Map.get(decision, :freshness_key, Key.latest())
    asset_step_id = AssetStepIdentity.asset_step_id(run_state.id, node_key, node.ref)
    execution_pool = ExecutionPool.for_node(run_state, node_key)

    result =
      NodeResult.new(%{
        node_key: node_key,
        ref: node.ref,
        window: node.window,
        stage: stage,
        execution_pool: execution_pool,
        runner_pool: RunnerPoolSelection.for_node(run_state, node_key),
        status: status,
        started_at: now,
        finished_at: now,
        duration_ms: 0,
        reason: decision.reason,
        freshness_key: freshness_key,
        input_versions: [],
        attempt_count: 0,
        max_attempts: (Map.get(node, :retry_policy) || RetryPolicy.default()).max_attempts,
        meta: decision_metadata(decision),
        error: if(status == :blocked, do: decision.reason, else: nil),
        asset_step_id: asset_step_id
      })

    next_run =
      RunState.transition(run_state, [])

    event_type = if status == :skipped_fresh, do: :step_skipped_fresh, else: :step_blocked

    PersistenceRetry.new(
      next_run,
      event_type,
      %{
        asset_ref: node.ref,
        node_key: node_key,
        window: node.window,
        asset_step_id: asset_step_id,
        stage: stage,
        execution_pool: execution_pool,
        reason: decision.reason,
        freshness_key: freshness_key,
        node_result: result
      },
      nil
    )
  end

  defp decision_metadata(decision),
    do: Map.drop(decision, [:decision, :node_key, :reason, :freshness_key])

  defp record_status(context, node_key, status) do
    %{
      context
      | completed_node_keys: MapSet.put(context.completed_node_keys, node_key),
        upstream_statuses: Map.put(context.upstream_statuses, node_key, status)
    }
  end
end
