defmodule FavnOrchestrator.RunServer.Execution.StageClassifier do
  @moduledoc """
  Classifies and persists freshness decisions for one pipeline stage.

  Runnable nodes retain plan order. Fresh and blocked nodes receive durable node
  results before later stages are considered.
  """

  alias Favn.Freshness.Key
  alias Favn.Manifest.Version
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.Freshness.Decider
  alias FavnOrchestrator.Freshness.StateWriter
  alias FavnOrchestrator.RunServer.Execution.ExecutionPool
  alias FavnOrchestrator.RunServer.Execution.FreshnessContext
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @type terminal_failure :: %{required(:status) => RunState.status(), required(:error) => term()}

  @type result ::
          {:ok, RunState.t(), [Favn.Plan.node_key()], map(), FreshnessContext.t(),
           terminal_failure() | nil}
          | {:error, RunState.t()}

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
    decisions = decisions(run_state, node_keys, freshness_context)

    node_keys
    |> Enum.reduce_while(
      {:ok, run_state, [], decisions, freshness_context, terminal_failure},
      &classify_node(&1, &2, version, stage)
    )
    |> restore_runnable_order()
  end

  defp decisions(run_state, node_keys, freshness_context) do
    Decider.decide_many(run_state.plan, node_keys,
      assets_by_ref: freshness_context.assets_by_ref,
      refresh_policy: freshness_context.refresh_policy,
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
         stage
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
          terminal_failure: current_failure
        })
    end
  end

  defp persist_non_running_decision(ctx) do
    case persist_decision(
           ctx.run,
           ctx.version,
           ctx.node_key,
           ctx.stage,
           ctx.status,
           ctx.decision
         ) do
      {:ok, next_run} ->
        next_context = record_status(ctx.context, ctx.node_key, ctx.status)

        next_failure =
          if ctx.status == :blocked and is_nil(ctx.terminal_failure) do
            %{status: :error, error: {:blocked, ctx.node_key, ctx.decision.reason}}
          else
            ctx.terminal_failure
          end

        {:cont, {:ok, next_run, ctx.runnable, ctx.decisions, next_context, next_failure}}

      {:error, :external_cancel} ->
        {:halt, {:error, Snapshots.cancelled_snapshot(ctx.run)}}

      {:error, reason} ->
        {:halt, {:error, RunState.transition(ctx.run, status: :error, error: reason)}}
    end
  end

  defp restore_runnable_order({:ok, run, runnable, decisions, context, failure}),
    do: {:ok, run, Enum.reverse(runnable), decisions, context, failure}

  defp restore_runnable_order({:error, %RunState{}} = error), do: error

  @doc "Persists one already-classified fresh or blocked node decision."
  @spec persist_decision(
          RunState.t(),
          Version.t(),
          Favn.Plan.node_key(),
          non_neg_integer(),
          :skipped_fresh | :blocked,
          map()
        ) :: {:ok, RunState.t()} | {:error, term()}
  def persist_decision(
        %RunState{} = run_state,
        %Version{} = version,
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
        status: status,
        started_at: now,
        finished_at: now,
        duration_ms: 0,
        reason: decision.reason,
        freshness_key: freshness_key,
        input_versions: [],
        attempt_count: 0,
        max_attempts: run_state.max_attempts,
        meta: decision_metadata(decision),
        error: if(status == :blocked, do: decision.reason, else: nil),
        asset_step_id: asset_step_id
      })

    next_run =
      RunState.transition(run_state, [])

    event_type = if status == :skipped_fresh, do: :step_skipped_fresh, else: :step_blocked

    case Persistence.persist_run_step(next_run, event_type, %{
           asset_ref: node.ref,
           node_key: node_key,
           window: node.window,
           asset_step_id: asset_step_id,
           stage: stage,
           execution_pool: execution_pool,
           reason: decision.reason,
           freshness_key: freshness_key,
           node_result: result
         }) do
      :ok ->
        next_run
        |> ResultBuilder.append_node_result(result)
        |> persist_freshness_state(version, node_key, status, freshness_key, decision)

      {:error, :external_cancel} ->
        {:error, :external_cancel}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist_freshness_state(run, version, node_key, status, freshness_key, decision) do
    case StateWriter.put_attempt_state(run, version, node_key, status, freshness_key, decision) do
      {:ok, _state} -> {:ok, run}
      {:error, reason} -> {:error, {:freshness_state_write_failed, reason}}
    end
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
