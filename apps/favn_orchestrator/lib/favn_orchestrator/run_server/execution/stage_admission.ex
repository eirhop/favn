defmodule FavnOrchestrator.RunServer.Execution.StageAdmission do
  @moduledoc """
  Admission and runner submission for one pipeline stage attempt.

  This module owns stage-local submit/defer decisions: execution admission
  leases, materialization claims, queued-step dedupe, `:step_queued`, and
  `:step_started` persistence. It does not await runner results or decide retry
  and failure-drain behavior.
  """

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.Freshness.StateWriter
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @type node_key :: Favn.Plan.node_key()
  @type entry :: map()
  @type result ::
          {:ok, RunState.t(), [entry()], [node_key()], MapSet.t(term())}
          | {:error, RunState.t(), [term()], [node_key()]}

  @spec submit(map()) :: result()
  def submit(%{queued_steps: queued_steps} = request) when is_map(request) do
    do_submit(
      request.node_keys,
      request.run,
      request.version,
      request.stage,
      request.decisions,
      request.freshness_context,
      request.attempt,
      request.runner_client,
      request.runner_opts,
      [],
      queued_steps
    )
  end

  def submit(%{
        run: %RunState{} = run_state,
        version: %Version{} = version,
        stage: stage,
        node_keys: node_keys,
        decisions: decisions,
        freshness_context: freshness_context,
        attempt: attempt,
        runner_client: runner_client,
        runner_opts: runner_opts
      })
      when is_list(node_keys) and is_map(decisions) and is_map(freshness_context) do
    submit(%{
      run: run_state,
      version: version,
      stage: stage,
      node_keys: node_keys,
      decisions: decisions,
      freshness_context: freshness_context,
      attempt: attempt,
      runner_client: runner_client,
      runner_opts: runner_opts,
      queued_steps: MapSet.new()
    })
  end

  defp do_submit(
         [],
         %RunState{} = run_state,
         _version,
         _stage,
         _decisions,
         _freshness_context,
         _attempt,
         _runner_client,
         _runner_opts,
         entries,
         queued_steps
       ) do
    {:ok, run_state, entries, [], queued_steps}
  end

  defp do_submit(
         [node_key | rest] = node_keys,
         %RunState{} = current_run,
         %Version{} = version,
         stage,
         decisions,
         freshness_context,
         attempt,
         runner_client,
         runner_opts,
         acc,
         queued_steps
       ) do
    if Persistence.externally_cancelled?(current_run.id) do
      {:error, Snapshots.cancelled_snapshot(current_run), [], Enum.map(acc, & &1.node_key)}
    else
      work = stage_work(current_run, version, node_key, stage, attempt)
      asset_step_id = Map.fetch!(work.metadata, :asset_step_id)

      case ExecutionAdmission.acquire(current_run, %{
             asset_step_id: asset_step_id,
             execution_pool: Map.get(work.metadata, :execution_pool)
           }) do
        {:ok, lease} ->
          handle_admitted_entry(%{
            rest: rest,
            node_keys: node_keys,
            current_run: current_run,
            version: version,
            stage: stage,
            decisions: decisions,
            freshness_context: freshness_context,
            attempt: attempt,
            runner_client: runner_client,
            runner_opts: runner_opts,
            acc: acc,
            queued_steps: queued_steps,
            node_key: node_key,
            work: work,
            lease: lease
          })

        {:queued, queue_reason, scope} ->
          persist_or_defer_queued_entry(%{
            queued_steps: queued_steps,
            queue_signature: queue_signature(asset_step_id, queue_reason, scope),
            current_run: current_run,
            work: work,
            stage: stage,
            attempt: attempt,
            queue_reason: queue_reason,
            scope: scope,
            acc: acc,
            node_keys: node_keys
          })

        {:error, reason} ->
          failed = RunState.transition(current_run, status: :error, error: reason)
          {:error, failed, [], Enum.map(acc, & &1.node_key)}
      end
    end
  end

  defp handle_admitted_entry(
         %{current_run: current_run, version: version, node_key: node_key} = ctx
       ) do
    case MaterializationClaims.acquire(
           current_run,
           version,
           node_key,
           ctx.decisions,
           ctx.freshness_context,
           ctx.work
         ) do
      {:ok, claim} ->
        submit_admitted_entry(Map.put(ctx, :materialization_claim, claim))

      {:already_succeeded, claim} ->
        :ok = release_entry_lease(%{lease: ctx.lease})
        maybe_skip_succeeded_claim(ctx, claim)

      {:already_claimed, claim} ->
        :ok = release_entry_lease(%{lease: ctx.lease})
        queue_reason = :materialization_claim
        scope = MaterializationClaims.scope(claim)

        persist_or_defer_queued_entry(%{
          queued_steps: ctx.queued_steps,
          queue_signature:
            queue_signature(Map.fetch!(ctx.work.metadata, :asset_step_id), queue_reason, scope),
          current_run: current_run,
          work: ctx.work,
          stage: ctx.stage,
          attempt: ctx.attempt,
          queue_reason: queue_reason,
          scope: scope,
          acc: ctx.acc,
          node_keys: ctx.node_keys
        })

      {:error, reason} ->
        :ok = release_entry_lease(%{lease: ctx.lease})
        failed = RunState.transition(current_run, status: :error, error: reason)
        {:error, failed, [], Enum.map(ctx.acc, & &1.node_key)}
    end
  end

  defp maybe_skip_succeeded_claim(ctx, claim) do
    if MaterializationClaims.reusable_success?(ctx.decisions, ctx.node_key) do
      decision =
        ctx.decisions
        |> Map.get(ctx.node_key, %{})
        |> Map.merge(%{
          decision: :skipped_fresh,
          reason: MaterializationClaims.skip_reason(claim)
        })

      case persist_decision_result(
             ctx.current_run,
             ctx.version,
             ctx.node_key,
             ctx.stage,
             :skipped_fresh,
             decision
           ) do
        {:ok, skipped_run} ->
          do_submit(
            ctx.rest,
            skipped_run,
            ctx.version,
            ctx.stage,
            ctx.decisions,
            ctx.freshness_context,
            ctx.attempt,
            ctx.runner_client,
            ctx.runner_opts,
            ctx.acc,
            ctx.queued_steps
          )

        {:error, :external_cancel} ->
          {:error, Snapshots.cancelled_snapshot(ctx.current_run), [],
           Enum.map(ctx.acc, & &1.node_key)}

        {:error, reason} ->
          failed = RunState.transition(ctx.current_run, status: :error, error: reason)
          {:error, failed, [], Enum.map(ctx.acc, & &1.node_key)}
      end
    else
      failed =
        RunState.transition(ctx.current_run,
          status: :error,
          error: {:non_reusable_materialization_claim_succeeded, MaterializationClaims.key(claim)}
        )

      {:error, failed, [], Enum.map(ctx.acc, & &1.node_key)}
    end
  end

  defp persist_or_defer_queued_entry(ctx) do
    case maybe_persist_step_queued(
           ctx.queued_steps,
           ctx.queue_signature,
           ctx.current_run,
           ctx.work,
           ctx.stage,
           ctx.attempt,
           ctx.queue_reason,
           ctx.scope
         ) do
      {:ok, queued_run, next_queued_steps} when ctx.acc == [] ->
        {:ok, queued_run, [], ctx.node_keys, next_queued_steps}

      {:ok, queued_run, next_queued_steps} ->
        {:ok, queued_run, ctx.acc, ctx.node_keys, next_queued_steps}

      {:error, :external_cancel} ->
        {:error, Snapshots.cancelled_snapshot(ctx.current_run), [],
         Enum.map(ctx.acc, & &1.node_key)}

      {:error, reason} ->
        failed = RunState.transition(ctx.current_run, status: :error, error: reason)
        {:error, failed, [], Enum.map(ctx.acc, & &1.node_key)}
    end
  end

  defp submit_admitted_entry(ctx) do
    asset_ref = ctx.work.asset_ref
    asset_step_id = Map.fetch!(ctx.work.metadata, :asset_step_id)

    case ctx.runner_client.submit_work(ctx.work, ctx.runner_opts) do
      {:ok, execution_id} ->
        updated_run = with_inflight_execution(ctx.current_run, execution_id, ctx.work.metadata)

        case Persistence.persist_run_step(updated_run, :step_started, %{
               asset_ref: asset_ref,
               runner_execution_id: execution_id,
               asset_step_id: asset_step_id,
               window: Map.get(ctx.work.metadata, :window),
               stage: ctx.stage,
               attempt: ctx.attempt,
               max_attempts: ctx.current_run.max_attempts,
               execution_pool: Map.get(ctx.work.metadata, :execution_pool),
               freshness_key: decision_freshness_key(ctx.decisions, ctx.node_key)
             }) do
          :ok ->
            entry = %{
              run_id: ctx.current_run.id,
              asset_step_id: asset_step_id,
              asset_ref: asset_ref,
              node_key: ctx.node_key,
              window: Map.get(ctx.work.metadata, :window),
              execution_id: execution_id,
              runner_execution_id: execution_id,
              version: ctx.version,
              decision: Map.get(ctx.decisions, ctx.node_key, %{}),
              freshness_context: ctx.freshness_context,
              attempt: ctx.attempt,
              stage: ctx.stage,
              lease: ctx.lease,
              materialization_claim: ctx.materialization_claim,
              execution_pool: Map.get(ctx.work.metadata, :execution_pool),
              freshness_key: decision_freshness_key(ctx.decisions, ctx.node_key)
            }

            do_submit(
              ctx.rest,
              updated_run,
              ctx.version,
              ctx.stage,
              ctx.decisions,
              ctx.freshness_context,
              ctx.attempt,
              ctx.runner_client,
              ctx.runner_opts,
              ctx.acc ++ [entry],
              ctx.queued_steps
            )

          {:error, :external_cancel} ->
            :ok = release_entry_lease(%{lease: ctx.lease})
            :ok = MaterializationClaims.fail(ctx.materialization_claim, :external_cancel)
            :ok = release_entry_leases(ctx.acc)

            cancelled =
              cancel_execution_ids(
                updated_run,
                [execution_id],
                %{
                  kind: :external_cancel,
                  asset_ref: asset_ref,
                  stage: ctx.stage,
                  attempt: ctx.attempt
                },
                ctx.runner_client,
                ctx.runner_opts
              )

            {:error, Snapshots.cancelled_snapshot(cancelled), [],
             Enum.map(ctx.acc, & &1.node_key) ++ [ctx.node_key]}
        end

      {:error, reason} ->
        :ok = release_entry_lease(%{lease: ctx.lease})
        :ok = MaterializationClaims.fail(ctx.materialization_claim, reason)
        :ok = release_entry_leases(ctx.acc)

        cancelled =
          cancel_execution_ids(
            ctx.current_run,
            Enum.map(ctx.acc, & &1.execution_id),
            %{kind: :submit_failure, asset_ref: asset_ref, error: reason},
            ctx.runner_client,
            ctx.runner_opts
          )

        failed =
          RunState.transition(cancelled, status: :error, error: reason, runner_execution_id: nil)

        case Persistence.persist_run_step(failed, :step_failed, %{
               asset_ref: asset_ref,
               error: reason,
               node_key: Map.get(ctx.work.metadata, :node_key),
               asset_step_id: Map.get(ctx.work.metadata, :asset_step_id),
               window: Map.get(ctx.work.metadata, :window),
               stage: ctx.stage,
               attempt: ctx.attempt,
               max_attempts: ctx.current_run.max_attempts,
               execution_pool: Map.get(ctx.work.metadata, :execution_pool)
             }) do
          :ok ->
            {:error, failed, [], Enum.map(ctx.acc, & &1.node_key)}

          {:error, :external_cancel} ->
            {:error, Snapshots.cancelled_snapshot(failed), [], Enum.map(ctx.acc, & &1.node_key)}
        end
    end
  end

  defp persist_step_queued(run_state, work, stage, attempt, queue_reason, scope) do
    queued_run = RunState.transition(run_state, status: :running, error: nil)

    case Persistence.persist_run_step(queued_run, :step_queued, %{
           asset_ref: work.asset_ref,
           node_key: Map.get(work.metadata, :node_key),
           asset_step_id: Map.get(work.metadata, :asset_step_id),
           window: Map.get(work.metadata, :window),
           stage: stage,
           attempt: attempt,
           max_attempts: run_state.max_attempts,
           execution_pool: Map.get(work.metadata, :execution_pool),
           queue_reason: queue_reason,
           admission_scope: scope
         }) do
      :ok -> {:ok, queued_run}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_persist_step_queued(
         queued_steps,
         queue_signature,
         run_state,
         work,
         stage,
         attempt,
         queue_reason,
         scope
       ) do
    if MapSet.member?(queued_steps, queue_signature) do
      {:ok, run_state, queued_steps}
    else
      case persist_step_queued(run_state, work, stage, attempt, queue_reason, scope) do
        {:ok, queued_run} -> {:ok, queued_run, MapSet.put(queued_steps, queue_signature)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp queue_signature(asset_step_id, queue_reason, scope) do
    scope_kind = Map.get(scope, :kind) || Map.get(scope, "kind")
    scope_key = Map.get(scope, :key) || Map.get(scope, "key")

    {asset_step_id, queue_reason, scope_kind, scope_key}
  end

  defp persist_decision_result(
         %RunState{} = run_state,
         %Version{} = version,
         node_key,
         stage,
         status,
         decision
       ) do
    node = Map.fetch!(run_state.plan.nodes, node_key)
    now = DateTime.utc_now()
    freshness_key = Map.get(decision, :freshness_key, Favn.Freshness.Key.latest())
    asset_step_id = AssetStepIdentity.asset_step_id(run_state.id, node_key, node.ref)

    result =
      NodeResult.new(%{
        node_key: node_key,
        ref: node.ref,
        window: node.window,
        stage: stage,
        execution_pool: effective_execution_pool(run_state, node_key),
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

    next_run = put_node_result(run_state, result)
    event_type = if status == :skipped_fresh, do: :step_skipped_fresh, else: :step_blocked

    case Persistence.persist_run_step(next_run, event_type, %{
           asset_ref: node.ref,
           node_key: node_key,
           window: node.window,
           asset_step_id: asset_step_id,
           stage: stage,
           execution_pool: effective_execution_pool(run_state, node_key),
           reason: decision.reason,
           freshness_key: freshness_key
         }) do
      :ok ->
        case StateWriter.put_attempt_state(
               next_run,
               version,
               node_key,
               status,
               freshness_key,
               decision
             ) do
          {:ok, _state} -> {:ok, next_run}
          {:error, reason} -> {:error, {:freshness_state_write_failed, reason}}
        end

      {:error, :external_cancel} ->
        {:error, :external_cancel}
    end
  end

  defp decision_metadata(decision) when is_map(decision),
    do: Map.drop(decision, [:decision, :node_key, :reason, :freshness_key])

  defp put_node_result(%RunState{} = run_state, %NodeResult{} = result) do
    node_results = existing_node_results(run_state) ++ [result]
    result_map = Map.merge(run_state.result || %{}, %{node_results: node_results})
    RunState.transition(run_state, result: result_map)
  end

  defp existing_node_results(%RunState{result: %{node_results: results}}) when is_list(results),
    do: results

  defp existing_node_results(_run_state), do: []

  defp stage_work(%RunState{} = run_state, %Version{} = version, node_key, stage, attempt) do
    node = Map.fetch!(run_state.plan.nodes, node_key)
    asset_ref = node.ref
    asset_step_id = AssetStepIdentity.asset_step_id(run_state.id, node_key, asset_ref)

    %RunnerWork{
      run_id: run_state.id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: asset_ref,
      asset_refs: [asset_ref],
      planned_asset_refs: planned_asset_refs(run_state),
      params: run_state.params,
      trigger:
        run_state.trigger
        |> Map.put(:window, node.window)
        |> maybe_put_pipeline_trigger(Map.get(run_state.metadata, :pipeline_context)),
      metadata:
        Map.merge(work_metadata(run_state.metadata), %{
          attempt: attempt,
          asset_step_id: asset_step_id,
          max_attempts: run_state.max_attempts,
          stage: stage,
          node_key: node_key,
          window: node.window,
          execution_pool: effective_execution_pool(run_state, node_key)
        })
    }
  end

  defp planned_asset_refs(%RunState{target_refs: refs}) when is_list(refs), do: refs
  defp planned_asset_refs(_run_state), do: []

  defp effective_execution_pool(%RunState{} = run_state, node_key) do
    node_pool =
      case run_state.plan do
        %Favn.Plan{nodes: nodes} when is_map(nodes) ->
          nodes |> Map.get(node_key, %{}) |> Map.get(:execution_pool)

        _other ->
          nil
      end

    node_pool || pipeline_default_execution_pool(run_state)
  end

  defp pipeline_default_execution_pool(%RunState{metadata: %{pipeline_execution_policy: policy}})
       when is_map(policy) do
    Map.get(policy, :execution_pool) || Map.get(policy, "execution_pool")
  end

  defp pipeline_default_execution_pool(%RunState{}), do: nil

  defp maybe_put_pipeline_trigger(trigger, pipeline_context) when is_map(pipeline_context),
    do: Map.put(trigger, :pipeline, pipeline_context)

  defp maybe_put_pipeline_trigger(trigger, _pipeline_context), do: trigger

  defp work_metadata(metadata) when is_map(metadata), do: Map.delete(metadata, :runner_metadata)

  defp with_inflight_execution(%RunState{} = run_state, execution_id, metadata) do
    ids =
      run_state
      |> inflight_ids_from_metadata()
      |> Kernel.++([execution_id])
      |> Enum.filter(&is_binary/1)
      |> Enum.uniq()

    RunState.transition(run_state,
      runner_execution_id: execution_id,
      metadata: Map.put(metadata, :in_flight_execution_ids, ids)
    )
  end

  defp inflight_ids_from_metadata(%RunState{} = run_state) do
    case Map.get(run_state.metadata, :in_flight_execution_ids, []) do
      ids when is_list(ids) -> ids
      _other -> []
    end
  end

  defp release_entry_leases(entries) when is_list(entries) do
    Enum.each(entries, &release_entry_lease/1)
    :ok
  end

  defp release_entry_lease(%{lease: lease}), do: release_entry_lease(lease)
  defp release_entry_lease(nil), do: :ok

  defp release_entry_lease(lease) when is_map(lease) do
    case ExecutionAdmission.release(lease) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp cancel_execution_ids(
         %RunState{} = run_state,
         execution_ids,
         reason,
         runner_client,
         runner_opts
       ) do
    execution_ids
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
    |> Enum.each(fn execution_id ->
      runner_client.cancel_work(execution_id, reason, runner_opts)
    end)

    clear_inflight_executions(run_state, execution_ids)
  end

  defp clear_inflight_executions(%RunState{} = run_state, execution_ids)
       when is_list(execution_ids) do
    ids = inflight_ids_from_metadata(run_state) -- Enum.filter(execution_ids, &is_binary/1)

    RunState.transition(run_state,
      metadata: Map.put(run_state.metadata, :in_flight_execution_ids, ids)
    )
  end

  defp decision_freshness_key(decisions, node_key) when is_map(decisions) do
    decisions
    |> Map.get(node_key, %{})
    |> Map.get(:freshness_key, Favn.Freshness.Key.latest())
  end
end
