defmodule FavnOrchestrator.RunServer.Execution.StageAdmission do
  @moduledoc """
  Admission and runner submission for one pipeline stage attempt.

  This module owns stage-local submit/defer decisions: execution admission
  leases, materialization claims, queued-step dedupe, `:step_queued`, and
  `:step_started` persistence. It does not await runner results or decide retry
  and failure-drain behavior.
  """

  alias Favn.Contracts.RunnerWork
  alias Favn.Freshness.Key
  alias Favn.Manifest.Version
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.RunWorkSet
  alias FavnOrchestrator.RunServer.Execution.StageClassifier
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @type node_key :: Favn.Plan.node_key()
  @type entry :: map()
  @type result ::
          {:ok, RunState.t(), [entry()], [node_key()], MapSet.t(term()), [map()]}
          | {:error, RunState.t(), [term()], [node_key()]}

  @spec submit(map()) :: result()
  def submit(request) when is_map(request) do
    request
    |> Map.put_new(:queued_steps, MapSet.new())
    |> submit_request()
  end

  defp submit_request(%{
         run: %RunState{} = run_state,
         version: %Version{} = version,
         stage: stage,
         node_keys: node_keys,
         decisions: decisions,
         freshness_context: freshness_context,
         attempt: attempt,
         runner_client: runner_client,
         runner_opts: runner_opts,
         queued_steps: %MapSet{} = queued_steps
       })
       when is_list(node_keys) and is_map(decisions) and is_map(freshness_context) do
    do_submit(node_keys, %{
      current_run: run_state,
      version: version,
      stage: stage,
      decisions: decisions,
      freshness_context: freshness_context,
      attempt: attempt,
      runner_client: runner_client,
      runner_opts: runner_opts,
      entries_rev: [],
      queued_steps: queued_steps,
      waiters: []
    })
  end

  defp do_submit([], ctx) do
    {:ok, ctx.current_run, entries(ctx), [], ctx.queued_steps, ctx.waiters}
  end

  defp do_submit([node_key | rest] = node_keys, ctx) do
    if Persistence.externally_cancelled?(ctx.current_run.id) do
      {:error, Snapshots.cancelled_snapshot(ctx.current_run), [], attempted_node_keys(ctx)}
    else
      work = stage_work(ctx.current_run, ctx.version, node_key, ctx.stage, ctx.attempt)

      entry_context =
        Map.merge(ctx, %{
          rest: rest,
          node_keys: node_keys,
          node_key: node_key,
          work: work
        })

      case ExecutionAdmission.acquire_or_wait(
             ctx.current_run,
             %{
               asset_step_id: work.asset_step_id,
               execution_pool: RunnerWork.execution_pool(work)
             },
             stage: ctx.stage,
             attempt: ctx.attempt
           ) do
        {:ok, lease} ->
          handle_admitted_entry(Map.put(entry_context, :lease, lease))

        {:waiting, waiter} ->
          persist_or_defer_queued_entry(
            entry_context
            |> Map.put(:queue_signature, queue_signature(work.asset_step_id, waiter))
            |> Map.put(:queue_reason, waiter.queue_reason)
            |> Map.put(:scope, waiter.blocked_scope)
            |> Map.put(:waiter, waiter)
          )

        {:error, {:run_not_admissible, run_id, _status}}
        when run_id == ctx.current_run.id ->
          {:error, ctx.current_run, [], attempted_node_keys(ctx)}

        {:error, reason} ->
          failed = RunState.transition(ctx.current_run, status: :error, error: reason)
          {:error, failed, [], attempted_node_keys(ctx)}
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

        persist_or_defer_queued_entry(
          ctx
          |> Map.put(
            :queue_signature,
            queue_signature(ctx.work.asset_step_id, queue_reason, scope)
          )
          |> Map.put(:queue_reason, queue_reason)
          |> Map.put(:scope, scope)
        )

      {:error, reason} ->
        :ok = release_entry_lease(%{lease: ctx.lease})
        failed = RunState.transition(current_run, status: :error, error: reason)
        {:error, failed, [], attempted_node_keys(ctx)}
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

      case StageClassifier.persist_decision(
             ctx.current_run,
             ctx.version,
             ctx.node_key,
             ctx.stage,
             :skipped_fresh,
             decision
           ) do
        {:ok, skipped_run} ->
          do_submit(ctx.rest, %{ctx | current_run: skipped_run})

        {:error, :external_cancel} ->
          {:error, Snapshots.cancelled_snapshot(ctx.current_run), [], attempted_node_keys(ctx)}

        {:error, reason} ->
          failed = RunState.transition(ctx.current_run, status: :error, error: reason)
          {:error, failed, [], attempted_node_keys(ctx)}
      end
    else
      failed =
        RunState.transition(ctx.current_run,
          status: :error,
          error: {:non_reusable_materialization_claim_succeeded, MaterializationClaims.key(claim)}
        )

      {:error, failed, [], attempted_node_keys(ctx)}
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
      {:ok, queued_run, next_queued_steps} when ctx.entries_rev == [] ->
        {:ok, queued_run, [], ctx.node_keys, next_queued_steps, maybe_add_waiter(ctx)}

      {:ok, queued_run, next_queued_steps} ->
        {:ok, queued_run, entries(ctx), ctx.node_keys, next_queued_steps, maybe_add_waiter(ctx)}

      {:error, :external_cancel} ->
        {:error, Snapshots.cancelled_snapshot(ctx.current_run), [], attempted_node_keys(ctx)}

      {:error, reason} ->
        failed = RunState.transition(ctx.current_run, status: :error, error: reason)
        {:error, failed, [], attempted_node_keys(ctx)}
    end
  end

  defp maybe_add_waiter(%{waiters: waiters, waiter: waiter}), do: waiters ++ [waiter]
  defp maybe_add_waiter(%{waiters: waiters}), do: waiters
  defp maybe_add_waiter(_ctx), do: []

  defp entries(%{entries_rev: entries_rev}), do: Enum.reverse(entries_rev)
  defp attempted_node_keys(ctx), do: Enum.map(entries(ctx), & &1.node_key)
  defp execution_ids(ctx), do: Enum.map(entries(ctx), & &1.execution_id)

  defp submit_admitted_entry(ctx) do
    asset_ref = ctx.work.asset_ref
    asset_step_id = ctx.work.asset_step_id

    ownership =
      RunExecutionOwnership.new(ctx.current_run,
        asset_step_id: asset_step_id,
        node_key: ctx.node_key,
        asset_ref: asset_ref,
        stage: ctx.stage,
        attempt: ctx.attempt,
        execution_pool: RunnerWork.execution_pool(ctx.work),
        deadline_at: run_deadline_at(ctx.current_run)
      )

    work = attach_ownership_metadata(ctx.work, ownership)
    ctx = %{ctx | work: work}

    case RunExecutionOwnership.persist(ownership) do
      :ok ->
        submit_owned_entry(ctx, ownership, work, asset_ref)

      {:error, reason} ->
        fail_unsubmitted_entry(ctx, asset_ref, reason)
    end
  end

  defp submit_owned_entry(ctx, ownership, work, asset_ref) do
    case ctx.runner_client.submit_work(work, ctx.runner_opts) do
      {:ok, execution_id} ->
        submitted_ownership = RunExecutionOwnership.submitted(ownership, execution_id)

        case persist_submitted_ownership_snapshot(submitted_ownership) do
          :ok ->
            submit_started_entry(ctx, submitted_ownership, execution_id)

          {:error, :external_cancel} ->
            fail_submitted_entry(ctx, asset_ref, execution_id, :external_cancel)

          {:error, reason} ->
            fail_submitted_entry(ctx, asset_ref, execution_id, reason)
        end

      {:error, reason} ->
        _ =
          RunExecutionOwnership.mark_dispatch_failed(
            ctx.current_run.id,
            ownership.ownership_id,
            reason
          )

        fail_unsubmitted_entry(ctx, asset_ref, reason)
    end
  end

  defp fail_unsubmitted_entry(ctx, asset_ref, reason) do
    :ok = release_entry_lease(%{lease: ctx.lease})
    :ok = fail_claim(ctx, reason)
    :ok = cleanup_entries(ctx.current_run, entries(ctx), reason)

    cancelled =
      cancel_execution_ids(
        ctx.current_run,
        execution_ids(ctx),
        %{kind: :submit_failure, asset_ref: asset_ref, error: reason},
        ctx.runner_client,
        ctx.runner_opts
      )

    failed =
      RunState.transition(cancelled, status: :error, error: reason, runner_execution_id: nil)

    persist_stage_submit_failure(ctx, failed, asset_ref, reason)
  end

  defp fail_submitted_entry(ctx, asset_ref, execution_id, :external_cancel) do
    :ok = release_entry_lease(%{lease: ctx.lease})
    :ok = fail_claim(ctx, :external_cancel)
    :ok = cleanup_entries(ctx.current_run, entries(ctx), :external_cancel)

    {cancelled, cancel_results} =
      cancel_execution_ids_with_results(
        ctx.current_run,
        execution_ids(ctx) ++ [execution_id],
        %{kind: :external_cancel, asset_ref: asset_ref, stage: ctx.stage, attempt: ctx.attempt},
        ctx.runner_client,
        ctx.runner_opts
      )

    persist_submit_persist_failure_outcome(ctx, execution_id, cancel_results, :external_cancel)

    {:error, Snapshots.cancelled_snapshot(cancelled), [],
     attempted_node_keys(ctx) ++ [ctx.node_key]}
  end

  defp fail_submitted_entry(ctx, asset_ref, execution_id, reason) do
    :ok = release_entry_lease(%{lease: ctx.lease})
    :ok = fail_claim(ctx, reason)
    :ok = cleanup_entries(ctx.current_run, entries(ctx), reason)

    {cancelled, cancel_results} =
      cancel_execution_ids_with_results(
        ctx.current_run,
        execution_ids(ctx) ++ [execution_id],
        %{kind: :step_submitted_persist_failed, asset_ref: asset_ref, error: reason},
        ctx.runner_client,
        ctx.runner_opts
      )

    persist_submit_persist_failure_outcome(ctx, execution_id, cancel_results, reason)

    failed =
      RunState.transition(cancelled, status: :error, error: reason, runner_execution_id: nil)

    persist_stage_submit_failure(ctx, failed, asset_ref, reason)
  end

  defp persist_stage_submit_failure(ctx, failed, asset_ref, reason) do
    case Persistence.persist_run_step(failed, :step_failed, %{
           asset_ref: asset_ref,
           error: reason,
           node_key: RunnerWork.node_key(ctx.work),
           asset_step_id: ctx.work.asset_step_id,
           window: RunnerWork.window(ctx.work),
           stage: ctx.stage,
           attempt: ctx.attempt,
           max_attempts: ctx.current_run.max_attempts,
           execution_pool: RunnerWork.execution_pool(ctx.work)
         }) do
      :ok ->
        {:error, failed, [], attempted_node_keys(ctx)}

      {:error, :external_cancel} ->
        {:error, Snapshots.cancelled_snapshot(failed), [], attempted_node_keys(ctx)}

      {:error, _persist_reason} ->
        {:error, failed, [], attempted_node_keys(ctx)}
    end
  end

  defp attach_ownership_metadata(%RunnerWork{} = work, %RunExecutionOwnership{} = ownership) do
    metadata =
      work.metadata
      |> Map.put(:ownership_id, ownership.ownership_id)
      |> Map.put(:dispatch_id, ownership.dispatch_id)
      |> Map.put(:deadline_at, ownership.deadline_at)

    %{work | metadata: metadata}
  end

  defp run_deadline_at(%RunState{timeout_ms: timeout_ms})
       when is_integer(timeout_ms) and timeout_ms > 0 do
    DateTime.add(DateTime.utc_now(), timeout_ms, :millisecond)
  end

  defp run_deadline_at(%RunState{}), do: nil

  defp submit_started_entry(ctx, ownership, execution_id) do
    asset_ref = ctx.work.asset_ref
    asset_step_id = ctx.work.asset_step_id
    started_ownership = RunExecutionOwnership.started(ownership)

    case RunExecutionOwnership.persist(started_ownership) do
      :ok ->
        submit_started_entry_after_ownership(ctx, execution_id, asset_ref, asset_step_id)

      {:error, reason} ->
        :ok = release_entry_lease(%{lease: ctx.lease})
        :ok = fail_claim(ctx, reason)
        :ok = cleanup_entries(ctx.current_run, entries(ctx), reason)

        cancelled =
          cancel_execution_ids(
            ctx.current_run,
            [execution_id],
            %{kind: :step_started_ownership_persist_failed, asset_ref: asset_ref, error: reason},
            ctx.runner_client,
            ctx.runner_opts
          )

        failed =
          RunState.transition(cancelled, status: :error, error: reason, runner_execution_id: nil)

        {:error, failed, [], attempted_node_keys(ctx) ++ [ctx.node_key]}
    end
  end

  defp submit_started_entry_after_ownership(ctx, execution_id, asset_ref, asset_step_id) do
    updated_run =
      with_inflight_execution(
        ctx.current_run,
        execution_id,
        RunnerWork.lifecycle_metadata(ctx.work)
      )

    case Persistence.persist_run_step(updated_run, :step_started, %{
           asset_ref: asset_ref,
           runner_execution_id: execution_id,
           asset_step_id: asset_step_id,
           window: RunnerWork.window(ctx.work),
           stage: ctx.stage,
           attempt: ctx.attempt,
           max_attempts: ctx.current_run.max_attempts,
           execution_pool: RunnerWork.execution_pool(ctx.work),
           freshness_key: decision_freshness_key(ctx.decisions, ctx.node_key)
         }) do
      :ok ->
        entry = %{
          run_id: ctx.current_run.id,
          asset_step_id: asset_step_id,
          asset_ref: asset_ref,
          node_key: ctx.node_key,
          window: RunnerWork.window(ctx.work),
          execution_id: execution_id,
          runner_execution_id: execution_id,
          version: ctx.version,
          decision: Map.get(ctx.decisions, ctx.node_key, %{}),
          freshness_context: ctx.freshness_context,
          attempt: ctx.attempt,
          stage: ctx.stage,
          lease: ctx.lease,
          materialization_claim: ctx.materialization_claim,
          execution_pool: RunnerWork.execution_pool(ctx.work),
          freshness_key: decision_freshness_key(ctx.decisions, ctx.node_key)
        }

        do_submit(ctx.rest, %{
          ctx
          | current_run: updated_run,
            entries_rev: [entry | ctx.entries_rev]
        })

      {:error, :external_cancel} ->
        :ok = release_entry_lease(%{lease: ctx.lease})
        :ok = fail_claim(ctx, :external_cancel)
        :ok = cleanup_entries(ctx.current_run, entries(ctx), :external_cancel)

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
         attempted_node_keys(ctx) ++ [ctx.node_key]}

      {:error, reason} ->
        :ok = release_entry_lease(%{lease: ctx.lease})
        :ok = fail_claim(ctx, reason)
        :ok = cleanup_entries(ctx.current_run, entries(ctx), reason)

        cancelled =
          cancel_execution_ids(
            updated_run,
            [execution_id],
            %{kind: :step_started_persist_failed, asset_ref: asset_ref, error: reason},
            ctx.runner_client,
            ctx.runner_opts
          )

        failed =
          RunState.transition(cancelled, status: :error, error: reason, runner_execution_id: nil)

        {:error, failed, [], attempted_node_keys(ctx) ++ [ctx.node_key]}
    end
  end

  defp persist_submitted_ownership_snapshot(%RunExecutionOwnership{} = ownership) do
    if Persistence.externally_cancelled?(ownership.run_id) do
      {:error, :external_cancel}
    else
      RunExecutionOwnership.persist(ownership)
    end
  end

  defp persist_submit_persist_failure_outcome(ctx, execution_id, cancel_results, reason) do
    result = Enum.find(cancel_results, &(&1.execution_id == execution_id))

    ctx.current_run
    |> RunExecutionOwnership.new(
      asset_step_id: ctx.work.asset_step_id,
      node_key: ctx.node_key,
      asset_ref: ctx.work.asset_ref,
      stage: ctx.stage,
      attempt: ctx.attempt,
      execution_pool: RunnerWork.execution_pool(ctx.work),
      deadline_at: run_deadline_at(ctx.current_run)
    )
    |> RunExecutionOwnership.submitted(execution_id)
    |> RunExecutionOwnership.mark_submit_persist_failed(result, reason)

    :ok
  end

  defp persist_step_queued(run_state, work, stage, attempt, queue_reason, scope) do
    queued_run = RunState.transition(run_state, status: :running, error: nil)

    case Persistence.persist_run_step(queued_run, :step_queued, %{
           asset_ref: work.asset_ref,
           node_key: RunnerWork.node_key(work),
           asset_step_id: work.asset_step_id,
           window: RunnerWork.window(work),
           stage: stage,
           attempt: attempt,
           max_attempts: run_state.max_attempts,
           execution_pool: RunnerWork.execution_pool(work),
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

  defp queue_signature(asset_step_id, waiter) do
    queue_signature(asset_step_id, waiter.queue_reason, waiter.blocked_scope)
  end

  defp stage_work(%RunState{} = run_state, %Version{} = version, node_key, stage, attempt) do
    {:ok, %{work: work}} =
      run_state
      |> StepAttemptLifecycle.new(version, node_key, stage, attempt)
      |> StepAttemptLifecycle.build_work()

    work
  end

  defp with_inflight_execution(%RunState{} = run_state, execution_id, metadata) do
    ids =
      run_state
      |> inflight_ids_from_metadata()
      |> Kernel.++([execution_id])
      |> Enum.filter(&is_binary/1)
      |> Enum.uniq()

    RunState.transition(run_state,
      runner_execution_id: execution_id,
      metadata:
        run_state.metadata |> Map.merge(metadata) |> Map.put(:in_flight_execution_ids, ids)
    )
  end

  defp inflight_ids_from_metadata(%RunState{} = run_state) do
    case Map.get(run_state.metadata, :in_flight_execution_ids, []) do
      ids when is_list(ids) -> ids
      _other -> []
    end
  end

  defp cleanup_entries(%RunState{} = run_state, entries, reason) when is_list(entries) do
    run_state
    |> RunWorkSet.from_entries(entries)
    |> RunWorkSet.cleanup_all(reason)
  end

  defp fail_claim(ctx, reason) do
    RunWorkSet.fail_entry_claim(%{materialization_claim: ctx.materialization_claim}, reason)
  end

  defp release_entry_lease(entry), do: RunWorkSet.release_entry(entry)

  defp cancel_execution_ids(
         %RunState{} = run_state,
         execution_ids,
         reason,
         runner_client,
         runner_opts
       ) do
    {run_state, _cancel_results} =
      cancel_execution_ids_with_results(
        run_state,
        execution_ids,
        reason,
        runner_client,
        runner_opts
      )

    run_state
  end

  defp cancel_execution_ids_with_results(
         %RunState{} = run_state,
         execution_ids,
         reason,
         runner_client,
         runner_opts
       ) do
    cancel_results =
      Cancellation.dispatch_runner_work(
        run_state,
        execution_ids,
        reason,
        runner_client,
        runner_opts
      )

    _ = RunExecutionOwnership.persist_cancel_outcomes(run_state.id, cancel_results, reason)

    {clear_inflight_executions(run_state, Enum.map(cancel_results, & &1.execution_id)),
     cancel_results}
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
    |> Map.get(:freshness_key, Key.latest())
  end
end
