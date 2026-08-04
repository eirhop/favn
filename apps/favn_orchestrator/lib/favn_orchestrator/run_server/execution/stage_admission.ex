defmodule FavnOrchestrator.RunServer.Execution.StageAdmission do
  @moduledoc """
  Admission and runner submission for one pipeline stage attempt.

  This module owns stage-local submit/defer decisions: execution admission
  leases, materialization claims, queued-step dedupe, `:step_queued`, and
  `:step_started` persistence. It does not await runner results or decide retry
  and failure-drain behavior.
  """

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerWork
  alias FavnOrchestrator.CancellationOutcome
  alias Favn.Freshness.Key
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias FavnOrchestrator.AssetRunnerTasks
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.ExecutionPackages
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.ResourceCircuits
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.PreSubmitFailure
  alias FavnOrchestrator.RunServer.Execution.PipelineTaskContinuation
  alias FavnOrchestrator.RunServer.Execution.StageClassifier
  alias FavnOrchestrator.RunServer.Execution.StageEntry
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Persistence.SystemContext

  @max_batch_nodes 4
  @max_batch_ms 25

  @type node_key :: Favn.Plan.node_key()
  @type entry :: StageEntry.t()
  @type result ::
          {:ok, RunState.t(), [entry()], [node_key()], MapSet.t(term()), [map()], map() | nil}
          | {:partial_retry, RunState.t(), [entry()], [node_key()], node_key(), term(),
             MapSet.t(term()), [map()], map() | nil}
          | {:error, RunState.t(), [term()], [node_key()]}
          | {:persist_retry, PersistenceRetry.t(), term()}

  @spec submit(map()) :: result()
  def submit(request) when is_map(request) do
    request
    |> Map.put_new(:queued_steps, MapSet.new())
    |> submit_request()
  end

  defp submit_request(%{
         run: %RunState{} = run_state,
         version: %Version{} = version,
         manifest_index: %Index{} = manifest_index,
         stage: stage,
         node_keys: node_keys,
         decisions: decisions,
         freshness_context: freshness_context,
         freshness_checkpoint: freshness_checkpoint,
         attempt: attempt,
         manifest_lease_id: manifest_lease_id,
         queued_steps: %MapSet{} = queued_steps
       })
       when is_list(node_keys) and is_map(decisions) and is_map(freshness_context) and
              is_map(freshness_checkpoint) do
    ctx = %{
      current_run: run_state,
      version: version,
      manifest_index: manifest_index,
      stage: stage,
      decisions: decisions,
      freshness_context: freshness_context,
      freshness_checkpoint: freshness_checkpoint,
      attempt: attempt,
      manifest_lease_id: manifest_lease_id,
      entries_rev: [],
      queued_steps: queued_steps,
      waiters: [],
      batch_started_ms: System.monotonic_time(:millisecond),
      batch_count: 0,
      terminal_failure: nil
    }

    do_submit(node_keys, ctx)
  end

  defp do_submit([], ctx) do
    {:ok, ctx.current_run, entries(ctx), [], ctx.queued_steps, ctx.waiters, ctx.terminal_failure}
  end

  defp do_submit([node_key | rest] = node_keys, ctx) do
    cond do
      yield_batch?(ctx) ->
        {:ok, ctx.current_run, entries(ctx), node_keys, ctx.queued_steps, ctx.waiters,
         ctx.terminal_failure}

      Persistence.externally_cancelled?(ctx.current_run) ->
        {:error, Snapshots.cancelled_snapshot(ctx.current_run), [], attempted_node_keys(ctx)}

      true ->
        case stage_work(
               ctx.current_run,
               ctx.version,
               ctx.manifest_index,
               ctx.manifest_lease_id,
               node_key,
               ctx.stage,
               ctx.attempt
             ) do
          {:ok, work} ->
            entry_context =
              Map.merge(ctx, %{
                rest: rest,
                node_keys: node_keys,
                node_key: node_key,
                work: work,
                batch_count: ctx.batch_count + 1
              })

            admit_execution_capacity(entry_context)

          {:error, reason} ->
            stop_after_stage_build_failure(ctx, node_key, reason)
        end
    end
  end

  defp admit_resource_circuits(ctx) do
    case ResourceCircuits.acquire(ctx.current_run, ctx.work, ctx.manifest_index) do
      {:ok, permits} ->
        handle_admitted_entry(Map.put(ctx, :resource_circuit_permits, permits))

      {:blocked, blockers} ->
        :ok = release_entry_lease(ctx)
        persist_resource_block(ctx, blockers)

      {:error, reason} ->
        :ok = release_entry_lease(ctx)
        failed = Snapshots.snapshot_update(ctx.current_run, status: :error, error: reason)
        {:error, failed, [], attempted_node_keys(ctx)}
    end
  end

  defp admit_execution_capacity(ctx) do
    case ExecutionAdmission.acquire_or_wait(
           ctx.current_run,
           %{
             asset_step_id: ctx.work.asset_step_id,
             execution_pool: RunnerWork.execution_pool(ctx.work)
           },
           stage: ctx.stage,
           attempt: ctx.attempt
         ) do
      {:ok, lease} ->
        admit_resource_circuits(Map.put(ctx, :lease, lease))

      {:waiting, waiter} ->
        persist_or_defer_queued_entry(
          ctx
          |> Map.put(:queue_signature, queue_signature(ctx.work.asset_step_id, waiter))
          |> Map.put(:queue_reason, waiter.queue_reason)
          |> Map.put(:scope, waiter.blocked_scope)
          |> Map.put(:waiter, waiter)
        )

      {:error, {:run_not_admissible, run_id, _status}}
      when run_id == ctx.current_run.id ->
        {:error, ctx.current_run, [], attempted_node_keys(ctx)}

      {:error, reason} ->
        failed = Snapshots.snapshot_update(ctx.current_run, status: :error, error: reason)
        {:error, failed, [], attempted_node_keys(ctx)}
    end
  end

  defp persist_resource_block(ctx, blockers) do
    blocker_maps =
      Enum.map(blockers, fn blocker ->
        blocker |> Map.from_struct() |> Map.delete(:probe_owner_id)
      end)

    reason =
      {:resource_circuit_open, blocker_maps}

    decision = %{
      decision: :blocked,
      reason: reason,
      resource_circuit_blockers: blocker_maps
    }

    with {:ok, blocked_run} <-
           StageClassifier.persist_decision(
             ctx.current_run,
             ctx.version,
             ctx.node_key,
             ctx.stage,
             :blocked,
             decision
           ),
         :ok <- ResourceCircuits.record_blocked(blocked_run, ctx.work, blockers) do
      failure =
        (ctx.terminal_failure || %{status: :error, error: {:blocked, ctx.node_key, reason}})
        |> Map.update(:node_statuses, %{ctx.node_key => :blocked}, fn statuses ->
          Map.put(statuses, ctx.node_key, :blocked)
        end)

      do_submit(ctx.rest, %{
        ctx
        | current_run: blocked_run,
          terminal_failure: failure,
          batch_count: ctx.batch_count
      })
    else
      {:error, :external_cancel} ->
        {:error, Snapshots.cancelled_snapshot(ctx.current_run), [], attempted_node_keys(ctx)}

      {:error, reason} ->
        failed = Snapshots.snapshot_update(ctx.current_run, status: :error, error: reason)
        {:error, failed, [], attempted_node_keys(ctx)}
    end
  end

  defp yield_batch?(%{batch_count: count, batch_started_ms: started_at}) when count > 0 do
    count >= @max_batch_nodes or System.monotonic_time(:millisecond) - started_at >= @max_batch_ms
  end

  defp yield_batch?(_ctx), do: false

  defp handle_admitted_entry(
         %{current_run: current_run, version: version, node_key: node_key} = ctx
       ) do
    case MaterializationClaims.acquire(
           current_run,
           version,
           ctx.manifest_index,
           node_key,
           ctx.decisions,
           ctx.freshness_context,
           ctx.work
         ) do
      {:ok, claim} ->
        submit_admitted_entry(Map.put(ctx, :materialization_claim, claim))

      {:already_succeeded, claim} ->
        with :ok <- release_pre_dispatch(ctx) do
          maybe_skip_succeeded_claim(ctx, claim)
        else
          {:error, reason} -> pre_dispatch_release_failed(ctx, reason)
        end

      {:already_claimed, claim} ->
        with :ok <- release_pre_dispatch(ctx) do
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
        else
          {:error, reason} -> pre_dispatch_release_failed(ctx, reason)
        end

      {:error, reason} ->
        case release_pre_dispatch(ctx) do
          :ok ->
            failed = Snapshots.snapshot_update(current_run, status: :error, error: reason)
            {:error, failed, [], attempted_node_keys(ctx)}

          {:error, release_reason} ->
            pre_dispatch_release_failed(ctx, release_reason)
        end
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
          failed = Snapshots.snapshot_update(ctx.current_run, status: :error, error: reason)
          {:error, failed, [], attempted_node_keys(ctx)}
      end
    else
      failed =
        Snapshots.snapshot_update(ctx.current_run,
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
        {:ok, queued_run, [], ctx.node_keys, next_queued_steps, maybe_add_waiter(ctx),
         ctx.terminal_failure}

      {:ok, queued_run, next_queued_steps} ->
        {:ok, queued_run, entries(ctx), ctx.node_keys, next_queued_steps, maybe_add_waiter(ctx),
         ctx.terminal_failure}

      {:error, :external_cancel} ->
        {:error, Snapshots.cancelled_snapshot(ctx.current_run), [], attempted_node_keys(ctx)}

      {:error, reason} ->
        failed = Snapshots.snapshot_update(ctx.current_run, status: :error, error: reason)
        {:error, failed, [], attempted_node_keys(ctx)}
    end
  end

  defp maybe_add_waiter(%{waiters: waiters, waiter: waiter}), do: waiters ++ [waiter]
  defp maybe_add_waiter(%{waiters: waiters}), do: waiters
  defp entries(%{entries_rev: entries_rev}), do: Enum.reverse(entries_rev)
  defp attempted_node_keys(ctx), do: Enum.map(entries(ctx), & &1.node_key)
  defp task_ids(ctx), do: Enum.map(entries(ctx), & &1.task_id)

  defp submit_admitted_entry(ctx) do
    package_context =
      SystemContext.workspace(ctx.current_run.workspace_id, :execution_package_fetch)

    with {:ok, work} <-
           ExecutionPackages.attach(
             package_context,
             ctx.current_run.deployment_id,
             ctx.work,
             ctx.version,
             ctx.manifest_index
           ) do
      do_submit_admitted_entry(%{
        ctx
        | work: work,
          materialization_claim: MaterializationClaims.enrich(ctx.materialization_claim, work)
      })
    else
      {:error, reason} -> fail_unsubmitted_entry(ctx, ctx.work.asset_ref, reason)
    end
  end

  defp do_submit_admitted_entry(ctx) do
    task_id =
      AssetRunnerTasks.task_id(
        ctx.current_run,
        ctx.work,
        ctx.node_key,
        ctx.attempt
      )

    intended_run =
      with_inflight_task(
        ctx.current_run,
        task_id,
        RunnerWork.lifecycle_metadata(ctx.work)
      )

    intent = %{
      asset_ref: ctx.work.asset_ref,
      runner_task_id: task_id,
      asset_step_id: ctx.work.asset_step_id,
      window: RunnerWork.window(ctx.work),
      stage: ctx.stage,
      attempt: ctx.attempt,
      max_attempts: ctx.work.max_attempts,
      execution_pool: RunnerWork.execution_pool(ctx.work),
      freshness_key: decision_freshness_key(ctx.decisions, ctx.node_key)
    }

    case Persistence.persist_run_step(intended_run, attempt_start_event(ctx.attempt), intent) do
      :ok ->
        enqueue_admitted_entry(%{ctx | current_run: intended_run}, task_id)

      {:error, reason} ->
        fail_unsubmitted_entry(
          %{ctx | current_run: without_inflight_task(ctx.current_run, task_id)},
          ctx.work.asset_ref,
          reason
        )
    end
  end

  defp enqueue_admitted_entry(ctx, task_id) do
    case AssetRunnerTasks.enqueue(
           ctx.current_run,
           ctx.work,
           ctx.node_key,
           ctx.stage,
           ctx.attempt,
           orchestration_context(ctx)
         ) do
      {:ok, %{task_id: ^task_id} = task, work} ->
        entry =
          StageEntry.new!(%{
            run_id: ctx.current_run.id,
            asset_step_id: work.asset_step_id,
            asset_ref: work.asset_ref,
            node_key: ctx.node_key,
            window: RunnerWork.window(work),
            task_id: task.task_id,
            assignment_generation: task.assignment_generation,
            runner_pool: task.runner_pool,
            required_runner_release_id: task.required_runner_release_id,
            decision: Map.get(ctx.decisions, ctx.node_key, %{}),
            attempt: ctx.attempt,
            stage: ctx.stage,
            lease: ctx.lease,
            materialization_claim: MaterializationClaims.enrich(ctx.materialization_claim, work),
            execution_pool: RunnerWork.execution_pool(work),
            resource_circuit_permits: ctx.resource_circuit_permits,
            freshness_key: decision_freshness_key(ctx.decisions, ctx.node_key)
          })

        do_submit(ctx.rest, %{
          ctx
          | work: work,
            current_run: ctx.current_run,
            entries_rev: [entry | ctx.entries_rev]
        })

      {:error, reason} ->
        fail_unsubmitted_entry(ctx, ctx.work.asset_ref, reason)
    end
  end

  defp orchestration_context(ctx) do
    PipelineTaskContinuation.new!(%{
      decision: Map.get(ctx.decisions, ctx.node_key, %{}),
      materialization_claim: MaterializationClaims.enrich(ctx.materialization_claim, ctx.work),
      resource_circuit_permits: ctx.resource_circuit_permits,
      freshness_checkpoint: ctx.freshness_checkpoint,
      freshness_key: decision_freshness_key(ctx.decisions, ctx.node_key)
    })
  end

  defp fail_unsubmitted_entry(ctx, asset_ref, reason) do
    with :ok <- release_entry_lease(%{lease: ctx.lease}),
         :ok <-
           ResourceCircuits.release(
             ctx.current_run,
             Map.get(ctx, :resource_circuit_permits, [])
           ),
         :ok <- fail_claim(ctx, reason) do
      if safe_retryable?(reason) and
           StepAttemptLifecycle.retry_allowed?(ctx.current_run, ctx.node_key, ctx.attempt) do
        persist_retryable_submit_failure(ctx, asset_ref, reason)
      else
        terminalize_unsubmitted_entry(ctx, asset_ref, reason)
      end
    else
      {:error, release_reason} -> pre_dispatch_release_failed(ctx, release_reason)
    end
  end

  defp persist_retryable_submit_failure(ctx, asset_ref, reason) do
    failed = RunState.transition(ctx.current_run, status: :error, error: reason)

    result =
      {:partial_retry, failed, entries(ctx), ctx.rest, ctx.node_key, reason, ctx.queued_steps,
       ctx.waiters, ctx.terminal_failure}

    case persist_stage_submit_failure_event(ctx, failed, asset_ref, reason, true, result) do
      :ok ->
        result

      {:error, :external_cancel} ->
        terminalize_unsubmitted_entry(ctx, asset_ref, :external_cancel)

      {:error, persist_reason, retry} ->
        {:persist_retry, retry, persist_reason}
    end
  end

  defp terminalize_unsubmitted_entry(ctx, asset_ref, reason) do
    :ok = cleanup_entries(ctx.current_run, entries(ctx), reason)

    cancelled =
      cancel_task_ids(
        ctx.current_run,
        task_ids(ctx),
        %{kind: :submit_failure, asset_ref: asset_ref, error: reason}
      )

    failed =
      RunState.transition(cancelled, status: :error, error: reason, runner_task_id: nil)

    persist_stage_submit_failure(ctx, failed, asset_ref, reason, safe_retryable?(reason))
  end

  defp persist_stage_submit_failure(ctx, failed, asset_ref, reason, retryable?) do
    result = {:error, failed, [], attempted_node_keys(ctx)}

    case persist_stage_submit_failure_event(
           ctx,
           failed,
           asset_ref,
           reason,
           retryable?,
           result
         ) do
      :ok ->
        result

      {:error, :external_cancel} ->
        {:error, Snapshots.cancelled_snapshot(failed), [], attempted_node_keys(ctx)}

      {:error, persist_reason, retry} ->
        {:persist_retry, retry, persist_reason}
    end
  end

  defp persist_stage_submit_failure_event(
         ctx,
         failed,
         asset_ref,
         reason,
         retryable?,
         resume_result
       ) do
    data = %{
      asset_ref: asset_ref,
      error: reason,
      node_key: RunnerWork.node_key(ctx.work),
      asset_step_id: ctx.work.asset_step_id,
      window: RunnerWork.window(ctx.work),
      stage: ctx.stage,
      attempt: ctx.attempt,
      max_attempts: ctx.work.max_attempts,
      retryable?: retryable?,
      retry_exhausted?: retryable? and ctx.attempt >= ctx.work.max_attempts,
      execution_pool: RunnerWork.execution_pool(ctx.work)
    }

    retry =
      PersistenceRetry.new(
        failed,
        :step_failed,
        data,
        {:stage_admission, ctx.attempt, resume_result}
      )

    case PersistenceRetry.persist(retry) do
      :ok -> :ok
      {:error, :external_cancel} -> {:error, :external_cancel}
      {:error, reason} -> {:error, reason, retry}
    end
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
           max_attempts: work.max_attempts,
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

  defp stage_work(
         %RunState{} = run_state,
         %Version{} = version,
         %Index{} = manifest_index,
         manifest_lease_id,
         node_key,
         stage,
         attempt
       ) do
    with {:ok, %{work: work}} <-
           run_state
           |> StepAttemptLifecycle.new(version, node_key, stage, attempt)
           |> StepAttemptLifecycle.build_work(manifest_index) do
      {:ok,
       work
       |> StepAttemptLifecycle.attach_deadline(run_state)
       |> Map.put(:manifest_lease_id, manifest_lease_id)}
    end
  end

  defp safe_retryable?(%RunnerError{retryable?: true, outcome: :safe_failure}), do: true
  defp safe_retryable?(_reason), do: false

  defp with_inflight_task(%RunState{} = run_state, task_id, metadata) do
    ids =
      run_state.metadata
      |> Map.get(:active_runner_task_ids, [])
      |> Kernel.++([task_id])
      |> Enum.filter(&is_binary/1)
      |> Enum.uniq()

    RunState.transition(run_state,
      runner_task_id: nil,
      metadata: run_state.metadata |> Map.merge(metadata) |> Map.put(:active_runner_task_ids, ids)
    )
  end

  defp without_inflight_task(%RunState{} = run_state, task_id) do
    ids =
      run_state.metadata
      |> Map.get(:active_runner_task_ids, [])
      |> Enum.reject(&(&1 == task_id))

    RunState.transition(run_state,
      runner_task_id: nil,
      metadata: Map.put(run_state.metadata, :active_runner_task_ids, ids)
    )
  end

  defp cleanup_entries(%RunState{} = run_state, entries, reason) when is_list(entries) do
    run_state
    |> ActiveTaskSet.from_entries(entries)
    |> ActiveTaskSet.cleanup_all(reason)
  end

  defp stop_after_stage_build_failure(ctx, node_key, reason) do
    reason = PreSubmitFailure.normalize(reason)

    failure =
      (ctx.terminal_failure || %{status: :error, error: reason})
      |> Map.update(:node_statuses, %{node_key => :error}, fn statuses ->
        Map.put(statuses, node_key, :error)
      end)

    {:ok, ctx.current_run, entries(ctx), [], ctx.queued_steps, ctx.waiters, failure}
  end

  defp fail_claim(ctx, reason) do
    ActiveTaskSet.fail_entry_claim(%{materialization_claim: ctx.materialization_claim}, reason)
  end

  defp release_entry_lease(entry), do: ActiveTaskSet.release_entry(entry)

  defp release_pre_dispatch(ctx) do
    with :ok <- release_entry_lease(ctx),
         :ok <-
           ResourceCircuits.release(
             ctx.current_run,
             Map.get(ctx, :resource_circuit_permits, [])
           ) do
      :ok
    end
  end

  defp pre_dispatch_release_failed(ctx, reason) do
    failed = Snapshots.snapshot_update(ctx.current_run, status: :error, error: reason)
    {:error, failed, [], attempted_node_keys(ctx)}
  end

  defp attempt_start_event(attempt) when attempt > 1, do: :step_retry_started
  defp attempt_start_event(_attempt), do: :step_started

  defp cancel_task_ids(
         %RunState{} = run_state,
         task_ids,
         reason
       ) do
    {run_state, _cancel_results} =
      cancel_task_ids_with_results(run_state, task_ids, reason)

    run_state
  end

  defp cancel_task_ids_with_results(
         %RunState{} = run_state,
         task_ids,
         reason
       ) do
    cancel_results = Cancellation.dispatch_runner_tasks(run_state, task_ids, reason)

    confirmed_ids =
      cancel_results
      |> Enum.filter(&CancellationOutcome.confirmed?/1)
      |> Enum.map(& &1.task_id)

    remaining_ids =
      run_state
      |> ActiveTaskSet.active_runner_task_ids()
      |> Enum.reject(&(&1 in confirmed_ids))

    metadata =
      run_state.metadata
      |> Map.delete(:active_runner_task_ids)
      |> Map.delete("active_runner_task_ids")
      |> Map.put(:active_runner_task_ids, remaining_ids)
      |> Map.put(:cancel_outcomes, Enum.map(cancel_results, &CancellationOutcome.to_map/1))

    {Snapshots.snapshot_update(run_state, metadata: metadata, runner_task_id: nil),
     cancel_results}
  end

  defp decision_freshness_key(decisions, node_key) when is_map(decisions) do
    decisions
    |> Map.get(node_key, %{})
    |> Map.get(:freshness_key, Key.latest())
  end
end
