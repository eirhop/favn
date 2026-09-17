defmodule FavnOrchestrator.RunServer.Execution.StageAdmission do
  @moduledoc """
  Admission and runner submission for one pipeline stage attempt.

  This module owns stage-local submit/defer decisions: execution admission
  leases, materialization claims, queued-step dedupe, `:step_queued`, and
  `:step_started` persistence. It does not await runner results or decide retry
  and failure-drain behavior.

  Nodes in a stage are independent, so a terminal admission failure that belongs
  to one node fails only that node. `node_specific_failure?/2` decides that by
  call site and error term for claim failures. A run-wide failure can stop
  further admission, but retains saved siblings for draining. A per-node failure persists the node's
  `:step_failed` with the run left `running`, remembers it as the stage's first
  terminal failure, and continues submitting the rest of the stage so
  already-submitted siblings are never cancelled.
  """

  require Logger

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
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.Persistence.Error, as: PersistenceError
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.PreSubmitFailure
  alias FavnOrchestrator.RunServer.Execution.PipelineTaskContinuation
  alias FavnOrchestrator.RunServer.Execution.RecoveryPosition
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
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
  @type deferred_refill_cause :: :batch_budget | :blocked | nil

  @typedoc """
  Call site of a terminal admission failure, used to classify it.

  Only `:materialization_claim` and `:execution_package` can fail one node on
  their own. `:attempt_start` is a run transition, so its failure means lost
  ownership or store trouble and always stops the stage. Invalid `:enqueue`
  failures also stop the stage after confirming the task was never saved.
  """
  @type call_site :: :materialization_claim | :execution_package | :attempt_start | :enqueue

  @typedoc """
  Persist-retry resume for a node-specific terminal admission failure.

  This shape is never returned by `submit/1`. It is carried inside the
  `:persist_retry` result as the resume payload and handed back to execution
  when the retried write succeeds.

  The failed node's outcome is durable once the retried write succeeds. The
  remaining nodes are carried as deferred work with the batch-budget refill
  cause so submission continues immediately. The completed node statuses are
  carried so the payload stays correct for a resume that rebuilds stage state
  from scratch; execution reaches that variant only on a stage's first attempt,
  where no node has completed yet, because a later attempt resumes through the
  refill variant and keeps the live stage state.
  """
  @type node_failure_resume ::
          {:node_failed, RunState.t(), [entry()], [node_key()], MapSet.t(term()), [map()],
           map() | nil, deferred_refill_cause(), %{optional(node_key()) => atom()}}

  @typedoc """
  Outcome of submitting a stage's runnable nodes.

  A `:persist_retry` carries either the partial-retry shape or
  `t:node_failure_resume/0` as its resume payload.
  """
  @type result ::
          {:ok, RunState.t(), [entry()], [node_key()], MapSet.t(term()), [map()], map() | nil,
           deferred_refill_cause()}
          | {:partial_retry, RunState.t(), [entry()], [node_key()], node_key(), term(),
             MapSet.t(term()), [map()], map() | nil, deferred_refill_cause()}
          | {:error, RunState.t(), [term()], [node_key()], [entry()]}
          | {:persist_retry, PersistenceRetry.t(), term()}
          | {:persist_retry, PersistenceRetry.t(), term(), map()}

  @spec submit(map()) :: result()
  def submit(request) when is_map(request) do
    request
    |> Map.put_new(:queued_steps, MapSet.new())
    |> Map.put_new(:completed_node_statuses, %{})
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
         queued_steps: %MapSet{} = queued_steps,
         completed_node_statuses: completed_node_statuses
       })
       when is_list(node_keys) and is_map(decisions) and is_map(freshness_context) and
              is_map(freshness_checkpoint) and is_map(completed_node_statuses) do
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
      terminal_failure: nil,
      completed_node_statuses: completed_node_statuses
    }

    do_submit(node_keys, ctx)
  end

  defp do_submit([], ctx) do
    {:ok, ctx.current_run, entries(ctx), [], ctx.queued_steps, ctx.waiters, ctx.terminal_failure,
     nil}
  end

  defp do_submit([node_key | rest] = node_keys, ctx) do
    ctx =
      Map.drop(ctx, [
        :lease,
        :resource_circuit_permits,
        :materialization_claim,
        :prepared_claim,
        :waiter,
        :queue_reason,
        :queue_signature,
        :scope
      ])

    cond do
      yield_batch?(ctx) ->
        {:ok, ctx.current_run, entries(ctx), node_keys, ctx.queued_steps, ctx.waiters,
         ctx.terminal_failure, :batch_budget}

      Persistence.externally_cancelled?(ctx.current_run) ->
        {:error, Snapshots.cancelled_snapshot(ctx.current_run), [], attempted_node_keys(ctx),
         entries(ctx)}

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
        {:error, failed, [], attempted_node_keys(ctx), entries(ctx)}
    end
  end

  defp admit_execution_capacity(ctx) do
    entry = %{
      asset_step_id: ctx.work.asset_step_id,
      execution_pool: RunnerWork.execution_pool(ctx.work),
      stage: ctx.stage,
      attempt: ctx.attempt
    }

    case ExecutionAdmission.prepare_acquire(ctx.current_run, entry, []) do
      {:ok, nil} ->
        handle_capacity_result(ctx, {:ok, nil})

      {:ok, command} ->
        pause = %{ctx: ctx, entries: entries(ctx), phase: :admission, admission_entry: entry}

        persist_operation(
          PersistenceRetry.command(
            ctx.current_run,
            :admission,
            command,
            operation_data(ctx),
            {:stage_operation, pause}
          ),
          pause
        )

      {:error, reason} ->
        handle_capacity_result(ctx, {:error, reason})
    end
  end

  defp handle_capacity_result(ctx, result) do
    case result do
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
        {:error, ctx.current_run, [], attempted_node_keys(ctx), entries(ctx)}

      {:error, reason} ->
        Logger.error(
          "execution admission failed " <>
            "run_id=#{ctx.current_run.id} " <>
            "asset_step_id=#{ctx.work.asset_step_id} " <>
            "reason=#{inspect(Redaction.redact_operational_bounded(reason))}"
        )

        failed = Snapshots.snapshot_update(ctx.current_run, status: :error, error: reason)
        {:error, failed, [], attempted_node_keys(ctx), entries(ctx)}
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

    outcome_run =
      RecoveryPosition.record_outcome(ctx.current_run, ctx.stage, ctx.attempt)

    retry =
      StageClassifier.prepare_decision(
        outcome_run,
        ctx.version,
        ctx.node_key,
        ctx.stage,
        :blocked,
        decision
      )

    pause = %{
      ctx: released_context(ctx),
      entries: entries(ctx),
      phase: :blocked_decision,
      decision_run: StageClassifier.decision_result(retry),
      blockers: blockers,
      block_reason: reason
    }

    persist_operation(%{retry | resume: {:stage_operation, pause}}, pause)
  end

  defp finish_resource_block(ctx, reason) do
    failure =
      (ctx.terminal_failure || %{status: :error, error: {:blocked, ctx.node_key, reason}})
      |> Map.update(
        :node_statuses,
        %{ctx.node_key => :blocked},
        &Map.put(&1, ctx.node_key, :blocked)
      )

    do_submit(ctx.rest, %{ctx | terminal_failure: failure})
  end

  defp yield_batch?(%{batch_count: count, batch_started_ms: started_at}) when count > 0 do
    count >= @max_batch_nodes or System.monotonic_time(:millisecond) - started_at >= @max_batch_ms
  end

  defp yield_batch?(_ctx), do: false

  defp handle_admitted_entry(
         %{current_run: current_run, version: version, node_key: node_key} = ctx
       ) do
    case MaterializationClaims.prepare_acquire(
           current_run,
           version,
           ctx.manifest_index,
           node_key,
           ctx.decisions,
           ctx.freshness_context,
           ctx.work
         ) do
      {:ok, claim, command} ->
        ctx = Map.put(ctx, :prepared_claim, claim)
        pause = %{ctx: ctx, entries: entries(ctx), phase: :materialization_claim}

        persist_operation(
          PersistenceRetry.command(
            current_run,
            :materialization_claim,
            command,
            operation_data(ctx),
            {:stage_operation, pause}
          ),
          pause
        )

      other ->
        handle_claim_result(ctx, other)
    end
  end

  defp handle_claim_result(ctx, result) do
    current_run = ctx.current_run

    case result do
      {:ok, claim} ->
        submit_admitted_entry(Map.put(ctx, :materialization_claim, claim))

      {:already_succeeded, claim} ->
        with :ok <- release_pre_dispatch(ctx) do
          maybe_skip_succeeded_claim(released_context(ctx), claim)
        else
          {:error, reason} -> pre_dispatch_release_failed(ctx, reason)
        end

      {:already_claimed, claim} ->
        with :ok <- release_pre_dispatch(ctx) do
          queue_reason = :materialization_claim
          scope = MaterializationClaims.scope(claim)

          persist_or_defer_queued_entry(
            released_context(ctx)
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
            if node_specific_failure?(:materialization_claim, reason) do
              fail_node_and_continue(ctx, reason)
            else
              failed = Snapshots.snapshot_update(current_run, status: :error, error: reason)
              {:error, failed, [], attempted_node_keys(ctx), entries(ctx)}
            end

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

      outcome_run =
        RecoveryPosition.record_outcome(ctx.current_run, ctx.stage, ctx.attempt)

      retry =
        StageClassifier.prepare_decision(
          outcome_run,
          ctx.version,
          ctx.node_key,
          ctx.stage,
          :skipped_fresh,
          decision
        )

      pause = %{
        ctx: ctx,
        entries: entries(ctx),
        phase: :skipped_decision,
        decision_run: StageClassifier.decision_result(retry)
      }

      persist_operation(%{retry | resume: {:stage_operation, pause}}, pause)
    else
      failed =
        Snapshots.snapshot_update(ctx.current_run,
          status: :error,
          error: {:non_reusable_materialization_claim_succeeded, MaterializationClaims.key(claim)}
        )

      {:error, failed, [], attempted_node_keys(ctx), entries(ctx)}
    end
  end

  defp persist_or_defer_queued_entry(ctx) do
    if MapSet.member?(ctx.queued_steps, ctx.queue_signature) do
      queued_result(ctx)
    else
      run = RunState.transition(ctx.current_run, status: :running, error: nil)

      data = %{
        asset_ref: ctx.work.asset_ref,
        asset_step_id: ctx.work.asset_step_id,
        node_key: ctx.node_key,
        window: RunnerWork.window(ctx.work),
        stage: ctx.stage,
        attempt: ctx.attempt,
        execution_pool: RunnerWork.execution_pool(ctx.work),
        queue_reason: ctx.queue_reason,
        scope: ctx.scope
      }

      pause = %{ctx: ctx, entries: entries(ctx), phase: :queued, run: run}
      retry = PersistenceRetry.new(run, :step_queued, data, {:stage_operation, pause})
      persist_operation(retry, pause)
    end
  end

  defp queued_result(ctx) do
    {:ok, ctx.current_run, entries(ctx), ctx.node_keys,
     MapSet.put(ctx.queued_steps, ctx.queue_signature), maybe_add_waiter(ctx),
     ctx.terminal_failure, :blocked}
  end

  @doc false
  @spec adopt_operation(map(), term()) :: map()
  def adopt_operation(%{phase: phase, ctx: ctx} = pause, result)
      when phase in [:admission, :admission_recheck] do
    resolved =
      ExecutionAdmission.resolve_admission(ctx.current_run, pause.admission_entry, result)

    ctx =
      case resolved do
        {:ok, lease} -> Map.put(ctx, :lease, lease)
        {:waiting, waiter} -> Map.put(ctx, :waiter, waiter)
        _ -> ctx
      end

    pause |> Map.put(:ctx, ctx) |> Map.put(:resolved_result, resolved)
  end

  def adopt_operation(%{phase: :materialization_claim, ctx: ctx} = pause, result) do
    resolved = MaterializationClaims.resolve_claim(ctx.prepared_claim, result)

    ctx =
      case resolved do
        {:ok, claim} -> Map.put(ctx, :materialization_claim, claim)
        _ -> Map.update!(ctx, :prepared_claim, &Map.delete(&1, :target_operation_lock))
      end

    pause |> Map.put(:ctx, ctx) |> Map.put(:resolved_result, resolved)
  end

  def adopt_operation(%{phase: :runner_enqueue, ctx: ctx} = pause, task) do
    pause
    |> Map.put(:submitted?, true)
    |> Map.put(:entries, pause.entries ++ [enqueued_entry(ctx, task)])
  end

  def adopt_operation(pause, _result), do: pause

  @doc false
  @spec resume_operation(map(), term()) :: result()
  def resume_operation(%{ctx: ctx} = pause, result) do
    if deadline_live?(ctx.work.deadline_at) or
         pause.phase in [
           :queued,
           :blocked_decision,
           :skipped_decision,
           :runner_enqueue,
           :resource_recovery_candidate
         ] do
      resume_operation_phase(pause, result)
    else
      expire_operation(pause)
    end
  end

  @doc false
  def expire_operation(%{ctx: ctx} = pause) do
    run = cleanup_paused(%{pause | ctx: ctx}, :runner_task_deadline_exceeded)
    fail_node_and_continue(%{ctx | current_run: run}, :runner_task_deadline_exceeded)
  end

  defp resume_operation_phase(%{phase: :queued, ctx: ctx, run: run}, :ok),
    do: queued_result(%{ctx | current_run: run})

  defp resume_operation_phase(
         %{phase: :admission, ctx: ctx, admission_entry: entry} = pause,
         _result
       ) do
    case pause.resolved_result do
      {:waiting, waiter} ->
        ctx = Map.put(ctx, :waiter, waiter)

        case ExecutionAdmission.prepare_acquire(ctx.current_run, entry,
               stage: ctx.stage,
               attempt: ctx.attempt
             ) do
          {:ok, command} when not is_nil(command) ->
            pause = %{pause | ctx: ctx, phase: :admission_recheck}

            retry =
              PersistenceRetry.command(
                ctx.current_run,
                :admission,
                command,
                operation_data(ctx),
                {:stage_operation, pause}
              )

            persist_operation(retry, pause)

          {:ok, nil} ->
            handle_capacity_result(ctx, {:ok, nil})

          {:error, reason} ->
            handle_capacity_result(ctx, {:error, reason})
        end

      other ->
        handle_capacity_result(ctx, other)
    end
  end

  defp resume_operation_phase(
         %{phase: :admission_recheck, ctx: ctx, resolved_result: resolved},
         _result
       ) do
    if match?({:ok, _}, resolved), do: ExecutionAdmission.cancel_wait(ctx.waiter)
    handle_capacity_result(ctx, resolved)
  end

  defp resume_operation_phase(
         %{phase: :materialization_claim, ctx: ctx, resolved_result: resolved},
         _result
       ),
       do: handle_claim_result(ctx, resolved)

  defp resume_operation_phase(%{phase: :attempt_start, ctx: ctx, task_id: task_id}, :ok),
    do: enqueue_admitted_entry(ctx, task_id)

  defp resume_operation_phase(%{phase: :runner_enqueue, ctx: ctx}, task),
    do: accept_enqueued_entry(ctx, task)

  defp resume_operation_phase(%{phase: :skipped_decision, ctx: ctx, decision_run: run}, :ok),
    do: do_submit(ctx.rest, %{ctx | current_run: run})

  defp resume_operation_phase(
         %{phase: :blocked_decision, ctx: ctx, decision_run: run} = pause,
         :ok
       ) do
    ctx = %{ctx | current_run: run}

    case ResourceCircuits.prepare_blocked(run, ctx.work, pause.blockers) do
      nil ->
        finish_resource_block(ctx, pause.block_reason)

      command ->
        pause = %{pause | ctx: ctx, phase: :resource_recovery_candidate}

        retry =
          PersistenceRetry.command(
            run,
            :resource_recovery_candidate,
            command,
            operation_data(ctx),
            {:stage_operation, pause}
          )

        persist_operation(retry, pause)
    end
  end

  defp resume_operation_phase(
         %{phase: :resource_recovery_candidate, ctx: ctx, block_reason: reason},
         :ok
       ),
       do: finish_resource_block(ctx, reason)

  defp operation_data(ctx),
    do: %{
      asset_step_id: ctx.work.asset_step_id,
      asset_ref: ctx.work.asset_ref,
      node_key: ctx.node_key,
      stage: ctx.stage,
      attempt: ctx.attempt
    }

  defp released_context(ctx),
    do: ctx |> Map.put(:lease, nil) |> Map.put(:resource_circuit_permits, [])

  defp persist_operation(retry, pause) do
    case PersistenceRetry.persist(retry) do
      :ok ->
        resume_operation(pause, :ok)

      {:ok, result} ->
        resume_operation(adopt_operation(pause, result), result)

      {:error, %{details: %{reason_code: "target_write_in_progress"}} = reason}
      when pause.phase == :materialization_claim ->
        reject_operation(pause, reason)

      {:error, reason} ->
        if PersistenceRetry.replayable?(reason),
          do: {:persist_retry, retry, reason, pause},
          else: reject_operation(pause, reason)
    end
  end

  @doc false
  @spec fail_operation(map(), term()) :: result()
  def fail_operation(%{ctx: ctx} = pause, reason) do
    run = cleanup_paused(pause, reason)
    fail_node_and_continue(%{ctx | current_run: run}, reason)
  end

  @doc false
  def reject_operation(%{phase: :admission, ctx: ctx}, reason),
    do: handle_capacity_result(ctx, {:error, reason})

  def reject_operation(
        %{phase: :materialization_claim, ctx: ctx},
        %{details: %{reason_code: "target_write_in_progress"}}
      ) do
    :ok = MaterializationClaims.release_prepared_claim(ctx.prepared_claim)
    handle_claim_result(ctx, {:already_claimed, %{claim_key: ctx.prepared_claim.claim_key}})
  end

  def reject_operation(%{phase: :materialization_claim, ctx: ctx}, reason) do
    :ok = MaterializationClaims.release_prepared_claim(ctx.prepared_claim)
    handle_claim_result(ctx, {:error, reason})
  end

  def reject_operation(%{ctx: ctx}, reason),
    do:
      {:error, Snapshots.snapshot_update(ctx.current_run, status: :error, error: reason), [],
       attempted_node_keys(ctx), entries(ctx)}

  defp maybe_add_waiter(%{waiters: waiters, waiter: waiter}), do: waiters ++ [waiter]
  defp maybe_add_waiter(%{waiters: waiters}), do: waiters
  defp deferred_refill_cause([], _cause), do: nil
  defp deferred_refill_cause([_ | _], cause), do: cause
  defp entries(%{entries_rev: entries_rev}), do: Enum.reverse(entries_rev)
  defp attempted_node_keys(ctx), do: Enum.map(entries(ctx), & &1.node_key)

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
      {:error, reason} ->
        fail_unsubmitted_entry(ctx, :execution_package, ctx.work.asset_ref, reason)
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

    pause = %{
      ctx: %{ctx | current_run: intended_run},
      phase: :attempt_start,
      task_id: task_id,
      entries: entries(ctx)
    }

    retry =
      PersistenceRetry.new(
        intended_run,
        attempt_start_event(ctx.attempt),
        intent,
        {:stage_operation, pause}
      )

    case PersistenceRetry.persist(retry) do
      :ok ->
        resume_operation(pause, :ok)

      {:error, reason} ->
        if replayable_attempt_start_failure?(reason),
          do: {:persist_retry, retry, reason, pause},
          else: fail_unsubmitted_entry(ctx, :attempt_start, ctx.work.asset_ref, reason)
    end
  end

  @doc false
  @spec dispatch_expired?(map()) :: boolean()
  def dispatch_expired?(%{phase: phase, ctx: ctx})
      when phase in [:admission, :admission_recheck, :materialization_claim, :runner_enqueue],
      do: not deadline_live?(ctx.work.deadline_at)

  def dispatch_expired?(_pause), do: false

  @doc false
  @spec cleanup_paused(map(), term()) :: RunState.t()
  def cleanup_paused(%{ctx: ctx} = pause, reason),
    do: cleanup_paused(pause, reason, ctx.current_run)

  @doc false
  @spec cleanup_paused(map(), term(), RunState.t()) :: RunState.t()
  def cleanup_paused(%{submitted?: true}, _reason, current_run), do: current_run

  def cleanup_paused(%{ctx: ctx} = pause, reason, %RunState{} = current_run) do
    if waiter = Map.get(ctx, :waiter), do: ExecutionAdmission.cancel_wait(waiter)
    :ok = release_entry_lease(%{lease: Map.get(ctx, :lease)})
    _ = ResourceCircuits.release(ctx.current_run, Map.get(ctx, :resource_circuit_permits, []))
    :ok = fail_claim(ctx, reason)

    if Map.get(pause, :task_id),
      do: without_inflight_task(current_run, pause.task_id),
      else: current_run
  end

  @doc false
  @spec renew_paused_claim(map()) :: :ok | {:error, term()}
  def renew_paused_claim(%{ctx: %{materialization_claim: claim}}) when is_map(claim),
    do: MaterializationClaims.renew_operation_lock(claim)

  def renew_paused_claim(%{ctx: %{prepared_claim: claim}}),
    do: MaterializationClaims.renew_operation_lock(claim)

  def renew_paused_claim(_pause), do: :ok

  defp deadline_live?(nil), do: true

  defp deadline_live?(%DateTime{} = deadline),
    do: DateTime.compare(deadline, DateTime.utc_now()) == :gt

  defp enqueue_admitted_entry(ctx, task_id) do
    case AssetRunnerTasks.prepare(
           ctx.current_run,
           ctx.work,
           ctx.node_key,
           ctx.attempt,
           orchestration_context(ctx)
         ) do
      {:ok, command, work} ->
        ctx = %{ctx | work: work}
        pause = %{ctx: ctx, entries: entries(ctx), phase: :runner_enqueue, task_id: task_id}

        retry =
          PersistenceRetry.command(
            ctx.current_run,
            :runner_enqueue,
            command,
            operation_data(ctx),
            {:stage_operation, pause}
          )

        case PersistenceRetry.persist(retry) do
          {:ok, task} ->
            accept_enqueued_entry(ctx, task)

          {:error,
           %PersistenceError{details: %{reason_code: "execution_history_owner_busy"}} = reason} ->
            {:persist_retry, retry, reason, pause}

          {:error, reason} ->
            fail_enqueue(ctx, task_id, reason)
        end

      {:error, reason} ->
        fail_enqueue(ctx, task_id, reason)
    end
  end

  defp accept_enqueued_entry(ctx, task) do
    entry = enqueued_entry(ctx, task)
    do_submit(ctx.rest, %{ctx | entries_rev: [entry | ctx.entries_rev]})
  end

  defp enqueued_entry(ctx, task) do
    work = ctx.work

    StageEntry.new!(%{
      run_id: ctx.current_run.id,
      asset_step_id: ctx.work.asset_step_id,
      asset_ref: ctx.work.asset_ref,
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
  end

  defp fail_enqueue(ctx, task_id, reason) do
    if AssetRunnerTasks.rejected_without_task?(ctx.current_run, task_id, reason) do
      fail_unsubmitted_entry(
        %{ctx | current_run: without_inflight_task(ctx.current_run, task_id)},
        :enqueue,
        ctx.work.asset_ref,
        reason
      )
    else
      fail_unknown_enqueue(ctx, task_id, ctx.work.asset_ref, reason)
    end
  end

  defp fail_unknown_enqueue(ctx, task_id, asset_ref, reason) do
    cancel_reason = %{kind: :runner_task_enqueue_unknown, asset_ref: asset_ref, error: reason}

    {cancelled, [_outcome]} =
      cancel_task_ids_with_results(ctx.current_run, [task_id], cancel_reason)

    failed =
      cancelled
      |> RunState.transition(status: :error, error: reason)
      |> RecoveryPosition.record_outcome(ctx.stage, ctx.attempt)

    persist_stage_submit_failure(
      ctx,
      failed,
      asset_ref,
      reason,
      false,
      entries(ctx) ++ [unknown_enqueue_entry(ctx, task_id)]
    )
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

  defp fail_unsubmitted_entry(ctx, call_site, asset_ref, reason) do
    with :ok <- release_entry_lease(%{lease: ctx.lease}),
         :ok <-
           ResourceCircuits.release(
             ctx.current_run,
             Map.get(ctx, :resource_circuit_permits, [])
           ),
         :ok <- fail_claim(ctx, reason) do
      cond do
        safe_retryable?(reason) and
            StepAttemptLifecycle.retry_allowed?(ctx.current_run, ctx.node_key, ctx.attempt) ->
          persist_retryable_submit_failure(ctx, asset_ref, reason)

        node_specific_failure?(call_site, reason) ->
          fail_node_and_continue(ctx, reason)

        true ->
          terminalize_unsubmitted_entry(ctx, asset_ref, reason)
      end
    else
      {:error, release_reason} -> pre_dispatch_release_failed(ctx, release_reason)
    end
  end

  defp persist_retryable_submit_failure(ctx, asset_ref, reason) do
    failed =
      ctx.current_run
      |> RunState.transition(status: :error, error: reason)
      |> RecoveryPosition.record_outcome(ctx.stage, ctx.attempt)

    result =
      {:partial_retry, failed, entries(ctx), ctx.rest, ctx.node_key, reason, ctx.queued_steps,
       ctx.waiters, ctx.terminal_failure, deferred_refill_cause(ctx.rest, :blocked)}

    case persist_stage_submit_failure_event(ctx, failed, asset_ref, reason, true, result) do
      :ok ->
        result

      {:error, :external_cancel} ->
        terminalize_unsubmitted_entry(ctx, asset_ref, :external_cancel)

      {:error, persist_reason, retry} ->
        {:persist_retry, retry, persist_reason}
    end
  end

  defp terminalize_unsubmitted_entry(ctx, _asset_ref, :external_cancel),
    do:
      {:error, Snapshots.cancelled_snapshot(ctx.current_run), [], attempted_node_keys(ctx),
       entries(ctx)}

  defp terminalize_unsubmitted_entry(ctx, _asset_ref, reason),
    do: fail_node_and_continue(ctx, reason)

  @doc """
  Returns whether a terminal admission failure belongs to one node alone.

  Classification is by call site and error term together, because the same term
  can mean different things at different call sites. Every term not listed here
  keeps the stage-stop behavior for claim admission. Stopping admission does
  not cancel independent tasks that were already saved.
  """
  @spec node_specific_failure?(call_site(), term()) :: boolean()
  def node_specific_failure?(call_site, reason)

  def node_specific_failure?(:materialization_claim, %PersistenceError{kind: :conflict}), do: true

  def node_specific_failure?(:materialization_claim, {:target_generation_pin_mismatch, _ref}),
    do: true

  def node_specific_failure?(:materialization_claim, {:target_generation_not_pinned, _ref}),
    do: true

  def node_specific_failure?(
        :materialization_claim,
        {:unexpected_materialization_decision, _status}
      ),
      do: true

  # Stage build already rejects an asset missing from the manifest index, so
  # this is kept only so a future call order cannot turn one node's missing
  # asset into a run-wide stop.
  def node_specific_failure?(call_site, :asset_not_found)
      when call_site in [:materialization_claim, :execution_package],
      do: true

  def node_specific_failure?(:execution_package, reason),
    do: execution_package_failure?(reason)

  def node_specific_failure?(_call_site, _reason), do: false

  defp execution_package_failure?(reason)
       when reason in [
              :execution_package_required,
              :execution_package_deployment_required,
              :execution_package_materialization_mismatch,
              :invalid_execution_package
            ],
       do: true

  defp execution_package_failure?({reason, _detail})
       when reason in [
              :execution_package_not_required,
              :execution_package_relation_inputs_mismatch,
              :invalid_execution_package_hash
            ],
       do: true

  defp execution_package_failure?({reason, _expected, _actual})
       when reason in [
              :execution_package_hash_mismatch,
              :execution_package_asset_mismatch,
              :unsupported_execution_package_schema
            ],
       do: true

  defp execution_package_failure?(_reason), do: false

  # Both call sites release the node's pre-dispatch resources before this runs,
  # so the failure only has to become durable while the stage keeps going. The
  # run stays `running` because siblings are still working; the stage's first
  # terminal failure becomes the run's error when the stage finalizes.
  defp fail_node_and_continue(ctx, reason) do
    node_result = failed_node_result(ctx)

    failed_run =
      ctx.current_run
      |> RunState.transition(status: :running, error: nil)
      |> RecoveryPosition.record_outcome(ctx.stage, ctx.attempt)
      |> ResultBuilder.append_node_result(node_result)

    failure =
      (ctx.terminal_failure || %{status: :error, error: reason})
      |> Map.update(:node_statuses, %{ctx.node_key => :error}, fn statuses ->
        Map.put(statuses, ctx.node_key, :error)
      end)

    resume =
      {:node_failed, failed_run, entries(ctx), ctx.rest, ctx.queued_steps, ctx.waiters, failure,
       deferred_refill_cause(ctx.rest, :batch_budget), ctx.completed_node_statuses}

    retry =
      PersistenceRetry.new(
        failed_run,
        :step_failed,
        failed_node_event_data(ctx, reason, node_result),
        {:stage_admission, ctx.attempt, resume}
      )

    case PersistenceRetry.persist(retry) do
      :ok ->
        do_submit(ctx.rest, %{ctx | current_run: failed_run, terminal_failure: failure})

      {:error, :external_cancel} ->
        {:error, Snapshots.cancelled_snapshot(ctx.current_run), [], attempted_node_keys(ctx),
         entries(ctx)}

      {:error, persist_reason} ->
        {:persist_retry, retry, persist_reason}
    end
  end

  defp failed_node_result(ctx) do
    ResultBuilder.execution_result(
      ctx.current_run,
      %{
        asset_ref: ctx.work.asset_ref,
        node_key: ctx.node_key,
        task_id: nil,
        execution_pool: RunnerWork.execution_pool(ctx.work),
        freshness_key: decision_freshness_key(ctx.decisions, ctx.node_key)
      },
      ctx.stage,
      ctx.attempt,
      :error,
      []
    )
  end

  defp failed_node_event_data(ctx, reason, node_result) do
    %{
      asset_ref: ctx.work.asset_ref,
      error: reason,
      node_key: RunnerWork.node_key(ctx.work),
      asset_step_id: ctx.work.asset_step_id,
      window: RunnerWork.window(ctx.work),
      stage: ctx.stage,
      attempt: ctx.attempt,
      max_attempts: ctx.work.max_attempts,
      retryable?: false,
      retry_exhausted?: false,
      execution_pool: RunnerWork.execution_pool(ctx.work),
      node_result: node_result
    }
  end

  defp persist_stage_submit_failure(
         ctx,
         failed,
         asset_ref,
         reason,
         retryable?,
         cleanup_entries
       ) do
    result = {:error, failed, [], attempted_node_keys(ctx), cleanup_entries}

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
        {:error, Snapshots.cancelled_snapshot(failed), [], attempted_node_keys(ctx),
         cleanup_entries}

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

  @doc false
  @spec replayable_attempt_start_failure?(term()) :: boolean()
  def replayable_attempt_start_failure?(%PersistenceError{
        retryable?: true,
        kind: kind
      })
      when kind in [:conflict, :timeout, :unavailable, :internal],
      do: true

  def replayable_attempt_start_failure?(_reason), do: false

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

    Snapshots.snapshot_update(run_state,
      runner_task_id: nil,
      metadata: Map.put(run_state.metadata, :active_runner_task_ids, ids)
    )
  end

  defp stop_after_stage_build_failure(ctx, node_key, reason) do
    reason = PreSubmitFailure.normalize(reason)

    failure =
      (ctx.terminal_failure || %{status: :error, error: reason})
      |> Map.update(:node_statuses, %{node_key => :error}, fn statuses ->
        Map.put(statuses, node_key, :error)
      end)

    {:ok, ctx.current_run, entries(ctx), [], ctx.queued_steps, ctx.waiters, failure, nil}
  end

  defp fail_claim(ctx, reason) do
    case Map.get(ctx, :materialization_claim) do
      nil ->
        if Map.get(ctx, :prepared_claim),
          do: MaterializationClaims.release_prepared_claim(ctx.prepared_claim),
          else: :ok

      claim ->
        ActiveTaskSet.fail_entry_claim(%{materialization_claim: claim}, reason)
    end
  end

  defp unknown_enqueue_entry(ctx, task_id) do
    runner_pool =
      case Favn.RunnerPool.encode(ctx.work.runner_pool) do
        {:ok, pool} -> pool
        {:error, _} -> nil
      end

    enqueued_entry(ctx, %{
      task_id: task_id,
      assignment_generation: nil,
      runner_pool: runner_pool,
      required_runner_release_id: ctx.work.required_runner_release_id
    })
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
    {:error, failed, [], attempted_node_keys(ctx), entries(ctx)}
  end

  defp attempt_start_event(attempt) when attempt > 1, do: :step_retry_started
  defp attempt_start_event(_attempt), do: :step_started

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
