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
  alias Favn.Freshness.Key
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.ExecutionPackages
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunnerDispatch
  alias FavnOrchestrator.ResourceCircuits
  alias FavnOrchestrator.RuntimeInputPins
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.RunWorkSet
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
          | {:retry, RunState.t(), [node_key()], [node_key()]}
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
         attempt: attempt,
         runner_client: runner_client,
         runner_opts: runner_opts,
         manifest_lease_id: manifest_lease_id,
         queued_steps: %MapSet{} = queued_steps
       })
       when is_list(node_keys) and is_map(decisions) and is_map(freshness_context) do
    ctx = %{
      current_run: run_state,
      version: version,
      manifest_index: manifest_index,
      stage: stage,
      decisions: decisions,
      freshness_context: freshness_context,
      attempt: attempt,
      runner_client: runner_client,
      runner_opts: runner_opts,
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
        work =
          stage_work(
            ctx.current_run,
            ctx.version,
            ctx.manifest_lease_id,
            node_key,
            ctx.stage,
            ctx.attempt
          )

        entry_context =
          Map.merge(ctx, %{
            rest: rest,
            node_keys: node_keys,
            node_key: node_key,
            work: work,
            batch_count: ctx.batch_count + 1
          })

        admit_execution_capacity(entry_context)
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
  defp maybe_add_waiter(_ctx), do: []

  defp entries(%{entries_rev: entries_rev}), do: Enum.reverse(entries_rev)
  defp attempted_node_keys(ctx), do: Enum.map(entries(ctx), & &1.node_key)
  defp execution_ids(ctx), do: Enum.map(entries(ctx), & &1.execution_id)

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
           ),
         {:ok, prepared} <-
           RuntimeInputPins.prepare(
             ctx.current_run,
             work,
             ctx.runner_client,
             ctx.runner_opts
           ) do
      do_submit_admitted_entry(%{ctx | work: prepared})
    else
      {:error, reason} -> fail_unsubmitted_entry(ctx, ctx.work.asset_ref, reason)
    end
  end

  defp do_submit_admitted_entry(ctx) do
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
        deadline_at: StepAttemptLifecycle.deadline_at(ctx.work)
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
    result = RunnerDispatch.submit_work(ctx.runner_client, work, ctx.runner_opts)

    case result do
      {:ok, execution_id} ->
        with :ok <- RunExecutionOwnership.validate_runner_execution_id(ownership, execution_id) do
          submitted_ownership = RunExecutionOwnership.submitted(ownership, execution_id)

          case persist_submitted_ownership_snapshot(submitted_ownership) do
            :ok ->
              submit_started_entry(ctx, submitted_ownership, execution_id)

            {:error, :external_cancel} ->
              fail_submitted_entry(ctx, asset_ref, execution_id, :external_cancel)

            {:error, reason} ->
              fail_submitted_entry(ctx, asset_ref, execution_id, reason)
          end
        else
          {:error, reason} ->
            fail_submitted_entry(ctx, asset_ref, execution_id, reason)
        end

      {:error, reason} ->
        _ = RunExecutionOwnership.fail_dispatch(ownership, reason)

        fail_unsubmitted_entry(ctx, asset_ref, reason)
    end
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
      cancel_execution_ids(
        ctx.current_run,
        execution_ids(ctx),
        %{kind: :submit_failure, asset_ref: asset_ref, error: reason},
        ctx.runner_client,
        ctx.runner_opts
      )

    failed =
      RunState.transition(cancelled, status: :error, error: reason, runner_execution_id: nil)

    persist_stage_submit_failure(ctx, failed, asset_ref, reason, safe_retryable?(reason))
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

    persist_stage_submit_failure(ctx, failed, asset_ref, reason, false)
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

  defp attach_ownership_metadata(%RunnerWork{} = work, %RunExecutionOwnership{} = ownership) do
    metadata =
      work.metadata
      |> Map.put(:ownership_id, ownership.ownership_id)
      |> Map.put(:dispatch_id, ownership.dispatch_id)
      |> Map.put(:deadline_at, ownership.deadline_at)

    %{work | execution_id: ownership.dispatch_id, metadata: metadata}
  end

  defp submit_started_entry(ctx, ownership, execution_id) do
    asset_ref = ctx.work.asset_ref
    asset_step_id = ctx.work.asset_step_id
    started_ownership = RunExecutionOwnership.started(ownership)

    case RunExecutionOwnership.persist(started_ownership) do
      :ok ->
        submit_started_entry_after_ownership(
          ctx,
          RunExecutionOwnership.advance_local_version(started_ownership),
          execution_id,
          asset_ref,
          asset_step_id
        )

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
          Snapshots.snapshot_update(cancelled,
            status: :error,
            error: reason,
            runner_execution_id: nil
          )

        {:error, failed, [], attempted_node_keys(ctx) ++ [ctx.node_key]}
    end
  end

  defp submit_started_entry_after_ownership(
         ctx,
         ownership,
         execution_id,
         asset_ref,
         asset_step_id
       ) do
    updated_run =
      with_inflight_execution(
        ctx.current_run,
        execution_id,
        RunnerWork.lifecycle_metadata(ctx.work)
      )

    case Persistence.persist_run_step(updated_run, attempt_start_event(ctx.attempt), %{
           asset_ref: asset_ref,
           runner_execution_id: execution_id,
           asset_step_id: asset_step_id,
           window: RunnerWork.window(ctx.work),
           stage: ctx.stage,
           attempt: ctx.attempt,
           max_attempts: ctx.work.max_attempts,
           execution_pool: RunnerWork.execution_pool(ctx.work),
           freshness_key: decision_freshness_key(ctx.decisions, ctx.node_key),
           runtime_input_event: Map.get(ctx.work.metadata, :runtime_input_event),
           runtime_input_lineage: Map.get(ctx.work.metadata, :runtime_input_lineage)
         }) do
      :ok ->
        entry =
          StageEntry.new!(%{
            run_id: ctx.current_run.id,
            asset_step_id: asset_step_id,
            asset_ref: asset_ref,
            node_key: ctx.node_key,
            window: RunnerWork.window(ctx.work),
            execution_id: execution_id,
            runner_execution_id: execution_id,
            ownership: ownership,
            decision: Map.get(ctx.decisions, ctx.node_key, %{}),
            attempt: ctx.attempt,
            stage: ctx.stage,
            lease: ctx.lease,
            materialization_claim: ctx.materialization_claim,
            execution_pool: RunnerWork.execution_pool(ctx.work),
            resource_circuit_permits: ctx.resource_circuit_permits,
            freshness_key: decision_freshness_key(ctx.decisions, ctx.node_key),
            version: ctx.version,
            freshness_context: ctx.freshness_context
          })

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
          Snapshots.snapshot_update(ctx.current_run,
            status: :error,
            error: reason,
            runner_execution_id: nil,
            metadata: cancelled.metadata
          )

        {:error, failed, [], attempted_node_keys(ctx) ++ [ctx.node_key]}
    end
  end

  defp persist_submitted_ownership_snapshot(%RunExecutionOwnership{} = ownership) do
    if Persistence.externally_cancelled?(ownership) do
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
      deadline_at: StepAttemptLifecycle.deadline_at(ctx.work)
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
         manifest_lease_id,
         node_key,
         stage,
         attempt
       ) do
    {:ok, %{work: work}} =
      run_state
      |> StepAttemptLifecycle.new(version, node_key, stage, attempt)
      |> StepAttemptLifecycle.build_work()

    work
    |> StepAttemptLifecycle.attach_deadline(run_state)
    |> Map.put(:manifest_lease_id, manifest_lease_id)
  end

  defp safe_retryable?(%RunnerError{retryable?: true, outcome: :safe_failure}), do: true
  defp safe_retryable?(_reason), do: false

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

    _ = RunExecutionOwnership.persist_cancel_outcomes(run_state, cancel_results, reason)

    {Snapshots.clear_inflight_executions(
       run_state,
       Enum.map(cancel_results, & &1.execution_id)
     ), cancel_results}
  end

  defp decision_freshness_key(decisions, node_key) when is_map(decisions) do
    decisions
    |> Map.get(node_key, %{})
    |> Map.get(:freshness_key, Key.latest())
  end
end
