defmodule FavnOrchestrator.RunServer.Execution do
  @moduledoc """
  Executes manifest-pinned runs through durable runner tasks.

  Pipeline runs execute one topological stage at a time. Entries in the same
  stage are independent siblings, so a failed sibling must not cancel the rest
  of that stage. The current stage is drained and all submitted sibling outcomes
  are persisted before the run decides whether later stages may continue.

  Freshness classification happens between drained stages: already-fresh nodes
  are recorded as skipped, successful executed nodes dirty downstream nodes in
  the same graph, and downstream nodes with failed dependencies are blocked.

  """

  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunServer.Execution.RecoveredTask
  alias Favn.Manifest.Version
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.TargetIdentity
  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.ResourceCircuits
  alias FavnOrchestrator.RunExecutionCleanup
  alias FavnOrchestrator.RunOwnership
  alias FavnOrchestrator.RunnerIdentityVerifier
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.AdmissionIntent
  alias FavnOrchestrator.RunServer.Execution.FreshnessContext
  alias FavnOrchestrator.RunServer.Execution.PipelineRetryCheckpoint
  alias FavnOrchestrator.RunServer.Execution.PipelineFreshnessCheckpoint
  alias FavnOrchestrator.RunServer.Execution.Restore
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.Sequential
  alias FavnOrchestrator.RunServer.Execution.StageAdmission
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunServer.Execution.StageClassifier
  alias FavnOrchestrator.RunServer.Execution.StageResult
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.RetryCheckpoint
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs

  @stage_admission_timeout_buffer_ms 2_000
  @stage_admission_backstop_retry_ms 1_000
  @deferred_stage_retry_ms 100
  @await_task_timeout_buffer_ms 2_000

  @type step_event ::
          :continue
          | {:runner_result, String.t(), term()}
          | {:runner_task_result, String.t(), term()}
          | {:runner_task_started, String.t(), term()}
          | {:runner_await_down, String.t(), reference(), term()}
          | {:attempt_timeout, String.t(), reference()}
          | {:retry_attempt, reference()}
          | {:stage_admission_timeout, reference()}
          | {:execution_admission_wakeup, String.t(), non_neg_integer()}

  @type compact_index :: %Favn.Manifest.Index{
          planning_index: nil,
          assets_by_ref: map(),
          pipelines_by_ref: %{},
          schedules_by_ref: %{}
        }

  @spec start_state(RunState.t(), Version.t()) ::
          {:ok, RunExecutionState.t()} | {:terminal, RunState.t()} | {:recovery_required, term()}
  def start_state(%RunState{submit_kind: submit_kind} = run_state, %Version{} = _version)
      when submit_kind in [:backfill_asset, :backfill_pipeline] do
    {:terminal,
     Snapshots.snapshot_update(run_state,
       status: :error,
       error: {:unsupported_submit_kind, submit_kind},
       runner_task_id: nil,
       result: %{status: :error, asset_results: [], metadata: run_state.metadata}
     )}
  end

  def start_state(%RunState{} = run_state, %Version{} = version) do
    case RunState.execution_mode(run_state) do
      :pipeline ->
        with :ok <- RunnerIdentityVerifier.verify_run_manifest(run_state, version),
             {:ok, manifest_index} <- ManifestIndexCache.fetch(version),
             execution_index <- compact_execution_index(run_state, manifest_index),
             {:ok, {freshness_context, freshness_checkpoint}} <-
               load_freshness_context(run_state, execution_index) do
          state =
            RunExecutionState.new(run_state, Version.identity(version),
              mode: :pipeline,
              manifest_index: execution_index,
              manifest_lease_id: nil,
              stage_groups: pipeline_stage_groups(run_state),
              freshness_context: freshness_context,
              freshness_checkpoint: freshness_checkpoint
            )

          if run_state.event_seq <= 2 and ActiveTaskSet.active_runner_task_ids(run_state) == [],
            do: {:ok, state},
            else: {:ok, Restore.start(state)}
        else
          {:error, reason} when run_state.event_seq > 2 -> {:recovery_required, reason}
          {:error, reason} -> pipeline_start_failure(run_state, reason)
        end

      :sequential ->
        with :ok <- RunnerIdentityVerifier.verify_run_manifest(run_state, version),
             {:ok, manifest_index} <- ManifestIndexCache.fetch(version) do
          state =
            RunExecutionState.new(run_state, Version.identity(version),
              mode: :sequential,
              manifest_index: compact_execution_index(run_state, manifest_index),
              manifest_lease_id: nil,
              sequential_refs: Sequential.refs(run_state)
            )

          if run_state.event_seq <= 2 and ActiveTaskSet.active_runner_task_ids(run_state) == [],
            do: {:ok, state},
            else: {:ok, Restore.start(state)}
        else
          {:error, reason} when run_state.event_seq > 2 -> {:recovery_required, reason}
          {:error, reason} -> pipeline_start_failure(run_state, reason)
        end
    end
  end

  @spec handle_event(RunExecutionState.t(), step_event()) ::
          {:cont, RunExecutionState.t()}
          | {:terminal, RunState.t()}
          | {:persist_retry, RunExecutionState.t(), PersistenceRetry.t(), term()}
  def handle_event(%RunExecutionState{} = state, event) do
    state
    |> dispatch_event(event)
  end

  @doc "Executes one bounded operation without coordinator timers or awaits."
  @spec perform_operation(tuple()) :: term()
  def perform_operation({:cancel_reconcile, state, _reason}) do
    context = SystemContext.workspace(state.run.workspace_id, :run_worker)

    with {:ok, latest} <- Runs.get(context, state.run.id) do
      latest =
        RunState.with_storage_fence(
          latest,
          state.run.storage_owner_id,
          state.run.storage_fencing_token
        )

      reconcile_cancelled_admission(%{state | run: latest})
    end
  end

  def perform_operation({:cancel_drain, state, reason}) do
    state = state |> cleanup_paused_admission(reason) |> clear_admission_waiters()

    Cancellation.dispatch_runner_tasks(state.run, ActiveTaskSet.task_ids(state.work_set), reason,
      wait_for_ack: false
    )

    state
  end

  def perform_operation({:sequential, action, state, args}),
    do: apply(Sequential, action, [state | args])

  def perform_operation({:resolve_transition, retry}), do: PersistenceRetry.resolve(retry)

  def perform_operation({:cancellation_check, run, _resume}),
    do: Persistence.externally_cancelled?(run)

  def perform_operation({:checkpoint, args, _resume}),
    do: apply(PipelineFreshnessCheckpoint, :put, args)

  def perform_operation({:classify, args}), do: apply(StageClassifier, :classify, args)

  def perform_operation({:stage_admission_continuation, action, pause, value}),
    do: apply(StageAdmission, action, [pause, value])

  def perform_operation({:stage_admit, input, waiters, _resume}) do
    Enum.each(waiters, &ExecutionAdmission.cancel_wait/1)
    StageAdmission.submit(input)
  end

  def perform_operation({:restore, input}), do: Restore.next(input)

  def perform_operation({:recover_task, run, id, entry, _kind}) do
    with {:ok, task} <- RunnerTasks.fetch(run.workspace_id, id),
         true <- RecoveredTask.verify_evidence(task, entry.recovery_evidence),
         {:ok, next} <-
           RecoveredTask.reconcile(
             run,
             task,
             Map.drop(entry, [:recovery_pending?, :recovery_evidence])
           ),
         do: {:ok, next}
  end

  def perform_operation({:read_terminal, run, id, evidence}) do
    with {:ok, task} <- RunnerTasks.fetch(run.workspace_id, id),
         :ok <- RecoveredTask.validate_terminal_read(task, evidence),
         do: durable_task_result(task)
  end

  def perform_operation({:settle_stage, stage, entry, result}) do
    StageResult.process(stage, entry, result, %{stage: entry.stage, attempt: entry.attempt})
  after
    ActiveTaskSet.release_entry(entry)
  end

  def perform_operation({:resume_stage, stage, resume}),
    do: StageResult.resume_persisted(stage, resume)

  def perform_operation({:persist, retry}),
    do: PersistenceRetry.persist(retry)

  @doc "Applies a durable operation receipt."
  @spec finish_operation(RunExecutionState.t(), tuple(), term()) :: term()
  def finish_operation(state, operation, result),
    do: apply_operation(state, operation, result)

  defp apply_operation(_state, {:cancel_reconcile, _, reason}, {:ok, next}),
    do: cancel_reconciled(next, reason)

  defp apply_operation(state, {:cancel_reconcile, _, _}, {:error, reason}),
    do: {:recovery_required, state, {:cancellation_admission_reconciliation_failed, reason}}

  defp apply_operation(_state, {:cancel_drain, _, _}, next) do
    next = %{next | cancellation_dispatched?: true}

    if map_size(next.awaits) > 0,
      do: {:cont, %{next | status: :awaiting}},
      else: {:terminal, Snapshots.cancelled_terminal(next.run, accumulated_results(next))}
  end

  defp apply_operation(state, {:sequential, _, _, _}, result) do
    result =
      case result do
        {kind, next} when kind in [:cont, :retry_timer] and is_struct(next, RunExecutionState) ->
          {kind, %{next | cancel_requested: state.cancel_requested}}

        {kind, next, value} when kind in [:await, :retry_timer] ->
          {kind, %{next | cancel_requested: state.cancel_requested}, value}

        other ->
          other
      end

    handle_sequential_directive(result)
  end

  defp apply_operation(state, {:resolve_transition, retry}, {:committed, _run}),
    do: resume_persisted(state, retry.resume)

  defp apply_operation(_state, {:resolve_transition, _}, {kind, run})
       when kind in [:terminal, :failed], do: {:durable_terminal, run}

  defp apply_operation(state, {:resolve_transition, _}, {:error, :cancellation_race}),
    do: cancel(state, :cancellation_during_transition)

  defp apply_operation(state, {:resolve_transition, _}, {:error, reason}),
    do: {:unconfirmed_transition, state, reason}

  defp apply_operation(state, {:cancellation_check, _run, :pipeline_progress}, cancelled?),
    do: pipeline_progress(state, cancelled?)

  defp apply_operation(
         state,
         {:cancellation_check, _run, {:restored, progress, entries}},
         cancelled?
       ),
       do: resume_restored_checked(state, progress, entries, cancelled?)

  defp apply_operation(state, {:cancellation_check, _run, :continue_pipeline}, cancelled?),
    do: continue_pipeline_checked(state, cancelled?)

  defp apply_operation(state, {:classify, [_run, _version, stage | _]}, result),
    do:
      handle_stage_classification(
        state,
        result,
        stage,
        state.pipeline_continuation.runnable_node_keys_rev
      )

  defp apply_operation(state, {:checkpoint, _, resume}, {:ok, reference, run}),
    do: resume_checkpoint(%{state | run: run, freshness_checkpoint: reference}, resume)

  defp apply_operation(state, {:checkpoint, _, _}, {:error, reason}),
    do: terminalize_checkpoint_failure(state, reason)

  defp apply_operation(state, {:stage_admission_continuation, _, pause, _}, result),
    do:
      handle_resumed_stage_admission(
        state,
        pause.ctx.attempt,
        result,
        pause.ctx.completed_node_statuses
      )

  defp apply_operation(state, {:stage_admit, _, _, :refill}, result),
    do: finish_refill(state, result)

  defp apply_operation(state, {:stage_admit, _, _, {:submit, attempt, statuses}}, result),
    do: finish_submission(state, attempt, statuses, result)

  defp apply_operation(state, {:restore, _}, {:more, next}),
    do: defer_pipeline_continue(struct!(state, next))

  defp apply_operation(state, {:restore, _}, {:ready, next, progress, tasks}),
    do: resume_restored(struct!(state, next), progress, tasks)

  defp apply_operation(state, {:restore, _}, error),
    do: {:recovery_required, state, {:execution_restore_failed, error}}

  defp apply_operation(state, {:recover_task, _, _, _, kind}, {:ok, entry}),
    do: {:cont, state |> RunExecutionState.add_work(entry) |> start_await(entry, kind)}

  defp apply_operation(state, {:recover_task, _, id, _, _}, error),
    do: {:recovery_required, state, {:recovered_task_reconciliation_failed, id, error}}

  defp apply_operation(state, {:read_terminal, _, id, _}, {:ok, %RunnerResult{}} = result),
    do: dispatch_event(state, {:runner_result, id, result})

  defp apply_operation(state, {:read_terminal, _, id, _}, error),
    do: {:recovery_required, state, {:recovered_terminal_read_failed, id, error}}

  defp apply_operation(state, {:settle_stage, _, _, _}, result),
    do: result |> prepare_pipeline_settlement(state) |> continue_pipeline_settlement()

  defp apply_operation(state, {:resume_stage, _, _}, result),
    do: result |> prepare_pipeline_settlement(state) |> continue_pipeline_settlement()

  defp apply_operation(state, {:persist, retry}, result),
    do: apply_persistence(state, retry, result)

  defp dispatch_event(%RunExecutionState{} = state, :continue), do: continue_state(state)

  defp dispatch_event(state, :recover_next) do
    case :queue.out(state.recovery_queue) do
      {:empty, _} ->
        {:cont, state}

      {{:value, {id, kind}}, queue} ->
        unless :queue.is_empty(queue), do: send(self(), :recover_next)
        dispatch_event(%{state | recovery_queue: queue}, {:runner_task_result, id, kind})
    end
  end

  defp dispatch_event(%RunExecutionState{} = state, {:runner_task_result, task_id, :recover}) do
    case state.awaits[task_id] do
      %{entry: %{recovery_pending?: true} = entry, kind: kind} ->
        {:operation, state, {:recover_task, state.run, task_id, entry, kind}}

      _ ->
        {:cont, state}
    end
  end

  defp dispatch_event(
         %RunExecutionState{} = state,
         {:runner_task_result, task_id, :read_terminal}
       ) do
    case state.awaits[task_id] do
      %{entry: %{terminal_evidence: evidence}} ->
        {:operation, state, {:read_terminal, state.run, task_id, evidence}}

      _ ->
        {:cont, state}
    end
  end

  defp dispatch_event(%RunExecutionState{} = state, {:runner_task_result, task_id, task}) do
    dispatch_event(state, {:runner_result, task_id, durable_task_result(task)})
  end

  # A runner reported the awaited task as started. The `:step_running` event is
  # an advisory presence signal for read models, but its durable sequence must
  # be resolved before persisting the attempt's terminal event.
  defp dispatch_event(%RunExecutionState{} = state, {:runner_task_started, task_id, _task}) do
    case Map.get(state.awaits, task_id) do
      %{started_persisted?: true} ->
        {:cont, state}

      %{entry: entry} = await ->
        persist_step_running(state, task_id, await, entry)

      nil ->
        {:cont, state}
    end
  end

  defp dispatch_event(%RunExecutionState{} = state, {:runner_result, task_id, result}) do
    case RunExecutionState.pop_await(state, task_id) do
      {nil, state} ->
        {:cont, state}

      {await, state} ->
        if is_reference(await.timeout_ref), do: Process.cancel_timer(await.timeout_ref)
        if is_reference(await.monitor_ref), do: Process.demonitor(await.monitor_ref, [:flush])
        handle_await_result(state, await.entry, result, await.kind)
    end
  end

  defp dispatch_event(
         %RunExecutionState{} = state,
         {:runner_await_down, task_id, monitor_ref, reason}
       ) do
    case Map.get(state.awaits, task_id) do
      %{monitor_ref: ^monitor_ref} ->
        {await, state} = RunExecutionState.pop_await(state, task_id)
        if is_reference(await.timeout_ref), do: Process.cancel_timer(await.timeout_ref)

        handle_await_failure_without_terminal_evidence(
          state,
          await,
          {:error, %{type: :await_task_failed, kind: :exit, reason: inspect(reason)}},
          %{kind: :await_worker_down, reason: inspect(reason)}
        )

      _stale_or_missing ->
        {:cont, state}
    end
  end

  defp dispatch_event(%RunExecutionState{} = state, {:attempt_timeout, task_id, timer_ref}) do
    case Map.get(state.awaits, task_id) do
      %{timeout_token: ^timer_ref} ->
        {await, state} = RunExecutionState.pop_await(state, task_id)
        if is_pid(await.pid), do: Process.exit(await.pid, :kill)
        if is_reference(await.monitor_ref), do: Process.demonitor(await.monitor_ref, [:flush])

        handle_await_failure_without_terminal_evidence(
          state,
          await,
          {:error, :timeout},
          %{kind: :await_timeout}
        )

      _stale_or_missing ->
        {:cont, state}
    end
  end

  defp dispatch_event(%RunExecutionState{} = state, {:retry_attempt, timer_ref}) do
    case RunExecutionState.pop_retry_timer(state, timer_ref) do
      {nil, state} -> {:cont, state}
      {%{payload: retry}, state} -> resume_retry(state, retry)
    end
  end

  defp dispatch_event(%RunExecutionState{} = state, {:stage_admission_timeout, timer_ref}) do
    case RunExecutionState.pop_admission_timer(state, timer_ref) do
      {nil, state} ->
        {:cont, state}

      {%{
         payload: %{
           kind: :deferred_refill,
           stage_index: stage_index,
           refill_cause: refill_cause
         }
       },
       %RunExecutionState{
         stage_index: stage_index,
         stage_state: %StageAttemptState{
           deferred_node_keys: [_ | _],
           deferred_refill_cause: refill_cause
         }
       } = state} ->
        after_pipeline_progress(state)

      {%{payload: %{kind: :deferred_refill}}, state} ->
        {:cont, state}

      {%{payload: _timer}, state}
      when map_size(state.awaits) > 0 ->
        {:cont, %{state | status: :awaiting}}

      {%{payload: %{kind: :admission_retry}}, state} ->
        refill_or_schedule_admission(state)

      {%{payload: _timer}, state} ->
        timeout_admission_wait(state)
    end
  end

  defp dispatch_event(
         %RunExecutionState{} = state,
         {:execution_admission_wakeup, waiter_id, generation}
       ) do
    case Map.get(state.admission_waiters, waiter_id) do
      %{wake_generation: ^generation} = waiter ->
        {^waiter, state} = RunExecutionState.pop_admission_waiter(state, waiter_id)
        :ok = ExecutionAdmission.cancel_wait(waiter)

        state
        |> RunExecutionState.cancel_admission_timers()
        |> after_pipeline_progress()

      _stale_or_missing ->
        {:cont, state}
    end
  end

  @doc false
  @spec retry_persistence(RunExecutionState.t(), PersistenceRetry.t()) ::
          {:cont, RunExecutionState.t()}
          | {:terminal, RunState.t()}
          | {:ownership_gate, RunExecutionState.t(), PersistenceRetry.t()}
          | {:recovery_required, RunExecutionState.t(), term()}
          | {:persist_retry, RunExecutionState.t(), PersistenceRetry.t(), term()}
  def retry_persistence(%RunExecutionState{} = state, %PersistenceRetry{} = retry) do
    dispatch_expired? =
      case retry.resume do
        {:stage_operation, pause} -> StageAdmission.dispatch_expired?(pause)
        _ -> false
      end

    if PersistenceRetry.exhausted?(retry) or (dispatch_expired? and not retry.ambiguous?),
      do: exhaust_persistence(state, retry),
      else: replay_persistence(state, retry)
  end

  defp replay_persistence(state, retry), do: {:operation, state, {:persist, retry}}

  defp apply_persistence(state, retry, result) do
    case result do
      :ok
      when elem(retry.resume, 0) in [
             :stage_operation,
             :sequential_operation
           ] or not is_nil(retry.command) ->
        {state, retry} = adopt_operation_result(state, retry, :ok)
        {:ownership_gate, state, retry}

      {:ok, result} ->
        {state, retry} = adopt_operation_result(state, retry, result)
        {:ownership_gate, state, %{retry | result: result}}

      :ok ->
        resume_persisted(state, retry.resume)

      {:error, :external_cancel} ->
        state = cleanup_paused_admission(state, :external_cancel)
        {:terminal, Snapshots.cancelled_terminal(state.run, [])}

      {:error, reason} ->
        handle_persistence_retry_failure(state, retry, reason)
    end
  end

  defp adopt_operation_result(state, %{resume: {:stage_operation, pause}} = retry, result) do
    pause = StageAdmission.adopt_operation(pause, result)
    state = %{state | paused_admission: pause, run: pause.ctx.current_run}

    state =
      if state.stage_state,
        do: %{state | stage_state: %{state.stage_state | run: state.run}},
        else: state

    state = Enum.reduce(pause.entries, state, &RunExecutionState.add_work(&2, &1))

    state =
      RunExecutionState.put_admission_waiters(
        state,
        pause.ctx.waiters ++ List.wrap(Map.get(pause.ctx, :waiter))
      )

    {state, %{retry | resume: {:stage_operation, pause}}}
  end

  defp adopt_operation_result(state, %{resume: {:sequential_operation, pause}} = retry, result),
    do: {Sequential.adopt_operation(state, pause, result), retry}

  defp adopt_operation_result(state, retry, _result), do: {state, retry}

  defp exhaust_persistence(
         state,
         %PersistenceRetry{resume: {:stage_operation, pause}, ambiguous?: false} = retry
       )
       when pause.phase in [
              :admission_intent,
              :runner_admission
            ] do
    {:operation, %{state | paused_admission: nil},
     {:stage_admission_continuation, :fail_operation, pause, PersistenceRetry.exhaustion(retry)}}
  end

  defp exhaust_persistence(state, %PersistenceRetry{event_type: :step_running} = retry),
    do: {:operation, state, {:resolve_transition, retry}}

  defp exhaust_persistence(state, retry),
    do: {:recovery_required, state, PersistenceRetry.exhaustion(retry)}

  @doc false
  @spec resume_persisted_retry(RunExecutionState.t(), PersistenceRetry.t()) ::
          {:cont, RunExecutionState.t()}
          | {:terminal, RunState.t()}
          | {:persist_retry, RunExecutionState.t(), PersistenceRetry.t(), term()}
  def resume_persisted_retry(%RunExecutionState{} = state, %PersistenceRetry{} = retry) do
    case retry.resume do
      {:sequential_operation, pause} ->
        sequential_operation(state, :resume_operation, [pause, retry.result || :ok])

      {:stage_operation, pause} ->
        {:operation, %{state | paused_admission: nil},
         {:stage_admission_continuation, :resume_operation, pause, retry.result || :ok}}

      _ ->
        resume_persisted(state, retry.resume)
    end
  end

  defp handle_persistence_retry_failure(
         state,
         %PersistenceRetry{resume: {:stage_operation, %{phase: :runner_admission} = pause}},
         %{details: %{reason_code: "target_write_in_progress"}} = reason
       ) do
    {:operation, %{state | paused_admission: nil},
     {:stage_admission_continuation, :reject_operation, pause, reason}}
  end

  defp handle_persistence_retry_failure(
         state,
         %PersistenceRetry{resume: {:sequential_operation, %{phase: :admission} = pause}},
         %{details: %{reason_code: "target_write_in_progress"}} = reason
       ) do
    sequential_operation(state, :reject_operation, [pause, reason])
  end

  defp handle_persistence_retry_failure(
         %RunExecutionState{} = state,
         %PersistenceRetry{resume: {:stage_operation, %{phase: :admission_intent}}} = retry,
         reason
       ) do
    cond do
      reason in [:fenced, :cancellation_race] ->
        {:persist_retry, state, retry, reason}

      StageAdmission.replayable_attempt_start_failure?(reason) ->
        {:persist_retry, state, retry, reason}

      true ->
        {:recovery_required, state, {:attempt_start_replay_rejected, reason}}
    end
  end

  defp handle_persistence_retry_failure(
         state,
         %PersistenceRetry{event_type: :step_running},
         :cancellation_race
       ),
       do: cancel(state, :cancellation_during_transition)

  defp handle_persistence_retry_failure(
         state,
         %PersistenceRetry{event_type: :step_running} = retry,
         reason
       ) do
    if reason in [:fenced, :cancellation_race] or PersistenceRetry.transition_retryable?(reason),
      do: {:persist_retry, state, retry, reason},
      else: {:operation, state, {:resolve_transition, PersistenceRetry.rejected(retry, reason)}}
  end

  defp handle_persistence_retry_failure(state, retry, reason) do
    if reason in [:fenced, :cancellation_race] or PersistenceRetry.replayable?(reason),
      do: {:persist_retry, state, retry, reason},
      else: {:recovery_required, state, {:persistence_replay_rejected, retry.event_type, reason}}
  end

  @doc "Stops future work and drains existing awaits to their durable cancellation outcomes."
  @spec cancel(RunExecutionState.t(), term()) ::
          {:cont, RunExecutionState.t()} | {:terminal, RunState.t()}
  def cancel(%RunExecutionState{recovery: recovery} = state, _reason) when is_map(recovery),
    do: state |> Restore.start() |> defer_pipeline_continue()

  def cancel(%RunExecutionState{cancellation_dispatched?: true} = state, _reason) do
    if RunExecutionState.in_flight_count(state) > 0,
      do: {:cont, %{state | status: :awaiting}},
      else: {:terminal, Snapshots.cancelled_terminal(state.run, accumulated_results(state))}
  end

  def cancel(%RunExecutionState{} = state, reason),
    do: {:operation, state, {:cancel_reconcile, state, reason}}

  defp cancel_reconciled(state, reason) do
    reason = %{kind: :external_cancel, reason: reason}

    state = track_paused_entries_for_cancellation(state)

    Enum.each(state.retry_timers, fn {_ref, timer} -> Process.cancel_timer(timer.timer_ref) end)

    state = %{state | retry_timers: %{}, pipeline_continuation: nil}
    {:operation, state, {:cancel_drain, state, reason}}
  end

  @doc "Stops local waiters while retaining durable tasks, claims and leases for recovery."
  @spec stop_for_recovery(RunExecutionState.t()) :: RunExecutionState.t()
  def stop_for_recovery(state),
    do:
      state
      |> cleanup_paused_admission(:run_server_stopped)
      |> stop_await_processes()
      |> clear_admission_waiters()
      |> RunExecutionState.cancel_timers()

  defp accumulated_results(%RunExecutionState{mode: :sequential} = state),
    do: ResultBuilder.sort_asset_results(state.run, state.accumulated_results)

  defp accumulated_results(%RunExecutionState{stage_state: %StageAttemptState{} = stage}),
    do: StageAttemptState.settled_results(stage)

  defp accumulated_results(%RunExecutionState{} = state), do: state.accumulated_results

  defp pipeline_start_failure(%RunState{} = run_state, reason) do
    {:terminal,
     Snapshots.snapshot_update(run_state,
       status: :error,
       error: reason,
       runner_task_id: nil,
       result: ResultBuilder.pipeline_result(run_state, :error, [])
     )}
  end

  @doc false
  @spec release_manifest_lease(RunState.t()) :: :ok
  def release_manifest_lease(%RunState{}), do: :ok

  @doc false
  @spec manifest_lease_expires_at(RunState.t()) :: DateTime.t()
  def manifest_lease_expires_at(%RunState{}) do
    lease_ms = max(RunOwnership.default_lease_duration_ms() * 2, 60_000)
    DateTime.add(DateTime.utc_now(), div(lease_ms + 999, 1_000), :second)
  end

  @doc false
  @spec compact_execution_index(RunState.t(), Favn.Manifest.Index.t()) ::
          compact_index()
  def compact_execution_index(%RunState{} = run, manifest_index) do
    refs_by_target_id = refs_by_target_id(manifest_index.assets_by_ref)

    refs =
      case run.plan do
        %Favn.Plan{nodes: nodes} ->
          Enum.reduce(nodes, MapSet.new(), fn {_node_key, node}, refs ->
            refs
            |> MapSet.put(node.ref)
            |> MapSet.union(input_generation_refs(node, refs_by_target_id))
          end)

        nil ->
          MapSet.new([run.asset_ref])
      end

    %Favn.Manifest.Index{
      planning_index: nil,
      assets_by_ref: Map.take(manifest_index.assets_by_ref, MapSet.to_list(refs)),
      pipelines_by_ref: %{},
      schedules_by_ref: %{}
    }
  end

  defp refs_by_target_id(assets_by_ref) do
    assets_by_ref
    |> Enum.reduce(%{}, fn {ref, asset}, refs ->
      persisted_target_id = asset.target_descriptor && asset.target_descriptor.target_id

      refs
      |> put_target_ref(TargetIdentity.for_asset(ref), ref)
      |> maybe_put_target_ref(persisted_target_id, ref)
    end)
  end

  defp input_generation_refs(node, refs_by_target_id) do
    node
    |> Map.get(:input_generations, [])
    |> Enum.reduce(MapSet.new(), fn generation, refs ->
      target_id = Map.get(generation, :target_id, Map.get(generation, "target_id"))

      case Map.get(refs_by_target_id, target_id) do
        %MapSet{} = input_refs -> MapSet.union(refs, input_refs)
        nil -> refs
      end
    end)
  end

  defp maybe_put_target_ref(refs, target_id, ref) when is_binary(target_id),
    do: put_target_ref(refs, target_id, ref)

  defp maybe_put_target_ref(refs, _target_id, _ref), do: refs

  defp put_target_ref(refs, target_id, ref),
    do: Map.update(refs, target_id, MapSet.new([ref]), &MapSet.put(&1, ref))

  defp continue_state(%RunExecutionState{recovery: recovery} = state) when is_map(recovery) do
    input =
      Map.take(state, [
        :run,
        :mode,
        :recovery,
        :freshness_checkpoint,
        :accumulated_results
      ])

    {:operation, state, {:restore, input}}
  end

  defp continue_state(%RunExecutionState{mode: :sequential, status: :awaiting} = state),
    do: {:cont, state}

  defp continue_state(%RunExecutionState{status: :retry_wait} = state), do: {:cont, state}

  defp continue_state(
         %RunExecutionState{pipeline_continuation: %{kind: :stage_classification}} = state
       ),
       do: continue_stage_classification(state)

  defp continue_state(
         %RunExecutionState{mode: :pipeline, stage_state: %StageAttemptState{}} = state
       ),
       do: after_pipeline_progress(state)

  defp continue_state(%RunExecutionState{mode: :sequential} = state),
    do: sequential_operation(state, :continue, [])

  defp continue_state(%RunExecutionState{mode: :pipeline} = state), do: continue_pipeline(state)

  defp sequential_operation(state, action, args),
    do: {:operation, state, {:sequential, action, state, args}}

  defp handle_sequential_directive({:retry_timer, state, retry}) do
    token = make_ref()
    timer = Process.send_after(self(), {:retry_attempt, token}, retry.retry_after_ms)
    {:cont, RunExecutionState.put_retry_timer(state, token, timer, retry)}
  end

  defp handle_sequential_directive({:await, %RunExecutionState{} = state, entry}),
    do: {:cont, start_await(state, entry, :sequential)}

  defp handle_sequential_directive({:cont, %RunExecutionState{}} = result), do: result
  defp handle_sequential_directive({:terminal, %RunState{}} = result), do: result

  defp handle_sequential_directive({:recovery_required, %RunExecutionState{}, _} = result),
    do: result

  defp handle_sequential_directive(
         {:persist_retry, %RunExecutionState{}, %PersistenceRetry{}, _reason} = result
       ),
       do: result

  defp resume_persisted(state, {:step_running, running, task_id}) do
    await = Map.put(state.awaits[task_id], :started_persisted?, true)
    {:cont, RunExecutionState.put_await(%{state | run: running}, task_id, await)}
  end

  defp resume_persisted(%RunExecutionState{} = state, {:sequential, resume}) do
    sequential_operation(state, :resume_persisted, [resume])
  end

  defp resume_persisted(%RunExecutionState{stage_state: %StageAttemptState{}} = state, {
         :pipeline,
         resume
       }) do
    {:operation, state, {:resume_stage, state.stage_state, resume}}
  end

  defp resume_persisted(state, {:stage_classification, ctx}) do
    # The frozen event is now durable; resume only the remaining classification batch.
    run = ctx.persisted_run
    result = StageClassifier.resume_persisted(ctx, run)

    handle_stage_classification(
      state,
      result,
      ctx.stage,
      state.pipeline_continuation.runnable_node_keys_rev
    )
  end

  defp resume_persisted(%RunExecutionState{} = state, {:pipeline_retry_checkpoint, resume}) do
    schedule_pipeline_retry_timer(
      %{state | run: resume.run},
      resume.node_keys,
      resume.stage,
      resume.attempt,
      resume.next_retry_at
    )
  end

  defp resume_persisted(
         %RunExecutionState{stage_state: nil} = state,
         {:stage_admission, attempt, {:node_failed, _, _, _, _, _, _, _, _} = result}
       ) do
    handle_initial_stage_node_failure(state, attempt, result)
  end

  defp resume_persisted(
         %RunExecutionState{stage_state: %StageAttemptState{}} = state,
         {:stage_admission, _attempt, {:node_failed, _, _, _, _, _, _, _, _} = result}
       ) do
    handle_refill_stage_node_failure(state, result)
  end

  defp resume_persisted(
         %RunExecutionState{} = state,
         {:stage_admission, _attempt,
          {:error, failed_run, step_results, _attempted_node_keys, cleanup_entries}}
       ) do
    terminalize_stage_admission_failure(state, failed_run, step_results, cleanup_entries)
  end

  defp queue_recovery(state, id, kind) do
    if :queue.is_empty(state.recovery_queue), do: send(self(), :recover_next)
    %{state | recovery_queue: :queue.in({id, kind}, state.recovery_queue)}
  end

  defp start_await(%RunExecutionState{} = state, %{recovery_pending?: true} = entry, kind) do
    state = queue_recovery(state, entry.task_id, :recover)

    RunExecutionState.put_await(state, entry.task_id, %{
      pid: nil,
      monitor_ref: nil,
      timeout_token: nil,
      timeout_ref: nil,
      entry: entry,
      kind: kind
    })
  end

  defp start_await(%RunExecutionState{} = state, %{terminal_task?: true} = entry, kind) do
    state = queue_recovery(state, entry.task_id, :read_terminal)

    RunExecutionState.put_await(state, entry.task_id, %{
      pid: nil,
      monitor_ref: nil,
      timeout_token: nil,
      timeout_ref: nil,
      entry: Map.delete(entry, :terminal_task?),
      kind: kind
    })
  end

  defp start_await(%RunExecutionState{} = state, entry, kind) do
    parent = self()
    task_id = entry.task_id

    timeout_ms =
      case Map.get(entry, :deadline_at) do
        %DateTime{} = deadline ->
          max(DateTime.diff(deadline, DateTime.utc_now(), :millisecond), 0)

        nil ->
          state.run.timeout_ms
      end

    {:ok, pid} =
      FavnOrchestrator.RunHelper.start_waiter(fn ->
        FavnOrchestrator.RunnerTaskResultRouter.await(
          state.run.workspace_id,
          entry.task_id,
          parent,
          notify_started?: true
        )
      end)

    monitor_ref = Process.monitor(pid)
    timeout_token = make_ref()

    timeout_ref =
      Process.send_after(
        parent,
        {:attempt_timeout, task_id, timeout_token},
        timeout_ms + @await_task_timeout_buffer_ms
      )

    RunExecutionState.put_await(state, task_id, %{
      pid: pid,
      monitor_ref: monitor_ref,
      timeout_token: timeout_token,
      timeout_ref: timeout_ref,
      entry: entry,
      kind: kind
    })
  end

  # A restart rebuilds awaits without the started_persisted? marker, so a
  # re-delivered started signal can persist a second `:step_running` event.
  # The event is advisory, so the duplicate is acceptable.
  defp persist_step_running(%RunExecutionState{} = state, task_id, _await, entry) do
    data = %{
      asset_ref: Map.get(entry, :asset_ref),
      runner_task_id: task_id,
      node_key: Map.get(entry, :node_key),
      asset_step_id: Map.get(entry, :asset_step_id),
      window: Map.get(entry, :window),
      stage: Map.get(entry, :stage),
      attempt: Map.get(entry, :attempt),
      execution_pool: Map.get(entry, :execution_pool)
    }

    running = RunState.transition(state.run, status: :running)

    retry = PersistenceRetry.new(running, :step_running, data, {:step_running, running, task_id})
    {:operation, state, {:persist, retry}}
  end

  defp resume_restored(state, progress, entries),
    do: {:operation, state, {:cancellation_check, state.run, {:restored, progress, entries}}}

  defp resume_restored_checked(state, progress, entries, cancelled?) do
    if cancelled? or not is_nil(state.cancel_requested) do
      state =
        if state.mode == :pipeline do
          stage =
            StageAttemptState.new(
              state.run,
              state.accumulated_results,
              entries,
              [],
              MapSet.new()
            )

          %{state | stage_state: stage, stage_freshness_context: state.freshness_context}
        else
          state
        end

      state =
        Enum.reduce(entries, state, fn entry, acc ->
          acc |> RunExecutionState.add_work(entry) |> start_await(entry, state.mode)
        end)

      cancel_reconciled(state, :cancellation_during_recovery)
    else
      resume_restored_mode(state, progress, entries)
    end
  end

  defp resume_restored_mode(%RunExecutionState{mode: :pipeline} = state, progress, tasks) do
    index = restored_stage_index(state, progress.position)
    state = %{state | stage_index: index, terminal_failure: progress.failure}

    if index >= length(state.stage_groups) do
      if tasks == [],
        do: terminalize_pipeline_state(state),
        else: {:recovery_required, state, :tasks_after_final_recovery_position}
    else
      {stage, keys} = Enum.at(state.stage_groups, index)
      steps = progress.steps |> Map.values() |> Enum.filter(&(&1.stage == stage))

      attempt =
        if state.freshness_checkpoint && state.freshness_checkpoint.stage == stage,
          do: state.freshness_checkpoint.attempt,
          else: 1

      settled =
        Enum.filter(
          steps,
          &(&1.phase == :settled and
              not (&1.retry_allowed? and &1.attempt < attempt))
        )

      statuses = Map.new(settled, &{&1.node_key, &1.status})

      failed_in_stage =
        Enum.find(
          settled,
          &(not &1.retry_allowed? and
              Map.get(&1, :settlement_status, &1.status) in [
                :error,
                :timed_out,
                :cancelled,
                :blocked
              ])
        )

      stage_failure =
        if failed_in_stage,
          do: %{
            status: :error,
            error: state.run.error || (progress.failure && progress.failure.error)
          }

      remaining = Enum.reject(keys, &Map.has_key?(statuses, &1))

      if is_nil(state.freshness_checkpoint) or state.freshness_checkpoint.stage != stage do
        if tasks == [] do
          context =
            Enum.reduce(statuses, state.freshness_context, fn {key, status}, ctx ->
              %{
                ctx
                | completed_node_keys: MapSet.put(ctx.completed_node_keys, key),
                  upstream_statuses: Map.put(ctx.upstream_statuses, key, status)
              }
            end)

          state = %{
            state
            | stage_decisions: %{},
              stage_freshness_context: context,
              pipeline_continuation: %{
                kind: :stage_classification,
                stage: stage,
                remaining_node_keys: remaining,
                runnable_node_keys_rev: []
              },
              status: :classifying
          }

          continue_stage_classification(state)
        else
          {:recovery_required, state, :runner_task_freshness_checkpoint_missing}
        end
      else
        entries = tasks

        active = MapSet.new(entries, & &1.node_key)

        decisions =
          StageClassifier.decisions(state.run, keys, state.freshness_context,
            forced_node_keys:
              MapSet.union(
                MapSet.new(
                  Map.get_lazy(state.freshness_context, :forced_node_keys, fn ->
                    RefreshPolicy.expand_force_set(
                      state.freshness_context.refresh_policy,
                      state.run.plan
                    )
                  end)
                ),
                MapSet.new(
                  Enum.map(
                    Enum.filter(steps, fn step ->
                      (Map.get(step, :retry_allowed?, false) and step.attempt < attempt) or
                        (step.attempt > 1 and step.phase in [:intended, :submitted])
                    end),
                    & &1.node_key
                  )
                )
              )
          )
          |> Map.merge(Map.new(entries, &{&1.node_key, &1.decision}))

        deferred = Enum.reject(remaining, &MapSet.member?(active, &1))
        pending = MapSet.new(Enum.filter(steps, &(&1.phase == :intended)), & &1.node_key)
        {intended, unstarted} = Enum.split_with(deferred, &MapSet.member?(pending, &1))
        deferred = intended ++ unstarted

        if Enum.all?(deferred, &match?(%{decision: :run}, decisions[&1])) do
          stage_state =
            StageAttemptState.new(
              state.run,
              state.accumulated_results,
              entries,
              deferred,
              MapSet.new(),
              nil,
              :batch_budget
            )

          stage_state = %{
            stage_state
            | node_statuses: statuses,
              terminal_failure: stage_failure
          }

          stage_state =
            Enum.reduce(settled, stage_state, fn step, acc ->
              if step.retry_allowed? do
                delay =
                  if step[:retry_at],
                    do: max(DateTime.diff(step.retry_at, DateTime.utc_now(), :millisecond), 0),
                    else: 0

                StageAttemptState.add_admission_retry(acc, step.node_key, delay)
              else
                acc
              end
            end)

          state =
            %{
              state
              | stage_state: stage_state,
                stage_decisions: decisions,
                stage_freshness_context: state.freshness_context,
                stage_attempt: state.freshness_checkpoint.attempt
            }
            |> start_pipeline_awaits(entries)

          if tasks == [] do
            case restore_retry_wait(state) do
              {:ok, %{status: :retry_wait} = next} -> {:cont, next}
              {:ok, next} -> after_pipeline_progress(next)
              {:error, reason} -> {:recovery_required, state, reason}
            end
          else
            after_starting_pipeline_awaits(state, entries)
          end
        else
          {:recovery_required, state, :pipeline_recovery_decision_mismatch}
        end
      end
    end
  end

  defp resume_restored_mode(%RunExecutionState{mode: :sequential} = state, progress, tasks) do
    settled = Map.new(progress.steps, fn {_id, step} -> {step.node_key, step} end)

    index =
      Enum.find_index(state.sequential_refs, fn {_ref, key, _stage} ->
        not match?(%{phase: :settled, status: :ok}, settled[key])
      end) || length(state.sequential_refs)

    state = %{state | sequential_index: index}

    case tasks do
      [] ->
        case restore_retry_wait(state) do
          {:ok, %{status: :retry_wait} = next} ->
            {:cont, next}

          {:ok, next} ->
            retry = Enum.find(Map.values(settled), &(&1.phase == :settled and &1.retry_allowed?))
            intended = Enum.find(Map.values(settled), &(&1.phase == :intended))

            cond do
              progress.failure ->
                {:terminal,
                 terminalize_pipeline_failed_run(
                   next.run,
                   next.accumulated_results,
                   progress.failure
                 )}

              intended ->
                sequential_operation(next, :restore_intent, [intended])

              retry ->
                sequential_operation(next, :restore_retry, [retry])

              true ->
                sequential_operation(next, :continue, [])
            end

          {:error, reason} ->
            {:recovery_required, state, reason}
        end

      [entry] ->
        with {_ref, key, _stage} when key == entry.node_key <-
               Enum.at(state.sequential_refs, index) do
          {:cont, state |> RunExecutionState.add_work(entry) |> start_await(entry, :sequential)}
        else
          error -> {:recovery_required, state, {:invalid_sequential_recovery, error}}
        end

      _ ->
        {:recovery_required, state, :invalid_sequential_runner_task_count}
    end
  end

  defp restored_stage_index(state, nil),
    do: if(state.freshness_checkpoint, do: state.freshness_checkpoint.stage, else: 0)

  defp restored_stage_index(_state, position) do
    index = metadata_field(position, :index)
    if metadata_field(position, :phase) == "advance", do: index + 1, else: index
  end

  defp handle_await_result(%RunExecutionState{} = state, entry, result, :pipeline) do
    process_await_result(state, entry, result, :pipeline)
  end

  defp handle_await_result(%RunExecutionState{} = state, entry, result, :sequential) do
    process_await_result(state, entry, result, :sequential)
  after
    :ok = ActiveTaskSet.release_entry(entry)
  end

  defp handle_await_failure_without_terminal_evidence(state, await, result, failure) do
    entry = await.entry

    outcome =
      RunnerTasks.request_cancellation(
        state.run.workspace_id,
        entry.task_id,
        Map.merge(failure, %{
          asset_ref: Map.get(entry, :asset_ref),
          stage: Map.get(entry, :stage),
          attempt: Map.get(entry, :attempt)
        })
      )

    if CancellationOutcome.confirmed?(outcome) do
      handle_confirmed_await_failure(state, await, result, failure, outcome)
    else
      terminalize_unknown_await_outcome(state, entry, failure, outcome)
    end
  end

  defp handle_confirmed_await_failure(
         state,
         await,
         _result,
         _failure,
         %CancellationOutcome{status: :already_completed}
       ) do
    with {:ok, task} <- RunnerTasks.fetch(state.run.workspace_id, await.entry.task_id),
         {:ok, %RunnerResult{}} = durable_result <- durable_task_result(task) do
      handle_await_result(state, await.entry, durable_result, await.kind)
    else
      {:error, reason} ->
        {:recovery_required, state,
         {:completed_runner_result_unavailable, await.entry.task_id, reason}}
    end
  end

  defp handle_confirmed_await_failure(state, await, result, _failure, _outcome) do
    handle_await_result(state, await.entry, result, await.kind)
  end

  defp terminalize_unknown_await_outcome(state, entry, failure, outcome) do
    state =
      state
      |> put_cancel_outcome(outcome)
      |> stop_all_awaits(%{kind: :sibling_await_outcome_unconfirmed, task_id: entry.task_id})

    error =
      RunnerError.new(
        type: :runner_await_outcome_unconfirmed,
        message: "Runner await outcome remains unconfirmed after cancellation",
        details: %{
          runner_task_id: entry.task_id,
          await_failure: failure,
          cancellation: CancellationOutcome.to_map(outcome)
        },
        retryable?: false,
        outcome: :unknown
      )

    failed_run =
      Snapshots.snapshot_update(state.run,
        status: :error,
        error: error,
        runner_task_id: nil
      )

    {:terminal, terminalize_unconfirmed_await(state, failed_run)}
  end

  defp terminalize_unconfirmed_await(
         %RunExecutionState{mode: :pipeline, stage_state: %StageAttemptState{} = stage_state},
         failed_run
       ) do
    results =
      failed_run
      |> ResultBuilder.sort_asset_results(StageAttemptState.settled_results(stage_state))

    terminalize_pipeline_failed_run(failed_run, results)
  end

  defp terminalize_unconfirmed_await(%RunExecutionState{mode: :pipeline} = state, failed_run) do
    results = ResultBuilder.sort_asset_results(failed_run, state.accumulated_results)
    terminalize_pipeline_failed_run(failed_run, results)
  end

  defp terminalize_unconfirmed_await(%RunExecutionState{} = state, failed_run) do
    results = ResultBuilder.sort_asset_results(failed_run, state.accumulated_results)
    Snapshots.terminalize_failed_run(failed_run, results)
  end

  defp process_await_result(%RunExecutionState{} = state, entry, result, :sequential) do
    result = validate_await_result(entry, result)
    state = elem(RunExecutionState.complete_work(state, entry.task_id), 1)

    sequential_operation(state, :handle_result, [entry, result])
  end

  defp process_await_result(%RunExecutionState{} = state, entry, result, :pipeline) do
    handle_pipeline_await_result(state, entry, validate_await_result(entry, result))
  end

  defp validate_await_result(entry, {:ok, %RunnerResult{} = result}) do
    with {:ok, required} <- Map.fetch(entry, :required_runner_release_id),
         :ok <- RunnerIdentityVerifier.verify_result(required, result) do
      {:ok, result}
    else
      :error ->
        runner_release_validation_error(:runner_task_release_identity_missing)

      {:error, reason} ->
        runner_release_validation_error(reason)
    end
  end

  defp validate_await_result(_entry, result), do: result

  defp runner_release_validation_error(reason) do
    {:error,
     RunnerError.new(
       type: :runner_release_mismatch,
       message: "Runner result release identity does not match the runner task",
       reason: reason,
       retryable?: false,
       outcome: :unknown
     )}
  end

  defp resume_retry(%RunExecutionState{mode: :sequential} = state, retry) do
    sequential_operation(state, :resume_retry, [retry])
  end

  defp resume_retry(%RunExecutionState{mode: :pipeline} = state, retry) do
    run =
      state.run
      |> Map.put(:metadata, clear_retry_state(state.run.metadata))
      |> RunState.with_snapshot_hash()

    completed_node_statuses =
      state.stage_state.node_statuses
      |> Map.drop(retry.node_keys)

    submit_pipeline_stage_attempt(
      %{state | run: run, stage_attempt: retry.next_attempt, stage_admission_deadline_ms: nil},
      retry.node_keys,
      retry.next_attempt,
      completed_node_statuses
    )
  end

  defp continue_pipeline(%RunExecutionState{cancel_requested: reason} = state)
       when not is_nil(reason),
       do: cancel(state, reason)

  defp continue_pipeline(%RunExecutionState{} = state) do
    if state.stage_index >= length(state.stage_groups),
      do: terminalize_pipeline_state(state),
      else: {:operation, state, {:cancellation_check, state.run, :continue_pipeline}}
  end

  defp continue_pipeline_checked(state, cancelled?) do
    {stage, node_keys} = Enum.at(state.stage_groups, state.stage_index)

    if cancelled? do
      {:terminal, Snapshots.cancelled_terminal(state.run, state.accumulated_results)}
    else
      state
      |> Map.put(:stage_decisions, %{})
      |> Map.put(:stage_freshness_context, state.freshness_context)
      |> Map.put(:pipeline_continuation, %{
        kind: :stage_classification,
        stage: stage,
        remaining_node_keys: node_keys,
        runnable_node_keys_rev: []
      })
      |> Map.put(:status, :classifying)
      |> continue_stage_classification()
    end
  end

  defp continue_stage_classification(
         %RunExecutionState{
           pipeline_continuation: %{
             kind: :stage_classification,
             stage: stage,
             remaining_node_keys: node_keys,
             runnable_node_keys_rev: _runnable_rev
           }
         } = state
       ) do
    {:operation, state,
     {:classify,
      [
        state.run,
        state.version,
        stage,
        node_keys,
        state.stage_freshness_context,
        state.terminal_failure
      ]}}
  end

  defp handle_stage_classification(state, result, stage, runnable_rev) do
    case result do
      {:persist_retry, retry, reason} ->
        {:persist_retry, state, retry, reason}

      {:ok, classified_run, runnable_node_keys, decisions, classified_context,
       next_terminal_failure, remaining_node_keys} ->
        runnable_rev = Enum.reduce(runnable_node_keys, runnable_rev, &[&1 | &2])

        state = %{
          state
          | run: classified_run,
            stage_decisions: Map.merge(state.stage_decisions, decisions),
            stage_freshness_context: classified_context,
            terminal_failure: next_terminal_failure || state.terminal_failure
        }

        if remaining_node_keys == [] do
          finish_stage_classification(state, Enum.reverse(runnable_rev))
        else
          state
          |> Map.put(:pipeline_continuation, %{
            kind: :stage_classification,
            stage: stage,
            remaining_node_keys: remaining_node_keys,
            runnable_node_keys_rev: runnable_rev
          })
          |> defer_pipeline_continue()
        end

      {:error, failed_run} ->
        all_results = ResultBuilder.sort_asset_results(failed_run, state.accumulated_results)
        {:terminal, terminalize_pipeline_failed_run(failed_run, all_results)}
    end
  end

  defp finish_stage_classification(state, runnable_node_keys) do
    {stage, _node_keys} = Enum.at(state.stage_groups, state.stage_index)

    checkpoint_operation(
      state,
      stage,
      1,
      state.stage_freshness_context,
      "admit",
      {:classified, runnable_node_keys}
    )
  end

  defp resume_checkpoint(state, {:classified, runnable_node_keys}) do
    case runnable_node_keys do
      [] ->
        state
        |> Map.put(:freshness_context, state.stage_freshness_context)
        |> Map.put(:stage_index, state.stage_index + 1)
        |> Map.put(:pipeline_continuation, nil)
        |> Map.put(:status, :starting)
        |> defer_pipeline_continue()

      _ ->
        state
        |> Map.put(:pipeline_continuation, nil)
        |> Map.put(:status, :submitting)
        |> submit_pipeline_stage_attempt(runnable_node_keys, 1)
    end
  end

  defp resume_checkpoint(state, {:submit, keys, attempt, statuses}),
    do: submit_pipeline_stage_attempt_with_checkpoint(state, keys, attempt, statuses)

  defp resume_checkpoint(state, :advance),
    do:
      continue_pipeline(%{
        state
        | stage_index: state.stage_index + 1,
          stage_admission_deadline_ms: nil,
          status: :starting
      })

  defp defer_pipeline_continue(%RunExecutionState{} = state) do
    send(self(), :continue_execution)
    {:cont, state}
  end

  defp submit_pipeline_stage_attempt(
         %RunExecutionState{} = state,
         node_keys,
         attempt,
         completed_node_statuses \\ %{}
       ) do
    {stage, _stage_node_keys} = Enum.at(state.stage_groups, state.stage_index)

    if match?(
         %{stage: ^stage, attempt: ^attempt, sequence: sequence}
         when sequence == state.run.event_seq,
         state.freshness_checkpoint
       ) do
      submit_pipeline_stage_attempt_with_checkpoint(
        state,
        node_keys,
        attempt,
        completed_node_statuses
      )
    else
      checkpoint_operation(
        state,
        stage,
        attempt,
        state.stage_freshness_context,
        "admit",
        {:submit, node_keys, attempt, completed_node_statuses}
      )
    end
  end

  defp submit_pipeline_stage_attempt_with_checkpoint(
         state,
         node_keys,
         attempt,
         completed_node_statuses
       ) do
    input =
      stage_admission_input(
        state,
        state.run,
        node_keys,
        attempt,
        MapSet.new(),
        completed_node_statuses
      )

    {:operation, state, {:stage_admit, input, [], {:submit, attempt, completed_node_statuses}}}
  end

  defp finish_submission(state, attempt, completed_node_statuses, result) do
    case result do
      {:ok, run_after_submit, entries, deferred_node_keys, queued_steps, waiters,
       admission_failure, deferred_refill_cause} ->
        stage_state =
          StageAttemptState.new(
            run_after_submit,
            state.accumulated_results,
            entries,
            deferred_node_keys,
            queued_steps,
            admission_failure,
            deferred_refill_cause
          )
          |> Map.update!(:node_statuses, &Map.merge(completed_node_statuses, &1))

        state =
          %{
            state
            | run: run_after_submit,
              stage_state: stage_state,
              stage_attempt: attempt,
              stage_admission_deadline_ms:
                state.stage_admission_deadline_ms ||
                  stage_admission_deadline(run_after_submit.timeout_ms)
          }
          |> RunExecutionState.put_admission_waiters(waiters)

        state
        |> start_pipeline_awaits(entries)
        |> after_starting_pipeline_awaits(entries)

      {:error, failed_run, step_results, _attempted_node_keys, cleanup_entries} ->
        terminalize_stage_admission_failure(state, failed_run, step_results, cleanup_entries)

      {:persist_retry, %PersistenceRetry{} = retry, reason} ->
        {:persist_retry, state, retry, reason}

      {:persist_retry, %PersistenceRetry{} = retry, reason, pause} ->
        state = pause_stage_admission(state, pause)
        {:persist_retry, state, retry, reason}
    end
  end

  # A node-specific admission failure is already durable here. The stage keeps
  # its already-submitted entries, remembers the failure, and refills the rest
  # of the stage immediately, so no sibling is cancelled by the retry.
  defp handle_initial_stage_node_failure(
         state,
         attempt,
         {:node_failed, failed_run, entries, deferred_node_keys, queued_steps, waiters,
          admission_failure, deferred_refill_cause, completed_node_statuses}
       ) do
    stage_state =
      failed_run
      |> StageAttemptState.new(
        state.accumulated_results,
        entries,
        deferred_node_keys,
        queued_steps,
        admission_failure,
        deferred_refill_cause
      )
      |> Map.update!(:node_statuses, &Map.merge(completed_node_statuses, &1))

    state =
      %{
        state
        | run: failed_run,
          stage_state: stage_state,
          stage_attempt: attempt,
          stage_admission_deadline_ms: stage_admission_deadline(failed_run.timeout_ms)
      }
      |> RunExecutionState.put_admission_waiters(waiters)

    state
    |> start_pipeline_awaits(entries)
    |> after_starting_pipeline_awaits(entries)
  end

  # The refill variant keeps the live stage state, which already holds the
  # statuses of nodes completed earlier in this attempt.
  defp handle_refill_stage_node_failure(
         state,
         {:node_failed, failed_run, entries, deferred_node_keys, queued_steps, waiters,
          admission_failure, deferred_refill_cause, _completed_node_statuses}
       ) do
    stage_state =
      state.stage_state
      |> StageAttemptState.add_entries(
        entries,
        failed_run,
        deferred_node_keys,
        queued_steps,
        deferred_refill_cause
      )
      |> StageAttemptState.add_admission_failure(admission_failure)

    %{state | run: failed_run, stage_state: stage_state}
    |> RunExecutionState.put_admission_waiters(waiters)
    |> start_pipeline_awaits(entries)
    |> after_starting_pipeline_awaits(entries)
  end

  defp start_pipeline_awaits(%RunExecutionState{} = state, entries) when is_list(entries) do
    Enum.reduce(entries, state, fn entry, acc ->
      acc
      |> RunExecutionState.add_work(entry)
      |> start_await(entry, :pipeline)
    end)
  end

  defp handle_pipeline_await_result(%RunExecutionState{} = state, entry, result) do
    state = elem(RunExecutionState.complete_work(state, entry.task_id), 1)

    entry =
      Map.merge(entry, %{
        version: state.version,
        manifest_index: state.manifest_index,
        freshness_context: state.stage_freshness_context
      })

    {:operation, state, {:settle_stage, %{state.stage_state | run: state.run}, entry, result}}
  end

  defp prepare_pipeline_settlement({:recovery_required, run, reason}, state),
    do: {:recovery_required, %{state | run: run}, reason}

  defp prepare_pipeline_settlement({:cont, next_stage_state}, state),
    do: {:pipeline_settled, %{state | run: next_stage_state.run, stage_state: next_stage_state}}

  defp prepare_pipeline_settlement(
         {:halt, {:error, failed_run, next_results, _attempted_node_keys}},
         state
       ) do
    state = stop_all_awaits(%{state | run: failed_run}, :stopped_pending_await)
    failed_run = state.run

    {:terminal,
     terminalize_pipeline_failed_run(
       failed_run,
       ResultBuilder.sort_asset_results(failed_run, next_results),
       %{status: failed_run.status, error: failed_run.error}
     )}
  end

  defp prepare_pipeline_settlement(
         {:persist_retry, %PersistenceRetry{event_type: :resource_outcomes} = retry, reason},
         state
       ) do
    state = %{state | run: retry.run, stage_state: %{state.stage_state | run: retry.run}}
    {:persist_retry, state, retry, reason}
  end

  defp prepare_pipeline_settlement(
         {:persist_retry, %PersistenceRetry{} = retry, reason},
         state
       ) do
    {:persist_retry, state, retry, reason}
  end

  # A worker started by this settlement is only in the post-settlement state,
  # so a terminal result reached in the same dispatch (for example an expired
  # admission deadline during refill) must stop workers from here as well.
  defp continue_pipeline_settlement({:pipeline_settled, state}) do
    state
    |> RunExecutionState.cancel_admission_timers()
    |> after_pipeline_progress()
  end

  defp continue_pipeline_settlement(result), do: result

  defp after_pipeline_progress(%RunExecutionState{cancel_requested: reason} = state)
       when not is_nil(reason),
       do: cancel(state, reason)

  defp after_pipeline_progress(%RunExecutionState{stage_state: nil} = state), do: {:cont, state}

  defp after_pipeline_progress(%RunExecutionState{} = state),
    do: {:operation, state, {:cancellation_check, state.run, :pipeline_progress}}

  defp pipeline_progress(%RunExecutionState{cancel_requested: reason} = state, _)
       when not is_nil(reason),
       do: cancel(state, reason)

  defp pipeline_progress(state, cancelled?) do
    if cancelled? do
      state = clear_admission_waiters(state)

      if RunExecutionState.in_flight_count(state) > 0 do
        {:cont, %{state | status: :awaiting}}
      else
        {:terminal,
         Snapshots.cancelled_terminal(
           state.run,
           StageAttemptState.settled_results(state.stage_state)
         )}
      end
    else
      continue_pipeline_progress(state)
    end
  end

  defp continue_pipeline_progress(state) do
    case pipeline_progress_action(
           state.stage_state,
           RunExecutionState.in_flight_count(state),
           map_size(state.admission_waiters)
         ) do
      :refill ->
        refill_or_schedule_admission(state)

      :admission_timeout ->
        schedule_admission_timeout(state)

      :await ->
        {:cont, %{state | status: :awaiting}}

      :retry ->
        schedule_pipeline_retry(state)

      :finalize ->
        finalize_pipeline_stage(state)
    end
  end

  @doc false
  @spec pipeline_progress_action(StageAttemptState.t(), non_neg_integer(), non_neg_integer()) ::
          :refill | :admission_timeout | :await | :retry | :finalize
  def pipeline_progress_action(%StageAttemptState{} = stage_state, await_count, waiter_count) do
    cond do
      stage_state.deferred_node_keys != [] and waiter_count > 0 and await_count == 0 ->
        :admission_timeout

      stage_state.deferred_node_keys != [] ->
        :refill

      await_count > 0 ->
        :await

      stage_state.retry_refs != [] ->
        :retry

      true ->
        :finalize
    end
  end

  @doc false
  @spec post_refill_action(
          [Favn.Plan.node_key()],
          StageAttemptState.deferred_refill_cause(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :finalize | :continue | :await | :admission_timeout
  def post_refill_action(deferred_node_keys, refill_cause, await_count, waiter_count) do
    cond do
      deferred_node_keys == [] and await_count > 0 -> :await
      deferred_node_keys == [] -> :finalize
      refill_cause == :batch_budget -> :continue
      await_count > 0 -> :await
      waiter_count > 0 -> :admission_timeout
      true -> :continue
    end
  end

  defp refill_or_schedule_admission(%RunExecutionState{} = state) do
    {waiters, state} = RunExecutionState.clear_admission_waiters(state)

    input =
      stage_admission_input(
        state,
        state.run,
        state.stage_state.deferred_node_keys,
        state.stage_attempt,
        state.stage_state.queued_steps
      )

    {:operation, state, {:stage_admit, input, waiters, :refill}}
  end

  defp finish_refill(state, result) do
    case result do
      {:ok, next_run, [], next_deferred_node_keys, next_queued_steps, waiters, admission_failure,
       deferred_refill_cause} ->
        stage_state =
          StageAttemptState.defer_only(
            state.stage_state,
            next_run,
            next_deferred_node_keys,
            next_queued_steps,
            deferred_refill_cause
          )
          |> StageAttemptState.add_admission_failure(admission_failure)

        state =
          %{state | run: next_run, stage_state: stage_state}
          |> RunExecutionState.put_admission_waiters(waiters)

        case post_refill_action(
               next_deferred_node_keys,
               deferred_refill_cause,
               RunExecutionState.in_flight_count(state),
               length(waiters)
             ) do
          :finalize ->
            finalize_pipeline_stage(state)

          :continue ->
            schedule_deferred_refill(state)

          :await ->
            {:cont, %{state | status: :awaiting}}

          :admission_timeout ->
            schedule_admission_timeout(state)
        end

      {:ok, next_run, entries, next_deferred_node_keys, next_queued_steps, waiters,
       admission_failure, deferred_refill_cause} ->
        stage_state =
          StageAttemptState.add_entries(
            state.stage_state,
            entries,
            next_run,
            next_deferred_node_keys,
            next_queued_steps,
            deferred_refill_cause
          )
          |> StageAttemptState.add_admission_failure(admission_failure)

        %{state | run: next_run, stage_state: stage_state}
        |> RunExecutionState.put_admission_waiters(waiters)
        |> start_pipeline_awaits(entries)
        |> after_starting_pipeline_awaits(entries)

      {:error, failed_run, step_results, _attempted_node_keys, cleanup_entries} ->
        terminalize_stage_admission_failure(state, failed_run, step_results, cleanup_entries)

      {:persist_retry, %PersistenceRetry{} = retry, reason} ->
        {:persist_retry, state, retry, reason}

      {:persist_retry, %PersistenceRetry{} = retry, reason, pause} ->
        state = pause_stage_admission(state, pause)
        {:persist_retry, state, retry, reason}
    end
  end

  defp handle_resumed_stage_admission(state, attempt, result, completed_node_statuses) do
    case {state.stage_state, result} do
      {nil,
       {:ok, run_after_submit, entries, deferred_node_keys, queued_steps, waiters,
        admission_failure, deferred_refill_cause}} ->
        stage_state =
          StageAttemptState.new(
            run_after_submit,
            state.accumulated_results,
            entries,
            deferred_node_keys,
            queued_steps,
            admission_failure,
            deferred_refill_cause
          )
          |> Map.update!(:node_statuses, &Map.merge(completed_node_statuses, &1))

        resumed =
          %{
            state
            | run: run_after_submit,
              stage_state: stage_state,
              stage_attempt: attempt,
              stage_admission_deadline_ms:
                state.stage_admission_deadline_ms ||
                  stage_admission_deadline(run_after_submit.timeout_ms)
          }
          |> RunExecutionState.put_admission_waiters(waiters)

        resumed
        |> start_pipeline_awaits(entries)
        |> after_starting_pipeline_awaits(entries)

      {%StageAttemptState{},
       {:ok, next_run, entries, next_deferred_node_keys, next_queued_steps, waiters,
        admission_failure, deferred_refill_cause}} ->
        stage_state =
          state.stage_state
          |> StageAttemptState.add_entries(
            entries,
            next_run,
            next_deferred_node_keys,
            next_queued_steps,
            deferred_refill_cause
          )
          |> StageAttemptState.add_admission_failure(admission_failure)

        %{state | run: next_run, stage_state: stage_state}
        |> RunExecutionState.put_admission_waiters(waiters)
        |> start_pipeline_awaits(entries)
        |> after_starting_pipeline_awaits(entries)

      {_stage_state, {:error, failed_run, step_results, _keys, cleanup_entries}} ->
        terminalize_stage_admission_failure(state, failed_run, step_results, cleanup_entries)

      {_stage_state, {:persist_retry, %PersistenceRetry{} = retry, reason, pause}} ->
        paused = pause_stage_admission(state, pause)
        {:persist_retry, paused, retry, reason}

      {_stage_state, {:persist_retry, %PersistenceRetry{} = retry, reason}} ->
        {:persist_retry, state, retry, reason}
    end
  end

  defp pause_stage_admission(%RunExecutionState{} = state, pause) do
    work_set =
      Enum.reduce(pause.entries, state.work_set, fn entry, acc ->
        ActiveTaskSet.add_entry(acc, entry)
      end)

    %{
      state
      | run: pause.ctx.current_run,
        work_set: work_set,
        paused_admission: pause,
        stage_admission_deadline_ms:
          state.stage_admission_deadline_ms ||
            stage_admission_deadline(pause.ctx.current_run.timeout_ms)
    }
    |> RunExecutionState.put_admission_waiters(
      pause.ctx.waiters ++ List.wrap(Map.get(pause.ctx, :waiter))
    )
  end

  defp cleanup_paused_admission(
         %RunExecutionState{paused_admission: %{kind: :sequential}} = state,
         _reason
       ),
       do: Sequential.cleanup_paused(state)

  defp cleanup_paused_admission(
         %RunExecutionState{paused_admission: pause} = state,
         reason
       )
       when is_map(pause) do
    run = StageAdmission.cleanup_paused(pause, reason, state.run)
    %{state | run: run, paused_admission: nil}
  end

  defp cleanup_paused_admission(%RunExecutionState{} = state, _reason), do: state

  defp reconcile_cancelled_admission(%{paused_admission: %{phase: phase} = pause} = state)
       when phase in [:runner_admission, :admission] do
    task_id = if phase == :runner_admission, do: pause.task_id, else: pause.intent.task_id

    if Map.has_key?(state.work_set.entries, task_id) do
      {:ok, state}
    else
      case RunnerTasks.fetch(state.run.workspace_id, task_id) do
        {:ok, task} ->
          intent = if phase == :runner_admission, do: pause.ctx.intent, else: pause.intent

          with %RunnerWork{} <- task.payload,
               {:ok, ^intent} <-
                 AdmissionIntent.new(
                   state.run,
                   task.payload,
                   intent.context,
                   intent.occurred_at
                 ),
               true <- task.task_id == intent.task_id,
               {:ok, entry} <- RecoveredTask.entry(state.run, task) do
            run =
              if pause.admitted_run.event_seq > state.run.event_seq,
                do: pause.admitted_run,
                else: state.run

            pause =
              if state.mode == :pipeline,
                do:
                  pause |> Map.put(:submitted?, true) |> Map.update!(:entries, &(&1 ++ [entry])),
                else: Map.put(pause, :submitted_entry, entry)

            {:ok, %{state | paused_admission: pause, run: run}}
          else
            _invalid -> {:error, {:invalid_cancelled_admission_task, task_id}}
          end

        {:error, %FavnOrchestrator.Persistence.Error{kind: :not_found}} ->
          if task_id in ActiveTaskSet.active_runner_task_ids(state.run),
            do: {:error, {:durable_runner_tasks_missing, [task_id]}},
            else: {:ok, state}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp reconcile_cancelled_admission(state), do: {:ok, state}

  defp track_paused_entries_for_cancellation(
         %{paused_admission: %{kind: :sequential, submitted_entry: entry}} = state
       ),
       do: state |> RunExecutionState.add_work(entry) |> start_await(entry, :sequential)

  defp track_paused_entries_for_cancellation(
         %RunExecutionState{paused_admission: %{entries: [_ | _] = entries} = pause} = state
       ) do
    stage_state =
      case state.stage_state do
        nil ->
          state.run
          |> StageAttemptState.new(
            state.accumulated_results,
            entries,
            [],
            pause.ctx.queued_steps,
            pause.ctx.terminal_failure,
            nil
          )
          |> Map.update!(:node_statuses, &Map.merge(pause.ctx.completed_node_statuses, &1))

        %StageAttemptState{} = stage_state ->
          StageAttemptState.add_entries(
            stage_state,
            entries,
            state.run,
            stage_state.deferred_node_keys,
            stage_state.queued_steps,
            stage_state.deferred_refill_cause
          )
      end

    %{state | stage_state: stage_state}
    |> start_pipeline_awaits(entries)
  end

  defp track_paused_entries_for_cancellation(%RunExecutionState{} = state), do: state

  defp terminalize_stage_admission_failure(
         state,
         %{error: {:runner_task_recovery_failed, reason}} = run,
         _results,
         entries
       ) do
    work_set = Enum.reduce(entries, state.work_set, &ActiveTaskSet.add_entry(&2, &1))
    {:recovery_required, %{state | run: run, work_set: work_set}, reason}
  end

  defp terminalize_stage_admission_failure(state, %{error: reason} = run, _results, entries)
       when reason in [:invalid_admission_intent, :admission_intent_mismatch, :fenced] do
    work_set = Enum.reduce(entries, state.work_set, &ActiveTaskSet.add_entry(&2, &1))
    {:recovery_required, %{state | run: run, work_set: work_set}, reason}
  end

  defp terminalize_stage_admission_failure(
         state,
         %{
           error: %FavnOrchestrator.Persistence.Error{kind: kind, retryable?: retryable?} = reason
         } = run,
         _results,
         entries
       )
       when retryable? or kind in [:timeout, :unavailable] do
    work_set = Enum.reduce(entries, state.work_set, &ActiveTaskSet.add_entry(&2, &1))
    {:recovery_required, %{state | run: run, work_set: work_set}, reason}
  end

  defp terminalize_stage_admission_failure(
         state,
         failed_run,
         step_results,
         cleanup_entries
       ) do
    failure = %{status: failed_run.status, error: failed_run.error}
    work_set = Enum.reduce(cleanup_entries, state.work_set, &ActiveTaskSet.add_entry(&2, &1))

    state = %{state | work_set: work_set, run: failed_run}

    entries = Enum.reject(Map.values(work_set.entries), &Map.has_key?(state.awaits, &1.task_id))

    stage_state =
      case state.stage_state do
        nil ->
          StageAttemptState.new(
            failed_run,
            state.accumulated_results,
            entries,
            [],
            MapSet.new(),
            failure,
            nil
          )

        stage ->
          StageAttemptState.add_entries(
            stage,
            entries,
            failed_run,
            stage.deferred_node_keys,
            stage.queued_steps,
            stage.deferred_refill_cause
          )
      end

    state = %{state | stage_state: stage_state} |> start_pipeline_awaits(entries)

    state =
      if failed_run.status in [:cancelled, :timed_out] do
        cancel_terminal_stage_tasks(state, failed_run, %{
          kind: :stage_admission_failure,
          error: failed_run.error
        })
      else
        state
      end

    if RunExecutionState.in_flight_count(state) > 0 and
         match?(%StageAttemptState{}, state.stage_state) do
      stage_state =
        state.stage_state
        |> Map.put(:run, state.run)
        |> Map.update!(:results, &Enum.reverse(step_results, &1))
        |> Map.put(:deferred_node_keys, [])
        |> Map.put(:deferred_refill_cause, nil)
        |> Map.put(:retry_refs, [])
        |> Map.put(:retry_ref_set, MapSet.new())
        |> Map.put(:retry_delays, %{})
        |> StageAttemptState.add_admission_failure(failure)

      %{state | stage_state: stage_state, terminal_failure: state.terminal_failure || failure}
      |> after_pipeline_progress()
    else
      failed_run = state.run

      {:terminal,
       terminalize_pipeline_failed_run(
         failed_run,
         ResultBuilder.sort_asset_results(
           failed_run,
           (state.accumulated_results ++ step_results)
           |> ResultBuilder.retain_asset_results()
         ),
         failure
       )}
    end
  end

  defp cancel_terminal_stage_tasks(state, failed_run, reason) do
    task_ids =
      (ActiveTaskSet.active_runner_task_ids(failed_run) ++
         ActiveTaskSet.task_ids(state.work_set))
      |> Enum.uniq()
      |> Enum.sort()

    outcomes = Cancellation.dispatch_runner_tasks(failed_run, task_ids, reason)
    await_ids = Map.keys(state.awaits) |> MapSet.new()

    confirmed_without_await =
      outcomes
      |> Enum.filter(
        &(CancellationOutcome.confirmed?(&1) and
            not MapSet.member?(await_ids, &1.task_id) and
            Map.has_key?(state.work_set.entries, &1.task_id))
      )
      |> Enum.map(& &1.task_id)

    remaining_ids = task_ids -- confirmed_without_await

    work_set =
      Enum.reduce(confirmed_without_await, state.work_set, fn task_id, work_set ->
        {entry, work_set} = ActiveTaskSet.complete_entry(work_set, task_id)
        :ok = ActiveTaskSet.release_entry(entry)
        :ok = ActiveTaskSet.fail_entry_claim(entry, reason)
        :ok = ResourceCircuits.release(failed_run, Map.get(entry, :resource_circuit_permits, []))
        work_set
      end)

    work_set = ActiveTaskSet.retain_task_ids(work_set, remaining_ids)

    run =
      Snapshots.snapshot_update(failed_run,
        metadata:
          failed_run.metadata
          |> Map.delete(:active_runner_task_ids)
          |> Map.delete("active_runner_task_ids")
          |> Map.put(:active_runner_task_ids, remaining_ids)
      )

    Enum.reduce(outcomes, %{state | run: run, work_set: work_set}, fn outcome, next ->
      put_cancel_outcome(next, outcome)
    end)
  end

  defp after_starting_pipeline_awaits(%RunExecutionState{} = state, [_ | _]) do
    if state.stage_state.deferred_node_keys != [] and
         RunExecutionState.in_flight_count(state) > 0 do
      schedule_deferred_refill(state)
    else
      after_pipeline_progress(state)
    end
  end

  defp after_starting_pipeline_awaits(%RunExecutionState{} = state, []),
    do: after_pipeline_progress(state)

  defp schedule_admission_timeout(%RunExecutionState{} = state) do
    if map_size(state.admission_timers) > 0 do
      {:cont, %{state | status: :admission_wait}}
    else
      now = System.monotonic_time(:millisecond)

      deadline =
        state.stage_admission_deadline_ms || stage_admission_deadline(state.run.timeout_ms)

      remaining_ms = max(deadline - now, 0)
      wait_ms = min(@stage_admission_backstop_retry_ms, remaining_ms)

      if wait_ms == 0 do
        timeout_admission_wait(state)
      else
        timer_token = make_ref()
        timer_ref = Process.send_after(self(), {:stage_admission_timeout, timer_token}, wait_ms)
        kind = if wait_ms == remaining_ms, do: :deadline, else: :admission_retry

        {:cont,
         RunExecutionState.put_admission_timer(state, timer_token, timer_ref, %{
           kind: kind,
           stage_index: state.stage_index
         })}
      end
    end
  end

  defp schedule_deferred_refill(%RunExecutionState{} = state) do
    now = System.monotonic_time(:millisecond)
    deadline = state.stage_admission_deadline_ms || stage_admission_deadline(state.run.timeout_ms)
    remaining_ms = max(deadline - now, 0)

    case deferred_refill_wait_ms(state.stage_state.deferred_refill_cause, remaining_ms) do
      :timeout ->
        timeout_admission_wait(state)

      wait_ms ->
        timer_token = make_ref()
        timer_ref = Process.send_after(self(), {:stage_admission_timeout, timer_token}, wait_ms)

        {:cont,
         RunExecutionState.put_admission_timer(state, timer_token, timer_ref, %{
           kind: :deferred_refill,
           stage_index: state.stage_index,
           refill_cause: state.stage_state.deferred_refill_cause
         })}
    end
  end

  @doc false
  @spec deferred_refill_wait_ms(StageAttemptState.deferred_refill_cause(), non_neg_integer()) ::
          non_neg_integer() | :timeout
  def deferred_refill_wait_ms(_cause, 0), do: :timeout
  def deferred_refill_wait_ms(:batch_budget, remaining_ms) when remaining_ms > 0, do: 0

  def deferred_refill_wait_ms(_blocked_or_unknown, remaining_ms) when remaining_ms > 0,
    do: min(@deferred_stage_retry_ms, remaining_ms)

  defp timeout_admission_wait(%RunExecutionState{} = state) do
    state = clear_admission_waiters(state)

    {:terminal,
     elem(
       StageResult.timeout_deferred(state.stage_state),
       1
     )}
  end

  defp clear_admission_waiters(%RunExecutionState{} = state) do
    {waiters, state} = RunExecutionState.clear_admission_waiters(state)

    Enum.each(waiters, fn waiter ->
      :ok = ExecutionAdmission.cancel_wait(waiter)
    end)

    RunExecutionState.cancel_admission_timers(state)
  end

  defp schedule_pipeline_retry(%RunExecutionState{} = state) do
    {stage, stage_node_keys} = Enum.at(state.stage_groups, state.stage_index)
    node_keys = StageAttemptState.retry_node_keys(state.stage_state)
    retry_after_ms = pipeline_retry_after_ms(state, node_keys)
    next_retry_at = System.system_time(:millisecond) + retry_after_ms

    persist_pipeline_retry_checkpoint(
      state,
      state.stage_state.run,
      stage_node_keys,
      node_keys,
      stage,
      state.stage_attempt,
      retry_after_ms,
      next_retry_at
    )
  end

  defp persist_pipeline_retry_checkpoint(
         state,
         retry_run,
         stage_node_keys,
         node_keys,
         stage,
         attempt,
         retry_after_ms,
         next_retry_at
       ) do
    with {:ok, selection} <- PipelineRetryCheckpoint.encode(stage_node_keys, node_keys) do
      {checkpointed, data} =
        pipeline_retry_checkpoint_transition(
          retry_run,
          selection,
          stage,
          attempt,
          state.stage_index,
          retry_after_ms,
          next_retry_at
        )

      resume =
        {:pipeline_retry_checkpoint,
         %{
           run: checkpointed,
           node_keys: node_keys,
           stage: stage,
           attempt: attempt,
           retry_after_ms: retry_after_ms,
           next_retry_at: next_retry_at
         }}

      retry = PersistenceRetry.new(checkpointed, :pipeline_retry_checkpointed, data, resume)

      {:operation, %{state | pipeline_continuation: nil}, {:persist, retry}}
    else
      {:error, reason} ->
        {:terminal,
         Snapshots.snapshot_update(retry_run,
           status: :error,
           error: %{type: :invalid_pipeline_retry_checkpoint, reason: reason}
         )}
    end
  end

  defp schedule_pipeline_retry_timer(state, node_keys, stage, attempt, next_retry_at) do
    timer_token = make_ref()
    retry_after_ms = retry_remaining_ms(next_retry_at)

    timer_ref =
      Process.send_after(
        self(),
        {:retry_attempt, timer_token},
        retry_after_ms
      )

    retry = %{
      node_keys: node_keys,
      next_attempt: attempt + 1,
      stage: stage
    }

    {:cont,
     state
     |> Map.put(:pipeline_continuation, nil)
     |> Map.put(:stage_state, %{state.stage_state | run: state.run, retry_refs: []})
     |> RunExecutionState.put_retry_timer(timer_token, timer_ref, retry)}
  end

  defp finalize_pipeline_stage(%RunExecutionState{} = state) do
    case StageResult.finalize(state.stage_state) do
      {:ok, next_run, next_results, [], _attempted_node_keys, node_statuses} ->
        {next_context, persisted_run} =
          FreshnessContext.record_successes(
            next_run,
            state.version,
            node_statuses,
            state.stage_decisions,
            state.stage_freshness_context
          )

        continue_after_stage_checkpoint(
          %{
            state
            | run: persisted_run,
              accumulated_results: next_results,
              freshness_context: next_context,
              stage_state: nil,
              terminal_failure: state.terminal_failure
          },
          next_context
        )

      {:ok, _next_run, _next_results, _retry_refs, _attempted_node_keys, _node_statuses} ->
        schedule_pipeline_retry(state)

      {:error, failed_run, next_results, _attempted_node_keys, node_statuses} ->
        {next_context, persisted_run} =
          FreshnessContext.record_completed_after_failure(
            failed_run,
            state.version,
            node_statuses,
            state.stage_decisions,
            state.stage_freshness_context
          )

        terminal_failure =
          state.terminal_failure || %{status: persisted_run.status, error: persisted_run.error}

        continue_after_stage_checkpoint(
          %{
            state
            | run: persisted_run,
              accumulated_results: next_results,
              freshness_context: next_context,
              stage_state: nil,
              terminal_failure: terminal_failure
          },
          next_context
        )
    end
  end

  defp terminalize_pipeline_state(%RunExecutionState{terminal_failure: nil} = state) do
    :ok = RunExecutionCleanup.release_admission(state.run)
    all_results = ResultBuilder.sort_asset_results(state.run, state.accumulated_results)

    {:terminal,
     Snapshots.snapshot_update(state.run,
       status: :ok,
       error: nil,
       runner_task_id: nil,
       result: ResultBuilder.pipeline_result(state.run, :ok, all_results)
     )}
  end

  defp terminalize_pipeline_state(%RunExecutionState{} = state) do
    :ok = RunExecutionCleanup.release_admission(state.run)
    all_results = ResultBuilder.sort_asset_results(state.run, state.accumulated_results)
    {:terminal, terminalize_pipeline_failed_run(state.run, all_results, state.terminal_failure)}
  end

  defp stop_all_awaits(%RunExecutionState{} = state, reason) do
    Enum.reduce(Map.keys(state.awaits), state, fn task_id, acc ->
      case RunExecutionState.pop_await(acc, task_id) do
        {nil, next} ->
          next

        {await, next} ->
          stop_await_process(await)

          outcome =
            RunnerTasks.request_cancellation(
              state.run.workspace_id,
              await.entry.task_id,
              reason
            )

          if CancellationOutcome.confirmed?(outcome) do
            :ok = ActiveTaskSet.release_entry(await.entry)
            :ok = ActiveTaskSet.fail_entry_claim(await.entry, reason)
            elem(RunExecutionState.complete_work(next, task_id), 1)
          else
            put_cancel_outcome(next, outcome)
          end
      end
    end)
  end

  defp stop_await_processes(%RunExecutionState{} = state) do
    Enum.reduce(Map.keys(state.awaits), state, fn task_id, acc ->
      case RunExecutionState.pop_await(acc, task_id) do
        {nil, next} ->
          next

        {await, next} ->
          stop_await_process(await)
          next
      end
    end)
  end

  defp stop_await_process(await) do
    if is_pid(await.pid) and Process.alive?(await.pid), do: Process.exit(await.pid, :kill)
    if is_reference(await.monitor_ref), do: Process.demonitor(await.monitor_ref, [:flush])
    if is_reference(await.timeout_ref), do: Process.cancel_timer(await.timeout_ref)
    :ok
  end

  defp put_cancel_outcome(%RunExecutionState{} = state, outcome) do
    existing = Map.get(state.run.metadata, :cancel_outcomes, [])

    metadata =
      Map.put(
        state.run.metadata,
        :cancel_outcomes,
        existing ++ [CancellationOutcome.to_map(outcome)]
      )

    %{state | run: Snapshots.snapshot_update(state.run, metadata: metadata)}
  end

  defp pipeline_retry_checkpoint_transition(
         %RunState{} = run_state,
         selection,
         stage,
         attempt,
         stage_index,
         retry_after_ms,
         next_retry_at
       ) do
    checkpoint_sequence = run_state.event_seq + 1

    retry_state = %{
      kind: :pipeline,
      checkpoint_sequence: checkpoint_sequence,
      stage_index: stage_index,
      next_attempt: attempt + 1,
      stage: stage,
      next_retry_at: next_retry_at
    }

    checkpointed =
      RunState.transition(run_state,
        status: :running,
        error: nil,
        runner_task_id: nil,
        metadata:
          Map.merge(run_state.metadata, %{
            retrying: true,
            next_attempt: attempt + 1,
            retry_state: retry_state,
            next_retry_at: next_retry_at
          })
      )

    data = %{
      stage: stage,
      attempt: attempt,
      next_attempt: attempt + 1,
      retry_backoff_ms: retry_after_ms,
      next_retry_at: next_retry_at,
      retry_selection: selection
    }

    {checkpointed, data}
  end

  defp pipeline_retry_after_ms(%RunExecutionState{} = state, node_keys) do
    existing = StageAttemptState.retry_delays(state.stage_state)

    Enum.reduce(node_keys, 0, fn node_key, maximum ->
      delay =
        Map.get_lazy(existing, node_key, fn ->
          StepAttemptLifecycle.retry_delay_ms(state.run, node_key, state.stage_attempt)
        end)

      max(maximum, delay)
    end)
  end

  defp clear_retry_state(metadata) do
    metadata
    |> Map.drop([:retry_state, "retry_state", :next_retry_at, "next_retry_at"])
    |> Map.put(:retrying, false)
  end

  defp restore_retry_wait(%RunExecutionState{} = state) do
    case RetryCheckpoint.validate(state.run.metadata, state.mode) do
      {:ok, {:sequential, retry_state}} ->
        retry = metadata_field(retry_state, :retry)
        token = make_ref()
        remaining_ms = retry_remaining_ms(metadata_field(retry_state, :next_retry_at))
        timer_ref = Process.send_after(self(), {:retry_attempt, token}, remaining_ms)

        restored = restore_retry_position(state, retry_state, retry)
        {:ok, RunExecutionState.put_retry_timer(restored, token, timer_ref, retry)}

      {:ok, {:pipeline, retry_state}} ->
        with {:ok, retry} <- load_pipeline_retry(state, retry_state) do
          token = make_ref()
          remaining_ms = retry_remaining_ms(metadata_field(retry_state, :next_retry_at))
          timer_ref = Process.send_after(self(), {:retry_attempt, token}, remaining_ms)

          restored = restore_retry_position(state, retry_state, retry)
          {:ok, RunExecutionState.put_retry_timer(restored, token, timer_ref, retry)}
        end

      {:ok, :none} ->
        {:ok, state}

      {:error, :invalid_retry_checkpoint} = error ->
        error
    end
  end

  defp restore_retry_position(
         %RunExecutionState{mode: :sequential} = state,
         retry_state,
         _retry
       ) do
    %{state | sequential_index: Map.get(retry_state, :sequential_index, 0)}
  end

  defp restore_retry_position(
         %RunExecutionState{mode: :pipeline} = state,
         retry_state,
         retry
       ) do
    node_keys = Map.fetch!(retry, :node_keys)

    decisions =
      StageClassifier.decisions(state.run, node_keys, state.freshness_context,
        forced_node_keys: node_keys
      )

    %{
      state
      | stage_index: Map.get(retry_state, :stage_index, 0),
        stage_attempt: Map.get(retry, :next_attempt, 1),
        stage_decisions: decisions,
        stage_freshness_context: state.freshness_context
    }
  end

  defp load_pipeline_retry(%RunExecutionState{} = state, retry_state) do
    with checkpoint_sequence when is_integer(checkpoint_sequence) and checkpoint_sequence > 0 <-
           Map.get(retry_state, :checkpoint_sequence),
         stage_index when is_integer(stage_index) and stage_index >= 0 <-
           Map.get(retry_state, :stage_index),
         {stage, stage_node_keys} <- Enum.at(state.stage_groups, stage_index),
         context <- SystemContext.workspace(state.run.workspace_id, :run_worker),
         {:ok, %{items: [event]}} <-
           Runs.page_events(context, state.run.id,
             after_sequence: checkpoint_sequence - 1,
             event_types: [:pipeline_retry_checkpointed],
             limit: 1
           ),
         ^checkpoint_sequence <- Map.get(event, :sequence),
         selection when is_map(selection) <-
           event |> Map.get(:data, %{}) |> metadata_field(:retry_selection),
         {:ok, node_keys} <- PipelineRetryCheckpoint.decode(selection, stage_node_keys) do
      {:ok,
       %{
         node_keys: node_keys,
         next_attempt: Map.get(retry_state, :next_attempt, 1),
         stage: stage
       }}
    else
      _invalid -> {:error, :pipeline_retry_checkpoint_unavailable}
    end
  end

  defp retry_remaining_ms(timestamp) when is_integer(timestamp),
    do: max(timestamp - System.system_time(:millisecond), 0)

  defp retry_remaining_ms(_timestamp), do: 0

  defp load_freshness_context(%RunState{} = run, manifest_index) do
    case PipelineFreshnessCheckpoint.load(run, manifest_index) do
      {:ok, {context, reference}} ->
        {:ok, {context, reference}}

      {:ok, nil} ->
        with {:ok, context} <- FreshnessContext.initialize(run, manifest_index) do
          {:ok, {context, nil}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp checkpoint_operation(state, stage, attempt, context, phase, resume) do
    args = [
      state.run,
      stage,
      attempt,
      context,
      state.freshness_checkpoint,
      %{version: 1, mode: "pipeline", phase: phase, index: state.stage_index, attempt: attempt}
    ]

    {:operation, state, {:checkpoint, args, resume}}
  end

  defp continue_after_stage_checkpoint(state, context) do
    {stage, _node_keys} = Enum.at(state.stage_groups, state.stage_index)
    checkpoint_operation(state, stage, state.stage_attempt, context, "advance", :advance)
  end

  defp terminalize_checkpoint_failure(state, reason) do
    all_results = ResultBuilder.sort_asset_results(state.run, state.accumulated_results)

    {:terminal,
     terminalize_pipeline_failed_run(
       Snapshots.snapshot_update(state.run,
         status: :error,
         error: {:pipeline_freshness_checkpoint_failed, reason}
       ),
       all_results
     )}
  end

  defp metadata_field(metadata, key) when is_map(metadata),
    do: Map.get(metadata, key, Map.get(metadata, Atom.to_string(key)))

  defp stage_admission_input(
         %RunExecutionState{} = state,
         %RunState{} = run_state,
         node_keys,
         attempt,
         queued_steps,
         completed_node_statuses \\ %{}
       ) do
    {stage, _node_keys} = Enum.at(state.stage_groups, state.stage_index)

    %{
      run: run_state,
      version: state.version,
      manifest_index: state.manifest_index,
      stage: stage,
      node_keys: node_keys,
      decisions: state.stage_decisions,
      freshness_context: state.stage_freshness_context,
      freshness_checkpoint: state.freshness_checkpoint,
      attempt: attempt,
      manifest_lease_id: state.manifest_lease_id,
      queued_steps: queued_steps,
      completed_node_statuses: completed_node_statuses
    }
  end

  defp durable_task_result(%{result: %RunnerResult{} = result}), do: {:ok, result}

  defp durable_task_result(%{data_state: :unavailable, persistence_failure: category}),
    do: {:error, {:runner_task_data_unavailable, category}}

  defp durable_task_result(%{status: status, error: error, payload: %RunnerWork{} = work})
       when status in [:failed, :cancelled, :unknown] do
    runner_status = if status == :cancelled, do: :cancelled, else: :error
    error = durable_runner_error(error)

    {:ok,
     %RunnerResult{
       run_id: work.run_id,
       manifest_version_id: work.manifest_version_id,
       manifest_content_hash: work.manifest_content_hash,
       required_runner_release_id: work.required_runner_release_id,
       status: runner_status,
       asset_results: [],
       error: error,
       metadata: RunnerWork.lifecycle_metadata(work)
     }}
  end

  defp durable_task_result(task), do: {:error, {:invalid_runner_task_result, task}}

  defp durable_runner_error(nil), do: nil
  defp durable_runner_error(%RunnerError{} = error), do: error

  defp durable_runner_error(error) when is_map(error) do
    RunnerError.new(
      kind: metadata_field(error, :kind),
      type: metadata_field(error, :type),
      phase: metadata_field(error, :phase),
      message: metadata_field(error, :message),
      reason: metadata_field(error, :reason),
      details: metadata_field(error, :details) || %{},
      retryable?: metadata_field(error, :retryable?) == true,
      retry_after_ms: metadata_field(error, :retry_after_ms),
      outcome: metadata_field(error, :outcome)
    )
  end

  defp durable_runner_error(error), do: RunnerError.normalize(error)

  defp stage_admission_deadline(timeout_ms),
    do: System.monotonic_time(:millisecond) + timeout_ms + @stage_admission_timeout_buffer_ms

  defp pipeline_stage_groups(%RunState{plan: %Favn.Plan{} = plan}) do
    plan.node_stages
    |> Enum.with_index()
    |> Enum.map(fn {node_keys, stage} -> {stage, node_keys} end)
  end

  defp terminalize_pipeline_failed_run(%RunState{} = failed_run, all_results) do
    Snapshots.snapshot_update(failed_run,
      runner_task_id: nil,
      result: ResultBuilder.pipeline_result(failed_run, failed_run.status, all_results)
    )
  end

  defp terminalize_pipeline_failed_run(
         %RunState{} = failed_run,
         all_results,
         %{status: status, error: error}
       ) do
    failed_run
    |> Snapshots.snapshot_update(status: status, error: error, runner_task_id: nil)
    |> then(
      &Snapshots.snapshot_update(&1,
        result: ResultBuilder.pipeline_result(&1, status, all_results)
      )
    )
  end
end
