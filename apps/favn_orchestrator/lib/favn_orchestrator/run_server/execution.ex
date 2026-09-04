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

  No callback waits on a runner task. Post-step reconciliation that needs runner
  inspection runs in a worker under `FavnOrchestrator.RunPostStepSupervisor`;
  the node's settlement completes when the worker's reply arrives. Pending
  continuations count as in-flight stage work exactly like awaits, so a stage
  cannot finalize, retry, time out its admission wait, or terminalize an
  admission failure while one is outstanding; deferred work still refills.
  Every terminal transition terminates pending workers.
  """

  alias Favn.Manifest.Version
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.TargetIdentity
  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.InitialTargetGenerationReconciler
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.ResourceCircuits
  alias FavnOrchestrator.RunExecutionCleanup
  alias FavnOrchestrator.RunOwnership
  alias FavnOrchestrator.RunnerIdentityVerifier
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.FreshnessContext
  alias FavnOrchestrator.RunServer.Execution.PipelineRetryCheckpoint
  alias FavnOrchestrator.RunServer.Execution.PipelineFreshnessCheckpoint
  alias FavnOrchestrator.RunServer.Execution.PipelineTaskContinuation
  alias FavnOrchestrator.RunServer.Execution.RecoveryPosition
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.Sequential
  alias FavnOrchestrator.RunServer.Execution.StageAdmission
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunServer.Execution.StageClassifier
  alias FavnOrchestrator.RunServer.Execution.StageEntry
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
  @post_step_supervisor FavnOrchestrator.RunPostStepSupervisor

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
          | {:post_step_reply, reference(), term()}
          | {:post_step_worker_down, reference(), term()}

  @typep compact_index :: %Favn.Manifest.Index{
           planning_index: nil,
           assets_by_ref: map(),
           pipelines_by_ref: %{},
           schedules_by_ref: %{}
         }

  @spec start_state(RunState.t(), Version.t()) ::
          {:ok, RunExecutionState.t()} | {:terminal, RunState.t()}
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

          case restore_active_tasks(state) do
            {:ok, restored} -> {:ok, restored}
            {:error, reason} -> pipeline_start_failure(run_state, reason)
          end
        else
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

          case restore_active_tasks(state) do
            {:ok, restored} -> {:ok, restored}
            {:error, reason} -> pipeline_start_failure(run_state, reason)
          end
        else
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
    |> stop_workers_on_terminal(state)
  end

  # A `{:terminal, run}` result terminates every pending post-step worker before
  # it is returned, so no terminal transition can leave a worker behind.
  defp dispatch_event(%RunExecutionState{} = state, :continue), do: continue_state(state)

  defp dispatch_event(%RunExecutionState{} = state, {:runner_task_result, task_id, task}) do
    dispatch_event(state, {:runner_result, task_id, durable_task_result(task)})
  end

  # A runner reported the awaited task as started. The `:step_running` event is
  # an advisory presence signal for read models: a persist failure is dropped
  # rather than retried, because the attempt's terminal step event still lands.
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
        Process.cancel_timer(await.timeout_ref)
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
        Process.cancel_timer(await.timeout_ref)

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
      when map_size(state.awaits) + map_size(state.post_step_continuations) > 0 ->
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

  # The worker's reply. A reply for a continuation that already settled, or
  # whose worker was terminated, matches nothing and is ignored.
  defp dispatch_event(%RunExecutionState{} = state, {:post_step_reply, ref, result}) do
    case RunExecutionState.pop_post_step_continuation(state, ref) do
      {nil, state} ->
        {:cont, state}

      {continuation, state} ->
        Process.demonitor(ref, [:flush])
        settle_post_step(state, continuation, post_step_result(result))
    end
  end

  defp dispatch_event(%RunExecutionState{} = state, {:post_step_worker_down, ref, reason}) do
    case RunExecutionState.pop_post_step_continuation(state, ref) do
      {nil, state} ->
        {:cont, state}

      {continuation, state} ->
        settle_post_step(
          state,
          continuation,
          {:error, {:post_step_worker_down, bounded_inspect(reason)}}
        )
    end
  end

  @doc false
  @spec retry_persistence(RunExecutionState.t(), PersistenceRetry.t()) ::
          {:cont, RunExecutionState.t()}
          | {:terminal, RunState.t()}
          | {:persist_retry, RunExecutionState.t(), PersistenceRetry.t(), term()}
  def retry_persistence(%RunExecutionState{} = state, %PersistenceRetry{} = retry) do
    case PersistenceRetry.persist(retry) do
      :ok -> resume_persisted(state, retry.resume)
      {:error, :external_cancel} -> {:terminal, Snapshots.cancelled_snapshot(state.run)}
      {:error, reason} -> {:persist_retry, state, retry, reason}
    end
    |> stop_workers_on_terminal(state)
  end

  defp stop_workers_on_terminal({:terminal, _run} = result, %RunExecutionState{} = state) do
    _ = stop_post_step_workers(state)
    result
  end

  defp stop_workers_on_terminal(result, _state), do: result

  @doc "Stops future work and drains existing awaits to their durable cancellation outcomes."
  @spec cancel(RunExecutionState.t(), term()) ::
          {:cont, RunExecutionState.t()} | {:terminal, RunState.t()}
  def cancel(%RunExecutionState{} = state, reason) do
    reason = %{kind: :external_cancel, reason: reason}

    state =
      if map_size(state.post_step_continuations) > 0 do
        %{
          state
          | run:
              Snapshots.snapshot_update(state.run,
                metadata: Map.put(state.run.metadata, "cancellation_needs_attention", true)
              )
        }
      else
        state
      end

    Enum.each(state.retry_timers, fn {_ref, timer} -> Process.cancel_timer(timer.timer_ref) end)

    state = state |> stop_post_step_workers() |> clear_admission_waiters()
    state = %{state | retry_timers: %{}, pipeline_continuation: nil}

    Cancellation.dispatch_runner_tasks(state.run, ActiveTaskSet.task_ids(state.work_set), reason,
      wait_for_ack: false
    )

    if map_size(state.awaits) > 0 do
      {:cont, %{state | status: :awaiting}}
    else
      {:terminal, Snapshots.cancelled_terminal(state.run, accumulated_results(state))}
    end
  end

  @doc "Stops local waiters while retaining durable tasks, claims and leases for recovery."
  @spec stop_for_recovery(RunExecutionState.t()) :: RunExecutionState.t()
  def stop_for_recovery(state),
    do:
      state
      |> stop_post_step_workers()
      |> stop_await_processes()
      |> clear_admission_waiters()
      |> RunExecutionState.cancel_timers()

  @doc """
  Terminates every pending post-step worker and drops its continuation.

  The run server calls this on every terminal transition and stop. The shared
  inspection task the worker was waiting on is not cancelled; it is an
  idempotent operation identity also used by activation and target recovery.
  A reply already in the mailbox is ignored later because its reference no
  longer matches a continuation.
  """
  @spec stop_post_step_workers(RunExecutionState.t()) :: RunExecutionState.t()
  def stop_post_step_workers(%RunExecutionState{post_step_continuations: continuations} = state)
      when map_size(continuations) == 0,
      do: state

  def stop_post_step_workers(%RunExecutionState{} = state) do
    Enum.each(state.post_step_continuations, fn {ref, %{pid: pid, pending: pending}} ->
      Process.demonitor(ref, [:flush])
      terminate_post_step_worker(pid)
      release_pending_permits(state.run, pending)
    end)

    %{state | post_step_continuations: %{}}
  end

  # The supervisor shuts the worker down without an error report. If the
  # supervisor itself is already gone the worker is killed directly.
  defp terminate_post_step_worker(pid) do
    case Task.Supervisor.terminate_child(@post_step_supervisor, pid) do
      :ok -> :ok
      {:error, :not_found} -> :ok
    end
  catch
    :exit, _reason ->
      Process.exit(pid, :kill)
      :ok
  end

  # The node's permits would otherwise be settled at finish; a dropped
  # continuation releases them so they do not wait for the probe lease.
  defp release_pending_permits(%RunState{} = run, %{entry: entry}) do
    case Map.get(entry, :resource_circuit_permits, []) do
      [] -> :ok
      permits -> _ = ResourceCircuits.release(run, permits)
    end

    :ok
  end

  defp release_pending_permits(_run, _pending), do: :ok

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
    do: state |> Sequential.continue() |> handle_sequential_directive()

  defp continue_state(%RunExecutionState{mode: :pipeline} = state), do: continue_pipeline(state)

  defp handle_sequential_directive({:await, %RunExecutionState{} = state, entry}),
    do: {:cont, start_await(state, entry, :sequential)}

  defp handle_sequential_directive({:cont, %RunExecutionState{}} = result), do: result
  defp handle_sequential_directive({:terminal, %RunState{}} = result), do: result

  defp handle_sequential_directive(
         {:persist_retry, %RunExecutionState{}, %PersistenceRetry{}, _reason} = result
       ),
       do: result

  defp resume_persisted(%RunExecutionState{} = state, {:sequential, resume}) do
    state
    |> Sequential.resume_persisted(resume)
    |> handle_sequential_directive()
  end

  defp resume_persisted(%RunExecutionState{stage_state: %StageAttemptState{}} = state, {
         :pipeline,
         resume
       }) do
    state.stage_state
    |> StageResult.resume_persisted(resume)
    |> prepare_pipeline_settlement(state)
    |> continue_pipeline_settlement()
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
         {:stage_admission, attempt, {:partial_retry, _, _, _, _, _, _, _, _, _} = result}
       ) do
    handle_initial_stage_partial_retry(state, attempt, result)
  end

  defp resume_persisted(
         %RunExecutionState{stage_state: %StageAttemptState{}} = state,
         {:stage_admission, _attempt, {:partial_retry, _, _, _, _, _, _, _, _, _} = result}
       ) do
    handle_refill_stage_partial_retry(state, result)
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

  defp resume_persisted(
         %RunExecutionState{} = state,
         {:stage_admission, _attempt, {:error, failed_run, step_results, _attempted_node_keys}}
       ) do
    terminalize_stage_admission_failure(state, failed_run, step_results)
  end

  defp start_await(%RunExecutionState{} = state, entry, kind) do
    parent = self()
    task_id = entry.task_id
    timeout_ms = state.run.timeout_ms

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        FavnOrchestrator.RunnerTaskResultRouter.await(
          state.run.workspace_id,
          entry.task_id,
          parent,
          notify_started?: true
        )
      end)

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
  defp persist_step_running(%RunExecutionState{} = state, task_id, await, entry) do
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

    case Persistence.persist_run_step(running, :step_running, data) do
      :ok ->
        marked = Map.put(await, :started_persisted?, true)
        state = RunExecutionState.put_await(%{state | run: running}, task_id, marked)
        {:cont, state}

      {:error, :external_cancel} ->
        {:terminal, Snapshots.cancelled_snapshot(state.run)}

      {:error, reason} ->
        OperationalEvents.emit(
          :step_running_persist_failed,
          %{},
          %{run_id: state.run.id, task_id: task_id, reason: reason},
          level: :warning
        )

        {:cont, state}
    end
  end

  defp restore_active_tasks(%RunExecutionState{} = state) do
    task_ids = ActiveTaskSet.active_runner_task_ids(state.run)

    with {:ok, tasks} <- fetch_active_tasks(state.run.workspace_id, task_ids) do
      case tasks do
        [] -> restore_retry_wait(state)
        tasks -> restore_task_waits(state, tasks)
      end
    end
  end

  defp fetch_active_tasks(_workspace_id, []), do: {:ok, []}

  defp fetch_active_tasks(workspace_id, task_ids) do
    Enum.reduce_while(task_ids, {:ok, [], []}, fn task_id, {:ok, tasks, missing_ids} ->
      case RunnerTasks.fetch(workspace_id, task_id) do
        {:ok, task} -> {:cont, {:ok, [task | tasks], missing_ids}}
        {:error, %{kind: :not_found}} -> {:cont, {:ok, tasks, [task_id | missing_ids]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, tasks, []} ->
        {:ok, Enum.reverse(tasks)}

      {:ok, _tasks, missing_ids} ->
        {:error, {:durable_runner_tasks_missing, Enum.sort(missing_ids)}}

      error ->
        error
    end
  end

  defp restore_task_waits(%RunExecutionState{mode: :sequential} = state, [task]) do
    with {:ok, entry} <- restored_entry(state, task),
         index when is_integer(index) <-
           Enum.find_index(state.sequential_refs, fn {_ref, key, _stage} ->
             key == entry.node_key
           end) do
      restored =
        %{state | sequential_index: index, accumulated_results: persisted_node_results(state.run)}
        |> RunExecutionState.add_work(entry)
        |> start_await(entry, :sequential)

      {:ok, %{restored | status: :awaiting}}
    else
      nil -> {:error, {:runner_task_continuation_node_missing, task.task_id}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp restore_task_waits(%RunExecutionState{mode: :sequential}, tasks),
    do: {:error, {:invalid_sequential_runner_task_count, length(tasks)}}

  defp restore_task_waits(%RunExecutionState{mode: :pipeline} = state, tasks) do
    with {:ok, state} <- prepare_pipeline_task_recovery(state, tasks),
         :ok <- ensure_active_pipeline_recovery_position(state.run),
         {:ok, entries} <- restore_entries(state, tasks),
         [stage] <- entries |> Enum.map(& &1.stage) |> Enum.uniq(),
         stage_index when is_integer(stage_index) <-
           Enum.find_index(state.stage_groups, fn {candidate, _keys} -> candidate == stage end),
         {_stage, stage_node_keys} <- Enum.at(state.stage_groups, stage_index),
         {:ok, decisions, deferred_node_keys} <-
           restore_pipeline_stage(state, entries, stage_node_keys) do
      stage_state =
        StageAttemptState.new(
          state.run,
          [],
          entries,
          deferred_node_keys,
          MapSet.new()
        )

      restored =
        %{
          state
          | stage_index: stage_index,
            stage_attempt: entries |> List.first() |> Map.fetch!(:attempt),
            stage_state: stage_state,
            stage_decisions: decisions,
            stage_freshness_context: state.freshness_context,
            accumulated_results: []
        }
        |> start_pipeline_awaits(entries)

      {:ok, %{restored | status: :awaiting}}
    else
      _other ->
        {:error, {:invalid_pipeline_runner_task_continuation, Enum.map(tasks, & &1.task_id)}}
    end
  end

  defp ensure_active_pipeline_recovery_position(run) do
    if RecoveryPosition.outcome_recorded?(run),
      do: {:error, :pipeline_recovery_completed_outcomes_unavailable},
      else: :ok
  end

  defp restore_entries(%RunExecutionState{} = state, tasks) do
    Enum.reduce_while(tasks, {:ok, []}, fn task, {:ok, entries} ->
      case restore_entry(state, task) do
        {:ok, entry} -> {:cont, {:ok, [entry | entries]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp restore_pipeline_stage(state, entries, stage_node_keys) do
    active_node_keys = entries |> Enum.map(& &1.node_key) |> MapSet.new()

    decisions =
      state.run
      |> StageClassifier.decisions(stage_node_keys, state.freshness_context)
      |> Map.merge(Map.new(entries, &{&1.node_key, &1.decision}))

    deferred_node_keys = Enum.reject(stage_node_keys, &MapSet.member?(active_node_keys, &1))

    if Enum.all?(deferred_node_keys, &match?(%{decision: :run}, Map.get(decisions, &1))) do
      {:ok, decisions, deferred_node_keys}
    else
      {:error, :pipeline_recovery_decision_result_missing}
    end
  end

  @spec restore_entry(RunExecutionState.t(), map()) ::
          {:ok, StageEntry.t()} | {:error, term()}
  defp restore_entry(%RunExecutionState{run: %RunState{} = run} = state, task) do
    with {:ok, entry} <- restored_entry(state, task),
         {:ok, lease} <-
           ExecutionAdmission.adopt(run, %{
             asset_step_id: entry.asset_step_id,
             execution_pool: entry.execution_pool,
             stage: entry.stage,
             attempt: entry.attempt
           }) do
      {:ok, %{entry | lease: lease}}
    else
      {:error, {:invalid_recovered_runner_task, _task_id} = reason} ->
        {:error, reason}

      {:error, reason} ->
        {:error, {:execution_lease_adoption_failed, reason}}
    end
  end

  @spec restored_entry(RunExecutionState.t(), map()) ::
          {:ok, StageEntry.t()} | {:error, term()}
  defp restored_entry(
         %RunExecutionState{run: %RunState{id: run_id}},
         %{
           task_id: task_id,
           payload:
             %RunnerWork{
               asset_step_id: asset_step_id,
               attempt: attempt,
               stage: stage
             } = work
         } = task
       )
       when is_binary(task_id) and is_binary(asset_step_id) and is_integer(attempt) and
              attempt > 0 and is_integer(stage) and stage >= 0 do
    context = if is_map(task.orchestration_context), do: task.orchestration_context, else: %{}
    asset_ref = RunnerWork.asset_ref(work)
    node_key = RunnerWork.node_key(work)

    if is_tuple(asset_ref) and is_tuple(node_key) do
      {:ok,
       %{
         run_id: run_id,
         asset_step_id: asset_step_id,
         asset_ref: asset_ref,
         node_key: node_key,
         window: RunnerWork.window(work),
         task_id: task_id,
         assignment_generation: task.assignment_generation,
         runner_pool: task.runner_pool,
         required_runner_release_id: task.required_runner_release_id,
         decision: Map.get(context, :decision, %{}),
         attempt: attempt,
         stage: stage,
         lease: nil,
         materialization_claim: Map.get(context, :materialization_claim),
         execution_pool: RunnerWork.execution_pool(work),
         resource_circuit_permits: Map.get(context, :resource_circuit_permits, []),
         freshness_key: Map.get(context, :freshness_key)
       }}
    else
      {:error, {:invalid_recovered_runner_task, task_id}}
    end
  end

  defp restored_entry(_state, task) when is_map(task),
    do: {:error, {:invalid_recovered_runner_task, Map.get(task, :task_id)}}

  defp restored_entry(_state, _task), do: {:error, {:invalid_recovered_runner_task, nil}}

  defp handle_await_result(%RunExecutionState{} = state, entry, result, :pipeline) do
    settlement =
      try do
        process_await_result(state, entry, result, :pipeline)
      after
        :ok = ActiveTaskSet.release_entry(entry)
      end

    continue_pipeline_settlement(settlement)
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
         failure,
         %CancellationOutcome{status: :already_completed} = outcome
       ) do
    case RunnerTasks.fetch(state.run.workspace_id, await.entry.task_id) do
      {:ok, task} ->
        case durable_task_result(task) do
          {:ok, %RunnerResult{}} = durable_result ->
            handle_await_result(state, await.entry, durable_result, await.kind)

          {:error, reason} ->
            terminalize_unknown_await_outcome(
              state,
              await.entry,
              Map.merge(failure, %{
                kind: :completed_runner_result_unavailable,
                fetch_error: inspect(reason, limit: 20, printable_limit: 1_000)
              }),
              outcome
            )
        end

      {:error, reason} ->
        terminalize_unknown_await_outcome(
          state,
          await.entry,
          Map.merge(failure, %{
            kind: :completed_runner_result_unavailable,
            fetch_error: inspect(reason, limit: 20, printable_limit: 1_000)
          }),
          outcome
        )
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

    state
    |> Sequential.handle_result(entry, result)
    |> handle_sequential_directive()
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
    state
    |> Sequential.resume_retry(retry)
    |> handle_sequential_directive()
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
      %{state | run: run, stage_attempt: retry.next_attempt},
      retry.node_keys,
      retry.next_attempt,
      completed_node_statuses
    )
  end

  defp continue_pipeline(%RunExecutionState{} = state) do
    if state.stage_index >= length(state.stage_groups) do
      terminalize_pipeline_state(state)
    else
      {stage, node_keys} = Enum.at(state.stage_groups, state.stage_index)

      if Persistence.externally_cancelled?(state.run) do
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
  end

  defp continue_stage_classification(
         %RunExecutionState{
           pipeline_continuation: %{
             kind: :stage_classification,
             stage: stage,
             remaining_node_keys: node_keys,
             runnable_node_keys_rev: runnable_rev
           }
         } = state
       ) do
    case StageClassifier.classify(
           state.run,
           state.version,
           stage,
           node_keys,
           state.stage_freshness_context,
           state.terminal_failure
         ) do
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

    case put_freshness_checkpoint(state, stage, 1, state.stage_freshness_context) do
      {:ok, checkpointed} when runnable_node_keys == [] ->
        checkpointed
        |> Map.put(:freshness_context, checkpointed.stage_freshness_context)
        |> Map.put(:stage_index, checkpointed.stage_index + 1)
        |> Map.put(:pipeline_continuation, nil)
        |> Map.put(:status, :starting)
        |> defer_pipeline_continue()

      {:ok, checkpointed} ->
        checkpointed
        |> Map.put(:pipeline_continuation, nil)
        |> Map.put(:status, :submitting)
        |> submit_pipeline_stage_attempt(runnable_node_keys, 1)

      {:error, reason} ->
        terminalize_checkpoint_failure(state, reason)
    end
  end

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

    with {:ok, state} <-
           ensure_freshness_checkpoint(
             state,
             stage,
             attempt,
             state.stage_freshness_context
           ) do
      submit_pipeline_stage_attempt_with_checkpoint(
        state,
        node_keys,
        attempt,
        completed_node_statuses
      )
    else
      {:error, reason} -> terminalize_checkpoint_failure(state, reason)
    end
  end

  defp submit_pipeline_stage_attempt_with_checkpoint(
         state,
         node_keys,
         attempt,
         completed_node_statuses
       ) do
    state = %{state | run: RecoveryPosition.clear_outcome(state.run)}

    case submit_stage_entries(
           state,
           state.run,
           node_keys,
           attempt,
           MapSet.new(),
           completed_node_statuses
         ) do
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
              stage_admission_deadline_ms: stage_admission_deadline(run_after_submit.timeout_ms)
          }
          |> RunExecutionState.put_admission_waiters(waiters)

        state
        |> start_pipeline_awaits(entries)
        |> after_starting_pipeline_awaits(entries)

      {:partial_retry, retry_run, entries, deferred_node_keys, retry_node_key, failure,
       queued_steps, waiters, admission_failure, deferred_refill_cause} ->
        handle_initial_stage_partial_retry(
          state,
          attempt,
          {:partial_retry, retry_run, entries, deferred_node_keys, retry_node_key, failure,
           queued_steps, waiters, admission_failure, deferred_refill_cause},
          completed_node_statuses
        )

      {:error, failed_run, step_results, _attempted_node_keys, cleanup_entries} ->
        terminalize_stage_admission_failure(state, failed_run, step_results, cleanup_entries)

      {:error, failed_run, step_results, _attempted_node_keys} ->
        terminalize_stage_admission_failure(state, failed_run, step_results)

      {:persist_retry, %PersistenceRetry{} = retry, reason} ->
        {:persist_retry, state, retry, reason}
    end
  end

  defp handle_initial_stage_partial_retry(
         state,
         attempt,
         {:partial_retry, retry_run, entries, deferred_node_keys, retry_node_key, failure,
          queued_steps, waiters, admission_failure, deferred_refill_cause},
         completed_node_statuses \\ %{}
       ) do
    stage_state =
      retry_run
      |> StageAttemptState.new(
        state.accumulated_results,
        entries,
        deferred_node_keys,
        queued_steps,
        admission_failure,
        deferred_refill_cause
      )
      |> Map.update!(:node_statuses, &Map.merge(completed_node_statuses, &1))
      |> add_admission_retry(retry_run, retry_node_key, attempt, failure)

    state =
      %{
        state
        | run: retry_run,
          stage_state: stage_state,
          stage_attempt: attempt,
          stage_admission_deadline_ms: stage_admission_deadline(retry_run.timeout_ms)
      }
      |> RunExecutionState.put_admission_waiters(waiters)

    state
    |> start_pipeline_awaits(entries)
    |> after_starting_pipeline_awaits(entries)
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

    state.stage_state
    |> Map.put(:run, state.run)
    |> StageResult.process(
      Map.merge(entry, %{
        version: state.version,
        manifest_index: state.manifest_index,
        freshness_context: state.stage_freshness_context
      }),
      result,
      %{
        stage: entry.stage,
        attempt: entry.attempt
      }
    )
    |> prepare_pipeline_settlement(state)
  end

  defp prepare_pipeline_settlement({:cont, next_stage_state}, state),
    do: {:pipeline_settled, %{state | run: next_stage_state.run, stage_state: next_stage_state}}

  defp prepare_pipeline_settlement({:post_step_pending, next_stage_state, pending}, state) do
    state = %{state | run: next_stage_state.run, stage_state: next_stage_state}
    {:pipeline_settled, start_post_step_worker(state, pending)}
  end

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
    |> stop_workers_on_terminal(state)
  end

  defp continue_pipeline_settlement(result), do: result

  # The unchanged reconciler runs in a worker so the blocking wait on runner
  # inspection tasks never occupies the run process. The reply and any exit
  # arrive as ordinary messages keyed by the monitor reference.
  defp start_post_step_worker(%RunExecutionState{} = state, pending) do
    entry = pending.entry

    task =
      Task.Supervisor.async_nolink(
        @post_step_supervisor,
        InitialTargetGenerationReconciler,
        :reconcile,
        [entry]
      )

    OperationalEvents.emit(
      :post_step_continuation_started,
      %{},
      %{workspace_id: state.run.workspace_id, run_id: state.run.id, node_key: entry.node_key},
      level: :debug
    )

    RunExecutionState.put_post_step_continuation(state, task.ref, %{
      pid: task.pid,
      pending: pending
    })
  end

  defp settle_post_step(
         %RunExecutionState{stage_state: %StageAttemptState{}} = state,
         %{pending: pending},
         result
       ) do
    emit_post_step_settled(state, pending.entry, result)

    state.stage_state
    |> Map.put(:run, state.run)
    |> StageResult.finish_post_step(pending, result)
    |> prepare_pipeline_settlement(state)
    |> continue_pipeline_settlement()
  end

  # Unreachable while pending continuations gate stage progress; kept loud so a
  # future regression never silently drops a node's settlement.
  defp settle_post_step(%RunExecutionState{} = state, %{pending: pending}, _result) do
    OperationalEvents.emit(
      :post_step_continuation_orphaned,
      %{},
      %{
        workspace_id: state.run.workspace_id,
        run_id: state.run.id,
        node_key: pending |> Map.get(:entry, %{}) |> Map.get(:node_key)
      },
      level: :warning
    )

    {:cont, state}
  end

  defp emit_post_step_settled(state, entry, :ok) do
    OperationalEvents.emit(
      :post_step_continuation_settled,
      %{},
      %{
        workspace_id: state.run.workspace_id,
        run_id: state.run.id,
        node_key: entry.node_key,
        outcome: :ok
      },
      level: :debug
    )
  end

  defp emit_post_step_settled(state, entry, {:error, reason}) do
    OperationalEvents.emit(
      :post_step_continuation_failed,
      %{},
      %{
        workspace_id: state.run.workspace_id,
        run_id: state.run.id,
        node_key: entry.node_key,
        reason: reason_class(reason)
      },
      level: :warning
    )
  end

  defp post_step_result(:ok), do: :ok
  defp post_step_result({:error, _reason} = error), do: error
  defp post_step_result(other), do: {:error, {:invalid_post_step_result, bounded_inspect(other)}}

  defp reason_class({class, _detail}) when is_atom(class), do: class
  defp reason_class(class) when is_atom(class), do: class
  defp reason_class(%{type: type}) when is_atom(type), do: type
  defp reason_class(_reason), do: :unknown

  defp bounded_inspect(term), do: inspect(term, limit: 20, printable_limit: 1_000)

  defp after_pipeline_progress(%RunExecutionState{stage_state: nil} = state), do: {:cont, state}

  defp after_pipeline_progress(%RunExecutionState{} = state) do
    if Persistence.externally_cancelled?(state.run) do
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
    state = clear_admission_waiters(state)

    case submit_stage_entries(
           state,
           state.run,
           state.stage_state.deferred_node_keys,
           state.stage_attempt,
           state.stage_state.queued_steps
         ) do
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

      {:partial_retry, retry_run, entries, next_deferred_node_keys, retry_node_key, failure,
       next_queued_steps, waiters, admission_failure, deferred_refill_cause} ->
        handle_refill_stage_partial_retry(
          state,
          {:partial_retry, retry_run, entries, next_deferred_node_keys, retry_node_key, failure,
           next_queued_steps, waiters, admission_failure, deferred_refill_cause}
        )

      {:error, failed_run, step_results, _attempted_node_keys, cleanup_entries} ->
        terminalize_stage_admission_failure(state, failed_run, step_results, cleanup_entries)

      {:error, failed_run, step_results, _attempted_node_keys} ->
        terminalize_stage_admission_failure(state, failed_run, step_results)

      {:persist_retry, %PersistenceRetry{} = retry, reason} ->
        {:persist_retry, state, retry, reason}
    end
  end

  defp handle_refill_stage_partial_retry(
         state,
         {:partial_retry, retry_run, entries, next_deferred_node_keys, retry_node_key, failure,
          next_queued_steps, waiters, admission_failure, deferred_refill_cause}
       ) do
    stage_state =
      state.stage_state
      |> StageAttemptState.add_entries(
        entries,
        retry_run,
        next_deferred_node_keys,
        next_queued_steps,
        deferred_refill_cause
      )
      |> add_admission_retry(retry_run, retry_node_key, state.stage_attempt, failure)
      |> StageAttemptState.add_admission_failure(admission_failure)

    %{state | run: retry_run, stage_state: stage_state}
    |> RunExecutionState.put_admission_waiters(waiters)
    |> start_pipeline_awaits(entries)
    |> after_starting_pipeline_awaits(entries)
  end

  defp terminalize_stage_admission_failure(
         state,
         failed_run,
         step_results,
         cleanup_entries \\ []
       ) do
    failure = %{status: failed_run.status, error: failed_run.error}
    work_set = Enum.reduce(cleanup_entries, state.work_set, &ActiveTaskSet.add_entry(&2, &1))

    state =
      cancel_terminal_stage_tasks(
        %{state | work_set: work_set},
        failed_run,
        %{kind: :stage_admission_failure, error: failed_run.error}
      )

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

  defp add_admission_retry(stage_state, run_state, node_key, attempt, failure) do
    retry_delay_ms =
      StepAttemptLifecycle.retry_delay_ms(run_state, node_key, attempt, failure)

    StageAttemptState.add_admission_retry(stage_state, node_key, retry_delay_ms)
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

      case PersistenceRetry.persist(retry) do
        :ok ->
          schedule_pipeline_retry_timer(
            %{state | run: checkpointed},
            node_keys,
            stage,
            attempt,
            next_retry_at
          )

        {:error, :external_cancel} ->
          {:terminal, Snapshots.cancelled_snapshot(checkpointed)}

        {:error, reason} ->
          {:persist_retry, %{state | pipeline_continuation: nil}, retry, reason}
      end
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
        next_run = RecoveryPosition.clear_outcome(next_run)

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
        failed_run = RecoveryPosition.clear_outcome(failed_run)

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
    Process.cancel_timer(await.timeout_ref)
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
        accumulated_results: persisted_node_results(state.run),
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

  defp persisted_node_results(%RunState{result: result}) when is_map(result) do
    Map.get(result, :node_results, Map.get(result, "node_results", []))
  end

  defp persisted_node_results(%RunState{}), do: []

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

  defp prepare_pipeline_task_recovery(
         %RunExecutionState{freshness_checkpoint: reference} = state,
         tasks
       )
       when is_map(reference) do
    if Enum.all?(tasks, fn task ->
         pipeline_task_checkpoint_valid?(task, reference) or
           legacy_pipeline_task_checkpoint_valid?(
             task,
             reference,
             state.freshness_context
           )
       end),
       do: {:ok, state},
       else: {:error, :runner_task_freshness_checkpoint_mismatch}
  end

  defp prepare_pipeline_task_recovery(
         %RunExecutionState{freshness_checkpoint: nil} = state,
         tasks
       ) do
    with {:ok, {legacy_context, stage, attempt}} <- legacy_pipeline_task_context(tasks),
         {:ok, checkpointed} <-
           put_freshness_checkpoint(
             %{state | freshness_context: legacy_context},
             stage,
             attempt,
             legacy_context
           ) do
      {:ok, checkpointed}
    end
  end

  defp pipeline_task_checkpoint_valid?(task, reference) do
    context = task.orchestration_context
    work = task.payload
    candidate = PipelineTaskContinuation.checkpoint(context)

    work.stage == reference.stage and work.attempt == reference.attempt and
      PipelineTaskContinuation.valid?(context) and
      PipelineFreshnessCheckpoint.matches?(reference, candidate)
  end

  defp legacy_pipeline_task_checkpoint_valid?(task, reference, expected_context) do
    with %RunnerWork{} = work <- task.payload,
         true <- work.stage == reference.stage and work.attempt == reference.attempt,
         {:ok, context} <-
           PipelineTaskContinuation.legacy_freshness_context(task.orchestration_context) do
      context == expected_context
    else
      _invalid -> false
    end
  end

  defp legacy_pipeline_task_context(tasks) when is_list(tasks) and tasks != [] do
    Enum.reduce_while(tasks, {:ok, nil}, fn task, {:ok, expected} ->
      with %RunnerWork{} = work <- task.payload,
           {:ok, context} <-
             PipelineTaskContinuation.legacy_freshness_context(task.orchestration_context),
           {:ok, next} <- merge_legacy_task_context(expected, context, work.stage, work.attempt) do
        {:cont, {:ok, next}}
      else
        _invalid -> {:halt, {:error, :legacy_pipeline_runner_task_continuation_invalid}}
      end
    end)
    |> case do
      {:ok, {context, stage, attempt}} -> {:ok, {context, stage, attempt}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp legacy_pipeline_task_context(_tasks),
    do: {:error, :legacy_pipeline_runner_task_continuation_invalid}

  defp merge_legacy_task_context(nil, context, stage, attempt),
    do: {:ok, {context, stage, attempt}}

  defp merge_legacy_task_context(
         {expected_context, expected_stage, expected_attempt} = expected,
         context,
         stage,
         attempt
       ) do
    if context == expected_context and stage == expected_stage and attempt == expected_attempt,
      do: {:ok, expected},
      else: {:error, :legacy_pipeline_runner_task_continuation_mismatch}
  end

  defp ensure_freshness_checkpoint(state, stage, attempt, context) do
    case state.freshness_checkpoint do
      %{stage: ^stage, attempt: ^attempt, sequence: sequence}
      when sequence == state.run.event_seq ->
        {:ok, state}

      _missing_or_stale ->
        put_freshness_checkpoint(state, stage, attempt, context)
    end
  end

  defp put_freshness_checkpoint(state, stage, attempt, context) do
    case PipelineFreshnessCheckpoint.put(
           state.run,
           stage,
           attempt,
           context,
           state.freshness_checkpoint
         ) do
      {:ok, reference} -> {:ok, %{state | freshness_checkpoint: reference}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp continue_after_stage_checkpoint(state, context) do
    {stage, _node_keys} = Enum.at(state.stage_groups, state.stage_index)

    case put_freshness_checkpoint(state, stage, state.stage_attempt, context) do
      {:ok, checkpointed} ->
        continue_pipeline(%{
          checkpointed
          | stage_index: checkpointed.stage_index + 1,
            status: :starting
        })

      {:error, reason} ->
        terminalize_checkpoint_failure(state, reason)
    end
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

  defp submit_stage_entries(
         %RunExecutionState{} = state,
         %RunState{} = run_state,
         node_keys,
         attempt,
         queued_steps,
         completed_node_statuses \\ %{}
       ) do
    {stage, _node_keys} = Enum.at(state.stage_groups, state.stage_index)

    StageAdmission.submit(%{
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
    })
  end

  defp durable_task_result(%{result: %RunnerResult{} = result}), do: {:ok, result}

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
