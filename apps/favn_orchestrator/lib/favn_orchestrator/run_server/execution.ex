defmodule FavnOrchestrator.RunServer.Execution do
  @moduledoc """
  Executes manifest-pinned runs against the configured runner client.

  Pipeline runs execute one topological stage at a time. Entries in the same
  stage are independent siblings, so a failed sibling must not cancel the rest
  of that stage. The current stage is drained and all submitted sibling outcomes
  are persisted before the run decides whether later stages may continue.

  Freshness classification happens between drained stages: already-fresh nodes
  are recorded as skipped, successful executed nodes dirty downstream nodes in
  the same graph, and downstream nodes with failed dependencies are blocked.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.RunExecutionCleanup
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunnerClientValidator
  alias FavnOrchestrator.RunnerLogBridge
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.ExecutionPool
  alias FavnOrchestrator.RunServer.Execution.FreshnessContext
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.RunWorkSet
  alias FavnOrchestrator.RunServer.Execution.Sequential
  alias FavnOrchestrator.RunServer.Execution.StageAdmission
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunServer.Execution.StageClassifier
  alias FavnOrchestrator.RunServer.Execution.StageResult
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RuntimeConfig

  @stage_admission_timeout_buffer_ms 2_000
  @stage_admission_backstop_retry_ms 1_000
  @deferred_stage_retry_ms 100
  @await_task_timeout_buffer_ms 2_000

  @type step_event ::
          :continue
          | {:runner_result, String.t(), term()}
          | {:runner_await_down, String.t(), reference(), term()}
          | {:attempt_timeout, String.t(), reference()}
          | {:retry_attempt, reference()}
          | {:stage_admission_timeout, reference()}
          | {:execution_admission_wakeup, String.t(), non_neg_integer()}

  @spec start_state(RunState.t(), Version.t()) ::
          {:ok, RunExecutionState.t()} | {:terminal, RunState.t()}
  def start_state(%RunState{submit_kind: submit_kind} = run_state, %Version{} = _version)
      when submit_kind in [:backfill_asset, :backfill_pipeline] do
    {:terminal,
     Snapshots.snapshot_update(run_state,
       status: :error,
       error: {:unsupported_submit_kind, submit_kind},
       runner_execution_id: nil,
       result: %{status: :error, asset_results: [], metadata: run_state.metadata}
     )}
  end

  def start_state(%RunState{} = run_state, %Version{} = version) do
    runner_client = configured_runner_client()
    runner_opts = configured_runner_opts()

    case execution_mode(run_state) do
      :pipeline ->
        with :ok <- RunnerClientValidator.validate(runner_client),
             :ok <- runner_client.register_manifest(version, runner_opts),
             {:ok, freshness_context} <- FreshnessContext.initialize(run_state, version) do
          state =
            RunExecutionState.new(run_state, version,
              mode: :pipeline,
              runner_client: runner_client,
              runner_opts: runner_opts,
              stage_groups: pipeline_stage_groups(run_state),
              freshness_context: freshness_context
            )

          {:ok, restore_retry_wait(state)}
        else
          {:error, reason} -> pipeline_start_failure(run_state, reason)
        end

      :sequential ->
        state =
          RunExecutionState.new(run_state, version,
            mode: :sequential,
            runner_client: runner_client,
            runner_opts: runner_opts,
            sequential_refs: Sequential.refs(run_state)
          )

        {:ok, restore_retry_wait(state)}
    end
  end

  @spec handle_event(RunExecutionState.t(), step_event()) ::
          {:cont, RunExecutionState.t()}
          | {:terminal, RunState.t()}
          | {:persist_retry, RunExecutionState.t(), PersistenceRetry.t(), term()}
  def handle_event(%RunExecutionState{} = state, :continue), do: continue_state(state)

  def handle_event(%RunExecutionState{} = state, {:runner_result, execution_id, result}) do
    case RunExecutionState.pop_await(state, execution_id) do
      {nil, state} ->
        {:cont, state}

      {await, state} ->
        Process.cancel_timer(await.timeout_ref)
        Process.demonitor(await.monitor_ref, [:flush])
        handle_await_result(state, await.entry, result, await.kind)
    end
  end

  def handle_event(
        %RunExecutionState{} = state,
        {:runner_await_down, execution_id, monitor_ref, reason}
      ) do
    case Map.get(state.awaits, execution_id) do
      %{monitor_ref: ^monitor_ref} ->
        {await, state} = RunExecutionState.pop_await(state, execution_id)
        Process.cancel_timer(await.timeout_ref)

        handle_await_result(
          state,
          await.entry,
          {:error, %{type: :await_task_failed, kind: :exit, reason: inspect(reason)}},
          await.kind
        )

      _stale_or_missing ->
        {:cont, state}
    end
  end

  def handle_event(%RunExecutionState{} = state, {:attempt_timeout, execution_id, timer_ref}) do
    case Map.get(state.awaits, execution_id) do
      %{timeout_token: ^timer_ref} ->
        {await, state} = RunExecutionState.pop_await(state, execution_id)
        Process.exit(await.pid, :kill)
        Process.demonitor(await.monitor_ref, [:flush])
        handle_await_result(state, await.entry, {:error, :timeout}, await.kind)

      _stale_or_missing ->
        {:cont, state}
    end
  end

  def handle_event(%RunExecutionState{} = state, {:retry_attempt, timer_ref}) do
    case RunExecutionState.pop_retry_timer(state, timer_ref) do
      {nil, state} -> {:cont, state}
      {%{payload: retry}, state} -> resume_retry(state, retry)
    end
  end

  def handle_event(%RunExecutionState{} = state, {:stage_admission_timeout, timer_ref}) do
    case RunExecutionState.pop_admission_timer(state, timer_ref) do
      {nil, state} ->
        {:cont, state}

      {%{payload: _timer}, state} when map_size(state.awaits) > 0 ->
        {:cont, %{state | status: :awaiting}}

      {%{payload: %{kind: :admission_retry}}, state} ->
        refill_or_schedule_admission(state)

      {%{payload: %{kind: :retry}}, state} ->
        after_pipeline_progress(state)

      {%{payload: _timer}, state} ->
        timeout_admission_wait(state)
    end
  end

  def handle_event(
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
          | {:persist_retry, RunExecutionState.t(), PersistenceRetry.t(), term()}
  def retry_persistence(%RunExecutionState{} = state, %PersistenceRetry{} = retry) do
    case PersistenceRetry.persist(retry) do
      :ok -> resume_persisted(state, retry.resume)
      {:error, :external_cancel} -> {:terminal, Snapshots.cancelled_snapshot(state.run)}
      {:error, reason} -> {:persist_retry, state, retry, reason}
    end
  end

  @spec cancel(RunExecutionState.t(), term()) :: RunState.t()
  def cancel(%RunExecutionState{} = state, reason) do
    reason = %{kind: :external_cancel, reason: reason}
    state = state |> stop_await_processes() |> RunExecutionState.cancel_timers()

    {cancelled_run, _work_set} =
      RunWorkSet.cancel_all(
        state.run,
        state.work_set,
        reason,
        state.runner_client,
        state.runner_opts
      )

    :ok = RunWorkSet.cleanup_all(state.work_set, reason)
    :ok = RunExecutionCleanup.release_admission(cancelled_run.id)

    cancellation_terminal(cancelled_run, accumulated_results(state))
  end

  defp accumulated_results(%RunExecutionState{mode: :sequential} = state),
    do: ResultBuilder.sort_asset_results(state.run, state.accumulated_results)

  defp accumulated_results(%RunExecutionState{} = state), do: state.accumulated_results

  defp cancellation_terminal(%RunState{} = run_state, accumulated_results) do
    outcomes = Map.get(run_state.metadata, :cancel_outcomes, [])

    cond do
      outcomes == [] ->
        Snapshots.cancelled_terminal(run_state, accumulated_results)

      Enum.all?(outcomes, &CancellationOutcome.confirmed?/1) ->
        Snapshots.cancelled_terminal(run_state, accumulated_results)

      true ->
        failed =
          RunState.transition(run_state,
            status: :error,
            runner_execution_id: nil,
            error: %{type: :runner_cancel_unconfirmed, outcomes: outcomes}
          )

        Snapshots.terminalize_failed_run(failed, accumulated_results)
    end
  end

  defp pipeline_start_failure(%RunState{} = run_state, reason) do
    {:terminal,
     Snapshots.snapshot_update(run_state,
       status: :error,
       error: reason,
       runner_execution_id: nil,
       result: ResultBuilder.pipeline_result(run_state, :error, [])
     )}
  end

  defp execution_mode(%RunState{submit_kind: :pipeline}), do: :pipeline

  defp execution_mode(%RunState{submit_kind: :rerun, metadata: %{replay_submit_kind: :pipeline}}),
    do: :pipeline

  defp execution_mode(%RunState{}), do: :sequential

  defp continue_state(%RunExecutionState{status: :retry_wait} = state), do: {:cont, state}

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
    |> handle_pipeline_settlement(state)
  end

  defp resume_persisted(%RunExecutionState{} = state, {:pipeline_retry, resume}) do
    state = %{state | run: resume.run}

    persist_pipeline_retry_events(
      state,
      resume.run,
      resume.remaining_node_keys,
      resume.all_node_keys,
      resume.stage,
      resume.attempt,
      resume.retry_delays,
      resume.next_retry_at
    )
  end

  defp start_await(%RunExecutionState{} = state, entry, kind) do
    parent = self()
    execution_id = entry.execution_id
    timeout_ms = state.run.timeout_ms
    runner_client = state.runner_client
    runner_opts = state.runner_opts

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        send(
          parent,
          {:runner_result, execution_id,
           await_runner_result(entry, timeout_ms, runner_client, runner_opts)}
        )
      end)

    timeout_token = make_ref()

    timeout_ref =
      Process.send_after(
        parent,
        {:attempt_timeout, execution_id, timeout_token},
        timeout_ms + @await_task_timeout_buffer_ms
      )

    RunExecutionState.put_await(state, execution_id, %{
      pid: pid,
      monitor_ref: monitor_ref,
      timeout_token: timeout_token,
      timeout_ref: timeout_ref,
      entry: entry,
      kind: kind
    })
  end

  defp handle_await_result(%RunExecutionState{} = state, entry, result, kind) do
    process_await_result(state, entry, result, kind)
  after
    :ok = RunWorkSet.release_entry(entry)
  end

  defp process_await_result(%RunExecutionState{} = state, entry, result, :sequential) do
    state = elem(RunExecutionState.complete_work(state, entry.execution_id), 1)
    _ = RunExecutionOwnership.mark_finish_persist_pending(state.run.id, entry.execution_id)

    state
    |> Sequential.handle_result(entry, result)
    |> handle_sequential_directive()
  end

  defp process_await_result(%RunExecutionState{} = state, entry, result, :pipeline) do
    _ = RunExecutionOwnership.mark_finish_persist_pending(state.run.id, entry.execution_id)
    handle_pipeline_await_result(state, entry, result)
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

    submit_pipeline_stage_attempt(
      %{state | run: run, stage_attempt: retry.next_attempt},
      retry.node_keys,
      retry.next_attempt
    )
  end

  defp continue_pipeline(%RunExecutionState{} = state) do
    if state.stage_index >= length(state.stage_groups) do
      terminalize_pipeline_state(state)
    else
      {stage, node_keys} = Enum.at(state.stage_groups, state.stage_index)

      if Persistence.externally_cancelled?(state.run.id) do
        {:terminal, Snapshots.cancelled_terminal(state.run, state.accumulated_results)}
      else
        case StageClassifier.classify(
               state.run,
               state.version,
               stage,
               node_keys,
               state.freshness_context,
               state.terminal_failure
             ) do
          {:ok, classified_run, runnable_node_keys, decisions, classified_context,
           next_terminal_failure} ->
            state = %{
              state
              | run: classified_run,
                stage_decisions: decisions,
                stage_freshness_context: classified_context,
                terminal_failure: next_terminal_failure || state.terminal_failure,
                status: :submitting
            }

            if runnable_node_keys == [] do
              continue_pipeline(%{
                state
                | stage_index: state.stage_index + 1,
                  freshness_context: classified_context
              })
            else
              submit_pipeline_stage_attempt(state, runnable_node_keys, 1)
            end

          {:error, failed_run} ->
            all_results = ResultBuilder.sort_asset_results(failed_run, state.accumulated_results)
            {:terminal, terminalize_pipeline_failed_run(failed_run, all_results)}
        end
      end
    end
  end

  defp submit_pipeline_stage_attempt(%RunExecutionState{} = state, node_keys, attempt) do
    case submit_stage_entries(state, state.run, node_keys, attempt) do
      {:ok, run_after_submit, entries, deferred_node_keys, queued_steps, waiters} ->
        stage_state =
          StageAttemptState.new(
            run_after_submit,
            state.accumulated_results,
            entries,
            deferred_node_keys,
            queued_steps
          )

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
       queued_steps, waiters} ->
        stage_state =
          retry_run
          |> StageAttemptState.new(
            state.accumulated_results,
            entries,
            deferred_node_keys,
            queued_steps
          )
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

      {:error, failed_run, step_results, _attempted_node_keys} ->
        {:terminal,
         terminalize_pipeline_failed_run(
           failed_run,
           ResultBuilder.sort_asset_results(
             failed_run,
             state.accumulated_results ++ step_results
           ),
           %{status: failed_run.status, error: failed_run.error}
         )}

      {:retry, retry_run, retry_node_keys, attempted_node_keys} ->
        attempted_node_keys = MapSet.new(attempted_node_keys)

        retry_delays =
          Map.new(retry_node_keys, fn node_key ->
            failure =
              if MapSet.member?(attempted_node_keys, node_key), do: retry_run.error, else: nil

            {node_key, StepAttemptLifecycle.retry_delay_ms(retry_run, node_key, attempt, failure)}
          end)

        stage_state = %StageAttemptState{
          run: retry_run,
          results: Enum.reverse(state.accumulated_results),
          retry_refs: Enum.reverse(retry_node_keys),
          retry_delays: retry_delays,
          attempted_node_keys: MapSet.to_list(attempted_node_keys)
        }

        schedule_pipeline_retry(%{
          state
          | run: retry_run,
            stage_state: stage_state,
            stage_attempt: attempt
        })
    end
  end

  defp start_pipeline_awaits(%RunExecutionState{} = state, entries) when is_list(entries) do
    Enum.reduce(entries, state, fn entry, acc ->
      acc
      |> RunExecutionState.add_work(entry)
      |> start_await(entry, :pipeline)
    end)
  end

  defp handle_pipeline_await_result(%RunExecutionState{} = state, entry, result) do
    state = elem(RunExecutionState.complete_work(state, entry.execution_id), 1)

    state.stage_state
    |> Map.put(:run, state.run)
    |> StageResult.process(entry, result, %{
      stage: entry.stage,
      attempt: entry.attempt,
      runner_client: state.runner_client,
      runner_opts: state.runner_opts
    })
    |> handle_pipeline_settlement(state)
  end

  defp handle_pipeline_settlement({:cont, next_stage_state}, state) do
    %{state | run: next_stage_state.run, stage_state: next_stage_state}
    |> after_pipeline_progress()
  end

  defp handle_pipeline_settlement(
         {:halt, {:error, failed_run, next_results, _attempted_node_keys}},
         state
       ) do
    _state = stop_all_awaits(%{state | run: failed_run}, :stopped_pending_await)

    {:terminal,
     terminalize_pipeline_failed_run(
       failed_run,
       ResultBuilder.sort_asset_results(failed_run, next_results),
       %{status: failed_run.status, error: failed_run.error}
     )}
  end

  defp handle_pipeline_settlement(
         {:persist_retry, %PersistenceRetry{} = retry, reason},
         state
       ) do
    {:persist_retry, state, retry, reason}
  end

  defp after_pipeline_progress(%RunExecutionState{stage_state: nil} = state), do: {:cont, state}

  defp after_pipeline_progress(%RunExecutionState{} = state) do
    cond do
      state.stage_state.deferred_node_keys != [] and state.stage_state.terminal_failure == nil ->
        cond do
          map_size(state.awaits) > 0 ->
            refill_or_schedule_admission(state)

          map_size(state.admission_waiters) > 0 ->
            schedule_admission_timeout(state)

          true ->
            refill_or_schedule_admission(state)
        end

      map_size(state.awaits) > 0 ->
        {:cont, %{state | status: :awaiting}}

      state.stage_state.retry_refs != [] and state.stage_state.terminal_failure == nil ->
        schedule_pipeline_retry(state)

      true ->
        finalize_pipeline_stage(state)
    end
  end

  defp refill_or_schedule_admission(%RunExecutionState{} = state) do
    state = clear_admission_waiters(state)

    case submit_stage_entries(
           state,
           state.stage_state.run,
           state.stage_state.deferred_node_keys,
           state.stage_attempt,
           state.stage_state.queued_steps
         ) do
      {:ok, next_run, [], next_deferred_node_keys, next_queued_steps, waiters} ->
        stage_state =
          StageAttemptState.defer_only(
            state.stage_state,
            next_run,
            next_deferred_node_keys,
            next_queued_steps
          )

        state =
          %{state | run: next_run, stage_state: stage_state}
          |> RunExecutionState.put_admission_waiters(waiters)

        cond do
          next_deferred_node_keys == [] ->
            finalize_pipeline_stage(state)

          map_size(state.awaits) > 0 ->
            {:cont, %{state | status: :awaiting}}

          waiters != [] ->
            schedule_admission_timeout(state)

          true ->
            schedule_deferred_retry(state)
        end

      {:ok, next_run, entries, next_deferred_node_keys, next_queued_steps, waiters} ->
        stage_state =
          StageAttemptState.add_entries(
            state.stage_state,
            entries,
            next_run,
            next_deferred_node_keys,
            next_queued_steps
          )

        %{state | run: next_run, stage_state: stage_state}
        |> RunExecutionState.put_admission_waiters(waiters)
        |> start_pipeline_awaits(entries)
        |> after_starting_pipeline_awaits(entries)

      {:partial_retry, retry_run, entries, next_deferred_node_keys, retry_node_key, failure,
       next_queued_steps, waiters} ->
        stage_state =
          state.stage_state
          |> StageAttemptState.add_entries(
            entries,
            retry_run,
            next_deferred_node_keys,
            next_queued_steps
          )
          |> add_admission_retry(retry_run, retry_node_key, state.stage_attempt, failure)

        %{state | run: retry_run, stage_state: stage_state}
        |> RunExecutionState.put_admission_waiters(waiters)
        |> start_pipeline_awaits(entries)
        |> after_starting_pipeline_awaits(entries)

      {:retry, retry_run, retry_node_keys, attempted_node_keys} ->
        attempted_node_keys = MapSet.new(attempted_node_keys)

        stage_state =
          Enum.reduce(retry_node_keys, state.stage_state, fn node_key, stage_state ->
            failure =
              if MapSet.member?(attempted_node_keys, node_key), do: retry_run.error, else: nil

            add_admission_retry(
              stage_state,
              retry_run,
              node_key,
              state.stage_attempt,
              failure
            )
          end)
          |> StageAttemptState.defer_only(retry_run, [], state.stage_state.queued_steps)

        %{state | run: retry_run, stage_state: stage_state}
        |> after_pipeline_progress()

      {:error, failed_run, step_results, _attempted_node_keys} ->
        {:terminal,
         terminalize_pipeline_failed_run(
           failed_run,
           ResultBuilder.sort_asset_results(
             failed_run,
             state.accumulated_results ++ step_results
           ),
           %{status: failed_run.status, error: failed_run.error}
         )}
    end
  end

  defp add_admission_retry(stage_state, run_state, node_key, attempt, failure) do
    retry_delay_ms =
      StepAttemptLifecycle.retry_delay_ms(run_state, node_key, attempt, failure)

    StageAttemptState.add_admission_retry(stage_state, node_key, retry_delay_ms)
  end

  defp after_starting_pipeline_awaits(%RunExecutionState{} = state, [_ | _]) do
    if state.stage_state.deferred_node_keys != [] and map_size(state.awaits) > 0 do
      {:cont, %{state | status: :awaiting}}
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

  defp schedule_deferred_retry(%RunExecutionState{} = state) do
    now = System.monotonic_time(:millisecond)
    deadline = state.stage_admission_deadline_ms || stage_admission_deadline(state.run.timeout_ms)
    wait_ms = min(@deferred_stage_retry_ms, max(deadline - now, 0))

    if wait_ms == 0 do
      timeout_admission_wait(state)
    else
      timer_token = make_ref()
      timer_ref = Process.send_after(self(), {:stage_admission_timeout, timer_token}, wait_ms)

      {:cont,
       RunExecutionState.put_admission_timer(state, timer_token, timer_ref, %{
         kind: :retry,
         stage_index: state.stage_index
       })}
    end
  end

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
    {stage, _stage_node_keys} = Enum.at(state.stage_groups, state.stage_index)
    node_keys = StageAttemptState.retry_node_keys(state.stage_state)

    retry_delays =
      pipeline_retry_delays(state.stage_state, state.run, node_keys, state.stage_attempt)

    retry_after_ms = retry_delays |> Map.values() |> Enum.max(fn -> 0 end)
    next_retry_at = System.system_time(:millisecond) + retry_after_ms

    persist_pipeline_retry_events(
      state,
      state.stage_state.run,
      node_keys,
      node_keys,
      stage,
      state.stage_attempt,
      retry_delays,
      next_retry_at
    )
  end

  defp persist_pipeline_retry_events(
         state,
         retry_run,
         [],
         all_node_keys,
         stage,
         _attempt,
         _retry_delays,
         next_retry_at
       ) do
    timer_token = make_ref()

    retry_after_ms = retry_remaining_ms(next_retry_at)

    timer_ref =
      Process.send_after(
        self(),
        {:retry_attempt, timer_token},
        retry_after_ms
      )

    retry = %{
      node_keys: all_node_keys,
      next_attempt: state.stage_attempt + 1,
      stage: stage
    }

    {:cont,
     state
     |> Map.put(:run, retry_run)
     |> Map.put(:stage_state, %{state.stage_state | run: retry_run, retry_refs: []})
     |> RunExecutionState.put_retry_timer(timer_token, timer_ref, retry)}
  end

  defp persist_pipeline_retry_events(
         state,
         run_state,
         [node_key | remaining_node_keys],
         all_node_keys,
         stage,
         attempt,
         retry_delays,
         next_retry_at
       ) do
    {retrying, data} =
      pipeline_retry_transition(
        run_state,
        node_key,
        all_node_keys,
        stage,
        attempt,
        state.stage_index,
        retry_delays,
        next_retry_at
      )

    resume =
      {:pipeline_retry,
       %{
         run: retrying,
         remaining_node_keys: remaining_node_keys,
         all_node_keys: all_node_keys,
         stage: stage,
         attempt: attempt,
         retry_delays: retry_delays,
         next_retry_at: next_retry_at
       }}

    retry = PersistenceRetry.new(retrying, :step_retry_scheduled, data, resume)

    case PersistenceRetry.persist(retry) do
      :ok ->
        persist_pipeline_retry_events(
          %{state | run: retrying},
          retrying,
          remaining_node_keys,
          all_node_keys,
          stage,
          attempt,
          retry_delays,
          next_retry_at
        )

      {:error, :external_cancel} ->
        {:terminal, Snapshots.cancelled_snapshot(retrying)}

      {:error, reason} ->
        {:persist_retry, state, retry, reason}
    end
  end

  defp finalize_pipeline_stage(%RunExecutionState{} = state) do
    case StageResult.finalize(state.stage_state) do
      {:ok, next_run, next_results, [], attempted_node_keys} ->
        {next_context, persisted_run} =
          FreshnessContext.record_successes(
            next_run,
            state.version,
            attempted_node_keys,
            state.stage_decisions,
            state.stage_freshness_context
          )

        continue_pipeline(%{
          state
          | run: persisted_run,
            accumulated_results: next_results,
            freshness_context: next_context,
            stage_index: state.stage_index + 1,
            stage_state: nil,
            terminal_failure: state.terminal_failure
        })

      {:ok, _next_run, _next_results, _retry_refs, _attempted_node_keys} ->
        schedule_pipeline_retry(state)

      {:error, failed_run, next_results, attempted_node_keys} ->
        {next_context, persisted_run} =
          FreshnessContext.record_completed_after_failure(
            failed_run,
            state.version,
            attempted_node_keys,
            state.stage_decisions,
            state.stage_freshness_context
          )

        terminal_failure =
          state.terminal_failure || %{status: persisted_run.status, error: persisted_run.error}

        continue_pipeline(%{
          state
          | run: persisted_run,
            accumulated_results: next_results,
            freshness_context: next_context,
            stage_index: state.stage_index + 1,
            stage_state: nil,
            terminal_failure: terminal_failure
        })
    end
  end

  defp terminalize_pipeline_state(%RunExecutionState{terminal_failure: nil} = state) do
    :ok = RunExecutionCleanup.release_admission(state.run.id)
    all_results = ResultBuilder.sort_asset_results(state.run, state.accumulated_results)

    {:terminal,
     Snapshots.snapshot_update(state.run,
       status: :ok,
       error: nil,
       runner_execution_id: nil,
       result: ResultBuilder.pipeline_result(state.run, :ok, all_results)
     )}
  end

  defp terminalize_pipeline_state(%RunExecutionState{} = state) do
    :ok = RunExecutionCleanup.release_admission(state.run.id)
    all_results = ResultBuilder.sort_asset_results(state.run, state.accumulated_results)
    {:terminal, terminalize_pipeline_failed_run(state.run, all_results, state.terminal_failure)}
  end

  defp stop_all_awaits(%RunExecutionState{} = state, reason) do
    Enum.reduce(Map.keys(state.awaits), state, fn execution_id, acc ->
      case RunExecutionState.pop_await(acc, execution_id) do
        {nil, next} ->
          next

        {await, next} ->
          _ =
            state.runner_client.cancel_work(
              await.entry.execution_id,
              Cancellation.envelope(state.run, reason),
              state.runner_opts
            )

          Process.exit(await.pid, :kill)
          Process.demonitor(await.monitor_ref, [:flush])
          Process.cancel_timer(await.timeout_ref)
          :ok = RunWorkSet.release_entry(await.entry)
          :ok = RunWorkSet.fail_entry_claim(await.entry, reason)
          elem(RunExecutionState.complete_work(next, execution_id), 1)
      end
    end)
  end

  defp stop_await_processes(%RunExecutionState{} = state) do
    Enum.reduce(Map.keys(state.awaits), state, fn execution_id, acc ->
      case RunExecutionState.pop_await(acc, execution_id) do
        {nil, next} ->
          next

        {await, next} ->
          Process.exit(await.pid, :kill)
          Process.demonitor(await.monitor_ref, [:flush])
          Process.cancel_timer(await.timeout_ref)
          next
      end
    end)
  end

  defp pipeline_retry_transition(
         %RunState{} = run_state,
         node_key,
         all_node_keys,
         stage,
         attempt,
         stage_index,
         retry_delays,
         next_retry_at
       ) do
    asset_ref = node_asset_ref(run_state, node_key)
    asset_step_id = AssetStepIdentity.asset_step_id(run_state.id, node_key, asset_ref)

    retry_after_ms = retry_delays |> Map.values() |> Enum.max(fn -> 0 end)

    retry_state = %{
      kind: :pipeline,
      retry: %{node_keys: all_node_keys, next_attempt: attempt + 1, stage: stage},
      stage_index: stage_index,
      next_retry_at: next_retry_at
    }

    retrying =
      RunState.transition(run_state,
        status: :running,
        error: nil,
        runner_execution_id: nil,
        metadata:
          Map.merge(run_state.metadata, %{
            retrying: true,
            next_attempt: attempt + 1,
            retry_state: retry_state,
            next_retry_at: next_retry_at
          })
      )

    policy = StepAttemptLifecycle.retry_policy(run_state, node_key)

    data = %{
      asset_ref: asset_ref,
      node_key: node_key,
      asset_step_id: asset_step_id,
      window: node_window(run_state, node_key),
      stage: stage,
      attempt: attempt,
      max_attempts: policy.max_attempts,
      execution_pool: ExecutionPool.for_node(run_state, node_key),
      next_attempt: attempt + 1,
      retry_backoff_ms: retry_after_ms,
      retry_policy: policy,
      retry_policy_source: get_in(run_state.plan.nodes, [node_key, :retry_policy_source]),
      next_retry_at: next_retry_at
    }

    {retrying, data}
  end

  defp pipeline_retry_delays(stage_state, run_state, node_keys, attempt) do
    existing = StageAttemptState.retry_delays(stage_state)

    Map.new(node_keys, fn node_key ->
      {node_key,
       Map.get_lazy(existing, node_key, fn ->
         StepAttemptLifecycle.retry_delay_ms(run_state, node_key, attempt)
       end)}
    end)
  end

  defp clear_retry_state(metadata) do
    metadata
    |> Map.drop([:retry_state, "retry_state", :next_retry_at, "next_retry_at"])
    |> Map.put(:retrying, false)
  end

  defp restore_retry_wait(%RunExecutionState{} = state) do
    case metadata_field(state.run.metadata, :retry_state) do
      %{kind: kind, retry: retry} = retry_state when kind in [:sequential, :pipeline] ->
        token = make_ref()
        remaining_ms = retry_remaining_ms(Map.get(retry_state, :next_retry_at))
        timer_ref = Process.send_after(self(), {:retry_attempt, token}, remaining_ms)

        state
        |> restore_retry_position(retry_state)
        |> RunExecutionState.put_retry_timer(token, timer_ref, retry)

      _missing ->
        state
    end
  end

  defp restore_retry_position(%RunExecutionState{mode: :sequential} = state, retry_state) do
    %{state | sequential_index: Map.get(retry_state, :sequential_index, 0)}
  end

  defp restore_retry_position(%RunExecutionState{mode: :pipeline} = state, retry_state) do
    node_keys = get_in(retry_state, [:retry, :node_keys]) || []

    decisions =
      StageClassifier.decisions(state.run, node_keys, state.freshness_context,
        forced_node_keys: node_keys
      )

    %{
      state
      | stage_index: Map.get(retry_state, :stage_index, 0),
        stage_attempt: get_in(retry_state, [:retry, :next_attempt]) || 1,
        accumulated_results: persisted_node_results(state.run),
        stage_decisions: decisions,
        stage_freshness_context: state.freshness_context
    }
  end

  defp retry_remaining_ms(timestamp) when is_integer(timestamp),
    do: max(timestamp - System.system_time(:millisecond), 0)

  defp retry_remaining_ms(_timestamp), do: 0

  defp persisted_node_results(%RunState{result: result}) when is_map(result) do
    Map.get(result, :node_results, Map.get(result, "node_results", []))
  end

  defp persisted_node_results(%RunState{}), do: []

  defp metadata_field(metadata, key),
    do: Map.get(metadata, key, Map.get(metadata, Atom.to_string(key)))

  defp submit_stage_entries(
         %RunExecutionState{} = state,
         %RunState{} = run_state,
         node_keys,
         attempt,
         queued_steps \\ MapSet.new()
       ) do
    {stage, _node_keys} = Enum.at(state.stage_groups, state.stage_index)

    StageAdmission.submit(%{
      run: run_state,
      version: state.version,
      stage: stage,
      node_keys: node_keys,
      decisions: state.stage_decisions,
      freshness_context: state.stage_freshness_context,
      attempt: attempt,
      runner_client: state.runner_client,
      runner_opts: state.runner_opts,
      queued_steps: queued_steps
    })
  end

  defp await_runner_result(entry, timeout_ms, runner_client, runner_opts) do
    bridge = start_runner_log_bridge(runner_client, entry.execution_id, runner_opts, entry)

    try do
      runner_client.await_result(entry.execution_id, timeout_ms, runner_opts)
    after
      stop_runner_log_bridge(bridge, runner_client, entry.execution_id, runner_opts)
    end
  rescue
    exception ->
      {:error,
       %{
         type: :await_task_failed,
         kind: :error,
         exception: inspect(exception.__struct__),
         reason: Exception.message(exception)
       }}
  catch
    kind, reason ->
      {:error, %{type: :await_task_failed, kind: kind, reason: inspect(reason)}}
  end

  defp stage_admission_deadline(timeout_ms),
    do: System.monotonic_time(:millisecond) + timeout_ms + @stage_admission_timeout_buffer_ms

  defp node_asset_ref(%Favn.Plan{nodes: nodes}, node_key) do
    case Map.fetch(nodes, node_key) do
      {:ok, node} -> node.ref
      :error -> elem(node_key, 0)
    end
  end

  defp node_asset_ref(%RunState{plan: %Favn.Plan{} = plan}, node_key),
    do: node_asset_ref(plan, node_key)

  defp node_window(%RunState{plan: %Favn.Plan{nodes: nodes}}, node_key) do
    case Map.fetch(nodes, node_key) do
      {:ok, node} -> node.window
      :error -> nil
    end
  end

  defp pipeline_stage_groups(%RunState{plan: %Favn.Plan{} = plan}) do
    plan.node_stages
    |> Enum.with_index()
    |> Enum.map(fn {node_keys, stage} -> {stage, node_keys} end)
  end

  defp terminalize_pipeline_failed_run(%RunState{} = failed_run, all_results) do
    Snapshots.snapshot_update(failed_run,
      runner_execution_id: nil,
      result: ResultBuilder.pipeline_result(failed_run, failed_run.status, all_results)
    )
  end

  defp terminalize_pipeline_failed_run(
         %RunState{} = failed_run,
         all_results,
         %{status: status, error: error}
       ) do
    failed_run
    |> Snapshots.snapshot_update(status: status, error: error, runner_execution_id: nil)
    |> then(
      &Snapshots.snapshot_update(&1,
        result: ResultBuilder.pipeline_result(&1, status, all_results)
      )
    )
  end

  defp start_runner_log_bridge(runner_client, execution_id, runner_opts, context) do
    case RunnerLogBridge.start(runner_client, execution_id, runner_opts, context) do
      {:ok, pid} -> pid
      {:error, _reason} -> nil
    end
  end

  defp stop_runner_log_bridge(nil, _runner_client, _execution_id, _runner_opts), do: :ok

  defp stop_runner_log_bridge(pid, runner_client, execution_id, runner_opts) when is_pid(pid) do
    RunnerLogBridge.stop(pid, runner_client, execution_id, runner_opts)
  end

  defp configured_runner_client do
    RuntimeConfig.current().runner_client
  end

  defp configured_runner_opts do
    RuntimeConfig.current().runner_client_opts
  end
end
