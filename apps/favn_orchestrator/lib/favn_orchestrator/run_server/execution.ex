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

  alias Favn.Contracts.RunnerClient
  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias Favn.Run.AssetResult
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.Freshness.Decider
  alias FavnOrchestrator.Freshness.StateWriter
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.RunnerLogBridge
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.RunWorkSet
  alias FavnOrchestrator.RunServer.Execution.StageAdmission
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

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
        case validate_runner_client(runner_client) do
          :ok ->
            case runner_client.register_manifest(version, runner_opts) do
              :ok ->
                case initial_freshness_context(run_state, version) do
                  {:ok, freshness_context} ->
                    {:ok,
                     RunExecutionState.new(run_state, version,
                       mode: :pipeline,
                       runner_client: runner_client,
                       runner_opts: runner_opts,
                       stage_groups: pipeline_stage_groups(run_state),
                       freshness_context: freshness_context
                     )}

                  {:error, reason} ->
                    {:terminal,
                     Snapshots.snapshot_update(run_state,
                       status: :error,
                       error: reason,
                       runner_execution_id: nil,
                       result: pipeline_result(run_state, :error, [])
                     )}
                end

              {:error, reason} ->
                {:terminal,
                 Snapshots.snapshot_update(run_state,
                   status: :error,
                   error: reason,
                   runner_execution_id: nil,
                   result: pipeline_result(run_state, :error, [])
                 )}
            end

          {:error, reason} ->
            {:terminal,
             Snapshots.snapshot_update(run_state,
               status: :error,
               error: reason,
               runner_execution_id: nil,
               result: pipeline_result(run_state, :error, [])
             )}
        end

      :sequential ->
        {:ok,
         RunExecutionState.new(run_state, version,
           mode: :sequential,
           runner_client: runner_client,
           runner_opts: runner_opts,
           sequential_refs: execution_refs_with_stage(run_state)
         )}
    end
  end

  @spec handle_event(RunExecutionState.t(), step_event()) ::
          {:cont, RunExecutionState.t()} | {:terminal, RunState.t()}
  def handle_event(%RunExecutionState{terminal?: true, run: run_state}, _event),
    do: {:terminal, run_state}

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
    case RunExecutionState.pop_await(state, execution_id) do
      {nil, state} ->
        {:cont, state}

      {await, state} ->
        if await.monitor_ref == monitor_ref do
          Process.cancel_timer(await.timeout_ref)

          handle_await_result(
            state,
            await.entry,
            {:error, %{type: :await_task_failed, kind: :exit, reason: inspect(reason)}},
            await.kind
          )
        else
          {:cont, state}
        end
    end
  end

  def handle_event(%RunExecutionState{} = state, {:attempt_timeout, execution_id, timer_ref}) do
    case RunExecutionState.pop_await(state, execution_id) do
      {nil, state} ->
        {:cont, state}

      {await, state} ->
        if await.timeout_token == timer_ref do
          Process.exit(await.pid, :kill)
          Process.demonitor(await.monitor_ref, [:flush])
          :ok = RunWorkSet.release_entry(await.entry)
          handle_await_result(state, await.entry, {:error, :timeout}, await.kind)
        else
          {:cont, state}
        end
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
    case RunExecutionState.pop_admission_waiter(state, waiter_id) do
      {nil, state} ->
        {:cont, state}

      {%{wake_generation: waiter_generation} = waiter, state}
      when waiter_generation == generation ->
        :ok = ExecutionAdmission.cancel_wait(waiter)

        state
        |> RunExecutionState.cancel_admission_timers()
        |> after_pipeline_progress()

      {_stale_waiter, state} ->
        {:cont, state}
    end
  end

  @spec cancel(RunExecutionState.t(), term()) :: RunState.t()
  def cancel(%RunExecutionState{} = state, reason) do
    reason = %{kind: :external_cancel, reason: reason}
    state = state |> stop_all_awaits(reason) |> RunExecutionState.cancel_timers()

    {cancelled_run, _work_set} =
      RunWorkSet.cancel_all(
        state.run,
        state.work_set,
        reason,
        state.runner_client,
        state.runner_opts
      )

    :ok = RunWorkSet.cleanup_all(state.work_set, reason)
    :ok = ExecutionAdmission.release_run(cancelled_run.id)
    :ok = ExecutionAdmission.cancel_run_waits(cancelled_run.id)

    cancellation_terminal(cancelled_run, state.accumulated_results)
  end

  defp cancellation_terminal(%RunState{} = run_state, accumulated_results) do
    outcomes = Map.get(run_state.metadata, :cancel_outcomes, [])

    cond do
      outcomes == [] ->
        Snapshots.cancelled_terminal(run_state, accumulated_results)

      Enum.all?(outcomes, &(Map.get(&1, :status) == :acknowledged)) ->
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

  defp execution_mode(%RunState{submit_kind: :pipeline}), do: :pipeline

  defp execution_mode(%RunState{submit_kind: :rerun, metadata: %{replay_submit_kind: :pipeline}}),
    do: :pipeline

  defp execution_mode(%RunState{}), do: :sequential

  defp continue_state(%RunExecutionState{mode: :sequential} = state),
    do: continue_sequential(state)

  defp continue_state(%RunExecutionState{mode: :pipeline} = state), do: continue_pipeline(state)

  defp continue_sequential(%RunExecutionState{} = state) do
    if state.sequential_index >= length(state.sequential_refs) do
      {:terminal,
       Snapshots.snapshot_update(state.run,
         status: :ok,
         error: nil,
         runner_execution_id: nil,
         result: %{
           status: :ok,
           asset_results: state.accumulated_results,
           metadata: state.run.metadata
         }
       )}
    else
      {asset_ref, node_key, stage} = Enum.at(state.sequential_refs, state.sequential_index)
      submit_sequential_attempt(state, asset_ref, node_key, stage, 1)
    end
  end

  defp submit_sequential_attempt(
         %RunExecutionState{} = state,
         asset_ref,
         node_key,
         stage,
         attempt
       ) do
    with :ok <- validate_runner_client(state.runner_client),
         :ok <- state.runner_client.register_manifest(state.version, state.runner_opts),
         {:ok, %{work: work} = lifecycle} <-
           state.run
           |> StepAttemptLifecycle.new(state.version, node_key, stage, attempt)
           |> StepAttemptLifecycle.build_work(),
         ownership <-
           RunExecutionOwnership.new(state.run,
             asset_step_id: work.asset_step_id,
             node_key: node_key,
             asset_ref: asset_ref,
             stage: stage,
             attempt: attempt,
             execution_pool: RunnerWork.execution_pool(work),
             deadline_at: run_deadline_at(state.run)
           ),
         work <- attach_ownership_metadata(work, ownership),
         :ok <- RunExecutionOwnership.persist(ownership),
         {:ok, execution_id} <- state.runner_client.submit_work(work, state.runner_opts) do
      submitted_ownership = RunExecutionOwnership.submitted(ownership, execution_id)

      case persist_submitted_ownership_snapshot(submitted_ownership) do
        :ok ->
          start_submitted_sequential_attempt(
            state,
            lifecycle,
            work,
            submitted_ownership,
            execution_id,
            asset_ref,
            node_key,
            stage,
            attempt
          )

        {:error, :external_cancel} ->
          state = cancel_work(state, [execution_id], %{kind: :external_cancel})
          {:terminal, Snapshots.cancelled_snapshot(state.run)}

        {:error, reason} ->
          state =
            cancel_work(state, [execution_id], %{
              kind: :step_submitted_persist_failed,
              error: reason
            })

          failed =
            RunState.transition(state.run,
              status: :error,
              runner_execution_id: nil,
              error: reason
            )

          {:terminal, failed}
      end
    else
      {:error, reason} ->
        failed =
          RunState.transition(state.run, status: :error, runner_execution_id: nil, error: reason)

        case Persistence.persist_run_step(failed, :step_failed, %{
               asset_ref: asset_ref,
               error: reason,
               node_key: node_key,
               asset_step_id: AssetStepIdentity.asset_step_id(state.run.id, node_key, asset_ref),
               stage: stage,
               attempt: attempt,
               max_attempts: state.run.max_attempts
             }) do
          :ok ->
            maybe_schedule_sequential_retry(
              %{state | run: failed},
              asset_ref,
              stage,
              attempt,
              true,
              []
            )

          {:error, :external_cancel} ->
            {:terminal, elem(Snapshots.cancelled_state(failed), 1)}

          {:error, _persist_reason} ->
            {:terminal, failed}
        end
    end
  end

  defp persist_submitted_ownership_snapshot(%RunExecutionOwnership{} = ownership) do
    if Persistence.externally_cancelled?(ownership.run_id) do
      {:error, :external_cancel}
    else
      RunExecutionOwnership.persist(ownership)
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

  defp start_submitted_sequential_attempt(
         state,
         lifecycle,
         work,
         ownership,
         execution_id,
         asset_ref,
         node_key,
         stage,
         attempt
       ) do
    entry = sequential_entry(state, lifecycle, work, execution_id)
    started_ownership = RunExecutionOwnership.started(ownership)

    case RunExecutionOwnership.persist(started_ownership) do
      :ok ->
        running =
          RunState.transition(state.run,
            runner_execution_id: execution_id,
            metadata: Map.merge(state.run.metadata, RunnerWork.lifecycle_metadata(work))
          )

        state = %{state | run: running} |> RunExecutionState.add_work(entry)

        case Persistence.persist_run_step(state.run, :step_started, %{
               asset_ref: asset_ref,
               runner_execution_id: execution_id,
               node_key: node_key,
               asset_step_id: work.asset_step_id,
               stage: stage,
               attempt: attempt,
               max_attempts: state.run.max_attempts
             }) do
          :ok ->
            {:cont, start_await(state, entry, :sequential)}

          {:error, :external_cancel} ->
            state =
              cancel_work(state, [execution_id], %{
                kind: :external_cancel,
                asset_ref: asset_ref,
                stage: stage,
                attempt: attempt
              })

            {:terminal, Snapshots.cancelled_snapshot(state.run)}

          {:error, reason} ->
            state =
              cancel_work(state, [execution_id], %{
                kind: :step_started_persist_failed,
                error: reason
              })

            failed =
              RunState.transition(state.run,
                status: :error,
                runner_execution_id: nil,
                error: reason
              )

            {:terminal, failed}
        end

      {:error, reason} ->
        state =
          cancel_work(state, [execution_id], %{
            kind: :step_started_ownership_persist_failed,
            error: reason
          })

        failed =
          RunState.transition(state.run, status: :error, runner_execution_id: nil, error: reason)

        {:terminal, failed}
    end
  end

  defp sequential_entry(state, lifecycle, work, execution_id) do
    %{
      run_id: state.run.id,
      asset_step_id: work.asset_step_id,
      asset_ref: lifecycle.asset_ref,
      node_key: lifecycle.node_key,
      execution_id: execution_id,
      runner_execution_id: execution_id,
      stage: lifecycle.stage,
      attempt: lifecycle.attempt,
      execution_pool: RunnerWork.execution_pool(work)
    }
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

  defp handle_await_result(%RunExecutionState{} = state, entry, result, :sequential) do
    state = elem(RunExecutionState.complete_work(state, entry.execution_id), 1)
    _ = RunExecutionOwnership.mark_finish_persist_pending(state.run.id, entry.execution_id)
    process_sequential_await_result(state, entry, result)
  end

  defp handle_await_result(%RunExecutionState{} = state, entry, result, :pipeline) do
    _ = RunExecutionOwnership.mark_finish_persist_pending(state.run.id, entry.execution_id)
    handle_pipeline_await_result(state, entry, result)
  end

  defp process_sequential_await_result(state, entry, {:ok, %RunnerResult{} = result}) do
    result = sanitize_runner_result(result)
    step_status = StepAttemptLifecycle.map_runner_status(result.status)
    {event_type, retryable?} = StepAttemptLifecycle.step_outcome(step_status)
    retryable? = retryable? and StepAttemptLifecycle.runner_result_retryable?(result)

    step_finished =
      RunState.transition(state.run,
        status: step_status,
        runner_execution_id: nil,
        error: result.error,
        metadata: merge_runner_metadata(state.run.metadata, result.metadata)
      )

    case Persistence.persist_run_step(step_finished, event_type, %{
           asset_ref: entry.asset_ref,
           result_status: result.status,
           error: result.error,
           node_key: entry.node_key,
           asset_step_id: entry.asset_step_id,
           stage: entry.stage,
           attempt: entry.attempt,
           max_attempts: state.run.max_attempts
         }) do
      :ok ->
        _ = RunExecutionOwnership.complete_execution(state.run.id, entry.execution_id)
        state = %{state | run: step_finished}

        cond do
          step_status == :ok ->
            continue_sequential(%{
              state
              | sequential_index: state.sequential_index + 1,
                accumulated_results:
                  state.accumulated_results ++ normalize_results(result.asset_results)
            })

          retryable? ->
            maybe_schedule_sequential_retry(
              state,
              entry.asset_ref,
              entry.stage,
              entry.attempt,
              true,
              result.asset_results
            )

          true ->
            terminalize_sequential_error(state, result.asset_results)
        end

      {:error, :external_cancel} ->
        {:terminal, elem(Snapshots.cancelled_state(state.run), 1)}
    end
  end

  defp process_sequential_await_result(state, entry, {:error, :timeout}) do
    state =
      cancel_work(state, [entry.execution_id], %{
        kind: :await_timeout,
        asset_ref: entry.asset_ref,
        stage: entry.stage,
        attempt: entry.attempt
      })

    timeout_state =
      RunState.transition(state.run,
        status: :timed_out,
        runner_execution_id: nil,
        error: :timeout
      )

    case Persistence.persist_run_step(timeout_state, :step_timed_out, %{
           asset_ref: entry.asset_ref,
           error: :timeout,
           node_key: entry.node_key,
           asset_step_id: entry.asset_step_id,
           stage: entry.stage,
           attempt: entry.attempt,
           max_attempts: state.run.max_attempts
         }) do
      :ok ->
        _ = RunExecutionOwnership.complete_execution(state.run.id, entry.execution_id)

        maybe_schedule_sequential_retry(
          %{state | run: timeout_state},
          entry.asset_ref,
          entry.stage,
          entry.attempt,
          true,
          []
        )

      {:error, :external_cancel} ->
        {:terminal, elem(Snapshots.cancelled_state(state.run), 1)}
    end
  end

  defp process_sequential_await_result(state, entry, {:error, reason}) do
    state =
      cancel_work(state, [entry.execution_id], %{
        kind: :await_error,
        asset_ref: entry.asset_ref,
        stage: entry.stage,
        attempt: entry.attempt,
        error: reason
      })

    failed =
      RunState.transition(state.run, status: :error, runner_execution_id: nil, error: reason)

    case Persistence.persist_run_step(failed, :step_failed, %{
           asset_ref: entry.asset_ref,
           error: reason,
           node_key: entry.node_key,
           asset_step_id: entry.asset_step_id,
           stage: entry.stage,
           attempt: entry.attempt,
           max_attempts: state.run.max_attempts
         }) do
      :ok ->
        _ = RunExecutionOwnership.complete_execution(state.run.id, entry.execution_id)

        maybe_schedule_sequential_retry(
          %{state | run: failed},
          entry.asset_ref,
          entry.stage,
          entry.attempt,
          true,
          []
        )

      {:error, :external_cancel} ->
        {:terminal, elem(Snapshots.cancelled_state(state.run), 1)}
    end
  end

  defp maybe_schedule_sequential_retry(state, asset_ref, stage, attempt, retryable, step_results) do
    if retryable and attempt < state.run.max_attempts do
      node_key = Map.get(state.run.metadata, :node_key) || {asset_ref, nil}
      lifecycle = StepAttemptLifecycle.new(state.run, state.version, node_key, stage, attempt)

      case StepAttemptLifecycle.schedule_retry(lifecycle, true) do
        {:ok, retry} ->
          retrying =
            RunState.transition(state.run,
              status: :running,
              error: nil,
              runner_execution_id: nil,
              metadata:
                Map.merge(state.run.metadata, %{retrying: true, next_attempt: retry.next_attempt})
            )

          case Persistence.persist_run_step(
                 retrying,
                 :step_retry_scheduled,
                 StepAttemptLifecycle.retry_event_payload(retry)
               ) do
            :ok ->
              timer_token = make_ref()

              timer_ref =
                Process.send_after(self(), {:retry_attempt, timer_token}, retry.retry_after_ms)

              {:cont,
               RunExecutionState.put_retry_timer(
                 %{state | run: retrying},
                 timer_token,
                 timer_ref,
                 retry
               )}

            {:error, :external_cancel} ->
              {:terminal, elem(Snapshots.cancelled_state(state.run), 1)}
          end

        :terminal ->
          terminalize_sequential_error(state, step_results)
      end
    else
      terminalize_sequential_error(state, step_results)
    end
  end

  defp terminalize_sequential_error(state, step_results) do
    {:terminal,
     Snapshots.snapshot_update(state.run,
       runner_execution_id: nil,
       result: %{
         status: state.run.status,
         asset_results: state.accumulated_results ++ normalize_results(step_results),
         metadata: state.run.metadata
       }
     )}
  end

  defp resume_retry(%RunExecutionState{mode: :sequential} = state, retry) do
    submit_sequential_attempt(
      state,
      retry.asset_ref,
      retry.node_key,
      retry.stage,
      retry.next_attempt
    )
  end

  defp resume_retry(%RunExecutionState{mode: :pipeline} = state, retry) do
    submit_pipeline_stage_attempt(
      %{state | stage_attempt: retry.next_attempt},
      retry.node_keys,
      retry.next_attempt
    )
  end

  defp cancel_work(%RunExecutionState{} = state, execution_ids, reason) do
    work_set =
      Enum.reduce(execution_ids, state.work_set, fn execution_id, acc ->
        case Map.get(acc.entries, execution_id) do
          nil ->
            RunWorkSet.add_entry(acc, %{
              execution_id: execution_id,
              runner_execution_id: execution_id
            })

          _entry ->
            acc
        end
      end)

    {run, work_set} =
      RunWorkSet.cancel_all(state.run, work_set, reason, state.runner_client, state.runner_opts)

    %{state | run: run, work_set: work_set}
  end

  defp continue_pipeline(%RunExecutionState{} = state) do
    if state.stage_index >= length(state.stage_groups) do
      terminalize_pipeline_state(state)
    else
      {stage, node_keys} = Enum.at(state.stage_groups, state.stage_index)

      if Persistence.externally_cancelled?(state.run.id) do
        {:terminal, Snapshots.cancelled_terminal(state.run, state.accumulated_results)}
      else
        case classify_pipeline_stage(
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
            all_results = sort_asset_results(failed_run, state.accumulated_results)
            {:terminal, terminalize_pipeline_failed_run(failed_run, all_results)}
        end
      end
    end
  end

  defp submit_pipeline_stage_attempt(%RunExecutionState{} = state, node_keys, attempt) do
    {stage, _stage_node_keys} = Enum.at(state.stage_groups, state.stage_index)

    case submit_stage_entries(
           state.run,
           state.version,
           stage,
           node_keys,
           state.stage_decisions,
           state.stage_freshness_context,
           attempt,
           state.runner_client,
           state.runner_opts
         ) do
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
              stage_admission_deadline_ms: stage_admission_deadline(run_after_submit.timeout_ms),
              stage_executed_node_keys: StageAttemptState.attempted_node_keys(stage_state)
          }
          |> RunExecutionState.put_admission_waiters(waiters)

        state
        |> start_pipeline_awaits(entries)
        |> after_starting_pipeline_awaits(entries)

      {:error, failed_run, step_results, _attempted_node_keys} ->
        {:terminal,
         terminalize_pipeline_failed_run(
           failed_run,
           sort_asset_results(failed_run, state.accumulated_results ++ step_results),
           %{status: failed_run.status, error: failed_run.error}
         )}
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

    case process_stage_attempt_result(
           %{state.stage_state | run: state.run},
           entry,
           result,
           entry.stage,
           entry.attempt,
           state.runner_client,
           state.runner_opts
         ) do
      {:cont, next_stage_state} ->
        %{state | run: next_stage_state.run, stage_state: next_stage_state}
        |> after_pipeline_progress()

      {:halt, {:error, failed_run, next_results, _attempted_node_keys}} ->
        _state = stop_all_awaits(%{state | run: failed_run}, :stopped_pending_await)

        {:terminal,
         terminalize_pipeline_failed_run(
           failed_run,
           sort_asset_results(failed_run, next_results),
           %{status: failed_run.status, error: failed_run.error}
         )}
    end
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
    {stage, _stage_node_keys} = Enum.at(state.stage_groups, state.stage_index)

    case submit_stage_entries(
           state.stage_state.run,
           state.version,
           stage,
           state.stage_state.deferred_node_keys,
           state.stage_decisions,
           state.stage_freshness_context,
           state.stage_attempt,
           state.runner_client,
           state.runner_opts,
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

      {:error, failed_run, step_results, _attempted_node_keys} ->
        {:terminal,
         terminalize_pipeline_failed_run(
           failed_run,
           sort_asset_results(failed_run, state.accumulated_results ++ step_results),
           %{status: failed_run.status, error: failed_run.error}
         )}
    end
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
       timeout_deferred_stage_attempt(state.stage_state),
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

    retry_run =
      Enum.reduce(state.stage_state.retry_refs, state.stage_state.run, fn node_key, current ->
        persist_retry_for_ref(current, node_key, stage, state.stage_attempt)
      end)

    timer_token = make_ref()

    timer_ref =
      Process.send_after(
        self(),
        {:retry_attempt, timer_token},
        max(retry_run.retry_backoff_ms, 0)
      )

    retry = %{
      node_keys: state.stage_state.retry_refs,
      next_attempt: state.stage_attempt + 1,
      stage: stage
    }

    {:cont,
     state
     |> Map.put(:run, retry_run)
     |> Map.put(:stage_state, %{state.stage_state | run: retry_run, retry_refs: []})
     |> RunExecutionState.put_retry_timer(timer_token, timer_ref, retry)}
  end

  defp finalize_pipeline_stage(%RunExecutionState{} = state) do
    case finalize_stage_attempt_state(state.stage_state) do
      {:ok, next_run, next_results, [], attempted_node_keys} ->
        {next_context, persisted_run} =
          record_successful_freshness(
            next_run,
            state.version,
            attempted_node_keys,
            state.stage_decisions,
            next_results,
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
          record_completed_stage_after_failure(
            failed_run,
            state.version,
            attempted_node_keys,
            state.stage_decisions,
            next_results,
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
    :ok = ExecutionAdmission.release_run(state.run.id)
    all_results = sort_asset_results(state.run, state.accumulated_results)

    {:terminal,
     Snapshots.snapshot_update(state.run,
       status: :ok,
       error: nil,
       runner_execution_id: nil,
       result: pipeline_result(state.run, :ok, all_results)
     )}
  end

  defp terminalize_pipeline_state(%RunExecutionState{} = state) do
    :ok = ExecutionAdmission.release_run(state.run.id)
    all_results = sort_asset_results(state.run, state.accumulated_results)
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

  defp persist_retry_for_ref(%RunState{} = run_state, node_key, stage, attempt) do
    asset_ref = node_asset_ref(run_state, node_key)
    asset_step_id = AssetStepIdentity.asset_step_id(run_state.id, node_key, asset_ref)

    retrying =
      RunState.transition(run_state,
        status: :running,
        error: nil,
        runner_execution_id: nil,
        metadata: Map.merge(run_state.metadata, %{retrying: true, next_attempt: attempt + 1})
      )

    case Persistence.persist_run_step(retrying, :step_retry_scheduled, %{
           asset_ref: asset_ref,
           node_key: node_key,
           asset_step_id: asset_step_id,
           window: node_window(run_state, node_key),
           stage: stage,
           attempt: attempt,
           max_attempts: run_state.max_attempts,
           execution_pool: effective_execution_pool(run_state, node_key),
           next_attempt: attempt + 1,
           retry_backoff_ms: run_state.retry_backoff_ms
         }) do
      :ok -> retrying
      {:error, :external_cancel} -> Snapshots.cancelled_snapshot(retrying)
    end
  end

  defp classify_pipeline_stage(
         %RunState{} = run_state,
         %Version{} = version,
         stage,
         node_keys,
         freshness_context,
         terminal_failure
       ) do
    decisions =
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

    Enum.reduce_while(
      node_keys,
      {:ok, run_state, [], decisions, freshness_context, terminal_failure},
      fn node_key, {:ok, current_run, runnable, decisions, current_context, current_failure} ->
        decision = Map.fetch!(decisions, node_key)

        case decision.decision do
          :run ->
            {:cont,
             {:ok, current_run, runnable ++ [node_key], decisions, current_context,
              current_failure}}

          status when status in [:skipped_fresh, :blocked] ->
            case persist_decision_result(current_run, version, node_key, stage, status, decision) do
              {:ok, next_run} ->
                next_context = record_decision_status(current_context, node_key, status)

                next_failure =
                  if status == :blocked and is_nil(current_failure) do
                    %{status: :error, error: {:blocked, node_key, decision.reason}}
                  else
                    current_failure
                  end

                {:cont, {:ok, next_run, runnable, decisions, next_context, next_failure}}

              {:error, :external_cancel} ->
                {:halt, {:error, Snapshots.cancelled_snapshot(current_run)}}

              {:error, reason} ->
                failed = RunState.transition(current_run, status: :error, error: reason)
                {:halt, {:error, failed}}
            end
        end
      end
    )
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

  defp decision_metadata(decision) when is_map(decision) do
    Map.drop(decision, [:decision, :node_key, :reason, :freshness_key])
  end

  defp record_decision_status(context, node_key, status) do
    %{
      context
      | completed_node_keys: MapSet.put(context.completed_node_keys, node_key),
        upstream_statuses: Map.put(context.upstream_statuses, node_key, status)
    }
  end

  defp put_node_result(%RunState{} = run_state, %NodeResult{} = result) do
    node_results = existing_node_results(run_state) ++ [result]
    result_map = Map.merge(run_state.result || %{}, %{node_results: node_results})

    RunState.transition(run_state, result: result_map)
  end

  defp submit_stage_entries(
         %RunState{} = run_state,
         %Version{} = version,
         stage,
         node_keys,
         decisions,
         freshness_context,
         attempt,
         runner_client,
         runner_opts
       ) do
    submit_stage_entries(
      run_state,
      version,
      stage,
      node_keys,
      decisions,
      freshness_context,
      attempt,
      runner_client,
      runner_opts,
      MapSet.new()
    )
  end

  defp submit_stage_entries(
         %RunState{} = run_state,
         %Version{} = version,
         stage,
         node_keys,
         decisions,
         freshness_context,
         attempt,
         runner_client,
         runner_opts,
         queued_steps
       ) do
    StageAdmission.submit(%{
      run: run_state,
      version: version,
      stage: stage,
      node_keys: node_keys,
      decisions: decisions,
      freshness_context: freshness_context,
      attempt: attempt,
      runner_client: runner_client,
      runner_opts: runner_opts,
      queued_steps: queued_steps
    })
  end

  defp await_runner_result(entry, timeout_ms, runner_client, runner_opts) do
    bridge = start_runner_log_bridge(runner_client, entry.execution_id, runner_opts, entry)

    try do
      runner_client.await_result(entry.execution_id, timeout_ms, runner_opts)
    after
      stop_runner_log_bridge(bridge, runner_client, entry.execution_id, runner_opts)
      :ok = release_entry_lease(entry)
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

  defp timeout_deferred_stage_attempt(%{run: %RunState{} = run_state} = state) do
    timed_out =
      RunState.transition(run_state,
        status: :timed_out,
        error: :timeout,
        runner_execution_id: nil
      )

    {:error, timed_out, state.results, StageAttemptState.attempted_node_keys(state)}
  end

  defp process_stage_attempt_result(
         %StageAttemptState{
           run: current_run,
           results: current_results,
           retry_refs: retry_refs,
           terminal_failure: terminal_failure,
           pending_ids: pending_ids
         } = state,
         entry,
         await_result,
         stage,
         attempt,
         runner_client,
         runner_opts
       ) do
    if Persistence.externally_cancelled?(current_run.id) do
      cancelled =
        cancel_execution_ids(
          current_run,
          inflight_ids_from_metadata(current_run),
          %{kind: :external_cancel},
          runner_client,
          runner_opts
        )

      :ok = MaterializationClaims.fail_entry(entry, :external_cancel)

      {:halt,
       {:error, Snapshots.cancelled_terminal(cancelled, current_results), current_results,
        StageAttemptState.attempted_node_keys(state)}}
    else
      {next_run, outcome, step_results} =
        process_one_stage_attempt_result(
          current_run,
          entry,
          await_result,
          stage,
          attempt,
          runner_client,
          runner_opts
        )

      :ok = release_entry_lease(entry)

      next_results = current_results ++ step_results
      next_pending_ids = MapSet.delete(pending_ids, entry.execution_id)

      reduce_stage_attempt_outcome(
        outcome,
        %{
          state: state,
          run: next_run,
          results: next_results,
          retry_refs: retry_refs,
          terminal_failure: terminal_failure,
          pending_ids: next_pending_ids
        },
        %{entry: entry, stage: stage, attempt: attempt}
      )
    end
  end

  defp finalize_stage_attempt_state(%StageAttemptState{
         run: next_run,
         results: next_results,
         retry_refs: retry_refs,
         terminal_failure: nil,
         attempted_node_keys: attempted_node_keys
       }) do
    {:ok, next_run, next_results, retry_refs, attempted_node_keys}
  end

  defp finalize_stage_attempt_state(%StageAttemptState{
         run: next_run,
         results: next_results,
         terminal_failure: terminal_failure,
         attempted_node_keys: attempted_node_keys
       }) do
    failed_run = failed_stage_terminal_state(next_run, terminal_failure)
    {:error, failed_run, next_results, attempted_node_keys}
  end

  defp record_stage_attempt_freshness(%RunState{} = run_state, entry, :ok) do
    StateWriter.put_success_state(
      run_state,
      Map.fetch!(entry, :version),
      Map.fetch!(entry, :node_key),
      Map.get(entry, :decision, %{}),
      Map.fetch!(entry, :freshness_context)
    )
  end

  defp record_stage_attempt_freshness(%RunState{} = run_state, entry, status) do
    StateWriter.put_attempt_state(
      run_state,
      Map.fetch!(entry, :version),
      Map.fetch!(entry, :node_key),
      status,
      Map.fetch!(entry, :freshness_key),
      Map.get(entry, :decision, %{})
    )
  end

  defp release_entry_lease(entry), do: RunWorkSet.release_entry(entry)

  defp reduce_stage_attempt_outcome(
         :ok,
         %{
           state: state,
           run: %RunState{} = next_run,
           results: next_results,
           retry_refs: retry_refs,
           terminal_failure: terminal_failure,
           pending_ids: pending_ids
         },
         _context
       ) do
    {:cont,
     StageAttemptState.record_result(
       state,
       next_run,
       next_results,
       retry_refs,
       terminal_failure,
       pending_ids
     )}
  end

  defp reduce_stage_attempt_outcome(
         :retry,
         %{
           state: state,
           run: %RunState{} = next_run,
           results: next_results,
           retry_refs: retry_refs,
           terminal_failure: terminal_failure,
           pending_ids: pending_ids
         },
         %{entry: entry}
       ) do
    next_retry_refs =
      if terminal_failure == nil, do: retry_refs ++ [entry.node_key], else: retry_refs

    {:cont,
     StageAttemptState.record_result(
       state,
       next_run,
       next_results,
       next_retry_refs,
       terminal_failure,
       pending_ids
     )}
  end

  defp reduce_stage_attempt_outcome(
         :error,
         %{
           state: state,
           run: %RunState{status: :cancelled} = next_run,
           results: next_results
         },
         _context
       ) do
    {:halt,
     {:error, Snapshots.cancelled_terminal(next_run, next_results), next_results,
      StageAttemptState.attempted_node_keys(state)}}
  end

  defp reduce_stage_attempt_outcome(
         :error,
         %{
           state: state,
           run: %RunState{} = next_run,
           results: next_results,
           retry_refs: retry_refs,
           terminal_failure: terminal_failure,
           pending_ids: pending_ids
         },
         %{entry: entry, stage: stage, attempt: attempt}
       ) do
    case remember_stage_failure(next_run, terminal_failure, entry, stage, attempt, pending_ids) do
      {:ok, failure_run, next_terminal_failure} ->
        {:cont,
         StageAttemptState.record_result(
           state,
           failure_run,
           next_results,
           retry_refs,
           next_terminal_failure,
           pending_ids
         )}

      {:error, cancelled} ->
        {:halt,
         {:error, Snapshots.cancelled_terminal(cancelled, next_results), next_results,
          StageAttemptState.attempted_node_keys(state)}}
    end
  end

  defp remember_stage_failure(run_state, terminal_failure, entry, stage, attempt, pending_ids)

  defp remember_stage_failure(
         %RunState{} = run_state,
         nil,
         entry,
         stage,
         attempt,
         pending_ids
       ) do
    terminal_failure = %{status: run_state.status, error: run_state.error}

    if MapSet.size(pending_ids) == 0 do
      {:ok, run_state, terminal_failure}
    else
      pending_execution_ids = MapSet.to_list(pending_ids)

      metadata =
        Map.put(run_state.metadata, :stage_draining_after_failure, %{
          stage: stage,
          attempt: attempt,
          failed_asset_ref: entry.asset_ref,
          pending_execution_ids: pending_execution_ids
        })

      draining =
        RunState.transition(run_state,
          status: :running,
          runner_execution_id: List.first(pending_execution_ids),
          metadata: metadata
        )

      case Persistence.persist_run_step(draining, :stage_draining_after_failure, %{
             stage: stage,
             attempt: attempt,
             failed_asset_ref: entry.asset_ref,
             pending_execution_ids: pending_execution_ids
           }) do
        :ok -> {:ok, draining, terminal_failure}
        {:error, :external_cancel} -> {:error, Snapshots.cancelled_snapshot(draining)}
      end
    end
  end

  defp remember_stage_failure(
         %RunState{} = run_state,
         terminal_failure,
         _entry,
         _stage,
         _attempt,
         _pending_ids
       ),
       do: {:ok, run_state, terminal_failure}

  defp failed_stage_terminal_state(%RunState{} = run_state, %{status: status, error: error}) do
    Snapshots.snapshot_update(run_state,
      status: status,
      error: error,
      runner_execution_id: nil
    )
  end

  defp process_one_stage_attempt_result(
         %RunState{} = run_state,
         %{asset_ref: asset_ref, execution_id: execution_id} = entry,
         {:ok, %RunnerResult{} = result},
         stage,
         attempt,
         _runner_client,
         _runner_opts
       ) do
    result = sanitize_runner_result(result)
    asset_results = result.asset_results
    cleared = clear_inflight_execution(run_state, execution_id)
    step_status = StepAttemptLifecycle.map_runner_status(result.status)
    {event_type, retryable?} = StepAttemptLifecycle.step_outcome(step_status)
    retryable? = retryable? and StepAttemptLifecycle.runner_result_retryable?(result)

    step_state =
      RunState.transition(cleared,
        status: step_status,
        error: result.error,
        metadata: merge_runner_metadata(cleared.metadata, result.metadata),
        runner_execution_id: nil
      )

    case Persistence.persist_run_step(step_state, event_type, %{
           asset_ref: asset_ref,
           result_status: result.status,
           error: result.error,
           node_key: Map.get(entry, :node_key),
           asset_step_id: Map.get(entry, :asset_step_id),
           stage: stage,
           attempt: attempt,
           max_attempts: run_state.max_attempts,
           execution_pool: Map.get(entry, :execution_pool)
         }) do
      :ok ->
        step_state =
          put_execution_node_result(step_state, entry, stage, attempt, step_status, asset_results)

        case persist_post_step_state(step_state, entry, step_status, result) do
          :ok ->
            outcome =
              cond do
                step_status == :ok -> :ok
                retryable? and attempt < run_state.max_attempts -> :retry
                true -> :error
              end

            {step_state, outcome, asset_results}

          {:error, reason} ->
            failed = post_step_persistence_failure(step_state, reason)
            {failed, :error, asset_results}
        end

      {:error, :external_cancel} ->
        return_external_cancel(run_state, asset_results)
    end
  end

  defp process_one_stage_attempt_result(
         %RunState{} = run_state,
         %{asset_ref: asset_ref, execution_id: execution_id} = entry,
         {:error, :timeout},
         stage,
         attempt,
         runner_client,
         runner_opts
       ) do
    cleared =
      cancel_execution_ids(
        run_state,
        [execution_id],
        %{kind: :await_timeout, asset_ref: asset_ref, stage: stage, attempt: attempt},
        runner_client,
        runner_opts
      )

    step_state =
      RunState.transition(cleared,
        status: :timed_out,
        error: :timeout,
        runner_execution_id: nil
      )

    case Persistence.persist_run_step(step_state, :step_timed_out, %{
           asset_ref: asset_ref,
           error: :timeout,
           node_key: Map.get(entry, :node_key),
           asset_step_id: Map.get(entry, :asset_step_id),
           stage: stage,
           attempt: attempt,
           max_attempts: run_state.max_attempts,
           execution_pool: Map.get(entry, :execution_pool)
         }) do
      :ok ->
        step_state = put_execution_node_result(step_state, entry, stage, attempt, :timed_out, [])

        case persist_post_step_state(step_state, entry, :timed_out, :timeout) do
          :ok ->
            outcome = if attempt < run_state.max_attempts, do: :retry, else: :error
            {step_state, outcome, []}

          {:error, reason} ->
            failed = post_step_persistence_failure(step_state, reason)
            {failed, :error, []}
        end

      {:error, :external_cancel} ->
        return_external_cancel(run_state, [])
    end
  end

  defp process_one_stage_attempt_result(
         %RunState{} = run_state,
         %{asset_ref: asset_ref, execution_id: execution_id} = entry,
         {:error, reason},
         stage,
         attempt,
         runner_client,
         runner_opts
       ) do
    cleared =
      cancel_execution_ids(
        run_state,
        [execution_id],
        %{
          kind: :await_error,
          asset_ref: asset_ref,
          stage: stage,
          attempt: attempt,
          error: reason
        },
        runner_client,
        runner_opts
      )

    step_state =
      RunState.transition(cleared,
        status: :error,
        error: reason,
        runner_execution_id: nil
      )

    case Persistence.persist_run_step(step_state, :step_failed, %{
           asset_ref: asset_ref,
           error: reason,
           node_key: Map.get(entry, :node_key),
           asset_step_id: Map.get(entry, :asset_step_id),
           stage: stage,
           attempt: attempt,
           max_attempts: run_state.max_attempts,
           execution_pool: Map.get(entry, :execution_pool)
         }) do
      :ok ->
        step_state = put_execution_node_result(step_state, entry, stage, attempt, :error, [])

        case persist_post_step_state(step_state, entry, :error, reason) do
          :ok ->
            outcome = if attempt < run_state.max_attempts, do: :retry, else: :error
            {step_state, outcome, []}

          {:error, persistence_reason} ->
            failed = post_step_persistence_failure(step_state, persistence_reason)
            {failed, :error, []}
        end

      {:error, :external_cancel} ->
        return_external_cancel(run_state, [])
    end
  end

  defp persist_post_step_state(%RunState{} = step_state, entry, :ok, %RunnerResult{} = result) do
    with {:ok, freshness_state} <- record_stage_attempt_freshness(step_state, entry, :ok),
         :ok <-
           MaterializationClaims.complete(
             Map.get(entry, :materialization_claim),
             result,
             freshness_state
           ) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_post_step_state(%RunState{} = step_state, entry, status, failure_reason) do
    with {:ok, _state} <- record_stage_attempt_freshness(step_state, entry, status),
         :ok <- MaterializationClaims.fail_entry(entry, failure_reason) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp post_step_persistence_failure(%RunState{} = step_state, reason) do
    RunState.transition(step_state,
      status: :error,
      error: %{type: :post_step_persistence_failed, reason: reason},
      runner_execution_id: nil
    )
  end

  defp put_execution_node_result(
         %RunState{} = run_state,
         %{
           asset_ref: asset_ref,
           node_key: node_key,
           execution_id: execution_id,
           execution_pool: execution_pool,
           freshness_key: freshness_key
         },
         stage,
         attempt,
         status,
         asset_results
       ) do
    node = Map.fetch!(run_state.plan.nodes, node_key)
    asset_result = Enum.find(asset_results, &(asset_result_ref(&1) == asset_ref))
    now = DateTime.utc_now()

    result =
      NodeResult.new(%{
        node_key: node_key,
        ref: asset_ref,
        window: node.window,
        stage: stage,
        execution_pool: Map.get(node, :execution_pool) || execution_pool,
        status: status,
        started_at: asset_result_started_at(asset_result) || now,
        finished_at: asset_result_finished_at(asset_result) || now,
        duration_ms: asset_result_duration_ms(asset_result) || 0,
        reason: execution_node_reason(status),
        freshness_key: freshness_key,
        input_versions: [],
        attempt_count: asset_result_attempt_count(asset_result) || attempt,
        max_attempts: asset_result_max_attempts(asset_result) || run_state.max_attempts,
        runner_execution_id: execution_id,
        meta: asset_result_meta(asset_result),
        error: asset_result_error(asset_result),
        attempts: asset_result_attempts(asset_result),
        asset_step_id:
          asset_result_asset_step_id(asset_result) ||
            AssetStepIdentity.asset_step_id(run_state.id, node_key, asset_ref)
      })

    put_node_result(run_state, result)
  end

  defp execution_node_reason(:ok), do: nil
  defp execution_node_reason(status), do: status

  defp asset_result_started_at(%RunnerAssetResult{started_at: started_at}), do: started_at
  defp asset_result_started_at(%AssetResult{started_at: started_at}), do: started_at
  defp asset_result_started_at(%{started_at: started_at}), do: started_at
  defp asset_result_started_at(%{"started_at" => started_at}), do: started_at
  defp asset_result_started_at(_result), do: nil

  defp asset_result_finished_at(%RunnerAssetResult{finished_at: finished_at}), do: finished_at
  defp asset_result_finished_at(%AssetResult{finished_at: finished_at}), do: finished_at
  defp asset_result_finished_at(%{finished_at: finished_at}), do: finished_at
  defp asset_result_finished_at(%{"finished_at" => finished_at}), do: finished_at
  defp asset_result_finished_at(_result), do: nil

  defp asset_result_duration_ms(%RunnerAssetResult{duration_ms: duration_ms}), do: duration_ms
  defp asset_result_duration_ms(%AssetResult{duration_ms: duration_ms}), do: duration_ms
  defp asset_result_duration_ms(%{duration_ms: duration_ms}), do: duration_ms
  defp asset_result_duration_ms(%{"duration_ms" => duration_ms}), do: duration_ms
  defp asset_result_duration_ms(_result), do: nil

  defp asset_result_attempt_count(%RunnerAssetResult{attempt_count: attempt_count}),
    do: attempt_count

  defp asset_result_attempt_count(%AssetResult{attempt_count: attempt_count}), do: attempt_count
  defp asset_result_attempt_count(%{attempt_count: attempt_count}), do: attempt_count
  defp asset_result_attempt_count(%{"attempt_count" => attempt_count}), do: attempt_count
  defp asset_result_attempt_count(_result), do: nil

  defp asset_result_max_attempts(%RunnerAssetResult{max_attempts: max_attempts}), do: max_attempts
  defp asset_result_max_attempts(%AssetResult{max_attempts: max_attempts}), do: max_attempts
  defp asset_result_max_attempts(%{max_attempts: max_attempts}), do: max_attempts
  defp asset_result_max_attempts(%{"max_attempts" => max_attempts}), do: max_attempts
  defp asset_result_max_attempts(_result), do: nil

  defp asset_result_meta(%RunnerAssetResult{meta: meta}) when is_map(meta), do: meta
  defp asset_result_meta(%AssetResult{meta: meta}) when is_map(meta), do: meta
  defp asset_result_meta(%{meta: meta}) when is_map(meta), do: meta
  defp asset_result_meta(%{"meta" => meta}) when is_map(meta), do: meta
  defp asset_result_meta(_result), do: %{}

  defp asset_result_error(%RunnerAssetResult{error: error}), do: error
  defp asset_result_error(%AssetResult{error: error}), do: error
  defp asset_result_error(%{error: error}), do: error
  defp asset_result_error(%{"error" => error}), do: error
  defp asset_result_error(_result), do: nil

  defp asset_result_attempts(%RunnerAssetResult{attempts: attempts}) when is_list(attempts),
    do: attempts

  defp asset_result_attempts(%AssetResult{attempts: attempts}) when is_list(attempts),
    do: attempts

  defp asset_result_attempts(%{attempts: attempts}) when is_list(attempts), do: attempts
  defp asset_result_attempts(%{"attempts" => attempts}) when is_list(attempts), do: attempts
  defp asset_result_attempts(_result), do: []

  defp asset_result_asset_step_id(%RunnerAssetResult{asset_step_id: asset_step_id}),
    do: asset_step_id

  defp asset_result_asset_step_id(%AssetResult{asset_step_id: asset_step_id}), do: asset_step_id
  defp asset_result_asset_step_id(%{asset_step_id: asset_step_id}), do: asset_step_id
  defp asset_result_asset_step_id(%{"asset_step_id" => asset_step_id}), do: asset_step_id
  defp asset_result_asset_step_id(_result), do: nil

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

  defp effective_execution_pool(%RunState{} = run_state, node_key) do
    node_pool =
      case run_state.plan do
        %Favn.Plan{nodes: nodes} when is_map(nodes) ->
          nodes
          |> Map.get(node_key, %{})
          |> Map.get(:execution_pool)

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

  defp clear_inflight_execution(%RunState{} = run_state, execution_id) do
    ids =
      run_state
      |> inflight_ids_from_metadata()
      |> Enum.reject(&(&1 == execution_id))

    metadata = Map.put(run_state.metadata, :in_flight_execution_ids, ids)
    next_execution_id = List.first(ids)

    Snapshots.snapshot_update(run_state,
      metadata: metadata,
      runner_execution_id: next_execution_id
    )
  end

  defp clear_inflight_executions(%RunState{} = run_state, execution_ids)
       when is_list(execution_ids) do
    rejected = MapSet.new(execution_ids)

    ids =
      run_state
      |> inflight_ids_from_metadata()
      |> Enum.reject(&MapSet.member?(rejected, &1))

    metadata = Map.put(run_state.metadata, :in_flight_execution_ids, ids)
    next_execution_id = List.first(ids)

    Snapshots.snapshot_update(run_state,
      metadata: metadata,
      runner_execution_id: next_execution_id
    )
  end

  defp cancel_execution_ids(
         %RunState{} = run_state,
         execution_ids,
         reason,
         runner_client,
         runner_opts
       )
       when is_list(execution_ids) do
    cancel_results =
      Cancellation.dispatch_runner_work(
        run_state,
        execution_ids,
        reason,
        runner_client,
        runner_opts
      )

    _ = RunExecutionOwnership.persist_cancel_outcomes(run_state.id, cancel_results, reason)
    clear_inflight_executions(run_state, Enum.map(cancel_results, & &1.execution_id))
  end

  defp inflight_ids_from_metadata(%RunState{} = run_state) do
    case Map.get(run_state.metadata, :in_flight_execution_ids, []) do
      ids when is_list(ids) -> ids
      _other -> []
    end
  end

  defp pipeline_stage_groups(%RunState{plan: %Favn.Plan{} = plan}) do
    plan.node_stages
    |> Enum.with_index()
    |> Enum.map(fn {node_keys, stage} -> {stage, node_keys} end)
  end

  defp initial_freshness_context(%RunState{} = run_state, %Version{} = version) do
    assets_by_ref = assets_by_ref(version)
    refresh_policy = refresh_policy_from_metadata(run_state.metadata)
    now = DateTime.utc_now()

    with {:ok, prior_states} <-
           load_prior_freshness_states(run_state, assets_by_ref, refresh_policy, now) do
      {:ok,
       %{
         assets_by_ref: assets_by_ref,
         refresh_policy: refresh_policy,
         prior_states: prior_states,
         current_states: prior_states,
         completed_node_keys: MapSet.new(),
         refreshed_node_keys: MapSet.new(),
         upstream_statuses: %{},
         now: now
       }}
    end
  end

  defp load_prior_freshness_states(
         %RunState{plan: %Favn.Plan{} = plan},
         assets_by_ref,
         refresh_policy,
         now
       ) do
    keys =
      Decider.planned_lookup_keys(plan,
        assets_by_ref: assets_by_ref,
        refresh_policy: refresh_policy,
        now: now
      )

    case Storage.get_asset_freshness_states_by_keys(keys) do
      {:ok, states_by_key} ->
        {:ok,
         states_by_key
         |> Map.values()
         |> index_freshness_states()}

      {:error, reason} ->
        {:error, {:freshness_state_lookup_failed, reason}}
    end
  end

  defp index_freshness_states(states) do
    Enum.reduce(states, %{}, fn %AssetFreshnessState{} = state, acc ->
      acc
      |> Map.put({{state.asset_ref_module, state.asset_ref_name}, state.freshness_key}, state)
      |> maybe_put_state_by_node_key(state)
    end)
  end

  defp maybe_put_state_by_node_key(
         acc,
         %AssetFreshnessState{latest_success_node_key: node_key} = state
       )
       when is_tuple(node_key) do
    Map.put(acc, node_key, state)
  end

  defp maybe_put_state_by_node_key(acc, _state), do: acc

  defp assets_by_ref(%Version{manifest: %{assets: assets}}) when is_list(assets) do
    Map.new(assets, fn asset -> {asset.ref, asset} end)
  end

  defp assets_by_ref(_version), do: %{}

  defp refresh_policy_from_metadata(metadata) when is_map(metadata) do
    metadata
    |> Map.get(:refresh_policy, Map.get(metadata, :refresh))
    |> RefreshPolicy.from_value()
    |> case do
      {:ok, policy} -> policy
      {:error, _reason} -> %RefreshPolicy{mode: :auto}
    end
  end

  defp refresh_policy_from_metadata(_metadata), do: %RefreshPolicy{mode: :auto}

  defp record_successful_freshness(
         %RunState{} = run_state,
         %Version{} = version,
         node_keys,
         decisions,
         stage_results,
         freshness_context
       ) do
    successful = successful_node_keys(run_state, node_keys, stage_results)

    Enum.reduce(successful, {freshness_context, run_state}, fn node_key, {context, current_run} ->
      state =
        StateWriter.build_success_state(
          current_run,
          version,
          node_key,
          Map.get(decisions, node_key, %{}),
          context
        )

      next_context = %{
        context
        | current_states:
            context.current_states
            |> Map.put(node_key, state)
            |> Map.put({state_asset_ref(state), state.freshness_key}, state),
          completed_node_keys: MapSet.put(context.completed_node_keys, node_key),
          refreshed_node_keys: MapSet.put(context.refreshed_node_keys, node_key),
          upstream_statuses: Map.put(context.upstream_statuses, node_key, :ok)
      }

      {next_context, current_run}
    end)
  end

  defp record_completed_stage_after_failure(
         %RunState{} = run_state,
         %Version{} = version,
         node_keys,
         decisions,
         stage_results,
         freshness_context
       ) do
    successful = MapSet.new(successful_node_keys(run_state, node_keys, stage_results))

    {context, next_run} =
      record_successful_freshness(
        run_state,
        version,
        node_keys,
        decisions,
        stage_results,
        freshness_context
      )

    failed_status = run_state.status

    next_context =
      Enum.reduce(node_keys, context, fn node_key, acc ->
        if MapSet.member?(successful, node_key) do
          acc
        else
          status = latest_node_result_status(next_run, node_key) || failed_status
          _decision = Map.get(decisions, node_key, %{})

          %{
            acc
            | completed_node_keys: MapSet.put(acc.completed_node_keys, node_key),
              upstream_statuses: Map.put(acc.upstream_statuses, node_key, status)
          }
        end
      end)

    {next_context, next_run}
  end

  defp successful_node_keys(%RunState{} = run_state, node_keys, _stage_results) do
    stage_node_keys = MapSet.new(node_keys)

    run_state
    |> existing_node_results()
    |> Enum.filter(fn result ->
      MapSet.member?(stage_node_keys, node_result_node_key(result)) and
        node_result_status(result) == :ok
    end)
    |> Enum.map(&node_result_node_key/1)
    |> Enum.uniq()
  end

  defp node_result_node_key(%NodeResult{node_key: node_key}), do: node_key
  defp node_result_node_key(%{node_key: node_key}), do: node_key
  defp node_result_node_key(%{"node_key" => node_key}), do: node_key
  defp node_result_node_key(_result), do: nil

  defp node_result_status(%NodeResult{status: status}), do: status
  defp node_result_status(%{status: status}), do: status

  defp node_result_status(%{"status" => status}) when is_binary(status),
    do: String.to_existing_atom(status)

  defp node_result_status(_result), do: nil

  defp latest_node_result_status(%RunState{} = run_state, node_key) do
    run_state
    |> existing_node_results()
    |> Enum.reverse()
    |> Enum.find(&(node_result_node_key(&1) == node_key))
    |> node_result_status()
  end

  defp state_asset_ref(%AssetFreshnessState{} = state) do
    {state.asset_ref_module, state.asset_ref_name}
  end

  defp existing_node_results(%RunState{result: %{node_results: results}}) when is_list(results),
    do: results

  defp existing_node_results(_run_state), do: []

  defp pipeline_result(%RunState{} = run_state, status, asset_results) do
    %{
      status: status,
      asset_results: asset_results,
      node_results: existing_node_results(run_state),
      metadata: run_state.metadata
    }
  end

  defp terminalize_pipeline_failed_run(%RunState{} = failed_run, all_results) do
    Snapshots.snapshot_update(failed_run,
      runner_execution_id: nil,
      result: pipeline_result(failed_run, failed_run.status, all_results)
    )
  end

  defp terminalize_pipeline_failed_run(
         %RunState{} = failed_run,
         all_results,
         %{status: status, error: error}
       ) do
    failed_run
    |> Snapshots.snapshot_update(status: status, error: error, runner_execution_id: nil)
    |> then(&Snapshots.snapshot_update(&1, result: pipeline_result(&1, status, all_results)))
  end

  defp sort_asset_results(%RunState{} = run_state, results) when is_list(results) do
    ref_order =
      run_state
      |> planned_asset_refs()
      |> Enum.with_index()
      |> Map.new()

    results
    |> Enum.with_index()
    |> Enum.sort_by(fn {result, index} ->
      {Map.get(ref_order, asset_result_ref(result), map_size(ref_order)), index}
    end)
    |> Enum.map(fn {result, _index} -> result end)
  end

  defp asset_result_ref(%RunnerAssetResult{ref: ref}), do: ref
  defp asset_result_ref(%AssetResult{ref: ref}), do: ref
  defp asset_result_ref(%{ref: ref}), do: ref
  defp asset_result_ref(%{"ref" => ref}), do: ref
  defp asset_result_ref(_result), do: nil

  defp planned_asset_refs(%RunState{plan: %Favn.Plan{topo_order: refs}})
       when is_list(refs) and refs != [],
       do: refs

  defp planned_asset_refs(%RunState{target_refs: refs}) when is_list(refs) and refs != [],
    do: refs

  defp planned_asset_refs(%RunState{asset_ref: ref}) when is_tuple(ref), do: [ref]
  defp planned_asset_refs(_run_state), do: []

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

  defp execution_refs_with_stage(%RunState{submit_kind: :pipeline, plan: %Favn.Plan{} = plan}) do
    plan.node_stages
    |> Enum.with_index()
    |> Enum.flat_map(fn {node_keys, stage} ->
      Enum.map(node_keys, fn node_key -> {node_asset_ref(plan, node_key), node_key, stage} end)
    end)
  end

  defp execution_refs_with_stage(%RunState{plan: %Favn.Plan{} = plan} = run_state) do
    node_keys =
      case plan.target_node_keys do
        [_ | _] = target_node_keys -> target_node_keys
        _other -> [{run_state.asset_ref, nil}]
      end

    Enum.map(node_keys, fn node_key ->
      asset_ref = node_asset_ref(plan, node_key)
      {asset_ref, node_key, stage_from_plan(plan, node_key, 0)}
    end)
  end

  defp execution_refs_with_stage(%RunState{} = run_state),
    do: [{run_state.asset_ref, {run_state.asset_ref, nil}, 0}]

  defp stage_from_plan(%Favn.Plan{nodes: nodes}, node_key, fallback_stage) do
    case Map.get(nodes, node_key) do
      %{stage: stage} when is_integer(stage) and stage >= 0 -> stage
      _other -> fallback_stage
    end
  end

  defp normalize_results(results) when is_list(results),
    do: Enum.map(results, &sanitize_asset_result/1)

  defp normalize_results(_other), do: []

  defp sanitize_runner_result(%RunnerResult{} = result) do
    %{
      result
      | error: sanitize_error(result.error),
        asset_results: normalize_results(result.asset_results)
    }
  end

  defp sanitize_asset_result(%RunnerAssetResult{} = result) do
    %{result | error: sanitize_error(result.error), attempts: sanitize_attempts(result.attempts)}
  end

  defp sanitize_asset_result(%AssetResult{} = result) do
    %{result | error: sanitize_error(result.error), attempts: sanitize_attempts(result.attempts)}
  end

  defp sanitize_asset_result(result), do: result

  defp sanitize_attempts(attempts) when is_list(attempts) do
    Enum.map(attempts, fn
      %{error: error} = attempt -> %{attempt | error: sanitize_error(error)}
      %{"error" => error} = attempt -> %{attempt | "error" => sanitize_error(error)}
      attempt -> attempt
    end)
  end

  defp sanitize_attempts(_attempts), do: []

  defp sanitize_error(nil), do: nil
  defp sanitize_error(%RunnerError{} = error), do: error

  defp sanitize_error(
         %{"kind" => _kind, "message" => _message, "reason" => _reason, "type" => _type} = error
       ) do
    %{
      "kind" => string_value(Map.fetch!(error, "kind")),
      "message" => safe_error_message(Map.fetch!(error, "message")),
      "reason" => safe_error_reason(Map.fetch!(error, "reason")),
      "type" => string_value(Map.fetch!(error, "type"))
    }
  end

  defp sanitize_error(%{kind: kind} = error) do
    reason = Map.get(error, :reason)
    message = Map.get(error, :message) || error_message(reason) || reason || "Runner error"

    %{
      "kind" => string_value(kind),
      "message" => safe_error_message(message),
      "reason" => safe_error_reason(reason),
      "type" => error_type(reason)
    }
  end

  defp sanitize_error(%{type: _type} = error), do: sanitize_structured_error(error)
  defp sanitize_error(%{"type" => _type} = error), do: sanitize_structured_error(error)

  defp sanitize_error(error) do
    %{
      "kind" => "error",
      "message" => safe_error_message(error_message(error) || error),
      "reason" => safe_error_reason(error),
      "type" => error_type(error)
    }
  end

  defp safe_error_message(value) do
    case redact_error_field(:message, value) do
      nil -> "Runner error"
      redacted -> string_value(redacted)
    end
  end

  defp safe_error_reason(value), do: redact_error_field(:reason, value) |> inspect_value()

  defp redact_error_field(key, value) when is_atom(key) do
    case Redaction.redact_operational(%{key => value}) do
      %{^key => redacted} -> redacted
      _other -> "[REDACTED]"
    end
  rescue
    _error -> "[REDACTED]"
  end

  defp sanitize_structured_error(%{type: :missing_runtime_config} = error),
    do: sanitize_runtime_config_diagnostic(error)

  defp sanitize_structured_error(%{"type" => "missing_runtime_config"} = error),
    do: sanitize_runtime_config_diagnostic(error)

  defp sanitize_structured_error(error) when is_map(error) do
    error
    |> Map.drop([:stacktrace, "stacktrace"])
    |> Map.new(fn {key, value} -> {key, sanitize_structured_error_value(key, value)} end)
  end

  defp sanitize_runtime_config_diagnostic(error) when is_map(error) do
    error
    |> Map.drop([:stacktrace, "stacktrace"])
    |> Map.new(fn {key, value} -> {key, sanitize_runtime_config_diagnostic_value(key, value)} end)
  end

  defp sanitize_runtime_config_diagnostic_value(key, value) when key in [:message, "message"],
    do: string_value(value)

  defp sanitize_runtime_config_diagnostic_value(_key, value) when is_map(value),
    do: sanitize_runtime_config_diagnostic(value)

  defp sanitize_runtime_config_diagnostic_value(_key, value) when is_list(value),
    do: Enum.map(value, &sanitize_runtime_config_diagnostic_nested/1)

  defp sanitize_runtime_config_diagnostic_value(_key, value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&sanitize_runtime_config_diagnostic_nested/1)
    |> List.to_tuple()
  end

  defp sanitize_runtime_config_diagnostic_value(_key, value), do: value

  defp sanitize_runtime_config_diagnostic_nested(value) when is_map(value),
    do: sanitize_runtime_config_diagnostic(value)

  defp sanitize_runtime_config_diagnostic_nested(value) when is_list(value),
    do: Enum.map(value, &sanitize_runtime_config_diagnostic_nested/1)

  defp sanitize_runtime_config_diagnostic_nested(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&sanitize_runtime_config_diagnostic_nested/1)
    |> List.to_tuple()
  end

  defp sanitize_runtime_config_diagnostic_nested(value), do: value

  defp sanitize_structured_error_value(key, value) do
    cond do
      operational_error_key?(key) ->
        redact_error_field(normalize_error_key(key), value)

      is_map(value) ->
        sanitize_structured_error(value)

      is_list(value) ->
        Enum.map(value, &sanitize_structured_error_nested/1)

      is_tuple(value) ->
        value
        |> Tuple.to_list()
        |> Enum.map(&sanitize_structured_error_nested/1)
        |> List.to_tuple()

      true ->
        value
    end
  end

  defp sanitize_structured_error_nested(value) when is_map(value),
    do: sanitize_structured_error(value)

  defp sanitize_structured_error_nested(value) when is_list(value),
    do: Enum.map(value, &sanitize_structured_error_nested/1)

  defp sanitize_structured_error_nested(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&sanitize_structured_error_nested/1)
    |> List.to_tuple()
  end

  defp sanitize_structured_error_nested(value), do: value

  defp operational_error_key?(key) when key in [:message, :reason, :error, :exception], do: true

  defp operational_error_key?(key) when key in ["message", "reason", "error", "exception"],
    do: true

  defp operational_error_key?(_key), do: false

  defp normalize_error_key(key) when is_atom(key), do: key
  defp normalize_error_key(key) when is_binary(key), do: String.to_existing_atom(key)

  defp error_message(%{__exception__: true} = exception) do
    Exception.message(exception)
  rescue
    _error -> nil
  end

  defp error_message(_error), do: nil

  defp error_type(%{__exception__: true, __struct__: module}), do: Atom.to_string(module)
  defp error_type(error) when is_atom(error), do: Atom.to_string(error)
  defp error_type(error), do: error |> term_type() |> Atom.to_string()

  defp term_type(term) when is_map(term), do: :map
  defp term_type(term) when is_tuple(term), do: :tuple
  defp term_type(term) when is_list(term), do: :list
  defp term_type(term) when is_binary(term), do: :string
  defp term_type(term) when is_number(term), do: :number
  defp term_type(term) when is_boolean(term), do: :boolean
  defp term_type(_term), do: :term

  defp string_value(value) when is_binary(value), do: truncate_string(value)
  defp string_value(value) when is_atom(value), do: Atom.to_string(value)
  defp string_value(value), do: inspect_value(value)

  defp inspect_value(value) do
    value
    |> inspect(limit: 20, printable_limit: 4_096)
    |> truncate_string()
  rescue
    _error -> "#Inspect.Error<>"
  end

  defp truncate_string(value) when is_binary(value) do
    if byte_size(value) > 8_192 do
      String.slice(value, 0, 8_192) <> "..."
    else
      value
    end
  end

  defp merge_runner_metadata(run_metadata, runner_metadata)
       when is_map(run_metadata) and is_map(runner_metadata) do
    if map_size(runner_metadata) == 0 do
      run_metadata
    else
      Map.put(run_metadata, :runner_metadata, runner_metadata)
    end
  end

  defp merge_runner_metadata(run_metadata, _runner_metadata) when is_map(run_metadata),
    do: run_metadata

  defp return_external_cancel(%RunState{} = run_state, step_results) do
    case Snapshots.cancelled_state(run_state) do
      {:error, cancelled, _} -> {cancelled, :error, step_results}
    end
  end

  defp configured_runner_client do
    RuntimeConfig.current().runner_client
  end

  defp configured_runner_opts do
    RuntimeConfig.current().runner_client_opts
  end

  defp validate_runner_client(module) when is_atom(module) do
    callbacks =
      RunnerClient.behaviour_info(:callbacks) -- RunnerClient.behaviour_info(:optional_callbacks)

    with {:module, ^module} <- Code.ensure_loaded(module),
         true <-
           Enum.all?(callbacks, fn {name, arity} -> function_exported?(module, name, arity) end) do
      :ok
    else
      _ -> {:error, :runner_client_not_available}
    end
  end

  defp validate_runner_client(_module), do: {:error, :runner_client_not_available}
end
