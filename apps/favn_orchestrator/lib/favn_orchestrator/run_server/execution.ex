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
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunExecutionCleanup
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunOwnership
  alias FavnOrchestrator.RunnerClientValidator
  alias FavnOrchestrator.RunnerManifestRegistration
  alias FavnOrchestrator.RunnerLogBridge
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.FreshnessContext
  alias FavnOrchestrator.RunServer.Execution.PipelineRetryCheckpoint
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
  alias FavnOrchestrator.RunServer.RetryCheckpoint
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs
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

    case RunState.execution_mode(run_state) do
      :pipeline ->
        with :ok <- preflight_execution_identities(run_state),
             :ok <- RunnerClientValidator.validate(runner_client),
             {:ok, manifest_index} <- ManifestIndexCache.fetch(version),
             execution_index <- execution_index(run_state, manifest_index),
             {:ok, freshness_context} <- FreshnessContext.initialize(run_state, execution_index),
             lease_id <- manifest_lease_id(run_state),
             :ok <-
               RunnerManifestRegistration.acquire(
                 runner_client,
                 version,
                 lease_id,
                 manifest_lease_expires_at(run_state),
                 planned_asset_refs(run_state),
                 runner_opts
               ) do
          state =
            RunExecutionState.new(run_state, manifest_identity(version),
              mode: :pipeline,
              manifest_index: execution_index,
              runner_client: runner_client,
              runner_opts: runner_opts,
              manifest_lease_id: lease_id,
              stage_groups: pipeline_stage_groups(run_state),
              freshness_context: freshness_context
            )

          case restore_retry_wait(state) do
            {:ok, restored} -> {:ok, restored}
            {:error, reason} -> pipeline_start_failure(run_state, reason)
          end
        else
          {:error, reason} -> pipeline_start_failure(run_state, reason)
        end

      :sequential ->
        with :ok <- preflight_execution_identities(run_state),
             :ok <- RunnerClientValidator.validate(runner_client),
             {:ok, manifest_index} <- ManifestIndexCache.fetch(version),
             lease_id <- manifest_lease_id(run_state),
             :ok <-
               RunnerManifestRegistration.acquire(
                 runner_client,
                 version,
                 lease_id,
                 manifest_lease_expires_at(run_state),
                 planned_asset_refs(run_state),
                 runner_opts
               ) do
          state =
            RunExecutionState.new(run_state, manifest_identity(version),
              mode: :sequential,
              manifest_index: execution_index(run_state, manifest_index),
              runner_client: runner_client,
              runner_opts: runner_opts,
              manifest_lease_id: lease_id,
              sequential_refs: Sequential.refs(run_state)
            )

          case restore_retry_wait(state) do
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
    :ok = RunExecutionCleanup.release_admission(cancelled_run)

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
          Snapshots.snapshot_update(run_state,
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

  defp manifest_identity(%Version{} = version), do: %{version | manifest: nil}

  @doc false
  @spec release_manifest_lease(RunState.t()) :: :ok
  def release_manifest_lease(%RunState{} = run) do
    RunnerManifestRegistration.release(
      configured_runner_client(),
      manifest_lease_id(run),
      configured_runner_opts()
    )
  end

  defp manifest_lease_id(%RunState{} = run) do
    digest =
      :crypto.hash(:sha256, :erlang.term_to_binary({run.workspace_id, run.id}, [:deterministic]))

    "run:" <> Base.url_encode64(digest, padding: false)
  end

  defp planned_asset_refs(%RunState{plan: %Favn.Plan{topo_order: refs}})
       when is_list(refs) and refs != [],
       do: refs

  defp planned_asset_refs(%RunState{target_refs: refs}) when is_list(refs) and refs != [],
    do: refs

  defp planned_asset_refs(%RunState{asset_ref: ref}) when is_tuple(ref), do: [ref]
  defp planned_asset_refs(%RunState{}), do: []

  defp preflight_execution_identities(%RunState{} = run) do
    run
    |> planned_execution_nodes()
    |> Enum.reduce_while(:ok, fn {node_key, asset_ref}, :ok ->
      asset_step_id = AssetStepIdentity.asset_step_id(run.id, node_key, asset_ref)

      case RunExecutionOwnership.validate_identity(run.id, asset_step_id, 1) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp planned_execution_nodes(%RunState{plan: %Favn.Plan{nodes: nodes}}) do
    Enum.map(nodes, fn {node_key, node} -> {node_key, node.ref} end)
  end

  defp planned_execution_nodes(%RunState{} = run) do
    Enum.map(Sequential.refs(run), fn {asset_ref, node_key, _stage} -> {node_key, asset_ref} end)
  end

  @doc false
  @spec manifest_lease_expires_at(RunState.t()) :: DateTime.t()
  def manifest_lease_expires_at(%RunState{}) do
    lease_ms = max(RunOwnership.default_lease_duration_ms() * 2, 60_000)
    DateTime.add(DateTime.utc_now(), div(lease_ms + 999, 1_000), :second)
  end

  defp execution_index(%RunState{} = run, manifest_index) do
    refs =
      case run.plan do
        %Favn.Plan{nodes: nodes} ->
          nodes |> Map.values() |> Enum.map(& &1.ref) |> MapSet.new()

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

  defp continue_state(%RunExecutionState{status: :retry_wait} = state), do: {:cont, state}

  defp continue_state(
         %RunExecutionState{pipeline_continuation: %{kind: :stage_classification}} = state
       ),
       do: continue_stage_classification(state)

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
         {:stage_admission, attempt, {:partial_retry, _, _, _, _, _, _, _, _} = result}
       ) do
    handle_initial_stage_partial_retry(state, attempt, result)
  end

  defp resume_persisted(
         %RunExecutionState{stage_state: %StageAttemptState{}} = state,
         {:stage_admission, _attempt, {:partial_retry, _, _, _, _, _, _, _, _} = result}
       ) do
    handle_refill_stage_partial_retry(state, result)
  end

  defp resume_persisted(
         %RunExecutionState{} = state,
         {:stage_admission, _attempt, {:error, failed_run, step_results, _attempted_node_keys}}
       ) do
    terminalize_stage_admission_failure(state, failed_run, step_results)
  end

  defp start_await(%RunExecutionState{} = state, entry, kind) do
    parent = self()
    execution_id = entry.execution_id
    timeout_ms = state.run.timeout_ms
    workspace_id = state.run.workspace_id
    runner_client = state.runner_client
    runner_opts = state.runner_opts

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        send(
          parent,
          {:runner_result, execution_id,
           await_runner_result(entry, workspace_id, timeout_ms, runner_client, runner_opts)}
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

    state
    |> Sequential.handle_result(entry, result)
    |> handle_sequential_directive()
  end

  defp process_await_result(%RunExecutionState{} = state, entry, result, :pipeline) do
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

  defp finish_stage_classification(state, []) do
    state
    |> Map.put(:freshness_context, state.stage_freshness_context)
    |> Map.put(:stage_index, state.stage_index + 1)
    |> Map.put(:pipeline_continuation, nil)
    |> Map.put(:status, :starting)
    |> defer_pipeline_continue()
  end

  defp finish_stage_classification(state, runnable_node_keys) do
    state
    |> Map.put(:pipeline_continuation, nil)
    |> Map.put(:status, :submitting)
    |> submit_pipeline_stage_attempt(runnable_node_keys, 1)
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
    case submit_stage_entries(state, state.run, node_keys, attempt) do
      {:ok, run_after_submit, entries, deferred_node_keys, queued_steps, waiters,
       admission_failure} ->
        stage_state =
          StageAttemptState.new(
            run_after_submit,
            state.accumulated_results,
            entries,
            deferred_node_keys,
            queued_steps,
            admission_failure
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
       queued_steps, waiters, admission_failure} ->
        handle_initial_stage_partial_retry(
          state,
          attempt,
          {:partial_retry, retry_run, entries, deferred_node_keys, retry_node_key, failure,
           queued_steps, waiters, admission_failure},
          completed_node_statuses
        )

      {:error, failed_run, step_results, _attempted_node_keys} ->
        terminalize_stage_admission_failure(state, failed_run, step_results)

      {:retry, retry_run, retry_node_keys, attempted_node_keys} ->
        attempted_node_key_set = MapSet.new(attempted_node_keys)

        retry_delays =
          Map.new(retry_node_keys, fn node_key ->
            failure =
              if MapSet.member?(attempted_node_key_set, node_key),
                do: retry_run.error,
                else: nil

            {node_key, StepAttemptLifecycle.retry_delay_ms(retry_run, node_key, attempt, failure)}
          end)

        stage_state = %StageAttemptState{
          run: retry_run,
          results: Enum.reverse(state.accumulated_results),
          retry_refs: Enum.reverse(retry_node_keys),
          retry_ref_set: MapSet.new(retry_node_keys),
          retry_delays: retry_delays,
          attempted_node_keys: Enum.reverse(attempted_node_keys),
          attempted_node_key_set: attempted_node_key_set,
          node_statuses: completed_node_statuses
        }

        schedule_pipeline_retry(%{
          state
          | run: retry_run,
            stage_state: stage_state,
            stage_attempt: attempt
        })

      {:persist_retry, %PersistenceRetry{} = retry, reason} ->
        {:persist_retry, state, retry, reason}
    end
  end

  defp handle_initial_stage_partial_retry(
         state,
         attempt,
         {:partial_retry, retry_run, entries, deferred_node_keys, retry_node_key, failure,
          queued_steps, waiters, admission_failure},
         completed_node_statuses \\ %{}
       ) do
    stage_state =
      retry_run
      |> StageAttemptState.new(
        state.accumulated_results,
        entries,
        deferred_node_keys,
        queued_steps,
        admission_failure
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
    case pipeline_progress_action(
           state.stage_state,
           map_size(state.awaits),
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

  defp refill_or_schedule_admission(%RunExecutionState{} = state) do
    state = clear_admission_waiters(state)

    case submit_stage_entries(
           state,
           state.stage_state.run,
           state.stage_state.deferred_node_keys,
           state.stage_attempt,
           state.stage_state.queued_steps
         ) do
      {:ok, next_run, [], next_deferred_node_keys, next_queued_steps, waiters, admission_failure} ->
        stage_state =
          StageAttemptState.defer_only(
            state.stage_state,
            next_run,
            next_deferred_node_keys,
            next_queued_steps
          )
          |> StageAttemptState.add_admission_failure(admission_failure)

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

      {:ok, next_run, entries, next_deferred_node_keys, next_queued_steps, waiters,
       admission_failure} ->
        stage_state =
          StageAttemptState.add_entries(
            state.stage_state,
            entries,
            next_run,
            next_deferred_node_keys,
            next_queued_steps
          )
          |> StageAttemptState.add_admission_failure(admission_failure)

        %{state | run: next_run, stage_state: stage_state}
        |> RunExecutionState.put_admission_waiters(waiters)
        |> start_pipeline_awaits(entries)
        |> after_starting_pipeline_awaits(entries)

      {:partial_retry, retry_run, entries, next_deferred_node_keys, retry_node_key, failure,
       next_queued_steps, waiters, admission_failure} ->
        handle_refill_stage_partial_retry(
          state,
          {:partial_retry, retry_run, entries, next_deferred_node_keys, retry_node_key, failure,
           next_queued_steps, waiters, admission_failure}
        )

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
        terminalize_stage_admission_failure(state, failed_run, step_results)

      {:persist_retry, %PersistenceRetry{} = retry, reason} ->
        {:persist_retry, state, retry, reason}
    end
  end

  defp handle_refill_stage_partial_retry(
         state,
         {:partial_retry, retry_run, entries, next_deferred_node_keys, retry_node_key, failure,
          next_queued_steps, waiters, admission_failure}
       ) do
    stage_state =
      state.stage_state
      |> StageAttemptState.add_entries(
        entries,
        retry_run,
        next_deferred_node_keys,
        next_queued_steps
      )
      |> add_admission_retry(retry_run, retry_node_key, state.stage_attempt, failure)
      |> StageAttemptState.add_admission_failure(admission_failure)

    %{state | run: retry_run, stage_state: stage_state}
    |> RunExecutionState.put_admission_waiters(waiters)
    |> start_pipeline_awaits(entries)
    |> after_starting_pipeline_awaits(entries)
  end

  defp terminalize_stage_admission_failure(state, failed_run, step_results) do
    {:terminal,
     terminalize_pipeline_failed_run(
       failed_run,
       ResultBuilder.sort_asset_results(
         failed_run,
         (state.accumulated_results ++ step_results)
         |> ResultBuilder.retain_asset_results()
       ),
       %{status: failed_run.status, error: failed_run.error}
     )}
  end

  defp add_admission_retry(stage_state, run_state, node_key, attempt, failure) do
    retry_delay_ms =
      StepAttemptLifecycle.retry_delay_ms(run_state, node_key, attempt, failure)

    StageAttemptState.add_admission_retry(stage_state, node_key, retry_delay_ms)
  end

  defp after_starting_pipeline_awaits(%RunExecutionState{} = state, [_ | _]) do
    if state.stage_state.deferred_node_keys != [] and map_size(state.awaits) > 0 do
      schedule_deferred_retry(state)
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
        {next_context, persisted_run} =
          FreshnessContext.record_successes(
            next_run,
            state.version,
            node_statuses,
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
    :ok = RunExecutionCleanup.release_admission(state.run)
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
    :ok = RunExecutionCleanup.release_admission(state.run)
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
        runner_execution_id: nil,
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

  defp metadata_field(metadata, key) when is_map(metadata),
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
      manifest_index: state.manifest_index,
      stage: stage,
      node_keys: node_keys,
      decisions: state.stage_decisions,
      freshness_context: state.stage_freshness_context,
      attempt: attempt,
      runner_client: state.runner_client,
      runner_opts: state.runner_opts,
      manifest_lease_id: state.manifest_lease_id,
      queued_steps: queued_steps
    })
  end

  defp await_runner_result(entry, workspace_id, timeout_ms, runner_client, runner_opts) do
    log_context = Map.put(entry, :workspace_id, workspace_id)
    bridge = start_runner_log_bridge(runner_client, entry.execution_id, runner_opts, log_context)

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
