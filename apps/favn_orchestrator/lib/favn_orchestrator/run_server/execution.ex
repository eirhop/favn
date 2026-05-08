defmodule FavnOrchestrator.RunServer.Execution do
  @moduledoc """
  Executes manifest-pinned runs against the configured runner client.

  Pipeline runs execute one topological stage at a time. Entries in the same
  stage are independent siblings, so a failed sibling must not cancel the rest
  of that stage. The current stage is drained and all submitted sibling outcomes
  are persisted before the run decides whether later stages may continue.

  Future freshness policy integration should hook in between drained stages:
  after this module knows which upstream refs finished, failed, or timed out,
  the next-stage planner can treat already-fresh upstream refs as satisfied and
  mark only downstream refs with unsatisfied failed dependencies as blocked.
  """

  alias Favn.Contracts.RunnerClient
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias Favn.Run.AssetResult
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @await_task_timeout_buffer_ms 2_000

  def execute_plan(%RunState{submit_kind: :pipeline} = run_state, %Version{} = version),
    do: execute_pipeline_parallel_once(run_state, version)

  def execute_plan(%RunState{submit_kind: submit_kind} = run_state, %Version{})
      when submit_kind in [:backfill_asset, :backfill_pipeline] do
    Snapshots.snapshot_update(run_state,
      status: :error,
      error: {:unsupported_submit_kind, submit_kind},
      runner_execution_id: nil,
      result: %{status: :error, asset_results: [], metadata: run_state.metadata}
    )
  end

  def execute_plan(
        %RunState{submit_kind: :rerun, metadata: metadata} = run_state,
        %Version{} = version
      )
      when is_map(metadata) do
    if Map.get(metadata, :replay_submit_kind) == :pipeline do
      execute_pipeline_parallel_once(run_state, version)
    else
      execute_plan_sequential(run_state, version)
    end
  end

  def execute_plan(%RunState{} = run_state, %Version{} = version),
    do: execute_plan_sequential(run_state, version)

  defp execute_plan_sequential(%RunState{} = run_state, %Version{} = version) do
    refs_with_stage = execution_refs_with_stage(run_state)

    refs_with_stage
    |> Enum.reduce_while({run_state, []}, fn {asset_ref, stage}, {current_run, acc_results} ->
      case execute_ref_with_retry(current_run, version, asset_ref, stage, 1) do
        {:ok, next_run, step_results} ->
          {:cont, {next_run, acc_results ++ step_results}}

        {:error, failed_run, step_results} ->
          terminal =
            Snapshots.snapshot_update(failed_run,
              runner_execution_id: nil,
              result: %{
                status: failed_run.status,
                asset_results: acc_results ++ step_results,
                metadata: failed_run.metadata
              }
            )

          {:halt, terminal}
      end
    end)
    |> case do
      {final_run, all_results} ->
        Snapshots.snapshot_update(final_run,
          status: :ok,
          error: nil,
          runner_execution_id: nil,
          result: %{status: :ok, asset_results: all_results, metadata: final_run.metadata}
        )

      %RunState{} = terminal ->
        terminal
    end
  end

  defp execute_pipeline_parallel_once(%RunState{} = run_state, %Version{} = version) do
    runner_client = configured_runner_client()
    runner_opts = configured_runner_opts()

    with :ok <- validate_runner_client(runner_client),
         :ok <- runner_client.register_manifest(version, runner_opts) do
      stage_groups = pipeline_stage_groups(run_state)

      stage_groups
      |> Enum.reduce_while({run_state, []}, fn {stage, node_keys}, {current_run, acc_results} ->
        if Persistence.externally_cancelled?(current_run.id) do
          {:halt, Snapshots.cancelled_terminal(current_run, acc_results)}
        else
          case run_stage_parallel_once(
                 current_run,
                 version,
                 stage,
                 node_keys,
                 runner_client,
                 runner_opts
               ) do
            {:ok, next_run, stage_results} ->
              {:cont, {next_run, acc_results ++ stage_results}}

            {:error, failed_run, stage_results} ->
              all_results = sort_asset_results(failed_run, acc_results ++ stage_results)

              {:halt, Snapshots.terminalize_failed_run(failed_run, all_results)}
          end
        end
      end)
      |> case do
        {final_run, all_results} ->
          all_results = sort_asset_results(final_run, all_results)

          Snapshots.snapshot_update(final_run,
            status: :ok,
            error: nil,
            runner_execution_id: nil,
            result: %{status: :ok, asset_results: all_results, metadata: final_run.metadata}
          )

        %RunState{} = terminal ->
          terminal
      end
    else
      {:error, reason} ->
        Snapshots.snapshot_update(run_state,
          status: :error,
          error: reason,
          runner_execution_id: nil,
          result: %{status: :error, asset_results: [], metadata: run_state.metadata}
        )
    end
  end

  defp run_stage_parallel_once(
         %RunState{} = run_state,
         %Version{} = version,
         stage,
         node_keys,
         runner_client,
         runner_opts
       ) do
    run_stage_attempt(run_state, version, stage, node_keys, 1, runner_client, runner_opts, [])
  end

  defp run_stage_attempt(
         %RunState{} = run_state,
         %Version{} = version,
         stage,
         node_keys,
         attempt,
         runner_client,
         runner_opts,
         acc_results
       ) do
    if Persistence.externally_cancelled?(run_state.id) do
      {:error, Snapshots.cancelled_snapshot(run_state), acc_results}
    else
      with {:ok, run_after_submit, entries} <-
             submit_stage_entries(
               run_state,
               version,
               stage,
               node_keys,
               attempt,
               runner_client,
               runner_opts
             ),
           {:ok, next_run, next_acc_results, retry_refs} <-
             process_stage_attempt_results(
               run_after_submit,
               entries,
               stage,
               attempt,
               acc_results,
               runner_client,
               runner_opts
             ) do
        if retry_refs == [] do
          {:ok, next_run, next_acc_results}
        else
          run_after_schedule =
            Enum.reduce(retry_refs, next_run, fn node_key, current ->
              schedule_retry_for_ref(current, node_key, stage, attempt)
            end)

          run_stage_attempt(
            run_after_schedule,
            version,
            stage,
            retry_refs,
            attempt + 1,
            runner_client,
            runner_opts,
            next_acc_results
          )
        end
      end
    end
  end

  defp submit_stage_entries(
         %RunState{} = run_state,
         %Version{} = version,
         stage,
         node_keys,
         attempt,
         runner_client,
         runner_opts
       ) do
    node_keys
    |> Enum.reduce_while({:ok, run_state, []}, fn node_key, {:ok, current_run, acc} ->
      work = stage_work(current_run, version, node_key, stage, attempt)
      asset_ref = work.asset_ref

      case runner_client.submit_work(work, runner_opts) do
        {:ok, execution_id} ->
          updated_run = with_inflight_execution(current_run, execution_id, work.metadata)

          case Persistence.persist_run_step(updated_run, :step_started, %{
                 asset_ref: asset_ref,
                 runner_execution_id: execution_id,
                 stage: stage,
                 attempt: attempt,
                 max_attempts: current_run.max_attempts
               }) do
            :ok ->
              entry = %{
                asset_ref: asset_ref,
                node_key: node_key,
                execution_id: execution_id,
                stage: stage
              }

              {:cont, {:ok, updated_run, acc ++ [entry]}}

            {:error, :external_cancel} ->
              cancelled =
                cancel_execution_ids(
                  updated_run,
                  [execution_id],
                  %{kind: :external_cancel, asset_ref: asset_ref, stage: stage, attempt: attempt},
                  runner_client,
                  runner_opts
                )

              {:halt, {:error, Snapshots.cancelled_snapshot(cancelled), []}}
          end

        {:error, reason} ->
          cancelled =
            cancel_execution_ids(
              current_run,
              Enum.map(acc, & &1.execution_id),
              %{kind: :submit_failure, asset_ref: asset_ref, error: reason},
              runner_client,
              runner_opts
            )

          failed =
            RunState.transition(cancelled,
              status: :error,
              error: reason,
              runner_execution_id: nil
            )

          case Persistence.persist_run_step(failed, :step_failed, %{
                 asset_ref: asset_ref,
                 error: reason,
                 stage: stage,
                 attempt: attempt,
                 max_attempts: current_run.max_attempts
               }) do
            :ok ->
              {:halt, {:error, failed, []}}

            {:error, :external_cancel} ->
              {:halt, {:error, Snapshots.cancelled_snapshot(failed), []}}
          end
      end
    end)
    |> case do
      {:ok, run_after_submit, entries} -> {:ok, run_after_submit, entries}
      {:error, failed_run, step_results} -> {:error, failed_run, step_results}
    end
  end

  defp await_stage_entries(entries, timeout_ms, runner_client, runner_opts) do
    entries
    |> start_await_tasks(timeout_ms, runner_client, runner_opts)
    |> collect_await_tasks(await_deadline(timeout_ms), [])
  end

  defp start_await_tasks(entries, timeout_ms, runner_client, runner_opts) do
    parent = self()

    entries
    |> Enum.reduce(%{replies: %{}, monitors: %{}}, fn entry, acc ->
      reply_ref = make_ref()

      pid =
        spawn(fn ->
          send(
            parent,
            {reply_ref, await_runner_result(entry, timeout_ms, runner_client, runner_opts)}
          )
        end)

      monitor_ref = Process.monitor(pid)
      await = %{pid: pid, monitor_ref: monitor_ref, entry: entry}

      %{
        replies: Map.put(acc.replies, reply_ref, await),
        monitors: Map.put(acc.monitors, monitor_ref, reply_ref)
      }
    end)
  end

  defp await_runner_result(entry, timeout_ms, runner_client, runner_opts) do
    runner_client.await_result(entry.execution_id, timeout_ms, runner_opts)
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

  defp await_deadline(timeout_ms),
    do: System.monotonic_time(:millisecond) + timeout_ms + @await_task_timeout_buffer_ms

  defp collect_await_tasks(%{replies: replies}, _deadline, acc) when map_size(replies) == 0,
    do: Enum.reverse(acc)

  defp collect_await_tasks(%{replies: replies, monitors: monitors}, deadline, acc) do
    receive_timeout_ms = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {reply_ref, result} when is_map_key(replies, reply_ref) ->
        {%{monitor_ref: monitor_ref, entry: entry}, next_replies} = Map.pop!(replies, reply_ref)
        Process.demonitor(monitor_ref, [:flush])

        next_pending_tasks = %{
          replies: next_replies,
          monitors: Map.delete(monitors, monitor_ref)
        }

        collect_await_tasks(next_pending_tasks, deadline, [{entry, result} | acc])

      {:DOWN, monitor_ref, :process, _pid, reason} when is_map_key(monitors, monitor_ref) ->
        {reply_ref, next_monitors} = Map.pop!(monitors, monitor_ref)
        {%{entry: entry}, next_replies} = Map.pop!(replies, reply_ref)

        next_pending_tasks = %{replies: next_replies, monitors: next_monitors}

        collect_await_tasks(next_pending_tasks, deadline, [
          {entry, await_exit_to_error(reason)} | acc
        ])
    after
      receive_timeout_ms ->
        timed_out =
          Enum.map(replies, fn {reply_ref, %{pid: pid, monitor_ref: monitor_ref, entry: entry}} ->
            Process.exit(pid, :kill)
            Process.demonitor(monitor_ref, [:flush])
            flush_await_reply(reply_ref)

            {entry, {:error, :timeout}}
          end)

        Enum.reverse(acc) ++ timed_out
    end
  end

  defp flush_await_reply(reply_ref) do
    receive do
      {^reply_ref, _result} -> :ok
    after
      0 -> :ok
    end
  end

  defp await_exit_to_error(reason) do
    {:error, %{type: :await_task_failed, kind: :exit, reason: inspect(reason)}}
  end

  defp process_stage_attempt_results(
         %RunState{} = run_state,
         entries,
         stage,
         attempt,
         acc_results,
         runner_client,
         runner_opts
       ) do
    pending_ids = pending_execution_ids(entries)

    entries
    |> await_stage_entries(run_state.timeout_ms, runner_client, runner_opts)
    |> Enum.reduce_while({:ok, run_state, acc_results, [], nil, pending_ids}, fn {entry,
                                                                                  await_result},
                                                                                 {:ok,
                                                                                  current_run,
                                                                                  current_results,
                                                                                  retry_refs,
                                                                                  terminal_failure,
                                                                                  pending_ids} ->
      if Persistence.externally_cancelled?(current_run.id) do
        cancelled =
          cancel_execution_ids(
            current_run,
            inflight_ids_from_metadata(current_run),
            %{kind: :external_cancel},
            runner_client,
            runner_opts
          )

        {:halt,
         {:error, Snapshots.cancelled_terminal(cancelled, current_results), current_results}}
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

        next_results = current_results ++ step_results
        next_pending_ids = MapSet.delete(pending_ids, entry.execution_id)

        reduce_stage_attempt_outcome(
          outcome,
          %{
            run: next_run,
            results: next_results,
            retry_refs: retry_refs,
            terminal_failure: terminal_failure,
            pending_ids: next_pending_ids
          },
          %{entry: entry, stage: stage, attempt: attempt}
        )
      end
    end)
    |> case do
      {:ok, next_run, next_results, retry_refs, nil, _pending_ids} ->
        {:ok, next_run, next_results, retry_refs}

      {:ok, next_run, next_results, _retry_refs, terminal_failure, _pending_ids} ->
        failed_run = failed_stage_terminal_state(next_run, terminal_failure)
        {:error, failed_run, next_results}

      {:error, failed_run, next_results} ->
        {:error, failed_run, next_results}
    end
  end

  defp reduce_stage_attempt_outcome(
         :ok,
         %{
           run: %RunState{} = next_run,
           results: next_results,
           retry_refs: retry_refs,
           terminal_failure: terminal_failure,
           pending_ids: pending_ids
         },
         _context
       ) do
    {:cont, {:ok, next_run, next_results, retry_refs, terminal_failure, pending_ids}}
  end

  defp reduce_stage_attempt_outcome(
         :retry,
         %{
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

    {:cont, {:ok, next_run, next_results, next_retry_refs, terminal_failure, pending_ids}}
  end

  defp reduce_stage_attempt_outcome(
         :error,
         %{run: %RunState{status: :cancelled} = next_run, results: next_results},
         _context
       ) do
    {:halt, {:error, Snapshots.cancelled_terminal(next_run, next_results), next_results}}
  end

  defp reduce_stage_attempt_outcome(
         :error,
         %{
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
        {:cont, {:ok, failure_run, next_results, retry_refs, next_terminal_failure, pending_ids}}

      {:error, cancelled} ->
        {:halt, {:error, Snapshots.cancelled_terminal(cancelled, next_results), next_results}}
    end
  end

  defp pending_execution_ids(entries) when is_list(entries) do
    entries
    |> Enum.map(& &1.execution_id)
    |> Enum.filter(&is_binary/1)
    |> MapSet.new()
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
      metadata =
        Map.put(run_state.metadata, :stage_draining_after_failure, %{
          stage: stage,
          attempt: attempt,
          failed_asset_ref: entry.asset_ref,
          pending_execution_ids: MapSet.to_list(pending_ids)
        })

      draining = RunState.transition(run_state, metadata: metadata)

      case Persistence.persist_run_step(draining, :stage_draining_after_failure, %{
             stage: stage,
             attempt: attempt,
             failed_asset_ref: entry.asset_ref,
             pending_execution_ids: MapSet.to_list(pending_ids)
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
         %{asset_ref: asset_ref, execution_id: execution_id},
         {:ok, %RunnerResult{} = result},
         stage,
         attempt,
         _runner_client,
         _runner_opts
       ) do
    result = sanitize_runner_result(result)
    cleared = clear_inflight_execution(run_state, execution_id)
    step_status = map_runner_status(result.status)
    {event_type, retryable?} = step_outcome(step_status)

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
           stage: stage,
           attempt: attempt,
           max_attempts: run_state.max_attempts
         }) do
      :ok ->
        outcome =
          if retryable? and attempt < run_state.max_attempts,
            do: :retry,
            else: if(retryable?, do: :error, else: :ok)

        {step_state, outcome, normalize_results(result.asset_results)}

      {:error, :external_cancel} ->
        return_external_cancel(run_state, normalize_results(result.asset_results))
    end
  end

  defp process_one_stage_attempt_result(
         %RunState{} = run_state,
         %{asset_ref: asset_ref, execution_id: execution_id},
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
           stage: stage,
           attempt: attempt,
           max_attempts: run_state.max_attempts
         }) do
      :ok ->
        outcome = if attempt < run_state.max_attempts, do: :retry, else: :error
        {step_state, outcome, []}

      {:error, :external_cancel} ->
        return_external_cancel(run_state, [])
    end
  end

  defp process_one_stage_attempt_result(
         %RunState{} = run_state,
         %{asset_ref: asset_ref, execution_id: execution_id},
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
           stage: stage,
           attempt: attempt,
           max_attempts: run_state.max_attempts
         }) do
      :ok ->
        outcome = if attempt < run_state.max_attempts, do: :retry, else: :error
        {step_state, outcome, []}

      {:error, :external_cancel} ->
        return_external_cancel(run_state, [])
    end
  end

  defp schedule_retry_for_ref(%RunState{} = run_state, node_key, stage, attempt) do
    asset_ref = node_asset_ref(run_state, node_key)

    retrying =
      RunState.transition(run_state,
        status: :running,
        error: nil,
        runner_execution_id: nil,
        metadata: Map.merge(run_state.metadata, %{retrying: true, next_attempt: attempt + 1})
      )

    case Persistence.persist_run_step(retrying, :step_retry_scheduled, %{
           asset_ref: asset_ref,
           stage: stage,
           attempt: attempt,
           max_attempts: run_state.max_attempts,
           next_attempt: attempt + 1,
           retry_backoff_ms: run_state.retry_backoff_ms
         }) do
      :ok ->
        if run_state.retry_backoff_ms > 0, do: Process.sleep(run_state.retry_backoff_ms)
        retrying

      {:error, :external_cancel} ->
        Snapshots.cancelled_snapshot(retrying)
    end
  end

  defp node_asset_ref(%RunState{plan: %Favn.Plan{nodes: nodes}}, node_key) do
    case Map.fetch(nodes, node_key) do
      {:ok, node} -> node.ref
      :error -> elem(node_key, 0)
    end
  end

  defp stage_work(%RunState{} = run_state, %Version{} = version, node_key, stage, attempt) do
    node = Map.fetch!(run_state.plan.nodes, node_key)
    asset_ref = node.ref

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
          max_attempts: run_state.max_attempts,
          stage: stage,
          node_key: node_key
        })
    }
  end

  defp maybe_put_pipeline_trigger(trigger, pipeline_context) when is_map(pipeline_context),
    do: Map.put(trigger, :pipeline, pipeline_context)

  defp maybe_put_pipeline_trigger(trigger, _pipeline_context), do: trigger

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
    unique_ids = execution_ids |> Enum.filter(&is_binary/1) |> Enum.uniq()

    Enum.each(unique_ids, fn execution_id ->
      _ =
        runner_client.cancel_work(
          execution_id,
          %{run_id: run_state.id, reason: reason, requested_at: DateTime.utc_now()},
          runner_opts
        )
    end)

    clear_inflight_executions(run_state, unique_ids)
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

  defp execute_ref_with_retry(
         %RunState{} = run_state,
         %Version{} = version,
         asset_ref,
         stage,
         attempt
       ) do
    if Persistence.externally_cancelled?(run_state.id) do
      Snapshots.cancelled_state(run_state)
    else
      runner_client = configured_runner_client()
      runner_opts = configured_runner_opts()
      max_attempts = run_state.max_attempts

      work = %RunnerWork{
        run_id: run_state.id,
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: asset_ref,
        asset_refs: [asset_ref],
        planned_asset_refs: planned_asset_refs(run_state),
        params: run_state.params,
        trigger: run_state.trigger,
        metadata:
          Map.merge(work_metadata(run_state.metadata), %{
            attempt: attempt,
            max_attempts: max_attempts,
            stage: stage,
            node_key: {asset_ref, nil}
          })
      }

      with :ok <- validate_runner_client(runner_client),
           :ok <- runner_client.register_manifest(version, runner_opts),
           {:ok, execution_id} <- runner_client.submit_work(work, runner_opts) do
        running_with_execution =
          RunState.transition(run_state,
            runner_execution_id: execution_id,
            metadata: work.metadata
          )

        case Persistence.persist_run_step(running_with_execution, :step_started, %{
               asset_ref: asset_ref,
               runner_execution_id: execution_id,
               stage: stage,
               attempt: attempt,
               max_attempts: max_attempts
             }) do
          :ok ->
            await_sequential_result(
              running_with_execution,
              version,
              asset_ref,
              %{
                stage: stage,
                attempt: attempt,
                execution_id: execution_id,
                max_attempts: max_attempts
              },
              runner_client,
              runner_opts
            )

          {:error, :external_cancel} ->
            cancelled =
              cancel_execution_ids(
                running_with_execution,
                [execution_id],
                %{kind: :external_cancel, asset_ref: asset_ref, stage: stage, attempt: attempt},
                runner_client,
                runner_opts
              )

            {:error, Snapshots.cancelled_snapshot(cancelled), []}
        end
      else
        {:error, reason} ->
          failed =
            RunState.transition(run_state,
              status: :error,
              runner_execution_id: nil,
              error: reason
            )

          case Persistence.persist_run_step(failed, :step_failed, %{
                 asset_ref: asset_ref,
                 error: reason,
                 stage: stage,
                 attempt: attempt,
                 max_attempts: max_attempts
               }) do
            :ok -> maybe_retry_step(failed, version, asset_ref, stage, attempt, true, [])
            {:error, :external_cancel} -> Snapshots.cancelled_state(failed)
          end
      end
    end
  end

  defp await_sequential_result(
         %RunState{} = running_with_execution,
         %Version{} = version,
         asset_ref,
         %{
           stage: stage,
           attempt: attempt,
           execution_id: execution_id,
           max_attempts: max_attempts
         },
         runner_client,
         runner_opts
       ) do
    case runner_client.await_result(execution_id, running_with_execution.timeout_ms, runner_opts) do
      {:ok, %RunnerResult{} = result} ->
        result = sanitize_runner_result(result)

        step_finished =
          RunState.transition(running_with_execution,
            status: map_runner_status(result.status),
            runner_execution_id: nil,
            error: result.error,
            metadata: merge_runner_metadata(running_with_execution.metadata, result.metadata)
          )

        {event_type, retryable?} = step_outcome(step_finished.status)

        case Persistence.persist_run_step(step_finished, event_type, %{
               asset_ref: asset_ref,
               result_status: result.status,
               error: result.error,
               stage: stage,
               attempt: attempt,
               max_attempts: max_attempts
             }) do
          :ok ->
            maybe_retry_step(
              step_finished,
              version,
              asset_ref,
              stage,
              attempt,
              retryable?,
              result.asset_results
            )

          {:error, :external_cancel} ->
            Snapshots.cancelled_state(running_with_execution)
        end

      {:error, :timeout} ->
        cancelled =
          cancel_execution_ids(
            running_with_execution,
            [execution_id],
            %{kind: :await_timeout, asset_ref: asset_ref, stage: stage, attempt: attempt},
            runner_client,
            runner_opts
          )

        timeout_state =
          RunState.transition(cancelled,
            status: :timed_out,
            runner_execution_id: nil,
            error: :timeout
          )

        case Persistence.persist_run_step(timeout_state, :step_timed_out, %{
               asset_ref: asset_ref,
               error: :timeout,
               stage: stage,
               attempt: attempt,
               max_attempts: max_attempts
             }) do
          :ok ->
            maybe_retry_step(timeout_state, version, asset_ref, stage, attempt, true, [])

          {:error, :external_cancel} ->
            Snapshots.cancelled_state(running_with_execution)
        end

      {:error, reason} ->
        cancelled =
          cancel_execution_ids(
            running_with_execution,
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

        failed =
          RunState.transition(cancelled,
            status: :error,
            runner_execution_id: nil,
            error: reason
          )

        case Persistence.persist_run_step(failed, :step_failed, %{
               asset_ref: asset_ref,
               error: reason,
               stage: stage,
               attempt: attempt,
               max_attempts: max_attempts
             }) do
          :ok ->
            maybe_retry_step(failed, version, asset_ref, stage, attempt, true, [])

          {:error, :external_cancel} ->
            Snapshots.cancelled_state(running_with_execution)
        end
    end
  end

  defp maybe_retry_step(
         %RunState{status: status} = run_state,
         _version,
         _asset_ref,
         _stage,
         _attempt,
         _retryable,
         step_results
       )
       when status in [:ok, :cancelled] do
    {:ok, run_state, normalize_results(step_results)}
  end

  defp maybe_retry_step(
         %RunState{} = run_state,
         version,
         asset_ref,
         stage,
         attempt,
         retryable,
         step_results
       ) do
    if Persistence.externally_cancelled?(run_state.id) do
      Snapshots.cancelled_state(run_state)
    else
      if retryable and attempt < run_state.max_attempts do
        retrying =
          RunState.transition(run_state,
            status: :running,
            error: nil,
            runner_execution_id: nil,
            metadata: Map.merge(run_state.metadata, %{retrying: true, next_attempt: attempt + 1})
          )

        case Persistence.persist_run_step(retrying, :step_retry_scheduled, %{
               asset_ref: asset_ref,
               stage: stage,
               attempt: attempt,
               max_attempts: run_state.max_attempts,
               next_attempt: attempt + 1,
               retry_backoff_ms: run_state.retry_backoff_ms
             }) do
          :ok ->
            if run_state.retry_backoff_ms > 0, do: Process.sleep(run_state.retry_backoff_ms)

            execute_ref_with_retry(retrying, version, asset_ref, stage, attempt + 1)

          {:error, :external_cancel} ->
            Snapshots.cancelled_state(run_state)
        end
      else
        {:error, run_state, normalize_results(step_results)}
      end
    end
  end

  defp execution_refs_with_stage(%RunState{submit_kind: :pipeline, plan: %Favn.Plan{} = plan}) do
    plan.topo_order
    |> Enum.with_index()
    |> Enum.map(fn {ref, fallback_stage} -> {ref, stage_from_plan(plan, ref, fallback_stage)} end)
  end

  defp execution_refs_with_stage(%RunState{} = run_state), do: [{run_state.asset_ref, 0}]

  defp stage_from_plan(%Favn.Plan{nodes: nodes}, ref, fallback_stage) do
    case Map.get(nodes, {ref, nil}) do
      %{stage: stage} when is_integer(stage) and stage >= 0 -> stage
      _other -> fallback_stage
    end
  end

  defp map_runner_status(:ok), do: :ok
  defp map_runner_status(:cancelled), do: :cancelled
  defp map_runner_status(:timed_out), do: :timed_out
  defp map_runner_status(_other), do: :error

  defp step_outcome(:ok), do: {:step_finished, false}
  defp step_outcome(:cancelled), do: {:step_cancelled, false}
  defp step_outcome(:timed_out), do: {:step_timed_out, true}
  defp step_outcome(:error), do: {:step_failed, true}
  defp step_outcome(_other), do: {:step_failed, true}

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

  defp work_metadata(metadata) when is_map(metadata) do
    Map.delete(metadata, :runner_metadata)
  end

  defp return_external_cancel(%RunState{} = run_state, step_results) do
    case Snapshots.cancelled_state(run_state) do
      {:error, cancelled, _} -> {cancelled, :error, step_results}
    end
  end

  defp configured_runner_client do
    Application.get_env(:favn_orchestrator, :runner_client, nil)
  end

  defp configured_runner_opts do
    Application.get_env(:favn_orchestrator, :runner_client_opts, [])
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
