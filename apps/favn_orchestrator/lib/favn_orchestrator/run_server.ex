defmodule FavnOrchestrator.RunServer do
  @moduledoc false

  use GenServer

  alias Favn.Contracts.RunnerClient
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @type init_arg :: %{
          required(:run_state) => RunState.t(),
          required(:version) => Version.t()
        }

  @spec start_link(init_arg()) :: GenServer.on_start()
  def start_link(args) when is_map(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(%{run_state: %RunState{}, version: %Version{}} = args) do
    {:ok, args, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, %{run_state: run_state, version: version} = state) do
    running = RunState.transition(run_state, status: :running)
    :ok = Projector.persist_snapshot_with_event(running, :run_started, %{status: running.status})

    terminal = execute_plan(running, version)

    if externally_cancelled?(terminal.id) do
      {:stop, :normal, %{state | run_state: terminal}}
    else
      terminal_event_type = terminal_event_type(terminal)

      finalized =
        RunState.transition(terminal,
          metadata: Map.put(terminal.metadata, :terminal_event_type, terminal_event_type)
        )

      :ok =
        Projector.persist_snapshot_with_event(finalized, terminal_event_type, %{
          status: finalized.status,
          error: finalized.error
        })

      {:stop, :normal, %{state | run_state: finalized}}
    end
  end

  defp execute_plan(%RunState{submit_kind: :pipeline} = run_state, %Version{} = version),
    do: execute_pipeline_parallel_once(run_state, version)

  defp execute_plan(
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

  defp execute_plan(%RunState{} = run_state, %Version{} = version),
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
            snapshot_update(failed_run,
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
        snapshot_update(final_run,
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
      |> Enum.reduce_while({run_state, []}, fn {stage, refs}, {current_run, acc_results} ->
        if externally_cancelled?(current_run.id) do
          {:halt, cancelled_terminal(current_run, acc_results)}
        else
          case run_stage_parallel_once(
                 current_run,
                 version,
                 stage,
                 refs,
                 runner_client,
                 runner_opts
               ) do
            {:ok, next_run, stage_results} ->
              {:cont, {next_run, acc_results ++ stage_results}}

            {:error, failed_run, stage_results} ->
              {:halt, terminalize_failed_run(failed_run, acc_results ++ stage_results)}
          end
        end
      end)
      |> case do
        {final_run, all_results} ->
          snapshot_update(final_run,
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
        snapshot_update(run_state,
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
         refs,
         runner_client,
         runner_opts
       ) do
    run_stage_attempt(run_state, version, stage, refs, 1, runner_client, runner_opts, [])
  end

  defp run_stage_attempt(
         %RunState{} = run_state,
         %Version{} = version,
         stage,
         refs,
         attempt,
         runner_client,
         runner_opts,
         acc_results
       ) do
    with {:ok, run_after_submit, entries} <-
           submit_stage_entries(
             run_state,
             version,
             stage,
             refs,
             attempt,
             runner_client,
             runner_opts
           ),
         awaited <- await_stage_entries(entries, run_state.timeout_ms, runner_client, runner_opts),
         {:ok, next_run, next_acc_results, retry_refs} <-
           process_stage_attempt_results(
             run_after_submit,
             awaited,
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
          Enum.reduce(retry_refs, next_run, fn ref, current ->
            schedule_retry_for_ref(current, ref, stage, attempt)
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

  defp submit_stage_entries(
         %RunState{} = run_state,
         %Version{} = version,
         stage,
         refs,
         attempt,
         runner_client,
         runner_opts
       ) do
    refs
    |> Enum.reduce_while({:ok, run_state, []}, fn asset_ref, {:ok, current_run, acc} ->
      work = stage_work(current_run, version, asset_ref, stage, attempt)

      case runner_client.submit_work(work, runner_opts) do
        {:ok, execution_id} ->
          updated_run = with_inflight_execution(current_run, execution_id, work.metadata)

          :ok =
            Projector.persist_snapshot_with_event(updated_run, :step_started, %{
              asset_ref: asset_ref,
              runner_execution_id: execution_id,
              stage: stage,
              attempt: attempt,
              max_attempts: current_run.max_attempts
            })

          entry = %{asset_ref: asset_ref, execution_id: execution_id, stage: stage}
          {:cont, {:ok, updated_run, acc ++ [entry]}}

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

          :ok =
            Projector.persist_snapshot_with_event(failed, :step_failed, %{
              asset_ref: asset_ref,
              error: reason,
              stage: stage,
              attempt: attempt,
              max_attempts: current_run.max_attempts
            })

          {:halt, {:error, failed, []}}
      end
    end)
    |> case do
      {:ok, run_after_submit, entries} -> {:ok, run_after_submit, entries}
      {:error, failed_run, step_results} -> {:error, failed_run, step_results}
    end
  end

  defp await_stage_entries(entries, timeout_ms, runner_client, runner_opts) do
    entries
    |> Task.async_stream(
      fn entry ->
        {entry, runner_client.await_result(entry.execution_id, timeout_ms, runner_opts)}
      end,
      ordered: true,
      timeout: timeout_ms + 2_000,
      max_concurrency: max(length(entries), 1),
      on_timeout: :kill_task
    )
    |> Enum.map(fn
      {:ok, {entry, result}} ->
        {entry, result}

      {:exit, _reason} = exit ->
        {%{asset_ref: nil, execution_id: nil, stage: 0}, exit_to_timeout(exit)}
    end)
  end

  defp process_stage_attempt_results(
         %RunState{} = run_state,
         awaited,
         stage,
         attempt,
         acc_results,
         runner_client,
         runner_opts
       ) do
    awaited
    |> Enum.reduce_while({:ok, run_state, acc_results, []}, fn {entry, await_result},
                                                               {:ok, current_run, current_results,
                                                                retry_refs} ->
      if externally_cancelled?(current_run.id) do
        cancelled =
          cancel_execution_ids(
            current_run,
            inflight_ids_from_metadata(current_run),
            %{kind: :external_cancel},
            runner_client,
            runner_opts
          )

        {:halt, {:error, cancelled_terminal(cancelled, current_results), current_results}}
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

        case outcome do
          :ok ->
            {:cont, {:ok, next_run, next_results, retry_refs}}

          :retry ->
            {:cont, {:ok, next_run, next_results, retry_refs ++ [entry.asset_ref]}}

          :error ->
            {:halt, {:error, next_run, next_results}}
        end
      end
    end)
    |> case do
      {:ok, next_run, next_results, retry_refs} -> {:ok, next_run, next_results, retry_refs}
      {:error, failed_run, next_results} -> {:error, failed_run, next_results}
    end
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
    cleared = clear_inflight_execution(run_state, execution_id)
    step_status = map_runner_status(result.status)
    {event_type, retryable?} = step_outcome(step_status)

    step_state =
      RunState.transition(cleared,
        status: step_status,
        error: result.error,
        metadata: result.metadata || %{},
        runner_execution_id: nil
      )

    :ok =
      Projector.persist_snapshot_with_event(step_state, event_type, %{
        asset_ref: asset_ref,
        result_status: result.status,
        error: result.error,
        stage: stage,
        attempt: attempt,
        max_attempts: run_state.max_attempts
      })

    outcome =
      if retryable? and attempt < run_state.max_attempts,
        do: :retry,
        else: if(retryable?, do: :error, else: :ok)

    {step_state, outcome, normalize_results(result.asset_results)}
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

    :ok =
      Projector.persist_snapshot_with_event(step_state, :step_timed_out, %{
        asset_ref: asset_ref,
        error: :timeout,
        stage: stage,
        attempt: attempt,
        max_attempts: run_state.max_attempts
      })

    outcome = if attempt < run_state.max_attempts, do: :retry, else: :error

    {step_state, outcome, []}
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

    :ok =
      Projector.persist_snapshot_with_event(step_state, :step_failed, %{
        asset_ref: asset_ref,
        error: reason,
        stage: stage,
        attempt: attempt,
        max_attempts: run_state.max_attempts
      })

    outcome = if attempt < run_state.max_attempts, do: :retry, else: :error

    {step_state, outcome, []}
  end

  defp schedule_retry_for_ref(%RunState{} = run_state, asset_ref, stage, attempt) do
    retrying =
      RunState.transition(run_state,
        status: :running,
        error: nil,
        runner_execution_id: nil,
        metadata: Map.merge(run_state.metadata, %{retrying: true, next_attempt: attempt + 1})
      )

    :ok =
      Projector.persist_snapshot_with_event(retrying, :step_retry_scheduled, %{
        asset_ref: asset_ref,
        stage: stage,
        attempt: attempt,
        max_attempts: run_state.max_attempts,
        next_attempt: attempt + 1,
        retry_backoff_ms: run_state.retry_backoff_ms
      })

    if run_state.retry_backoff_ms > 0, do: Process.sleep(run_state.retry_backoff_ms)

    retrying
  end

  defp stage_work(%RunState{} = run_state, %Version{} = version, asset_ref, stage, attempt) do
    %RunnerWork{
      run_id: run_state.id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: asset_ref,
      asset_refs: [asset_ref],
      params: run_state.params,
      trigger: run_state.trigger,
      metadata:
        Map.merge(run_state.metadata, %{
          attempt: attempt,
          max_attempts: run_state.max_attempts,
          stage: stage,
          node_key: {asset_ref, nil}
        })
    }
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

    snapshot_update(run_state, metadata: metadata, runner_execution_id: next_execution_id)
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

    snapshot_update(run_state, metadata: metadata, runner_execution_id: next_execution_id)
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
    |> Enum.map(fn {node_keys, stage} ->
      refs =
        node_keys
        |> Enum.map(fn
          {ref, _window} -> ref
          ref -> ref
        end)
        |> Enum.uniq()

      {stage, refs}
    end)
  end

  defp cancelled_terminal(%RunState{} = run_state, acc_results) do
    snapshot_update(run_state,
      status: :cancelled,
      runner_execution_id: nil,
      result: %{status: :cancelled, asset_results: acc_results, metadata: run_state.metadata}
    )
  end

  defp terminalize_failed_run(%RunState{} = failed_run, all_results) do
    snapshot_update(failed_run,
      runner_execution_id: nil,
      result: %{
        status: failed_run.status,
        asset_results: all_results,
        metadata: failed_run.metadata
      }
    )
  end

  defp exit_to_timeout(_exit), do: {:error, :timeout}

  defp execute_ref_with_retry(
         %RunState{} = run_state,
         %Version{} = version,
         asset_ref,
         stage,
         attempt
       ) do
    if externally_cancelled?(run_state.id) do
      cancelled_state(run_state)
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
        params: run_state.params,
        trigger: run_state.trigger,
        metadata:
          Map.merge(run_state.metadata, %{
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

        :ok =
          Projector.persist_snapshot_with_event(running_with_execution, :step_started, %{
            asset_ref: asset_ref,
            runner_execution_id: execution_id,
            stage: stage,
            attempt: attempt,
            max_attempts: max_attempts
          })

        case runner_client.await_result(execution_id, run_state.timeout_ms, runner_opts) do
          {:ok, %RunnerResult{} = result} ->
            step_finished =
              RunState.transition(running_with_execution,
                status: map_runner_status(result.status),
                runner_execution_id: nil,
                error: result.error,
                metadata: result.metadata || %{}
              )

            {event_type, retryable?} = step_outcome(step_finished.status)

            :ok =
              Projector.persist_snapshot_with_event(step_finished, event_type, %{
                asset_ref: asset_ref,
                result_status: result.status,
                error: result.error,
                stage: stage,
                attempt: attempt,
                max_attempts: max_attempts
              })

            maybe_retry_step(
              step_finished,
              version,
              asset_ref,
              stage,
              attempt,
              retryable?,
              result.asset_results
            )

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

            :ok =
              Projector.persist_snapshot_with_event(timeout_state, :step_timed_out, %{
                asset_ref: asset_ref,
                error: :timeout,
                stage: stage,
                attempt: attempt,
                max_attempts: max_attempts
              })

            maybe_retry_step(timeout_state, version, asset_ref, stage, attempt, true, [])

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

            :ok =
              Projector.persist_snapshot_with_event(failed, :step_failed, %{
                asset_ref: asset_ref,
                error: reason,
                stage: stage,
                attempt: attempt,
                max_attempts: max_attempts
              })

            maybe_retry_step(failed, version, asset_ref, stage, attempt, true, [])
        end
      else
        {:error, reason} ->
          failed =
            RunState.transition(run_state,
              status: :error,
              runner_execution_id: nil,
              error: reason
            )

          :ok =
            Projector.persist_snapshot_with_event(failed, :step_failed, %{
              asset_ref: asset_ref,
              error: reason,
              stage: stage,
              attempt: attempt,
              max_attempts: max_attempts
            })

          maybe_retry_step(failed, version, asset_ref, stage, attempt, true, [])
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
    if retryable and attempt < run_state.max_attempts do
      retrying =
        RunState.transition(run_state,
          status: :running,
          error: nil,
          runner_execution_id: nil,
          metadata: Map.merge(run_state.metadata, %{retrying: true, next_attempt: attempt + 1})
        )

      :ok =
        Projector.persist_snapshot_with_event(retrying, :step_retry_scheduled, %{
          asset_ref: asset_ref,
          stage: stage,
          attempt: attempt,
          max_attempts: run_state.max_attempts,
          next_attempt: attempt + 1,
          retry_backoff_ms: run_state.retry_backoff_ms
        })

      if run_state.retry_backoff_ms > 0, do: Process.sleep(run_state.retry_backoff_ms)

      execute_ref_with_retry(retrying, version, asset_ref, stage, attempt + 1)
    else
      {:error, run_state, normalize_results(step_results)}
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

  defp cancelled_state(%RunState{} = run_state) do
    case Storage.get_run(run_state.id) do
      {:ok, %RunState{status: :cancelled} = cancelled} ->
        {:error, cancelled, []}

      _other ->
        cancelled =
          RunState.transition(run_state,
            status: :cancelled,
            runner_execution_id: nil,
            error: {:cancelled, %{reason: :external_cancel_request}}
          )

        {:error, cancelled, []}
    end
  end

  defp normalize_results(results) when is_list(results), do: results
  defp normalize_results(_other), do: []

  defp snapshot_update(%RunState{} = run_state, attrs) when is_list(attrs) do
    run_state
    |> Map.merge(Enum.into(attrs, %{}))
    |> Map.put(:updated_at, DateTime.utc_now())
    |> RunState.with_snapshot_hash()
  end

  defp terminal_event_type(%RunState{status: :ok}), do: :run_finished
  defp terminal_event_type(%RunState{status: :cancelled}), do: :run_cancelled
  defp terminal_event_type(%RunState{status: :timed_out}), do: :run_timed_out
  defp terminal_event_type(%RunState{}), do: :run_failed

  defp externally_cancelled?(run_id) when is_binary(run_id) do
    case Storage.get_run(run_id) do
      {:ok, %RunState{status: :cancelled}} -> true
      _ -> false
    end
  end

  defp configured_runner_client do
    Application.get_env(:favn_orchestrator, :runner_client, nil)
  end

  defp configured_runner_opts do
    Application.get_env(:favn_orchestrator, :runner_client_opts, [])
  end

  defp validate_runner_client(module) when is_atom(module) do
    callbacks = RunnerClient.behaviour_info(:callbacks)

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
