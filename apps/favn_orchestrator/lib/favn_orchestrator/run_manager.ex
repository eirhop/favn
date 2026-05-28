defmodule FavnOrchestrator.RunManager do
  @moduledoc """
  Orchestrator run admission, rerun, cancellation, and per-run server startup.
  """

  use GenServer

  alias Favn.Contracts.RunnerClient
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunManager.Submission
  alias FavnOrchestrator.RunManager.SubmissionBuilder
  alias FavnOrchestrator.RunServer
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TransitionWriter

  @type state :: %{
          run_pids: %{required(String.t()) => pid()},
          monitors: %{required(reference()) => String.t()}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec submit_asset_run(Favn.Ref.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def submit_asset_run({module, name} = asset_ref, opts \\ [])
      when is_atom(module) and is_atom(name) and is_list(opts) do
    prepare_and_admit(:manual, fn -> SubmissionBuilder.asset(asset_ref, opts) end)
  end

  @spec submit_pipeline_run([Favn.Ref.t()], keyword()) :: {:ok, String.t()} | {:error, term()}
  def submit_pipeline_run(target_refs, opts \\ []) when is_list(target_refs) and is_list(opts) do
    prepare_and_admit(:pipeline, fn -> SubmissionBuilder.pipeline(target_refs, opts) end)
  end

  @spec submit_pipeline_module_run(module(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def submit_pipeline_module_run(pipeline_module, opts \\ [])
      when is_atom(pipeline_module) and is_list(opts) do
    prepare_and_admit(:pipeline, fn ->
      SubmissionBuilder.pipeline_module(pipeline_module, opts)
    end)
  end

  @spec rerun(String.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def rerun(source_run_id, opts \\ []) when is_binary(source_run_id) and is_list(opts) do
    prepare_and_admit(:rerun, fn -> SubmissionBuilder.rerun(source_run_id, opts) end)
  end

  @spec prepare_rerun(String.t(), keyword()) :: {:ok, Submission.t()} | {:error, term()}
  def prepare_rerun(source_run_id, opts \\ []) when is_binary(source_run_id) and is_list(opts) do
    SubmissionBuilder.rerun(source_run_id, opts)
  end

  @spec admit_prepared_submission(Submission.t()) :: {:ok, String.t()} | {:error, term()}
  def admit_prepared_submission(%Submission{} = submission) do
    call_manager({:admit_submission, submission})
  end

  @spec cancel_run(String.t(), map()) :: :ok | {:error, term()}
  def cancel_run(run_id, reason \\ %{}) when is_binary(run_id) and is_map(reason) do
    call_manager({:cancel_run, run_id, reason})
  end

  @impl true
  def init(_args), do: {:ok, %{run_pids: %{}, monitors: %{}}}

  @impl true
  def handle_call({:admit_submission, %Submission{} = submission}, _from, state) do
    case admit_submission(submission, state) do
      {{:ok, run_id}, next_state} ->
        OperationalEvents.emit(:run_submitted, %{count: 1}, submission.event_metadata)

        {:reply, {:ok, run_id}, next_state}

      {:error, reason} ->
        OperationalEvents.emit(
          :run_submission_failed,
          %{},
          %{submit_kind: submission.submit_kind, reason: reason},
          level: :warning
        )

        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cancel_run, run_id, reason}, _from, state) do
    reply =
      case Storage.get_run(run_id) do
        {:ok, run} ->
          with :ok <- validate_cancel_reason(reason),
               :ok <- reject_backfill_parent_cancel(run),
               {:ok, cancel_requested, cancelled} <- build_cancel_snapshots(run, reason),
               :ok <-
                 TransitionWriter.persist_transition(cancel_requested, :run_cancel_requested, %{
                   reason: reason
                 }) do
            if active_run_server?(state, run_id) do
              notify_active_run_server(state, run_id, reason)
              :ok
            else
              case forward_cancel_result(run, reason) do
                :ok ->
                  TransitionWriter.persist_transition(cancelled, :run_cancelled, %{reason: reason})

                {:already_completed, details} ->
                  {:error, {:runner_cancel_already_completed, details}}

                {:error, cancel_error} ->
                  {:error, {:runner_cancel_failed, cancel_error}}
              end
            end
          end

        {:error, _reason} = error ->
          error
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {run_id, monitors} ->
        if reason != :normal do
          terminalize_active_run(run_id, run_server_down_error(reason))
        end

        {:noreply, %{state | monitors: monitors, run_pids: Map.delete(state.run_pids, run_id)}}
    end
  end

  defp call_manager(message) do
    GenServer.call(__MODULE__, message, run_manager_call_timeout())
  catch
    :exit, :timeout ->
      run_manager_timeout_error()

    :exit, {:timeout, _call} ->
      run_manager_timeout_error()
  end

  defp prepare_and_admit(submit_kind, prepare) when is_function(prepare, 0) do
    case prepare.() do
      {:ok, %Submission{} = submission} ->
        call_manager({:admit_submission, submission})

      {:error, reason} = error ->
        OperationalEvents.emit(
          :run_submission_failed,
          %{},
          %{submit_kind: submit_kind, reason: reason},
          level: :warning
        )

        error
    end
  end

  defp run_manager_timeout_error do
    {:error, {:run_manager_timeout, :admission_state_unknown}}
  end

  defp run_manager_call_timeout do
    case Application.get_env(:favn_orchestrator, :run_manager_call_timeout_ms, 5_000) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> 5_000
    end
  end

  defp admit_submission(%Submission{run_state: %RunState{} = run_state} = submission, state) do
    with :ok <- validate_admission(run_state, state),
         :ok <-
           TransitionWriter.persist_transition(
             run_state,
             :run_created,
             submission.transition_metadata
           ),
         {:ok, pid} <- start_run_server(run_state, submission.manifest_version) do
      ref = Process.monitor(pid)

      next_state =
        state
        |> put_in([:run_pids, run_state.id], pid)
        |> put_in([:monitors, ref], run_state.id)

      {{:ok, run_state.id}, next_state}
    end
  end

  defp validate_admission(%RunState{id: run_id}, state) do
    if active_run_server?(state, run_id) do
      {:error, {:run_already_active, run_id}}
    else
      :ok
    end
  end

  defp start_run_server(%RunState{} = run_state, version) do
    child_spec = %{
      id: {RunServer, run_state.id},
      start: {RunServer, :start_link, [%{run_state: run_state, version: version}]},
      restart: :temporary,
      shutdown: 5000,
      type: :worker
    }

    DynamicSupervisor.start_child(FavnOrchestrator.RunSupervisor, child_spec)
  end

  defp terminalize_active_run(run_id, error) when is_binary(run_id) and is_map(error) do
    case Storage.get_run(run_id) do
      {:ok, %RunState{status: status} = run} when status in [:pending, :running] ->
        {run, cleanup_statuses} =
          cleanup_active_runner_ownerships(run, %{kind: :run_server_down, error: error})

        terminalize_run(run, error, cleanup_statuses)

      {:ok, %RunState{}} ->
        :ok

      {:error, reason} ->
        OperationalEvents.emit(
          :run_crash_terminalization_failed,
          %{},
          %{run_id: run_id, reason: reason},
          level: :error
        )
    end
  end

  defp cleanup_active_runner_ownerships(%RunState{} = run, reason) do
    case RunExecutionOwnership.fetch_active(run.id) do
      {:ok, active} ->
        execution_ids = active |> Enum.map(& &1.runner_execution_id) |> Enum.filter(&is_binary/1)
        missing_id_result = persist_missing_execution_ids(run.id, reason)

        cleanup_statuses =
          case missing_id_result do
            :ok -> []
            {:error, error} -> [%{status: :unknown_runner_outcome, error: error}]
          end

        if execution_ids == [] do
          {run, cleanup_statuses}
        else
          runtime_config = RuntimeConfig.current()
          runner_client = runtime_config.runner_client
          runner_opts = runtime_config.runner_client_opts

          results =
            case validate_runner_client(runner_client) do
              :ok ->
                FavnOrchestrator.RunServer.Cancellation.dispatch_runner_work(
                  run,
                  execution_ids,
                  reason,
                  runner_client,
                  runner_opts
                )

              {:error, error} ->
                Enum.map(execution_ids, fn execution_id ->
                  %{execution_id: execution_id, status: :unknown_runner_outcome, error: error}
                end)
            end

          persist_result = RunExecutionOwnership.persist_cancel_outcomes(run.id, results, reason)

          statuses =
            cleanup_statuses ++
              cleanup_statuses_from_results(results) ++
              persist_error_statuses(persist_result)

          {run, statuses}
        end

      {:error, error} ->
        {run,
         [
           %{
             runner_execution_id: nil,
             status: :unknown_runner_outcome,
             error: {:execution_ownership_read_failed, error}
           }
         ]}
    end
  end

  defp terminalize_run(%RunState{} = run, error, cleanup_statuses) when is_map(error) do
    failed =
      RunState.transition(run,
        status: :error,
        error: Map.put(error, :runner_cleanup, cleanup_statuses),
        runner_execution_id: nil,
        metadata: Map.put(run.metadata, :terminal_event_type, :run_failed)
      )

    TransitionWriter.persist_transition(failed, :run_failed, %{
      status: failed.status,
      error: failed.error
    })
  end

  defp persist_missing_execution_ids(run_id, reason) do
    RunExecutionOwnership.persist_unknown_without_execution_id(run_id, reason)
  end

  defp cleanup_statuses_from_results(results) do
    Enum.map(results, fn result ->
      %{
        runner_execution_id: Map.get(result, :execution_id),
        status: RunExecutionOwnership.cancel_outcome_status(result),
        error: Map.get(result, :error)
      }
    end)
  end

  defp persist_error_statuses(:ok), do: []

  defp persist_error_statuses({:error, error}) do
    [%{runner_execution_id: nil, status: :unknown_runner_outcome, error: error}]
  end

  defp run_server_down_error(reason) do
    %{
      type: :run_server_down,
      exit_reason: inspect(reason),
      crashed_at: DateTime.utc_now()
    }
  end

  defp validate_cancel_reason(value) when is_map(value), do: :ok
  defp validate_cancel_reason(_value), do: {:error, :invalid_cancel_reason}

  defp reject_backfill_parent_cancel(%RunState{submit_kind: :backfill_pipeline}),
    do: {:error, :backfill_parent_cancel_not_supported}

  defp reject_backfill_parent_cancel(_run), do: :ok

  defp build_cancel_snapshots(%RunState{} = run, reason) do
    if RunState.finalized?(run) do
      {:error, :run_already_terminal}
    else
      build_active_cancel_snapshots(run, reason)
    end
  end

  defp build_active_cancel_snapshots(%RunState{} = run, reason) do
    cancel_requested =
      RunState.transition(run,
        metadata:
          Map.merge(run.metadata, %{
            cancel_requested: true,
            cancel_reason: reason,
            cancel_requested_at: DateTime.utc_now()
          })
      )

    cancelled =
      RunState.transition(cancel_requested,
        status: :cancelled,
        error: {:cancelled, reason},
        runner_execution_id: nil,
        metadata:
          Map.merge(cancel_requested.metadata, %{
            cancelled: true,
            in_flight_execution_ids: []
          })
      )

    {:ok, cancel_requested, cancelled}
  end

  defp active_run_server?(state, run_id) do
    case Map.get(state.run_pids, run_id) do
      pid when is_pid(pid) -> Process.alive?(pid)
      _other -> false
    end
  end

  defp notify_active_run_server(state, run_id, reason) do
    case Map.get(state.run_pids, run_id) do
      pid when is_pid(pid) -> send(pid, {:favn_run_cancel_requested, reason})
      _other -> :ok
    end

    :ok
  end

  defp forward_cancel_result(%RunState{} = run, reason) do
    runtime_config = RuntimeConfig.current()
    runner_client = runtime_config.runner_client
    runner_opts = runtime_config.runner_client_opts

    with {:ok, execution_ids} <- inflight_execution_ids(run) do
      if execution_ids == [] do
        :ok
      else
        with :ok <- validate_runner_client(runner_client) do
          results =
            FavnOrchestrator.RunServer.Cancellation.dispatch_runner_work(
              run,
              execution_ids,
              reason,
              runner_client,
              runner_opts
            )

          case RunExecutionOwnership.persist_cancel_outcomes(run.id, results, reason) do
            :ok -> classify_cancel_results(results)
            {:error, error} -> {:error, %{type: :cancel_outcome_persist_failed, reason: error}}
          end
        end
      end
    end
  end

  defp classify_cancel_results(results) do
    already_completed = Enum.filter(results, &(Map.get(&1, :status) == :already_completed))
    unconfirmed_failures = Enum.reject(results, &cancel_terminalizable?/1)

    cond do
      unconfirmed_failures != [] ->
        {:error,
         %{
           type: :runner_cancel_failed,
           reasons: Enum.map(unconfirmed_failures, &cancel_failure_reason/1)
         }}

      already_completed != [] ->
        {:already_completed,
         %{
           type: :runner_cancel_already_completed,
           executions: Enum.map(already_completed, &cancel_failure_reason/1)
         }}

      true ->
        :ok
    end
  end

  defp cancel_terminalizable?(%{status: status}),
    do: status in [:acknowledged, :already_completed]

  defp cancel_terminalizable?(_result), do: false

  defp cancel_failure_reason(result) when is_map(result) do
    %{
      execution_id: Map.get(result, :execution_id),
      status: Map.get(result, :status),
      reason: inspect(Map.get(result, :error))
    }
  end

  defp inflight_execution_ids(%RunState{} = run) do
    metadata_ids =
      case Map.get(run.metadata, :in_flight_execution_ids, []) do
        ids when is_list(ids) -> ids
        _other -> []
      end

    case RunExecutionOwnership.fetch_active(run.id) do
      {:ok, ownerships} ->
        ledger_ids =
          ownerships
          |> Enum.map(& &1.runner_execution_id)
          |> Enum.filter(&is_binary/1)

        {:ok,
         [run.runner_execution_id | metadata_ids ++ ledger_ids]
         |> Enum.filter(&is_binary/1)
         |> Enum.uniq()}

      {:error, reason} ->
        {:error, {:execution_ownership_read_failed, reason}}
    end
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
