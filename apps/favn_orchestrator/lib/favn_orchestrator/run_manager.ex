defmodule FavnOrchestrator.RunManager do
  @moduledoc """
  Orchestrator run admission, rerun, cancellation, and per-run server startup.
  """

  use GenServer

  alias Favn.Contracts.RunnerClient
  alias Favn.Contracts.RunnerCancellation
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.RuntimeConfig
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
              case TransitionWriter.persist_transition(cancelled, :run_cancelled, %{
                     reason: reason
                   }) do
                :ok ->
                  notify_active_run_server(state, run_id, reason)
                  :ok

                error ->
                  error
              end
            else
              case forward_cancel_result(run, reason) do
                :ok ->
                  TransitionWriter.persist_transition(cancelled, :run_cancelled, %{reason: reason})

                {:recoverable, cancel_error} ->
                  cancelled = maybe_put_cancel_forward_error(cancelled, cancel_error)

                  TransitionWriter.persist_transition(cancelled, :run_cancelled, %{
                    reason: reason,
                    cancel_forward_error: cancel_error
                  })

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
        terminalize_run(run, error)

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

  defp terminalize_run(%RunState{} = run, error) when is_map(error) do
    failed =
      RunState.transition(run,
        status: :error,
        error: error,
        runner_execution_id: nil,
        metadata: Map.put(run.metadata, :terminal_event_type, :run_failed)
      )

    TransitionWriter.persist_transition(failed, :run_failed, %{
      status: failed.status,
      error: failed.error
    })
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
    if RunState.terminal?(run) do
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
    execution_ids = inflight_execution_ids(run)

    if execution_ids == [] do
      :ok
    else
      with :ok <- validate_runner_client(runner_client) do
        execution_ids
        |> Enum.map(&cancel_execution_id(run.id, &1, reason, runner_client, runner_opts))
        |> classify_cancel_results()
      end
    end
  end

  defp cancel_execution_id(run_id, execution_id, reason, runner_client, runner_opts) do
    case runner_client.cancel_work(
           execution_id,
           RunnerCancellation.request(run_id, reason),
           runner_opts
         ) do
      {:ok, %{status: status}} when status in [:acknowledged, :already_completed] ->
        {:ok, execution_id}

      {:ok, %{status: :not_found}} ->
        {:error, execution_id, :not_found}

      {:ok, %{status: status}} ->
        {:error, execution_id, status}

      :ok ->
        {:ok, execution_id}

      {:error, reason} ->
        {:error, execution_id, reason}
    end
  end

  defp classify_cancel_results(results) do
    unconfirmed_failures = Enum.reject(results, &cancel_recoverable?/1)
    recoverable_failures = Enum.filter(results, &cancel_recovered?/1)

    cond do
      unconfirmed_failures != [] ->
        {:error,
         %{
           type: :runner_cancel_failed,
           reasons: Enum.map(unconfirmed_failures, &cancel_failure_reason/1)
         }}

      recoverable_failures != [] ->
        {:recoverable,
         %{
           type: :runner_cancel_recovered,
           reasons: Enum.map(recoverable_failures, &cancel_failure_reason/1)
         }}

      true ->
        :ok
    end
  end

  defp cancel_recoverable?({:ok, _execution_id}), do: true

  defp cancel_recoverable?({:error, _execution_id, reason}),
    do: reason in [:stale_execution_id, :not_found]

  defp cancel_recovered?({:error, _execution_id, reason}),
    do: reason in [:stale_execution_id, :not_found]

  defp cancel_recovered?(_result), do: false

  defp cancel_failure_reason({:error, execution_id, reason}) do
    %{execution_id: execution_id, reason: inspect(reason)}
  end

  defp maybe_put_cancel_forward_error(%RunState{} = run, error) when is_map(error) do
    run
    |> Map.put(:metadata, Map.put(run.metadata, :cancel_forward_error, error))
    |> RunState.with_snapshot_hash()
  end

  defp inflight_execution_ids(%RunState{} = run) do
    metadata_ids =
      case Map.get(run.metadata, :in_flight_execution_ids, []) do
        ids when is_list(ids) -> ids
        _other -> []
      end

    [run.runner_execution_id | metadata_ids]
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
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
