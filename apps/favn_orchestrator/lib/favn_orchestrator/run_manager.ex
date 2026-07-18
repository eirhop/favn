defmodule FavnOrchestrator.RunManager do
  @moduledoc """
  Orchestrator run admission, rerun, cancellation, and per-run server startup.
  """

  use GenServer

  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunOwnership, as: Ownership
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunExecutionCleanup
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunCancellation
  alias FavnOrchestrator.RunManager.Submission
  alias FavnOrchestrator.RunManager.SubmissionBuilder
  alias FavnOrchestrator.RunOwnership
  alias FavnOrchestrator.RunnerClientValidator
  alias FavnOrchestrator.RunServer
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.TransitionWriter

  @type state :: %{
          run_pids: %{required({String.t(), String.t()}) => pid()},
          monitors: %{required(reference()) => {String.t(), String.t()}}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec admit_prepared_submission(Submission.t()) :: {:ok, String.t()} | {:error, term()}
  def admit_prepared_submission(%Submission{} = submission) do
    call_manager({:admit_submission, submission})
  end

  @spec submit_asset_run(WorkspaceContext.t(), Favn.Ref.t(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def submit_asset_run(%WorkspaceContext{} = context, {module, name} = asset_ref, opts)
      when is_atom(module) and is_atom(name) and is_list(opts) do
    prepare_and_admit(:manual, fn -> SubmissionBuilder.asset(context, asset_ref, opts) end)
  end

  @spec submit_pipeline_run(WorkspaceContext.t(), [Favn.Ref.t()], keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def submit_pipeline_run(%WorkspaceContext{} = context, target_refs, opts)
      when is_list(target_refs) and is_list(opts) do
    prepare_and_admit(:pipeline, fn -> SubmissionBuilder.pipeline(context, target_refs, opts) end)
  end

  @spec submit_pipeline_module_run(WorkspaceContext.t(), module(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def submit_pipeline_module_run(%WorkspaceContext{} = context, pipeline_module, opts)
      when is_atom(pipeline_module) and is_list(opts) do
    prepare_and_admit(:pipeline, fn ->
      SubmissionBuilder.pipeline_module(context, pipeline_module, opts)
    end)
  end

  @doc "Submits one exact named manifest pipeline in an authorized workspace."
  @spec submit_pipeline_ref_run(WorkspaceContext.t(), {module(), atom()}, keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def submit_pipeline_ref_run(%WorkspaceContext{} = context, {module, name} = pipeline_ref, opts)
      when is_atom(module) and is_atom(name) and is_list(opts) do
    prepare_and_admit(:pipeline, fn ->
      SubmissionBuilder.pipeline_ref(context, pipeline_ref, opts)
    end)
  end

  @spec rerun(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def rerun(%WorkspaceContext{} = context, source_run_id, opts)
      when is_binary(source_run_id) and is_list(opts) do
    prepare_and_admit(:rerun, fn -> SubmissionBuilder.rerun(context, source_run_id, opts) end)
  end

  @spec prepare_rerun(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, Submission.t()} | {:error, term()}
  def prepare_rerun(%WorkspaceContext{} = context, source_run_id, opts)
      when is_binary(source_run_id) and is_list(opts) do
    SubmissionBuilder.rerun(context, source_run_id, opts)
  end

  @spec cancel_run(WorkspaceContext.t(), String.t(), map()) :: :ok | {:error, term()}
  def cancel_run(%WorkspaceContext{} = context, run_id, reason)
      when is_binary(run_id) and is_map(reason) do
    cancel_run(context, run_id, reason, [])
  end

  @doc false
  @spec cancel_run(WorkspaceContext.t(), String.t(), map(), keyword()) ::
          :ok | {:error, term()}
  def cancel_run(%WorkspaceContext{} = context, run_id, reason, opts)
      when is_binary(run_id) and is_map(reason) and is_list(opts) do
    call_manager({:cancel_run, context, run_id, reason, opts})
  end

  @doc false
  @spec recover_run(WorkspaceContext.t(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def recover_run(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    call_manager({:recover_run, context, run_id})
  end

  @doc false
  @spec recover_claimed_run(WorkspaceContext.t(), Ownership.t()) ::
          {:ok, String.t()} | {:error, term()}
  def recover_claimed_run(%WorkspaceContext{} = context, %Ownership{} = ownership) do
    call_manager({:recover_claimed_run, context, ownership})
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

  def handle_call(
        {:cancel_run, %WorkspaceContext{} = context, run_id, reason, opts},
        _from,
        state
      ) do
    run_key = {context.workspace_id, run_id}

    reply =
      with {:ok, safe_reason} <- sanitize_cancel_reason(reason),
           {:ok, committed} <-
             Runs.request_cancellation(
               context,
               run_id,
               safe_reason,
               Keyword.take(opts, [:command_id, :idempotency, :occurred_at])
             ),
           :ok <- TransitionWriter.publish_committed(context, committed) do
        continue_cancellation(context, state, run_key, committed.run, safe_reason)
      else
        {:error, %Error{} = error} -> {:error, normalize_cancellation_error(error)}
        {:error, reason} -> {:error, reason}
      end

    {:reply, reply, state}
  end

  def handle_call({:recover_run, %WorkspaceContext{} = context, run_id}, _from, state) do
    case recover_run_server(context, run_id, state) do
      {{:ok, ^run_id}, next_state} -> {:reply, {:ok, run_id}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:recover_claimed_run, %WorkspaceContext{} = context, %Ownership{} = ownership},
        _from,
        state
      ) do
    case recover_claimed_run_server(context, ownership, state) do
      {{:ok, run_id}, next_state} -> {:reply, {:ok, run_id}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {run_id, monitors} ->
        next_state = %{state | monitors: monitors, run_pids: Map.delete(state.run_pids, run_id)}

        if reason != :normal and retry_wait_run?(run_id) do
          case recover_run_server(run_id, next_state) do
            {{:ok, _recovered_run_id}, recovered_state} ->
              {:noreply, recovered_state}

            {:error, _reason} ->
              terminalize_or_schedule(run_id, run_server_down_error(reason), 1)
              {:noreply, next_state}
          end
        else
          if reason != :normal,
            do: terminalize_or_schedule(run_id, run_server_down_error(reason), 1)

          {:noreply, next_state}
        end
    end
  end

  def handle_info({:retry_run_crash_recovery, run_key, error, attempt}, state) do
    cond do
      active_run_server?(state, run_key) ->
        {:noreply, state}

      retry_wait_run?(run_key) ->
        case recover_run_server(run_key, state) do
          {{:ok, _run_id}, recovered_state} ->
            {:noreply, recovered_state}

          {:error, _reason} ->
            terminalize_or_schedule(run_key, error, attempt)
            {:noreply, state}
        end

      true ->
        terminalize_or_schedule(run_key, error, attempt)
        {:noreply, state}
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
    with {:ok, replayed?} <- persist_submission(submission) do
      if replayed? do
        {{:ok, run_state.id}, state}
      else
        with :ok <- validate_admission(run_state, state) do
          case start_run_server(run_state, submission.manifest_version) do
            {:ok, pid} -> track_run_server(state, run_state, pid)
            {:error, reason} -> compensate_run_server_start(run_state, reason)
          end
        end
      end
    end
  end

  defp persist_submission(
         %Submission{
           workspace_context: %WorkspaceContext{} = context,
           run_state: %RunState{} = run_state
         } = submission
       ) do
    TransitionWriter.persist_transition(
      context,
      run_state,
      :run_submitted,
      submission.transition_metadata,
      pipeline_refs: submission.pipeline_refs,
      idempotency: submission.idempotency,
      return_commit?: true
    )
  end

  defp track_run_server(state, %RunState{} = run, pid) do
    key = run_key(run)
    track_run_server(state, key, run.id, pid)
  end

  defp track_run_server(state, key, run_id, pid) do
    ref = Process.monitor(pid)

    next_state =
      state
      |> put_in([:run_pids, key], pid)
      |> put_in([:monitors, ref], key)

    {{:ok, run_id}, next_state}
  end

  defp recover_run_server({workspace_id, run_id}, state) do
    recover_run_server(SystemContext.workspace(workspace_id, :run_recovery), run_id, state)
  end

  defp recover_run_server(%WorkspaceContext{} = context, run_id, state) do
    key = {context.workspace_id, run_id}

    with false <- active_run_server?(state, key),
         {:ok, %RunState{} = run} <- Runs.get(context, run_id),
         true <- retry_wait?(run),
         {:ok, version} <- ManifestStore.get_manifest(context, run.manifest_version_id),
         {:ok, pid} <- start_run_server(run, version, recovering?: true) do
      track_run_server(state, key, run_id, pid)
    else
      true -> {:error, {:run_already_active, run_id}}
      false -> {:error, :run_not_recoverable}
      {:error, reason} -> {:error, reason}
    end
  end

  defp recover_claimed_run_server(
         %WorkspaceContext{} = context,
         %Ownership{} = ownership,
         state
       ) do
    key = {context.workspace_id, ownership.run_id}

    result =
      with true <- ownership.workspace_id == context.workspace_id,
           false <- active_run_server?(state, key),
           {:ok, %RunState{} = run} <- Runs.get(context, ownership.run_id),
           true <- run.status in [:pending, :running],
           {:ok, version} <- ManifestStore.get_manifest(context, run.manifest_version_id),
           {:ok, pid} <-
             start_run_server(run, version,
               recovering?: true,
               storage_ownership: ownership
             ) do
        track_run_server(state, key, run.id, pid)
      else
        false -> {:error, :run_not_recoverable}
        true -> {:error, {:run_already_active, ownership.run_id}}
        {:error, reason} -> {:error, reason}
      end

    case result do
      {{:ok, _run_id}, _next_state} = success ->
        success

      {:error, reason} ->
        _release = RunOwnership.release(context, ownership)
        {:error, reason}
    end
  end

  defp retry_wait_run?({workspace_id, run_id}) do
    context = SystemContext.workspace(workspace_id, :run_recovery)

    case Runs.get(context, run_id) do
      {:ok, run} -> retry_wait?(run)
      {:error, _reason} -> false
    end
  end

  defp retry_wait?(%RunState{status: status, metadata: metadata})
       when status in [:pending, :running] and is_map(metadata) do
    is_map(Map.get(metadata, :retry_state, Map.get(metadata, "retry_state")))
  end

  defp retry_wait?(%RunState{}), do: false

  defp compensate_run_server_start(%RunState{} = run, reason) do
    diagnostic = JsonSafe.error(reason)

    error = %{
      type: :run_server_start_failed,
      reason: diagnostic,
      failed_at: DateTime.utc_now()
    }

    case terminalize_run(run, error, []) do
      :ok ->
        {:error, {:run_server_start_failed, diagnostic}}

      {:error, terminalization_error} ->
        {:error,
         {:run_server_start_failed, diagnostic,
          {:terminalization_failed, JsonSafe.error(terminalization_error)}}}
    end
  end

  defp validate_admission(%RunState{} = run, state) do
    if active_run_server?(state, run_key(run)) do
      {:error, {:run_already_active, run.id}}
    else
      :ok
    end
  end

  defp start_run_server(%RunState{} = run_state, version, opts \\ []) when is_list(opts) do
    args =
      %{
        run_state: run_state,
        version: version,
        recovering?: Keyword.get(opts, :recovering?, false)
      }
      |> then(fn args ->
        case Keyword.get(opts, :storage_ownership) do
          %Ownership{} = ownership -> Map.put(args, :storage_ownership, ownership)
          nil -> args
        end
      end)

    child_spec = %{
      id: {RunServer, run_key(run_state)},
      start: {RunServer, :start_link, [args]},
      restart: :temporary,
      shutdown: 5000,
      type: :worker
    }

    DynamicSupervisor.start_child(FavnOrchestrator.RunSupervisor, child_spec)
  end

  defp terminalize_active_run({workspace_id, run_id}, error) when is_map(error) do
    context = SystemContext.workspace(workspace_id, :run_recovery)

    with {:ok, %RunState{status: status} = run} when status in [:pending, :running] <-
           Runs.get(context, run_id),
         owner_id = RunOwnership.owner_id(run_id),
         {:ok, ownership} <- RunOwnership.claim(context, run_id, owner_id),
         owned_run <-
           RunState.with_storage_fence(run, ownership.owner_id, ownership.fencing_token) do
      cleanup_statuses =
        RunExecutionCleanup.cancel_active(owned_run, %{kind: :run_server_down, error: error})

      result = terminalize_run(owned_run, error, cleanup_statuses)
      _release = RunOwnership.release(context, ownership)
      result
    else
      {:ok, %RunState{}} ->
        :ok

      {:error, reason} ->
        {:retry, reason}
    end
  end

  defp terminalize_or_schedule(run_key, error, attempt) do
    case terminalize_active_run(run_key, error) do
      {:retry, reason} when is_tuple(run_key) and attempt < 3 ->
        Process.send_after(
          self(),
          {:retry_run_crash_recovery, run_key, error, attempt + 1},
          RunOwnership.default_lease_duration_ms() + 1_000
        )

        OperationalEvents.emit(
          :run_crash_recovery_deferred,
          %{},
          %{run_key: run_key, attempt: attempt, reason: reason},
          level: :warning
        )

      {:retry, reason} ->
        OperationalEvents.emit(
          :run_crash_terminalization_failed,
          %{},
          %{run_key: run_key, reason: reason},
          level: :error
        )

      _result ->
        :ok
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

    transition_data = %{status: failed.status, error: failed.error}

    context = SystemContext.workspace(failed.workspace_id, :run_recovery)
    TransitionWriter.persist_transition(context, failed, :run_failed, transition_data)
  end

  defp run_key(%RunState{workspace_id: workspace_id, id: run_id})
       when is_binary(workspace_id),
       do: {workspace_id, run_id}

  defp run_server_down_error(reason) do
    %{
      type: :run_server_down,
      exit_reason: JsonSafe.error(reason),
      crashed_at: DateTime.utc_now()
    }
  end

  defp sanitize_cancel_reason(value) when is_map(value),
    do: {:ok, Redaction.redact_operational_bounded(value)}

  defp sanitize_cancel_reason(_value), do: {:error, :invalid_cancel_reason}

  defp continue_cancellation(context, state, run_key, %RunState{} = run, reason) do
    cond do
      RunState.terminal_status?(run.status) ->
        :ok

      active_run_server?(state, run_key) ->
        notify_active_run_server(state, run_key, reason)

      true ->
        case forward_cancel_result(run, reason) do
          :ok ->
            {cancelled, _event} = RunCancellation.finish(run, reason, DateTime.utc_now())

            TransitionWriter.persist_transition(context, cancelled, :run_cancelled, %{
              reason: reason
            })

          {:already_completed, details} ->
            {:error, {:runner_cancel_already_completed, details}}

          {:error, cancel_error} ->
            {:error, {:runner_cancel_failed, cancel_error}}
        end
    end
  end

  defp normalize_cancellation_error(%Error{kind: :not_found}), do: :not_found

  defp normalize_cancellation_error(%Error{kind: :conflict, details: %{reason: reason}}),
    do: reason

  defp normalize_cancellation_error(%Error{kind: :conflict, message: message}) do
    if String.contains?(message, "idempotency key"),
      do: :idempotency_conflict,
      else: {:persistence_conflict, message}
  end

  defp normalize_cancellation_error(%Error{} = error), do: error

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
        with :ok <- RunnerClientValidator.validate(runner_client) do
          results =
            Cancellation.dispatch_runner_work(
              run,
              execution_ids,
              reason,
              runner_client,
              runner_opts
            )

          case RunExecutionOwnership.persist_cancel_outcomes(run, results, reason) do
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

    case RunExecutionOwnership.fetch_active(run) do
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
end
