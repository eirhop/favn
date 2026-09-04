defmodule FavnOrchestrator.RunManager do
  @moduledoc """
  Final durable run admission, cancellation, and per-run server startup.

  Run producers enqueue through `FavnOrchestrator.RunSubmissions`; only its
  fenced preparation workers call the internal admission entrypoint here.
  """

  use GenServer

  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunOwnership, as: Ownership
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunExecutionCleanup
  alias FavnOrchestrator.RunCancellation
  alias FavnOrchestrator.RunManager.Submission
  alias FavnOrchestrator.RunManager.PlanCapacity
  alias FavnOrchestrator.RunOwnership
  alias FavnOrchestrator.RunnerIdentityVerifier
  alias FavnOrchestrator.RunServer
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.TransitionWriter

  @type state :: %{
          run_pids: %{required({String.t(), String.t()}) => pid()},
          monitors: %{required(reference()) => {String.t(), String.t()}},
          plan_capacity: PlanCapacity.t()
        }

  defguardp is_run_plan_capacity_error(reason)
            when is_tuple(reason) and tuple_size(reason) == 2 and
                   elem(reason, 0) == :run_plan_capacity_exhausted

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc false
  @spec admit_claimed_submission(Submission.t()) :: {:ok, String.t()} | {:error, term()}
  def admit_claimed_submission(%Submission{} = submission) do
    persist_and_admit(submission)
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
    run_key = {context.workspace_id, run_id}

    with {:ok, safe_reason} <- sanitize_cancel_reason(reason),
         {:ok, committed} <-
           Runs.request_cancellation(
             context,
             run_id,
             safe_reason,
             Keyword.take(opts, [:command_id, :idempotency, :occurred_at])
           ) do
      continue_cancellation(context, run_key, committed, safe_reason, opts)
    else
      {:error, %Error{} = error} -> {:error, normalize_cancellation_error(error)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @spec recover_run(WorkspaceContext.t(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def recover_run(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    with {:ok, %RunState{} = run} <- Runs.get(context, run_id),
         true <- retry_wait?(run),
         {:ok, version} <- load_run_manifest(context, run) do
      call_manager({:recover_prepared_run, context, run, version})
    else
      false -> {:error, :run_not_recoverable}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @spec recover_claimed_run(WorkspaceContext.t(), Ownership.t()) ::
          {:ok, String.t()} | {:error, term()}
  def recover_claimed_run(%WorkspaceContext{} = context, %Ownership{} = ownership) do
    result =
      with true <- ownership.workspace_id == context.workspace_id,
           {:ok, %RunState{} = run} <- Runs.get(context, ownership.run_id),
           true <- run.status in [:pending, :running],
           {:ok, version} <- load_run_manifest(context, run) do
        call_manager({:recover_prepared_claimed_run, context, ownership, run, version})
      else
        false -> {:error, :run_not_recoverable}
        {:error, reason} -> {:error, reason}
      end

    case result do
      {:ok, _run_id} = success ->
        success

      {:error, reason} ->
        _release = RunOwnership.release(context, ownership)
        {:error, reason}
    end
  end

  @doc false
  @spec plan_capacity_diagnostics() :: {:ok, map()} | {:error, term()}
  def plan_capacity_diagnostics do
    call_manager(:plan_capacity_diagnostics)
  end

  @doc false
  @spec resize_active_run_memory(RunState.t(), term()) :: :ok | {:error, term()}
  def resize_active_run_memory(%RunState{} = run, retained_state) do
    call_manager(
      {:resize_active_run_memory, run_key(run), PlanCapacity.retained_term_bytes(retained_state)}
    )
  end

  @doc false
  @spec active_runs(pos_integer()) ::
          {:ok, [%{workspace_id: String.t(), run_id: String.t()}]} | {:error, term()}
  def active_runs(timeout_ms \\ run_manager_call_timeout())
      when is_integer(timeout_ms) and timeout_ms > 0,
      do: call_manager(:active_runs, timeout_ms)

  @doc false
  @spec stop_active_for_shutdown(pos_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def stop_active_for_shutdown(timeout_ms \\ run_manager_call_timeout())
      when is_integer(timeout_ms) and timeout_ms > 0,
      do: call_manager(:stop_active_for_shutdown, timeout_ms)

  @impl true
  def init(opts) do
    {:ok, %{run_pids: %{}, monitors: %{}, plan_capacity: PlanCapacity.new(opts)}}
  end

  @impl true
  def handle_call({:validate_submission, %RunState{} = run}, _from, state) do
    {:reply, PlanCapacity.validate_run(state.plan_capacity, run), state}
  end

  def handle_call(
        {:admit_persisted_submission, %Submission{} = submission, replayed?},
        _from,
        state
      ) do
    case admit_persisted_submission(submission, replayed?, state) do
      {{:ok, run_id}, next_state} ->
        OperationalEvents.emit(:run_submitted, %{count: 1}, submission.event_metadata)

        {:reply, {:ok, run_id}, next_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:plan_capacity_diagnostics, _from, state) do
    {:reply, {:ok, PlanCapacity.diagnostics(state.plan_capacity)}, state}
  end

  def handle_call({:resize_active_run_memory, run_key, required_bytes}, _from, state) do
    case PlanCapacity.resize(state.plan_capacity, run_key, required_bytes) do
      {:ok, plan_capacity} ->
        {:reply, :ok, %{state | plan_capacity: plan_capacity}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:active_runs, _from, state) do
    runs =
      state.run_pids
      |> Enum.flat_map(fn
        {{workspace_id, run_id}, pid} when is_pid(pid) ->
          if Process.alive?(pid), do: [%{workspace_id: workspace_id, run_id: run_id}], else: []

        _other ->
          []
      end)
      |> Enum.sort_by(&{&1.workspace_id, &1.run_id})

    {:reply, {:ok, runs}, state}
  end

  def handle_call(:stop_active_for_shutdown, _from, state) do
    pids =
      state.run_pids
      |> Map.values()
      |> Enum.filter(&(is_pid(&1) and Process.alive?(&1)))

    Enum.each(pids, &Process.exit(&1, :kill))
    {:reply, {:ok, length(pids)}, state}
  end

  def handle_call({:notify_cancellation, run_key, reason}, _from, state) do
    if active_run_server?(state, run_key) do
      :ok = notify_active_run_server(state, run_key, reason)
      {:reply, :active, state}
    else
      {:reply, :inactive, state}
    end
  end

  def handle_call(
        {:recover_prepared_run, %WorkspaceContext{} = context, %RunState{} = run, version},
        _from,
        state
      ) do
    case recover_prepared_run_server(context, run, version, state) do
      {{:ok, run_id}, next_state} -> {:reply, {:ok, run_id}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:recover_prepared_claimed_run, %WorkspaceContext{} = context, %Ownership{} = ownership,
         %RunState{} = run, version},
        _from,
        state
      ) do
    case recover_prepared_claimed_run_server(context, ownership, run, version, state) do
      {{:ok, run_id}, next_state} -> {:reply, {:ok, run_id}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {run_key, monitors} ->
        next_state = %{
          state
          | monitors: monitors,
            run_pids: Map.delete(state.run_pids, run_key),
            plan_capacity: PlanCapacity.release(state.plan_capacity, run_key)
        }

        if reason != :normal and Lifecycle.ensure_accepting() == :ok,
          do: schedule_crash_recovery(run_key, run_server_down_error(reason), 1)

        {:noreply, next_state}
    end
  end

  def handle_info({:retry_run_crash_recovery, run_key, error, attempt}, state) do
    if not active_run_server?(state, run_key) and Lifecycle.ensure_accepting() == :ok do
      Task.Supervisor.start_child(FavnOrchestrator.RunManagerTaskSupervisor, fn ->
        Lifecycle.with_admission(fn ->
          recover_or_terminalize_crashed_run(run_key, error, attempt)
        end)
      end)
    end

    {:noreply, state}
  end

  defp call_manager(message), do: call_manager(message, run_manager_call_timeout())

  defp call_manager(message, timeout_ms) do
    GenServer.call(__MODULE__, message, timeout_ms)
  catch
    :exit, :timeout ->
      run_manager_timeout_error()

    :exit, {:timeout, _call} ->
      run_manager_timeout_error()
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

  defp persist_and_admit(%Submission{run_state: %RunState{} = run_state} = submission) do
    result =
      with :ok <- call_manager({:validate_submission, run_state}),
           {:ok, replayed?} <- persist_submission(submission) do
        call_manager({:admit_persisted_submission, submission, replayed?})
      end

    case result do
      {:ok, _run_id} = success ->
        success

      {:error, reason} = error ->
        OperationalEvents.emit(
          :run_submission_failed,
          %{},
          %{submit_kind: submission.submit_kind, reason: reason},
          level: :warning
        )

        error
    end
  end

  defp admit_persisted_submission(
         %Submission{run_state: %RunState{} = run_state} = submission,
         replayed?,
         state
       ) do
    if replayed? do
      {{:ok, run_state.id}, state}
    else
      with :ok <- validate_admission(run_state, state) do
        case reserve_run_plan(state, run_state) do
          {:ok, reserved_state} ->
            case start_run_server(run_state, submission.manifest_version) do
              {:ok, pid} ->
                track_run_server(reserved_state, run_state, pid)

              {:error, reason} ->
                schedule_run_server_start_compensation(run_state, reason)
                {:error, {:run_server_start_failed, JsonSafe.error(reason)}}
            end

          {:error, capacity_error} ->
            emit_run_plan_capacity_deferred(run_state, capacity_error)
            {{:ok, run_state.id}, state}
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

  defp recover_prepared_run_server(
         %WorkspaceContext{} = context,
         %RunState{} = run,
         version,
         state
       ) do
    key = {context.workspace_id, run.id}

    with false <- active_run_server?(state, key),
         true <- retry_wait?(run),
         {:ok, reserved_state} <- reserve_run_plan(state, run),
         {:ok, pid} <- start_run_server(run, version, recovering?: true) do
      track_run_server(reserved_state, key, run.id, pid)
    else
      true -> {:error, {:run_already_active, run.id}}
      false -> {:error, :run_not_recoverable}
      {:error, reason} -> {:error, reason}
    end
  end

  defp recover_prepared_claimed_run_server(
         %WorkspaceContext{} = context,
         %Ownership{} = ownership,
         %RunState{} = run,
         version,
         state
       ) do
    key = {context.workspace_id, ownership.run_id}

    if active_run_server?(state, key) do
      {{:ok, run.id}, state}
    else
      with true <- ownership.workspace_id == context.workspace_id,
           true <- run.status in [:pending, :running],
           {:ok, reserved_state} <- reserve_run_plan(state, run),
           {:ok, pid} <-
             start_run_server(run, version,
               recovering?: true,
               storage_ownership: ownership
             ) do
        track_run_server(reserved_state, key, run.id, pid)
      else
        false ->
          {:error, :run_not_recoverable}

        {:error, capacity_error} when is_run_plan_capacity_error(capacity_error) ->
          emit_run_plan_capacity_deferred(ownership, capacity_error)
          {:error, capacity_error}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp load_run_manifest(%WorkspaceContext{} = context, %RunState{} = run) do
    with {:ok, version} <-
           ManifestStore.get_deployment_manifest(
             context,
             run.deployment_id,
             run.manifest_version_id
           ),
         :ok <- RunnerIdentityVerifier.verify_run_manifest(run, version) do
      {:ok, version}
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

  defp schedule_run_server_start_compensation(%RunState{} = run, reason) do
    worker = fn ->
      Lifecycle.with_admission(fn -> compensate_run_server_start(run, reason) end)
    end

    case Process.whereis(FavnOrchestrator.RunManagerTaskSupervisor) do
      pid when is_pid(pid) -> Task.Supervisor.start_child(pid, worker)
      nil -> Task.start(worker)
    end

    :ok
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
        recovering?: Keyword.get(opts, :recovering?, false),
        capacity_managed?: true
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
      shutdown: 5_000,
      type: :worker
    }

    DynamicSupervisor.start_child(FavnOrchestrator.RunSupervisor, child_spec)
  end

  defp terminalize_active_run({workspace_id, run_id}, error) when is_map(error) do
    context = SystemContext.workspace(workspace_id, :run_recovery)

    with {:ok, %RunState{} = run} <- Runs.get(context, run_id),
         false <- RunState.finalized?(run),
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
      true ->
        :ok

      {:error, reason} ->
        {:retry, reason}
    end
  end

  defp recover_or_terminalize_crashed_run({workspace_id, run_id} = run_key, error, attempt) do
    context = SystemContext.workspace(workspace_id, :run_recovery)

    case recover_run(context, run_id) do
      {:ok, ^run_id} ->
        :ok

      {:error, capacity_error} when is_run_plan_capacity_error(capacity_error) ->
        schedule_crash_recovery(run_key, error, attempt)

      {:error, _recovery_error} ->
        terminalize_or_schedule(run_key, error, attempt)
    end
  end

  defp terminalize_or_schedule(run_key, error, attempt) do
    case terminalize_active_run(run_key, error) do
      {:retry, reason} when is_tuple(run_key) and attempt < 3 ->
        Process.send_after(
          __MODULE__,
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

  defp reserve_run_plan(state, %RunState{} = run) do
    case PlanCapacity.reserve(state.plan_capacity, run_key(run), run) do
      {:ok, plan_capacity} -> {:ok, %{state | plan_capacity: plan_capacity}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp schedule_crash_recovery(run_key, error, attempt) do
    Process.send_after(__MODULE__, {:retry_run_crash_recovery, run_key, error, attempt}, 1_000)
  end

  defp emit_run_plan_capacity_deferred(
         %RunState{workspace_id: workspace_id, id: run_id},
         capacity_error
       ) do
    do_emit_run_plan_capacity_deferred(workspace_id, run_id, capacity_error)
  end

  defp emit_run_plan_capacity_deferred(
         %Ownership{workspace_id: workspace_id, run_id: run_id},
         capacity_error
       ) do
    do_emit_run_plan_capacity_deferred(workspace_id, run_id, capacity_error)
  end

  defp do_emit_run_plan_capacity_deferred(
         workspace_id,
         run_id,
         {:run_plan_capacity_exhausted, details}
       ) do
    OperationalEvents.emit(
      :run_plan_capacity_deferred,
      %{required_bytes: details.required_bytes, allocated_bytes: details.allocated_bytes},
      %{
        workspace_id: workspace_id,
        run_id: run_id,
        max_bytes: details.max_bytes
      },
      level: :warning
    )
  end

  defp terminalize_run(%RunState{} = run, error, cleanup_statuses) when is_map(error) do
    failed =
      RunState.transition(run,
        status: :error,
        error: Map.put(error, :runner_cleanup, cleanup_statuses),
        runner_task_id: nil,
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

  defp continue_cancellation(context, run_key, %{replayed?: true}, reason, opts) do
    with {:ok, %RunState{} = run} <- Runs.get(context, elem(run_key, 1)) do
      if RunState.terminal_status?(run.status) do
        {:error, :run_already_terminal}
      else
        notify_cancellation(context, run_key, run, reason, opts)
      end
    end
  end

  defp continue_cancellation(context, run_key, committed, reason, opts) do
    with :ok <- TransitionWriter.publish_committed(context, committed) do
      notify_cancellation(context, run_key, committed.run, reason, opts)
    end
  end

  defp notify_cancellation(context, run_key, run, reason, opts) do
    case call_manager({:notify_cancellation, run_key, reason}) do
      :active -> enforce_active_cancellation(run, reason, opts)
      :inactive -> continue_inactive_cancellation(context, run, reason, opts)
      {:error, _reason} = error -> error
    end
  end

  # The message to an active run server is a hint that can be lost. Durable
  # enforcement is the task cancellation itself: the store cancels queued
  # tasks atomically and the result router delivers the cancelled outcome to
  # the awaiting run server, which settles through its normal result path.
  # Dispatch runs off the caller because acknowledgement waits for
  # live-assigned tasks can take up to a second each.
  defp enforce_active_cancellation(%RunState{} = run, reason, opts) do
    dispatch = fn ->
      _outcomes =
        Cancellation.dispatch_runner_tasks(
          run,
          ActiveTaskSet.active_runner_task_ids(run),
          reason,
          Keyword.take(opts, [:wait_for_ack])
        )

      :ok
    end

    case Process.whereis(FavnOrchestrator.RunManagerTaskSupervisor) do
      nil ->
        dispatch.()

      _supervisor ->
        {:ok, _pid} =
          Task.Supervisor.start_child(FavnOrchestrator.RunManagerTaskSupervisor, dispatch)

        :ok
    end
  end

  defp continue_inactive_cancellation(context, %RunState{} = run, reason, opts) do
    if RunState.terminal_status?(run.status) do
      :ok
    else
      case forward_cancel_result(run, reason, opts) do
        :ok ->
          {cancelled, _event} = RunCancellation.finish(run, reason, DateTime.utc_now())

          TransitionWriter.persist_transition(context, cancelled, :run_cancelled, %{
            reason: reason
          })

        :pending ->
          :ok

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

  defp forward_cancel_result(%RunState{} = run, reason, opts) do
    case ActiveTaskSet.active_runner_task_ids(run) do
      [_ | _] = task_ids ->
        run
        |> Cancellation.dispatch_runner_tasks(
          task_ids,
          reason,
          Keyword.take(opts, [:wait_for_ack])
        )
        |> classify_cancel_results()

      [] ->
        :ok
    end
  end

  defp classify_cancel_results(results) do
    already_completed = Enum.filter(results, &(Map.get(&1, :status) == :already_completed))

    unconfirmed_failures =
      Enum.reject(results, &(cancel_terminalizable?(&1) or Map.get(&1, :status) == :requested))

    cond do
      unconfirmed_failures != [] ->
        {:error,
         %{
           type: :runner_cancel_failed,
           reasons: Enum.map(unconfirmed_failures, &cancel_failure_reason/1)
         }}

      Enum.any?(results, &(Map.get(&1, :status) == :requested)) ->
        :pending

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
      task_id: Map.get(result, :task_id),
      status: Map.get(result, :status),
      reason: inspect(Map.get(result, :error))
    }
  end
end
