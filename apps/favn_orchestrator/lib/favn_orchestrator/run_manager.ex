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
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.Budget
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
    persist_and_admit(submission, nil)
  end

  @doc false
  @spec admit_claimed_submission(Submission.t(), MemoryCapacity.t()) ::
          {:ok, String.t()} | {:error, term()}
  def admit_claimed_submission(%Submission{} = submission, %MemoryCapacity{} = handoff_token) do
    persist_and_admit(submission, handoff_token)
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
      continue_cancellation(context, run_key, committed, safe_reason)
    else
      {:error, %Error{} = error} -> {:error, normalize_cancellation_error(error)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @spec recover_run(WorkspaceContext.t(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def recover_run(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    case MemoryCapacity.acquire(16 * 1_024 * 1_024, kind: :run_recovery) do
      {:ok, recovery_token} -> recover_run_with_capacity(context, run_id, recovery_token)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @spec recover_claimed_run(WorkspaceContext.t(), Ownership.t()) ::
          {:ok, String.t()} | {:error, term()}
  def recover_claimed_run(%WorkspaceContext{} = context, %Ownership{} = ownership) do
    result =
      case MemoryCapacity.acquire(16 * 1_024 * 1_024, kind: :run_recovery) do
        {:ok, recovery_token} ->
          recover_claimed_run_with_capacity(context, ownership, recovery_token)

        {:error, reason} ->
          {:error, reason}
      end

    case result do
      {:ok, _run_id} = success ->
        success

      {:error, reason} ->
        _release = RunOwnership.release(context, ownership)
        {:error, reason}
    end
  end

  defp recover_run_with_capacity(context, run_id, recovery_token) do
    case Runs.get(context, run_id, memory_capacity_token: recovery_token) do
      {:ok, %RunState{} = run} ->
        if retry_wait?(run) do
          with_run_manifest(context, run, recovery_token, fn version ->
            retained_bytes = Budget.retained_term_bytes({run, version.version})

            call_manager(
              {:recover_prepared_run, context, run, version.version, version.capacity_token,
               retained_bytes, recovery_token}
            )
          end)
        else
          MemoryCapacity.release(recovery_token)
          {:error, :run_not_recoverable}
        end

      {:error, reason} ->
        MemoryCapacity.release(recovery_token)
        {:error, reason}
    end
  end

  defp recover_claimed_run_with_capacity(context, ownership, recovery_token) do
    cond do
      ownership.workspace_id != context.workspace_id ->
        MemoryCapacity.release(recovery_token)
        {:error, :run_not_recoverable}

      true ->
        case Runs.get(context, ownership.run_id, memory_capacity_token: recovery_token) do
          {:ok, %RunState{status: status} = run} when status in [:pending, :running] ->
            with_run_manifest(context, run, recovery_token, fn version ->
              retained_bytes = Budget.retained_term_bytes({run, version.version})

              call_manager(
                {:recover_prepared_claimed_run, context, ownership, run, version.version,
                 version.capacity_token, retained_bytes, recovery_token}
              )
            end)

          {:ok, %RunState{}} ->
            MemoryCapacity.release(recovery_token)
            {:error, :run_not_recoverable}

          {:error, reason} ->
            MemoryCapacity.release(recovery_token)
            {:error, reason}
        end
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

      {{:error, reason}, next_state} ->
        {:reply, {:error, reason}, next_state}
    end
  end

  def handle_call(
        {:admit_persisted_submission, %Submission{} = submission, replayed?,
         %MemoryCapacity{} = handoff_token},
        from,
        state
      ) do
    try do
      handle_call({:admit_persisted_submission, submission, replayed?}, from, state)
    after
      MemoryCapacity.release(handoff_token)
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
        {:recover_prepared_run, %WorkspaceContext{} = context, %RunState{} = run, version,
         memory_token, reserved_bytes},
        _from,
        state
      ) do
    case recover_prepared_run_server(context, run, version, state,
           memory_capacity_token: memory_token,
           transferred_retained_bytes: reserved_bytes,
           preserve_working_memory: true
         ) do
      {{:ok, run_id}, next_state} -> {:reply, {:ok, run_id}, next_state}
      {{:error, reason}, next_state} -> {:reply, {:error, reason}, next_state}
    end
  end

  def handle_call(
        {:recover_prepared_run, %WorkspaceContext{} = context, %RunState{} = run, version,
         memory_token, reserved_bytes, %MemoryCapacity{} = recovery_token},
        from,
        state
      ) do
    try do
      handle_call(
        {:recover_prepared_run, context, run, version, memory_token, reserved_bytes},
        from,
        state
      )
    after
      MemoryCapacity.release(recovery_token)
    end
  end

  def handle_call(
        {:recover_prepared_claimed_run, %WorkspaceContext{} = context, %Ownership{} = ownership,
         %RunState{} = run, version, memory_token, reserved_bytes},
        _from,
        state
      ) do
    case recover_prepared_claimed_run_server(context, ownership, run, version, state,
           memory_capacity_token: memory_token,
           transferred_retained_bytes: reserved_bytes,
           preserve_working_memory: true
         ) do
      {{:ok, run_id}, next_state} -> {:reply, {:ok, run_id}, next_state}
      {{:error, reason}, next_state} -> {:reply, {:error, reason}, next_state}
    end
  end

  def handle_call(
        {:recover_prepared_claimed_run, %WorkspaceContext{} = context, %Ownership{} = ownership,
         %RunState{} = run, version, memory_token, reserved_bytes,
         %MemoryCapacity{} = recovery_token},
        from,
        state
      ) do
    try do
      handle_call(
        {:recover_prepared_claimed_run, context, ownership, run, version, memory_token,
         reserved_bytes},
        from,
        state
      )
    after
      MemoryCapacity.release(recovery_token)
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

  defp persist_and_admit(
         %Submission{run_state: %RunState{} = run_state} = submission,
         handoff_token
       ) do
    admission =
      with :ok <- call_manager({:validate_submission, run_state}),
           {:ok, replayed?} <- persist_submission(submission) do
        handoff_and_admit(submission, replayed?, handoff_token)
      else
        {:error, _reason} = error -> {:before_manager, error}
      end

    result =
      case admission do
        {:manager_call, result} ->
          result

        {:before_manager, result} ->
          release_handoff_token(handoff_token)
          result
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

  defp handoff_and_admit(
         %Submission{memory_capacity_token: nil} = submission,
         replayed?,
         nil
       ) do
    {:manager_call, call_manager({:admit_persisted_submission, submission, replayed?})}
  end

  defp handoff_and_admit(
         %Submission{memory_capacity_token: %MemoryCapacity{} = token} = submission,
         replayed?,
         nil
       ) do
    case Process.whereis(__MODULE__) do
      manager when is_pid(manager) ->
        case MemoryCapacity.handoff(token, manager) do
          :ok ->
            {:manager_call, call_manager({:admit_persisted_submission, submission, replayed?})}

          {:error, reason} ->
            MemoryCapacity.release(token)
            {:before_manager, {:error, reason}}
        end

      nil ->
        MemoryCapacity.release(token)
        {:before_manager, {:error, :run_manager_unavailable}}
    end
  end

  defp handoff_and_admit(
         %Submission{memory_capacity_token: %MemoryCapacity{} = token} = submission,
         replayed?,
         %MemoryCapacity{} = handoff_token
       ) do
    case Process.whereis(__MODULE__) do
      manager when is_pid(manager) ->
        with :ok <- MemoryCapacity.handoff(token, manager),
             :ok <- MemoryCapacity.handoff(handoff_token, manager) do
          {:manager_call,
           call_manager({:admit_persisted_submission, submission, replayed?, handoff_token})}
        else
          {:error, reason} ->
            MemoryCapacity.release(token)
            {:before_manager, {:error, reason}}
        end

      nil ->
        MemoryCapacity.release(token)
        {:before_manager, {:error, :run_manager_unavailable}}
    end
  end

  defp release_handoff_token(%MemoryCapacity{} = token), do: MemoryCapacity.release(token)
  defp release_handoff_token(nil), do: :ok

  defp admit_persisted_submission(
         %Submission{run_state: %RunState{} = run_state} = submission,
         replayed?,
         state
       ) do
    if replayed? do
      release_submission_token(submission)
      {{:ok, run_state.id}, state}
    else
      case validate_admission(run_state, state) do
        :ok ->
          case reserve_run_plan(state, run_state,
                 memory_capacity_token: submission.memory_capacity_token,
                 transferred_retained_bytes: submission.memory_capacity_bytes
               ) do
            {:ok, reserved_state} ->
              case start_run_server(run_state, submission.manifest_version,
                     memory_capacity_token:
                       PlanCapacity.memory_token(reserved_state.plan_capacity, run_key(run_state))
                   ) do
                {:ok, pid} ->
                  track_run_server(reserved_state, run_state, pid)

                {:error, reason} ->
                  schedule_run_server_start_compensation(run_state, reason)
                  next_state = release_run_plan(reserved_state, run_state)

                  {{:error, {:run_server_start_failed, JsonSafe.error(reason)}}, next_state}
              end

            {:error, capacity_error} ->
              emit_run_plan_capacity_deferred(run_state, capacity_error)
              release_submission_token(submission)
              {{:ok, run_state.id}, state}
          end

        {:error, reason} ->
          release_submission_token(submission)
          {{:error, reason}, state}
      end
    end
  end

  defp release_submission_token(%Submission{memory_capacity_token: %MemoryCapacity{} = token}),
    do: MemoryCapacity.release(token)

  defp release_submission_token(%Submission{}), do: :ok

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
         state,
         capacity_opts
       ) do
    key = {context.workspace_id, run.id}

    cond do
      active_run_server?(state, key) ->
        release_recovery_token(capacity_opts)
        {{:error, {:run_already_active, run.id}}, state}

      not retry_wait?(run) ->
        release_recovery_token(capacity_opts)
        {{:error, :run_not_recoverable}, state}

      true ->
        start_recovered_run(state, run, version, key, capacity_opts, recovering?: true)
    end
  end

  defp recover_prepared_claimed_run_server(
         %WorkspaceContext{} = context,
         %Ownership{} = ownership,
         %RunState{} = run,
         version,
         state,
         capacity_opts
       ) do
    key = {context.workspace_id, ownership.run_id}

    if active_run_server?(state, key) do
      release_recovery_token(capacity_opts)
      {{:ok, run.id}, state}
    else
      if ownership.workspace_id == context.workspace_id and run.status in [:pending, :running] do
        case start_recovered_run(state, run, version, key, capacity_opts,
               recovering?: true,
               storage_ownership: ownership
             ) do
          {{:error, capacity_error}, next_state}
          when is_run_plan_capacity_error(capacity_error) ->
            emit_run_plan_capacity_deferred(ownership, capacity_error)
            {{:error, capacity_error}, next_state}

          result ->
            result
        end
      else
        release_recovery_token(capacity_opts)
        {{:error, :run_not_recoverable}, state}
      end
    end
  end

  defp start_recovered_run(state, run, version, key, capacity_opts, server_opts) do
    case reserve_run_plan(state, run, capacity_opts) do
      {:ok, reserved_state} ->
        server_opts =
          Keyword.put(
            server_opts,
            :memory_capacity_token,
            PlanCapacity.memory_token(reserved_state.plan_capacity, key)
          )

        case start_run_server(run, version, server_opts) do
          {:ok, pid} ->
            track_run_server(reserved_state, key, run.id, pid)

          {:error, reason} ->
            next_state = release_run_plan(reserved_state, run)
            {{:error, reason}, next_state}
        end

      {:error, reason} ->
        release_recovery_token(capacity_opts)
        {{:error, reason}, state}
    end
  end

  defp release_recovery_token(opts) do
    case Keyword.get(opts, :memory_capacity_token) do
      %MemoryCapacity{} = token -> MemoryCapacity.release(token)
      nil -> :ok
    end
  end

  defp with_run_manifest(
         %WorkspaceContext{} = context,
         %RunState{} = run,
         %MemoryCapacity{} = recovery_token,
         fun
       ) do
    with manager when is_pid(manager) <- Process.whereis(__MODULE__),
         {:ok, lease} <-
           ManifestStore.checkout_deployment_manifest(
             context,
             run.deployment_id,
             run.manifest_version_id,
             copies: 3
           ) do
      case RunnerIdentityVerifier.verify_run_manifest(run, lease.version) do
        :ok ->
          with :ok <- MemoryCapacity.handoff(lease.capacity_token, manager),
               :ok <- MemoryCapacity.handoff(recovery_token, manager) do
            fun.(lease)
          else
            {:error, _reason} = error ->
              MemoryCapacity.release(recovery_token)
              release_manifest_lease(lease, error)
          end

        {:error, _reason} = error ->
          MemoryCapacity.release(recovery_token)
          release_manifest_lease(lease, error)
      end
    else
      nil ->
        MemoryCapacity.release(recovery_token)
        {:error, :run_manager_unavailable}

      {:error, _reason} = error ->
        MemoryCapacity.release(recovery_token)
        error
    end
  end

  defp release_manifest_lease(lease, result) do
    :ok = ManifestStore.release_manifest(lease)
    result
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

  defp start_run_server(%RunState{} = run_state, version, opts) when is_list(opts) do
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
      |> then(fn args ->
        case Keyword.get(opts, :memory_capacity_token) do
          %MemoryCapacity{} = token -> Map.put(args, :memory_capacity_token, token)
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

  defp reserve_run_plan(state, %RunState{} = run, opts) do
    case PlanCapacity.reserve(state.plan_capacity, run_key(run), run, opts) do
      {:ok, plan_capacity} -> {:ok, %{state | plan_capacity: plan_capacity}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp release_run_plan(state, %RunState{} = run) do
    %{state | plan_capacity: PlanCapacity.release(state.plan_capacity, run_key(run))}
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

  defp continue_cancellation(context, run_key, %{replayed?: true}, reason) do
    with {:ok, %RunState{} = run} <- Runs.get(context, elem(run_key, 1)) do
      if RunState.terminal_status?(run.status) do
        {:error, :run_already_terminal}
      else
        notify_cancellation(context, run_key, run, reason)
      end
    end
  end

  defp continue_cancellation(context, run_key, committed, reason) do
    with :ok <- TransitionWriter.publish_committed(context, committed) do
      notify_cancellation(context, run_key, committed.run, reason)
    end
  end

  defp notify_cancellation(context, run_key, run, reason) do
    case call_manager({:notify_cancellation, run_key, reason}) do
      :active -> enforce_active_cancellation(run, reason)
      :inactive -> continue_inactive_cancellation(context, run, reason)
      {:error, _reason} = error -> error
    end
  end

  # The message to an active run server is a hint that can be lost. Durable
  # enforcement is the task cancellation itself: the store cancels queued
  # tasks atomically and the result router delivers the cancelled outcome to
  # the awaiting run server, which settles through its normal result path.
  # Dispatch runs off the caller because acknowledgement waits for
  # live-assigned tasks can take up to a second each.
  defp enforce_active_cancellation(%RunState{} = run, reason) do
    dispatch = fn ->
      _outcomes =
        Cancellation.dispatch_runner_tasks(
          run,
          ActiveTaskSet.active_runner_task_ids(run),
          reason
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

  defp continue_inactive_cancellation(context, %RunState{} = run, reason) do
    if RunState.terminal_status?(run.status) do
      :ok
    else
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
    case ActiveTaskSet.active_runner_task_ids(run) do
      [_ | _] = task_ids ->
        run
        |> Cancellation.dispatch_runner_tasks(task_ids, reason)
        |> classify_cancel_results()

      [] ->
        :ok
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
      task_id: Map.get(result, :task_id),
      status: Map.get(result, :status),
      reason: inspect(Map.get(result, :error))
    }
  end
end
