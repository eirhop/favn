defmodule FavnOrchestrator.RunManager do
  @moduledoc """
  Final durable run admission, cancellation, and per-run server startup.

  Run producers enqueue through `FavnOrchestrator.RunSubmissions`; only its
  fenced preparation workers call the internal admission entrypoint here.

  A crashed run server releases local tracking and memory capacity only.
  `FavnOrchestrator.RunRecovery` resumes its durable work after ownership is
  released or expires. A process exit is not an instruction to cancel its tasks.
  """

  use GenServer

  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunOwnership, as: Ownership
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunManager.Submission
  alias FavnOrchestrator.RunManager.PlanCapacity
  alias FavnOrchestrator.RunOwnership
  alias FavnOrchestrator.RunServer
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.TransitionWriter

  @type state :: %{
          run_pids: %{required({String.t(), String.t()}) => pid()},
          lifecycles: map(),
          process_monitors: map(),
          resume_waiters: map(),
          max_active: pos_integer(),
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
  @spec recover_claimed_run(WorkspaceContext.t(), Ownership.t()) ::
          {:ok, String.t()} | {:error, term()}
  def recover_claimed_run(%WorkspaceContext{} = context, %Ownership{} = ownership) do
    call_manager({:recover_claimed_run, context, ownership})
  end

  @doc false
  def recover_candidate(context, run_id), do: call_manager({:recover_candidate, context, run_id})

  @doc false
  def recover_cancellation(context, run_id),
    do: call_manager({:cleanup_candidate, context, run_id})

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
    {:ok,
     %{
       run_pids: %{},
       plan_capacity: PlanCapacity.new(opts),
       lifecycles: %{},
       process_monitors: %{},
       resume_waiters: %{},
       max_active: Keyword.get(opts, :max_active_runs, 64)
     }}
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
      state.lifecycles
      |> Map.values()
      |> Enum.map(& &1.key)
      |> Enum.uniq()
      |> Enum.map(fn {workspace_id, run_id} -> %{workspace_id: workspace_id, run_id: run_id} end)
      |> Enum.sort_by(&{&1.workspace_id, &1.run_id})

    {:reply, {:ok, runs}, state}
  end

  def handle_call(:stop_active_for_shutdown, _from, state) do
    ids = Map.keys(state.lifecycles)
    next = Enum.reduce(ids, state, &stop_lifecycle(&2, &1, :shutdown))
    {:reply, {:ok, length(ids)}, next}
  end

  def handle_call({:notify_cancellation, run_key, reason}, _from, state) do
    if active_run_server?(state, run_key) do
      :ok = notify_active_run_server(state, run_key, reason)
      {:reply, :active, state}
    else
      {:reply, :inactive, state}
    end
  end

  def handle_call({:reserve_resume, key}, {caller, _}, state) do
    if Map.has_key?(state.resume_waiters, key) do
      {:reply, {:error, :resume_in_progress}, state}
    else
      token = make_ref()

      waiter = %{
        from: nil,
        token: token,
        monitor: Process.monitor(caller),
        stopped?: false,
        abandoned?: false
      }

      {:reply, {:ok, token}, put_in(state.resume_waiters[key], waiter)}
    end
  end

  def handle_call({:stop_for_resume, key, token}, from, state) do
    case state.resume_waiters[key] do
      %{token: ^token, from: nil} = waiter ->
        ids = for {id, entry} <- state.lifecycles, entry.key == key, do: id
        Process.send_after(self(), {:resume_barrier_timeout, key, token}, 10_000)
        state = put_in(state.resume_waiters[key], %{waiter | from: from, stopped?: ids == []})
        state = Enum.reduce(ids, state, &stop_lifecycle(&2, &1, :operator_resume))
        if ids == [], do: GenServer.reply(from, {:ok, token})
        {:noreply, state}

      _ ->
        {:reply, {:error, :resume_barrier_lost}, state}
    end
  end

  def handle_call({:finish_resume, key, token}, _from, state) do
    case state.resume_waiters[key] do
      %{token: ^token, monitor: monitor} ->
        Process.demonitor(monitor, [:flush])
        state = abandon_resume(state, key)
        {:reply, :ok, state}

      _ ->
        {:reply, {:error, :resume_barrier_lost}, state}
    end
  end

  def handle_call({:cleanup_candidate, context, run_id}, _from, state) do
    case begin_lifecycle(state, context, run_id, nil, :cleanup) do
      {:ok, next} -> {:reply, :ok, next}
      {:error, :run_start_capacity} -> {:reply, :ok, state}
      error -> {:reply, error, state}
    end
  end

  def handle_call({:recover_candidate, context, run_id}, _from, state) do
    key = {context.workspace_id, run_id}

    if Enum.any?(state.lifecycles, fn {_, e} -> e.key == key and e.phase == :preparing end) do
      {:reply, {:error, :already_preparing}, state}
    else
      case begin_lifecycle(state, context, run_id, nil) do
        {:ok, state} -> {:reply, {:ok, run_id}, state}
        error -> {:reply, error, state}
      end
    end
  end

  def handle_call({:recover_claimed_run, context, ownership}, _from, state) do
    case begin_lifecycle(state, context, ownership.run_id, ownership) do
      {:ok, state} -> {:reply, {:ok, ownership.run_id}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:helper_authority, parent}, _from, state) do
    entry =
      Enum.find_value(state.lifecycles, fn {_, e} ->
        if MapSet.member?(e.pids, parent), do: e
      end)

    case entry do
      nil -> {:reply, nil, state}
      %{phase: :stopping} -> {:reply, :stopping, state}
      entry -> {:reply, entry.ownership, state}
    end
  end

  def handle_call({:register_helper, authority, parent, pid}, _from, state) do
    key = authority_key(authority)
    generation = authority_generation(authority)

    entry =
      Enum.find(state.lifecycles, fn {_id, entry} ->
        entry.key == key and not is_nil(entry.ownership) and
          entry.ownership.fencing_token == generation and
          entry.phase != :stopping and MapSet.member?(entry.pids, parent)
      end)

    case entry do
      {id, _} -> {:reply, :ok, monitor_process(state, id, pid, :helper)}
      nil -> {:reply, {:error, :run_lifecycle_stopped}, state}
    end
  end

  @impl true
  def handle_info({:preparation_claim, id, pid, started, {:ok, ownership}}, state) do
    case state.lifecycles[id] do
      %{preparer: ^pid, phase: :preparing, ownership: nil, claim_deadline: deadline} ->
        if System.monotonic_time(:millisecond) < deadline do
          {:noreply, attach_keeper(state, id, ownership, started)}
        else
          release_async(state, ownership)
          {:noreply, stop_lifecycle(state, id, :run_claim_timeout)}
        end

      _ ->
        release_async(state, ownership)
        {:noreply, state}
    end
  end

  def handle_info({:preparation_claim, id, _, _, error}, state),
    do: {:noreply, stop_lifecycle(state, id, error)}

  def handle_info({:claim_deadline, id}, state) do
    case state.lifecycles[id] do
      %{ownership: nil, phase: :preparing} ->
        {:noreply, stop_lifecycle(state, id, :run_claim_timeout)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:preparation_failed, id, reason}, state),
    do: {:noreply, stop_lifecycle(state, id, reason)}

  def handle_info({:preparation_ready, id, run, version}, state) do
    case state.lifecycles[id] do
      %{phase: :preparing} = entry ->
        with {:ok, reserved} <- reserve_run_plan(state, run),
             {:ok, pid} <-
               start_run_server(run, version,
                 recovering?: true,
                 storage_ownership: entry.ownership,
                 lease_keeper: entry.keeper
               ) do
          next = monitor_process(reserved, id, pid, :coordinator)
          entry = %{next.lifecycles[id] | phase: :running, coordinator: pid}
          send(pid, :activate)

          {:noreply,
           %{
             next
             | lifecycles: Map.put(next.lifecycles, id, entry),
               run_pids: Map.put(next.run_pids, entry.key, pid)
           }}
        else
          error -> {:noreply, stop_lifecycle(state, id, error)}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:coordinator_ready, pid, ownership}, state) do
    case Enum.find(state.lifecycles, fn {_, entry} ->
           entry.coordinator == pid and entry.ownership == ownership
         end) do
      {_, entry} -> send(entry.preparer, :stop_preparer)
      nil -> :ok
    end

    {:noreply, state}
  end

  def handle_info({:lease_revoked, keeper, ownership, reason}, state) do
    case Enum.find(state.lifecycles, fn {_, entry} ->
           entry.keeper == keeper and entry.ownership.fencing_token == ownership.fencing_token
         end) do
      {id, _} -> {:noreply, stop_lifecycle(state, id, reason)}
      nil -> {:noreply, state}
    end
  end

  def handle_info({:target_maintenance_lost, maintenance, _ownership}, state) do
    case Enum.find(state.lifecycles, fn {_, entry} -> entry.maintenance == maintenance end) do
      {id, _} -> {:noreply, stop_lifecycle(state, id, :target_maintenance_lost)}
      nil -> {:noreply, state}
    end
  end

  def handle_info({:target_maintenance_needs_attention, maintenance, reason}, state) do
    case Enum.find(state.lifecycles, fn {_, entry} -> entry.maintenance == maintenance end) do
      {id, %{phase: :preparing}} ->
        next =
          state
          |> put_in([:lifecycles, id, :diagnostic_reason], reason)
          |> put_in([:lifecycles, id, :maintenance_ready?], true)
          |> maybe_load(id)

        {:noreply, next}

      {id, _} ->
        {:noreply, stop_lifecycle(state, id, reason)}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:target_maintenance_ready, maintenance}, state) do
    case Enum.find(state.lifecycles, fn {_, entry} -> entry.maintenance == maintenance end) do
      {id, _} ->
        {:noreply,
         state |> put_in([:lifecycles, id, :maintenance_ready?], true) |> maybe_load(id)}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:resume_barrier_timeout, key, token}, state) do
    case state.resume_waiters[key] do
      %{token: ^token, stopped?: false, from: from, monitor: monitor} ->
        Process.demonitor(monitor, [:flush])
        GenServer.reply(from, {:error, :local_stop_unconfirmed})
        {:noreply, abandon_resume(state, key)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:force_stop, id}, state) do
    if entry = state.lifecycles[id], do: Enum.each(entry.pids, &Process.exit(&1, :kill))
    Process.send_after(self(), {:stop_ack_timeout, id}, 5_000)
    {:noreply, state}
  end

  def handle_info({:stop_ack_timeout, id}, state) do
    if entry = state.lifecycles[id] do
      OperationalEvents.emit(
        :run_lifecycle_stop_unconfirmed,
        %{remaining_processes: MapSet.size(entry.pids)},
        %{workspace_id: elem(entry.key, 0), run_id: elem(entry.key, 1)},
        level: :error
      )
    end

    {:noreply, state}
  end

  def handle_info({:handoff_deadline, id}, state) do
    case state.lifecycles[id] do
      %{phase: :preparing, waiting: [_ | _]} = entry ->
        reason = :prior_generation_stop_unconfirmed
        send(entry.preparer, {:load, entry.keeper, entry.ownership, reason})
        {:noreply, put_in(state.lifecycles[id].diagnostic_reason, reason)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state)
      when is_map_key(state.process_monitors, ref) do
    {{id, role}, monitors} = Map.pop(state.process_monitors, ref)
    state = %{state | process_monitors: monitors}

    case state.lifecycles[id] do
      nil ->
        {:noreply, state}

      entry ->
        entry = %{entry | pids: MapSet.delete(entry.pids, pid)}
        state = put_in(state.lifecycles[id], entry)

        expected =
          role == :helper or (role == :preparer and entry.phase == :running and reason == :normal)

        state =
          if expected or entry.phase == :stopping,
            do: state,
            else: stop_lifecycle(state, id, {role, reason})

        {:noreply, finish_stopped(state, id)}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    state =
      Enum.reduce(state.resume_waiters, state, fn {key, waiter}, acc ->
        if waiter.monitor == ref, do: abandon_resume(acc, key), else: acc
      end)

    {:noreply, state}
  end

  defp abandon_resume(state, key) do
    stopping =
      Enum.any?(state.lifecycles, fn {_, entry} ->
        entry.key == key and entry.phase == :stopping
      end)

    if stopping,
      do: put_in(state.resume_waiters[key].abandoned?, true),
      else: %{state | resume_waiters: Map.delete(state.resume_waiters, key)}
  end

  defp begin_lifecycle(state, context, run_id, ownership, purpose \\ :execution) do
    key = {context.workspace_id, run_id}
    previous = Enum.filter(state.lifecycles, fn {_, e} -> e.key == key end)

    existing =
      Enum.find(previous, fn {_, e} ->
        e.ownership && ownership && e.ownership.fencing_token >= ownership.fencing_token
      end)

    active_count =
      state.lifecycles |> Map.values() |> Enum.map(& &1.key) |> Enum.uniq() |> length()

    preparing = Enum.count(state.lifecycles, fn {_, e} -> e.phase == :preparing end)

    cond do
      Map.has_key?(state.resume_waiters, key) ->
        {:error, :resume_in_progress}

      existing != nil ->
        {_, e} = existing

        if e.ownership.owner_id == ownership.owner_id and
             e.ownership.fencing_token == ownership.fencing_token and e.phase != :stopping,
           do: {:ok, state},
           else: {:error, :stale_handoff}

      preparing >= 4 or (previous == [] and active_count >= state.max_active) ->
        {:error, :run_start_capacity}

      true ->
        id = make_ref()
        args = [id: id, context: context, run_id: run_id, purpose: purpose]

        with {:ok, pid} <- start_temporary(FavnOrchestrator.RunPreparation, args) do
          entry = %{
            key: key,
            context: context,
            ownership: nil,
            claim_deadline: System.monotonic_time(:millisecond) + 20_000,
            phase: :preparing,
            preparer: pid,
            coordinator: nil,
            keeper: nil,
            maintenance: nil,
            maintenance_ready?: false,
            diagnostic_reason: nil,
            pids: MapSet.new(),
            waiting: Enum.map(previous, &elem(&1, 0))
          }

          state = put_in(state.lifecycles[id], entry) |> monitor_process(id, pid, :preparer)

          if ownership do
            {:ok, attach_keeper(state, id, ownership, System.monotonic_time(:millisecond))}
          else
            Process.send_after(self(), {:claim_deadline, id}, 20_000)
            send(pid, :claim)
            {:ok, state}
          end
        end
    end
  end

  defp attach_keeper(state, id, ownership, started) do
    entry = state.lifecycles[id]
    state = Enum.reduce(entry.waiting, state, &stop_lifecycle(&2, &1, :takeover))

    if entry.waiting != [], do: Process.send_after(self(), {:handoff_deadline, id}, 10_000)

    opts = [
      context: entry.context,
      ownership: ownership,
      started_at: started,
      owner: entry.preparer,
      manager: self()
    ]

    case start_temporary(FavnOrchestrator.RunLeaseKeeper, opts) do
      {:ok, keeper} ->
        state = put_in(state.lifecycles[id], %{entry | ownership: ownership, keeper: keeper})
        state = monitor_process(state, id, keeper, :keeper)

        if ownership.claim_purpose in [:diagnosis, :cleanup] do
          state |> put_in([:lifecycles, id, :maintenance_ready?], true) |> maybe_load(id)
        else
          case start_temporary(FavnOrchestrator.RunTargetMaintenance,
                 context: entry.context,
                 ownership: ownership,
                 manager: self()
               ) do
            {:ok, maintenance} ->
              state
              |> put_in([:lifecycles, id, :maintenance], maintenance)
              |> monitor_process(id, maintenance, :maintenance)
              |> maybe_load(id)

            error ->
              stop_lifecycle(state, id, error)
          end
        end

      error ->
        stop_lifecycle(put_in(state.lifecycles[id].ownership, ownership), id, error)
    end
  end

  defp maybe_load(state, id) do
    entry = state.lifecycles[id]

    if entry.keeper && entry.maintenance_ready? && entry.waiting == [] &&
         entry.phase == :preparing do
      send(entry.preparer, {:load, entry.keeper, entry.ownership, entry.diagnostic_reason})
    end

    state
  end

  defp stop_lifecycle(state, id, reason) do
    case state.lifecycles[id] do
      nil ->
        state

      %{phase: :stopping} ->
        state

      entry ->
        OperationalEvents.emit(:run_lifecycle_stopping, %{}, %{
          workspace_id: elem(entry.key, 0),
          run_id: elem(entry.key, 1),
          reason: JsonSafe.error(reason)
        })

        Enum.each(entry.pids, &Process.exit(&1, :shutdown))
        Process.send_after(self(), {:force_stop, id}, 5_000)
        put_in(state.lifecycles[id].phase, :stopping)
    end
  end

  defp finish_stopped(state, id) do
    entry = state.lifecycles[id]

    if entry.phase == :stopping and MapSet.size(entry.pids) == 0 do
      if entry.ownership, do: release_async(state, entry.ownership)

      run_pids =
        if state.run_pids[entry.key] == entry.coordinator,
          do: Map.delete(state.run_pids, entry.key),
          else: state.run_pids

      state = %{state | lifecycles: Map.delete(state.lifecycles, id), run_pids: run_pids}
      replacements = Enum.filter(state.lifecycles, fn {_, e} -> e.key == entry.key end)

      state =
        case {replacements, state.resume_waiters[entry.key]} do
          {[], %{abandoned?: true}} ->
            %{state | resume_waiters: Map.delete(state.resume_waiters, entry.key)}

          {[], %{from: from, token: token, stopped?: false} = waiter} when not is_nil(from) ->
            GenServer.reply(from, {:ok, token})
            put_in(state.resume_waiters[entry.key], %{waiter | stopped?: true})

          _ ->
            state
        end

      state =
        if replacements == [],
          do: %{state | plan_capacity: PlanCapacity.release(state.plan_capacity, entry.key)},
          else: state

      Enum.reduce(replacements, state, fn {next_id, e}, acc ->
        acc
        |> put_in([:lifecycles, next_id, :waiting], List.delete(e.waiting, id))
        |> maybe_load(next_id)
      end)
    else
      state
    end
  end

  defp release_async(_state, ownership) do
    Task.Supervisor.start_child(FavnOrchestrator.RunManagerTaskSupervisor, fn ->
      RunOwnership.release(
        SystemContext.workspace(ownership.workspace_id, :run_recovery),
        ownership
      )
    end)
  end

  defp monitor_process(state, id, pid, role) do
    ref = Process.monitor(pid)

    state
    |> put_in([:process_monitors, ref], {id, role})
    |> update_in([:lifecycles, id, :pids], &MapSet.put(&1, pid))
  end

  defp start_temporary(module, args) do
    DynamicSupervisor.start_child(
      FavnOrchestrator.RunSupervisor,
      %{id: module, start: {module, :start_link, [args]}, restart: :temporary, shutdown: 5_000}
    )
  end

  defp authority_key(%Ownership{} = o), do: {o.workspace_id, o.run_id}
  defp authority_key(run), do: {run.workspace_id, run.id}
  defp authority_generation(%Ownership{} = o), do: o.fencing_token
  defp authority_generation(run), do: run.storage_fencing_token

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
         %Submission{run_state: run, workspace_context: context},
         replayed?,
         state
       ) do
    if replayed? do
      {{:ok, run.id}, state}
    else
      with :ok <- validate_admission(run, state),
           {:ok, reserved} <- reserve_run_plan(state, run),
           {:ok, next} <- begin_lifecycle(reserved, context, run.id, nil) do
        {{:ok, run.id}, next}
      else
        {:error, reason} when is_run_plan_capacity_error(reason) ->
          emit_run_plan_capacity_deferred(run, reason)
          {{:ok, run.id}, state}

        {:error, :run_start_capacity} ->
          {{:ok, run.id}, state}

        error ->
          error
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
        capacity_managed?: true,
        lease_keeper: Keyword.get(opts, :lease_keeper)
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

  defp reserve_run_plan(state, %RunState{} = run) do
    case PlanCapacity.reserve(state.plan_capacity, run_key(run), run) do
      {:ok, plan_capacity} -> {:ok, %{state | plan_capacity: plan_capacity}}
      {:error, reason} -> {:error, reason}
    end
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

  defp run_key(%RunState{workspace_id: workspace_id, id: run_id})
       when is_binary(workspace_id),
       do: {workspace_id, run_id}

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

  defp continue_inactive_cancellation(context, %RunState{} = run, _reason, _opts) do
    if RunState.terminal_status?(run.status),
      do: :ok,
      else: call_manager({:cleanup_candidate, context, run.id})
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
end
