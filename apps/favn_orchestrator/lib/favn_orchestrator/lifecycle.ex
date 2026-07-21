defmodule FavnOrchestrator.Lifecycle do
  @moduledoc """
  Owns the control-plane admission and shutdown lifecycle for one BEAM.

  Admission permits make a drain boundary explicit: work admitted before a
  drain may finish, while work arriving after the transition is rejected with
  a stable reason. Permits are monitored so a failed caller cannot keep the
  runtime permanently busy.
  """

  use GenServer

  alias FavnOrchestrator.OperationalEvents

  @type state_name :: :starting | :accepting | :draining | :stopping
  @type admission_error :: :runtime_starting | :runtime_draining | :runtime_not_accepting

  @type state :: %{
          status: state_name(),
          admissions: %{optional(reference()) => {pid(), reference()}},
          monitors: %{optional(reference()) => reference()},
          changed_at: DateTime.t(),
          shutdown_drain_timeout_ms: pos_integer(),
          shutdown: :not_started | {:in_progress, pid(), reference()} | {:complete, map()},
          shutdown_waiters: [GenServer.from()]
        }

  @doc "Starts the lifecycle authority in `:starting`."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Transitions a successfully booted runtime from `:starting` to `:accepting`."
  @spec mark_accepting(GenServer.server()) :: :ok | {:error, admission_error()}
  def mark_accepting(server \\ __MODULE__), do: GenServer.call(server, :mark_accepting)

  @doc "Monotonically transitions the runtime into `:draining`."
  @spec drain(GenServer.server()) :: :ok
  def drain(server \\ __MODULE__), do: GenServer.call(server, :drain)

  @doc "Monotonically transitions the runtime into `:stopping`."
  @spec stop(GenServer.server()) :: :ok
  def stop(server \\ __MODULE__), do: GenServer.call(server, :stop)

  @doc "Atomically elects one shutdown coordinator."
  @spec begin_shutdown(GenServer.server()) :: :leader | :in_progress | {:complete, map()}
  def begin_shutdown(server \\ __MODULE__), do: GenServer.call(server, :begin_shutdown)

  @doc "Waits for the elected shutdown coordinator's reusable result."
  @spec await_shutdown(timeout(), GenServer.server()) ::
          {:complete, map()} | {:error, :shutdown_coordinator_failed | :shutdown_wait_timeout}
  def await_shutdown(timeout_ms, server \\ __MODULE__)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    GenServer.call(server, :await_shutdown, timeout_ms)
  catch
    :exit, _reason -> {:error, :shutdown_wait_timeout}
  end

  @doc "Records the reusable terminal result from the elected shutdown coordinator."
  @spec complete_shutdown(map(), GenServer.server()) ::
          :ok | {:error, :not_shutdown_coordinator}
  def complete_shutdown(result, server \\ __MODULE__) when is_map(result) do
    GenServer.call(server, {:complete_shutdown, result})
  end

  @doc "Returns `:ok` only while new work may be admitted."
  @spec ensure_accepting(GenServer.server()) :: :ok | {:error, admission_error()}
  def ensure_accepting(server \\ __MODULE__) do
    GenServer.call(server, :ensure_accepting)
  catch
    :exit, _reason -> {:error, :runtime_not_accepting}
  end

  @doc "Acquires a monitored permit for work admitted before the drain boundary."
  @spec acquire_admission(GenServer.server()) ::
          {:ok, reference()} | {:error, admission_error()}
  def acquire_admission(server \\ __MODULE__) do
    GenServer.call(server, {:acquire, self()})
  catch
    :exit, _reason -> {:error, :runtime_not_accepting}
  end

  @doc "Releases a previously acquired admission permit."
  @spec release_admission(reference(), GenServer.server()) :: :ok
  def release_admission(permit, server \\ __MODULE__) when is_reference(permit) do
    GenServer.call(server, {:release, permit})
  catch
    :exit, _reason -> :ok
  end

  @doc "Runs a function under a monitored admission permit."
  @spec with_admission((-> result), GenServer.server()) :: result | {:error, admission_error()}
        when result: term()
  def with_admission(fun, server \\ __MODULE__) when is_function(fun, 0) do
    case acquire_admission(server) do
      {:ok, permit} ->
        try do
          fun.()
        after
          release_admission(permit, server)
        end

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Returns bounded lifecycle diagnostics."
  @spec diagnostics(GenServer.server()) :: map()
  def diagnostics(server \\ __MODULE__) do
    GenServer.call(server, :diagnostics)
  catch
    :exit, _reason ->
      %{
        status: :unavailable,
        ready?: false,
        accepting?: false,
        available?: false,
        active_admissions: :unknown
      }
  end

  @doc "Returns the frozen shutdown drain timeout."
  @spec shutdown_drain_timeout_ms(GenServer.server()) :: pos_integer()
  def shutdown_drain_timeout_ms(server \\ __MODULE__) do
    GenServer.call(server, :shutdown_drain_timeout_ms)
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       status: :starting,
       admissions: %{},
       monitors: %{},
       changed_at: DateTime.utc_now(),
       shutdown_drain_timeout_ms: Keyword.fetch!(opts, :shutdown_drain_timeout_ms),
       shutdown: :not_started,
       shutdown_waiters: []
     }}
  end

  @impl true
  def handle_call(:mark_accepting, _from, %{status: :starting} = state) do
    {:reply, :ok, transition(state, :accepting)}
  end

  def handle_call(:mark_accepting, _from, %{status: :accepting} = state),
    do: {:reply, :ok, state}

  def handle_call(:mark_accepting, _from, state),
    do: {:reply, admission_error(state.status), state}

  def handle_call(:drain, _from, %{status: status} = state)
      when status in [:starting, :accepting] do
    {:reply, :ok, transition(state, :draining)}
  end

  def handle_call(:drain, _from, state), do: {:reply, :ok, state}

  def handle_call(:stop, _from, state), do: {:reply, :ok, transition(state, :stopping)}

  def handle_call(:begin_shutdown, {owner, _tag}, %{shutdown: :not_started} = state) do
    state = if state.status == :stopping, do: state, else: transition(state, :draining)
    monitor = Process.monitor(owner)
    {:reply, :leader, %{state | shutdown: {:in_progress, owner, monitor}}}
  end

  def handle_call(:begin_shutdown, _from, %{shutdown: {:in_progress, _owner, _monitor}} = state),
    do: {:reply, :in_progress, state}

  def handle_call(:begin_shutdown, _from, %{shutdown: {:complete, result}} = state),
    do: {:reply, {:complete, result}, state}

  def handle_call(:await_shutdown, _from, %{shutdown: {:complete, result}} = state),
    do: {:reply, {:complete, result}, state}

  def handle_call(:await_shutdown, from, %{shutdown: {:in_progress, _owner, _monitor}} = state) do
    {:noreply, %{state | shutdown_waiters: [from | state.shutdown_waiters]}}
  end

  def handle_call(:await_shutdown, _from, state),
    do: {:reply, {:error, :shutdown_coordinator_failed}, state}

  def handle_call(
        {:complete_shutdown, result},
        {owner, _tag},
        %{shutdown: {:in_progress, owner, monitor}} = state
      ) do
    Process.demonitor(monitor, [:flush])
    reply_shutdown_waiters(state.shutdown_waiters, {:complete, result})

    {:reply, :ok,
     %{
       transition(state, :stopping)
       | shutdown: {:complete, result},
         shutdown_waiters: []
     }}
  end

  def handle_call({:complete_shutdown, _result}, _from, state),
    do: {:reply, {:error, :not_shutdown_coordinator}, state}

  def handle_call(:ensure_accepting, _from, %{status: :accepting} = state),
    do: {:reply, :ok, state}

  def handle_call(:ensure_accepting, _from, state),
    do: {:reply, admission_error(state.status), state}

  def handle_call({:acquire, owner}, _from, %{status: :accepting} = state) do
    admit(owner, state)
  end

  def handle_call({:acquire, owner}, _from, %{status: :draining} = state) do
    if admitted_owner?(state, owner),
      do: admit(owner, state),
      else: {:reply, admission_error(:draining), state}
  end

  def handle_call({:acquire, _owner}, _from, state),
    do: {:reply, admission_error(state.status), state}

  def handle_call({:release, permit}, _from, state) do
    {:reply, :ok, release_permit(state, permit)}
  end

  def handle_call(:diagnostics, _from, state) do
    {:reply,
     %{
       status: state.status,
       ready?: state.status == :accepting,
       accepting?: state.status == :accepting,
       available?: true,
       active_admissions: map_size(state.admissions),
       changed_at: state.changed_at
     }, state}
  end

  def handle_call(:shutdown_drain_timeout_ms, _from, state),
    do: {:reply, state.shutdown_drain_timeout_ms, state}

  @impl true
  def handle_info(
        {:DOWN, monitor, :process, owner, _reason},
        %{shutdown: {:in_progress, owner, monitor}} = state
      ) do
    reply_shutdown_waiters(state.shutdown_waiters, {:error, :shutdown_coordinator_failed})
    {:noreply, %{state | shutdown: :not_started, shutdown_waiters: []}}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    case Map.fetch(state.monitors, monitor) do
      {:ok, permit} -> {:noreply, release_permit(state, permit, false)}
      :error -> {:noreply, state}
    end
  end

  defp reply_shutdown_waiters(waiters, result) do
    Enum.each(waiters, &GenServer.reply(&1, result))
  end

  defp admit(owner, state) do
    permit = make_ref()
    monitor = Process.monitor(owner)

    state = %{
      state
      | admissions: Map.put(state.admissions, permit, {owner, monitor}),
        monitors: Map.put(state.monitors, monitor, permit)
    }

    {:reply, {:ok, permit}, state}
  end

  defp release_permit(state, permit, demonitor? \\ true) do
    case Map.pop(state.admissions, permit) do
      {nil, _admissions} ->
        state

      {{_owner, monitor}, admissions} ->
        if demonitor?, do: Process.demonitor(monitor, [:flush])
        %{state | admissions: admissions, monitors: Map.delete(state.monitors, monitor)}
    end
  end

  defp admitted_owner?(state, owner) do
    Enum.any?(state.admissions, fn {_permit, {admitted_owner, _monitor}} ->
      admitted_owner == owner
    end)
  end

  defp transition(%{status: status} = state, status), do: state

  defp transition(state, status) do
    now = DateTime.utc_now()

    OperationalEvents.emit(
      :lifecycle_transition,
      %{
        duration_in_previous_state_ms: max(DateTime.diff(now, state.changed_at, :millisecond), 0)
      },
      %{from: state.status, to: status}
    )

    %{state | status: status, changed_at: now}
  end

  defp admission_error(:starting), do: {:error, :runtime_starting}
  defp admission_error(:draining), do: {:error, :runtime_draining}
  defp admission_error(:stopping), do: {:error, :runtime_draining}
end
