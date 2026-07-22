defmodule FavnRunner.Lifecycle do
  @moduledoc """
  Owns runner admission and monotonic shutdown state for one runner BEAM.

  A monitored admission permit lets work already accepted before a drain finish
  while rejecting every operation that arrives after the drain transition.
  """

  use GenServer

  alias FavnRunner.OperationalEvents

  @type state_name :: :starting | :accepting | :draining | :stopping
  @type admission_error :: :runtime_starting | :runtime_draining | :runtime_not_accepting

  @doc "Starts the lifecycle authority in `:starting`."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Marks a successfully booted runner as accepting work."
  @spec mark_accepting(GenServer.server()) :: :ok | {:error, admission_error()}
  def mark_accepting(server \\ __MODULE__), do: GenServer.call(server, :mark_accepting)

  @doc "Monotonically enters drain state."
  @spec drain(GenServer.server()) :: :ok
  def drain(server \\ __MODULE__), do: GenServer.call(server, :drain)

  @doc "Monotonically enters stopping state."
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

  @doc "Returns `:ok` only while new runner work may be admitted."
  @spec ensure_accepting(GenServer.server()) :: :ok | {:error, admission_error()}
  def ensure_accepting(server \\ __MODULE__) do
    GenServer.call(server, :ensure_accepting)
  catch
    :exit, _reason -> {:error, :runtime_not_accepting}
  end

  @doc "Runs a function under a monitored admission permit."
  @spec with_admission((-> result), GenServer.server()) :: result | {:error, admission_error()}
        when result: term()
  def with_admission(fun, server \\ __MODULE__) when is_function(fun, 0) do
    case call(server, {:acquire, self()}, {:error, :runtime_not_accepting}) do
      {:ok, permit} ->
        try do
          fun.()
        after
          _ = call(server, {:release, permit}, :ok)
        end

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Returns bounded runner lifecycle diagnostics."
  @spec diagnostics(GenServer.server()) :: map()
  def diagnostics(server \\ __MODULE__) do
    call(server, :diagnostics, %{
      status: :unavailable,
      ready?: false,
      accepting?: false,
      available?: false,
      active_admissions: :unknown
    })
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
  def handle_call(:mark_accepting, _from, %{status: :starting} = state),
    do: {:reply, :ok, transition(state, :accepting)}

  def handle_call(:mark_accepting, _from, %{status: :accepting} = state),
    do: {:reply, :ok, state}

  def handle_call(:mark_accepting, _from, state),
    do: {:reply, admission_error(state.status), state}

  def handle_call(:drain, _from, %{status: status} = state)
      when status in [:starting, :accepting],
      do: {:reply, :ok, transition(state, :draining)}

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

  def handle_call({:release, permit}, _from, state),
    do: {:reply, :ok, release_permit(state, permit)}

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

    {:reply, {:ok, permit},
     %{
       state
       | admissions: Map.put(state.admissions, permit, {owner, monitor}),
         monitors: Map.put(state.monitors, monitor, permit)
     }}
  end

  defp call(server, message, fallback) do
    GenServer.call(server, message)
  catch
    :exit, _reason -> fallback
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
