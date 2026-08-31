defmodule FavnOrchestrator.ManifestMemory.Slot do
  @moduledoc "Node-local, non-blocking ownership for one manifest upload or activation phase."

  use GenServer

  @type lease :: reference()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, :ok)
      name -> GenServer.start_link(__MODULE__, :ok, name: name)
    end
  end

  @doc "Acquires the phase slot for the calling process without waiting."
  @spec acquire(keyword()) :: {:ok, lease()} | {:error, :manifest_capacity_busy}
  def acquire(opts \\ []) do
    GenServer.call(Keyword.get(opts, :server, __MODULE__), {:acquire, self()})
  end

  @doc "Releases a lease owned by the calling process."
  @spec release(lease(), keyword()) :: :ok
  def release(lease, opts \\ []) when is_reference(lease) do
    GenServer.call(Keyword.get(opts, :server, __MODULE__), {:release, self(), lease})
  end

  @doc false
  @spec track_worker(lease(), pid(), keyword()) :: :ok | {:error, :manifest_capacity_busy}
  def track_worker(lease, worker, opts \\ [])
      when is_reference(lease) and is_pid(worker) do
    GenServer.call(
      Keyword.get(opts, :server, __MODULE__),
      {:track_worker, self(), lease, worker}
    )
  end

  @impl true
  def init(:ok) do
    Process.flag(:trap_exit, true)
    {:ok, nil}
  end

  @impl true
  def handle_call({:acquire, owner}, _from, nil) do
    lease = make_ref()
    owner_monitor = Process.monitor(owner)

    {:reply, {:ok, lease},
     %{
       owner: owner,
       lease: lease,
       owner_monitor: owner_monitor,
       workers: MapSet.new(),
       release_requested?: false
     }}
  end

  def handle_call({:acquire, _owner}, _from, state) do
    {:reply, {:error, :manifest_capacity_busy}, state}
  end

  def handle_call(
        {:track_worker, owner, lease, worker},
        _from,
        %{owner: owner, lease: lease, release_requested?: false} = state
      ) do
    Process.link(worker)
    {:reply, :ok, %{state | workers: MapSet.put(state.workers, worker)}}
  end

  def handle_call({:track_worker, _owner, _lease, _worker}, _from, state) do
    {:reply, {:error, :manifest_capacity_busy}, state}
  end

  def handle_call({:release, owner, lease}, _from, %{owner: owner, lease: lease} = state) do
    Process.demonitor(state.owner_monitor, [:flush])

    if MapSet.size(state.workers) == 0 do
      {:reply, :ok, nil}
    else
      {:reply, :ok, %{state | owner_monitor: nil, release_requested?: true}}
    end
  end

  def handle_call({:release, _owner, _lease}, _from, state), do: {:reply, :ok, state}

  @impl true
  def handle_info(
        {:DOWN, owner_monitor, :process, owner, _reason},
        %{owner_monitor: owner_monitor, owner: owner} = state
      ) do
    Enum.each(state.workers, &Process.exit(&1, :kill))

    if MapSet.size(state.workers) == 0 do
      {:noreply, nil}
    else
      {:noreply, %{state | owner: nil, owner_monitor: nil, release_requested?: true}}
    end
  end

  def handle_info({:EXIT, worker, _reason}, %{workers: workers} = state) do
    if MapSet.member?(workers, worker) do
      worker_stopped(state, worker)
    else
      {:stop, :linked_process_stopped, state}
    end
  end

  def handle_info({:EXIT, _pid, reason}, nil), do: {:stop, reason, nil}

  def handle_info({:DOWN, _monitor, :process, _owner, _reason}, state), do: {:noreply, state}

  defp worker_stopped(state, worker) do
    remaining = MapSet.delete(state.workers, worker)

    if MapSet.size(remaining) == 0 and state.release_requested? do
      {:noreply, nil}
    else
      {:noreply, %{state | workers: remaining}}
    end
  end
end
