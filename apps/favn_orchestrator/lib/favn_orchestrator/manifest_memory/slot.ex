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

  @impl true
  def init(:ok), do: {:ok, nil}

  @impl true
  def handle_call({:acquire, owner}, _from, nil) do
    lease = make_ref()
    monitor = Process.monitor(owner)
    {:reply, {:ok, lease}, %{owner: owner, lease: lease, monitor: monitor}}
  end

  def handle_call({:acquire, _owner}, _from, state) do
    {:reply, {:error, :manifest_capacity_busy}, state}
  end

  def handle_call({:release, owner, lease}, _from, %{owner: owner, lease: lease} = state) do
    Process.demonitor(state.monitor, [:flush])
    {:reply, :ok, nil}
  end

  def handle_call({:release, _owner, _lease}, _from, state), do: {:reply, :ok, state}

  @impl true
  def handle_info({:DOWN, monitor, :process, owner, _reason}, %{monitor: monitor, owner: owner}) do
    {:noreply, nil}
  end

  def handle_info({:DOWN, _monitor, :process, _owner, _reason}, state), do: {:noreply, state}
end
