defmodule FavnRunner.ControlPlaneConnection do
  @moduledoc "Bounded explicit connection to the single configured control-plane node."
  use GenServer

  @max_node_bytes 255

  def start_link(opts) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def gateway(server \\ __MODULE__), do: GenServer.call(server, :gateway)
  def reconnect(server \\ __MODULE__), do: GenServer.cast(server, :connect)

  def register(gateway, registration, agent_pid),
    do: GenServer.call(gateway, {:register, registration, agent_pid}, 15_000)

  def request(gateway, message),
    do: GenServer.call(gateway, {:request, message}, 60_000)

  def fetch_manifest(gateway, assignment),
    do: GenServer.call(gateway, {:fetch_manifest, assignment}, 60_000)

  @impl true
  def init(opts) do
    with {:ok, node_name} <- parse_node(Keyword.fetch!(opts, :node)) do
      state = %{node: node_name, connected?: false}
      send(self(), :connect)
      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:gateway, _from, %{connected?: true, node: node_name} = state),
    do: {:reply, {:ok, {:"Elixir.FavnOrchestrator.RunnerGateway", node_name}}, state}

  def handle_call(:gateway, _from, state),
    do: {:reply, {:error, :control_plane_unavailable}, state}

  @impl true
  def handle_cast(:connect, state) do
    send(self(), :connect)
    {:noreply, state}
  end

  @impl true
  def handle_info(:connect, state) do
    connected? = state.node == node() or Node.connect(state.node)

    if connected? do
      :ok = :net_kernel.monitor_nodes(true, node_type: :all)
      {:noreply, %{state | connected?: true}}
    else
      Process.send_after(self(), :connect, 1_000)
      {:noreply, %{state | connected?: false}}
    end
  end

  def handle_info({:nodedown, node_name, _info}, %{node: node_name} = state) do
    Process.send_after(self(), :connect, 250)
    {:noreply, %{state | connected?: false}}
  end

  def handle_info({:nodeup, node_name, _info}, %{node: node_name} = state),
    do: {:noreply, %{state | connected?: true}}

  def handle_info(_message, state), do: {:noreply, state}

  defp parse_node(value) when is_atom(value), do: parse_node(Atom.to_string(value))

  defp parse_node(value)
       when is_binary(value) and byte_size(value) in 3..@max_node_bytes do
    if Regex.match?(~r/^[A-Za-z0-9_.-]+@[A-Za-z0-9_.-]+$/, value),
      do: {:ok, String.to_atom(value)},
      else: {:error, :invalid_control_plane_node}
  end

  defp parse_node(_value), do: {:error, :invalid_control_plane_node}
end
