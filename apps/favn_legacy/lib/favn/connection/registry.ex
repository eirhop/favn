defmodule Favn.Connection.Registry do
  @moduledoc false

  use GenServer

  alias Favn.Connection.Resolved

  @type state :: %{
          by_name: %{atom() => Resolved.t()},
          ordered_names: [atom()]
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec list() :: [Resolved.t()]
  def list do
    GenServer.call(__MODULE__, :list)
  end

  @spec fetch(atom()) :: {:ok, Resolved.t()} | :error
  def fetch(name) when is_atom(name) do
    GenServer.call(__MODULE__, {:fetch, name})
  end

  @spec registered?(atom()) :: boolean()
  def registered?(name) when is_atom(name) do
    match?({:ok, _}, fetch(name))
  end

  @doc false
  @spec reload(%{atom() => Resolved.t()}) :: :ok
  def reload(connections) when is_map(connections) do
    GenServer.call(__MODULE__, {:reload, connections})
  end

  @impl true
  def init(opts) do
    connections = Keyword.get(opts, :connections, %{})
    {:ok, build_state(connections)}
  end

  @impl true
  def handle_call(:list, _from, state) do
    entries = Enum.map(state.ordered_names, &Map.fetch!(state.by_name, &1))
    {:reply, entries, state}
  end

  def handle_call({:fetch, name}, _from, state) do
    reply = Map.fetch(state.by_name, name)
    {:reply, reply, state}
  end

  def handle_call({:reload, connections}, _from, _state) do
    {:reply, :ok, build_state(connections)}
  end

  defp build_state(connections) do
    %{
      by_name: connections,
      ordered_names: Map.keys(connections) |> Enum.sort()
    }
  end
end
