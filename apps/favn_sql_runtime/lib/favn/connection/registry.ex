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
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec list(keyword()) :: [Resolved.t()]
  def list(opts \\ []) when is_list(opts) do
    GenServer.call(registry_name(opts), :list)
  end

  @spec fetch(atom(), keyword()) :: {:ok, Resolved.t()} | :error
  def fetch(name, opts \\ []) when is_atom(name) and is_list(opts) do
    GenServer.call(registry_name(opts), {:fetch, name})
  end

  @spec registered?(atom(), keyword()) :: boolean()
  def registered?(name, opts \\ []) when is_atom(name) and is_list(opts) do
    match?({:ok, _}, fetch(name, opts))
  end

  @doc false
  @spec reload(%{atom() => Resolved.t()}, keyword()) :: :ok
  def reload(connections, opts \\ []) when is_map(connections) and is_list(opts) do
    GenServer.call(registry_name(opts), {:reload, connections})
  end

  defp registry_name(opts), do: Keyword.get(opts, :registry_name, __MODULE__)

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
