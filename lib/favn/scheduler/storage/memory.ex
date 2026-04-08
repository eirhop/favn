defmodule Favn.Scheduler.Storage.Memory do
  @moduledoc false
  use GenServer

  alias Favn.Scheduler.State

  @table __MODULE__.Table

  def child_spec(_opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [[]]},
      type: :worker,
      restart: :permanent,
      shutdown: 5000
    }
  end

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(state) do
    _ = :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    {:ok, state}
  end

  @spec get_state(module()) :: {:ok, State.t() | nil}
  def get_state(pipeline_module) when is_atom(pipeline_module) do
    case :ets.lookup(@table, pipeline_module) do
      [{^pipeline_module, %State{} = state}] -> {:ok, state}
      [] -> {:ok, nil}
    end
  end

  @spec put_state(State.t()) :: :ok
  def put_state(%State{} = state) do
    true = :ets.insert(@table, {state.pipeline_module, state})
    :ok
  end
end
