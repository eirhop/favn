defmodule FavnOrchestrator.MemoryCapacity.Ledger do
  @moduledoc false

  use GenServer

  @table FavnOrchestrator.MemoryCapacity.Leases

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec checkout(pid(), GenServer.server()) :: {:ok, :ets.tid()} | {:error, term()}
  def checkout(owner, server \\ __MODULE__), do: GenServer.call(server, {:checkout, owner})

  @spec release(reference(), GenServer.server()) :: :ok | {:error, :lease_ledger_not_owner}
  def release(id, server \\ __MODULE__) when is_reference(id),
    do: GenServer.call(server, {:release, id})

  @impl true
  def init(opts) do
    {:ok,
     %{
       table: nil,
       table_name: Keyword.get(opts, :table, @table),
       initialized?: false
     }}
  end

  @impl true
  def handle_call({:checkout, owner}, _from, %{table: nil, initialized?: false} = state) do
    table =
      :ets.new(state.table_name, [
        :named_table,
        :set,
        :protected,
        {:heir, self(), :coordinator_crash},
        read_concurrency: true,
        write_concurrency: false
      ])

    true = :ets.give_away(table, owner, :ledger_checkout)
    {:reply, {:ok, table}, %{state | initialized?: true}}
  end

  def handle_call({:checkout, owner}, _from, %{table: table} = state) when not is_nil(table) do
    true = :ets.setopts(table, {:heir, self(), :coordinator_crash})
    true = :ets.give_away(table, owner, :ledger_checkout)
    {:reply, {:ok, table}, %{state | table: nil, initialized?: true}}
  end

  def handle_call({:checkout, _owner}, _from, state) do
    {:reply, {:error, :lease_ledger_unavailable}, state}
  end

  def handle_call({:release, id}, _from, %{table: table} = state) when not is_nil(table) do
    true = :ets.delete(table, id)
    {:reply, :ok, state}
  end

  def handle_call({:release, _id}, _from, state) do
    {:reply, {:error, :lease_ledger_not_owner}, state}
  end

  @impl true
  def handle_info({:"ETS-TRANSFER", table, _from, :coordinator_crash}, state) do
    {:noreply, %{state | table: table, initialized?: true}}
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end
end
