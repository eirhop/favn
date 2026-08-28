defmodule FavnOrchestrator.MemoryCapacity.Supervisor do
  @moduledoc false

  use Supervisor

  alias FavnOrchestrator.MemoryCapacity.Coordinator
  alias FavnOrchestrator.MemoryCapacity.Ledger

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    ledger_opts = Keyword.get(opts, :ledger_opts, [])
    ledger = Keyword.get(ledger_opts, :name, Ledger)
    coordinator_opts = Keyword.put_new(opts, :ledger, ledger)

    Supervisor.init([{Ledger, ledger_opts}, {Coordinator, coordinator_opts}],
      strategy: :rest_for_one
    )
  end
end
