defmodule FavnOrchestrator.RunControlSupervisor do
  @moduledoc false
  use Supervisor
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  @impl true
  def init(opts) do
    Supervisor.init(
      [
        {Registry, keys: :unique, name: FavnOrchestrator.RunLeaseRegistry},
        {DynamicSupervisor, strategy: :one_for_one, name: FavnOrchestrator.RunSupervisor},
        {Task.Supervisor, name: FavnOrchestrator.RunPostStepSupervisor},
        {FavnOrchestrator.RunManager, opts}
      ],
      strategy: :one_for_all
    )
  end
end
