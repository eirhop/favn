defmodule FavnOrchestrator.RunnerQueueSupervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    Supervisor.init(
      [
        {Registry, keys: :unique, name: FavnOrchestrator.RunnerQueueRegistry},
        {DynamicSupervisor,
         strategy: :one_for_one, name: FavnOrchestrator.RunnerQueueDynamicSupervisor}
      ],
      strategy: :one_for_all
    )
  end
end
