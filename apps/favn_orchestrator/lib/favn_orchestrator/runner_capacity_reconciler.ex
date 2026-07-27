defmodule FavnOrchestrator.RunnerCapacityReconciler do
  @moduledoc "Slow bounded wake reconciliation for missed local queue hints."
  use GenServer

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunnerQueueCoordinator
  alias FavnOrchestrator.RunnerRegistry

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, 15_000)
    Process.send_after(self(), :reconcile, interval)
    {:ok, %{interval_ms: interval}}
  end

  @impl true
  def handle_info(:reconcile, state) do
    RunnerRegistry.list()
    |> Enum.map(&{&1.runner_pool, &1.required_runner_release_id})
    |> Enum.uniq()
    |> Enum.take(100)
    |> Enum.each(&reconcile/1)

    Process.send_after(self(), :reconcile, state.interval_ms)
    {:noreply, state}
  end

  defp reconcile({pool, release}) do
    query = %Q.GetRunnerCapacityDemand{
      platform_context:
        SystemContext.platform(:runner_capacity_reconciler, roles: [:platform_operator]),
      runner_pool: pool,
      required_runner_release_id: release
    }

    case Persistence.stores().runner_tasks.demand(query) do
      {:ok, %{queued_count: queued}} when queued > 0 ->
        RunnerQueueCoordinator.notify(pool, release, queued)

      _other ->
        :ok
    end
  end
end
