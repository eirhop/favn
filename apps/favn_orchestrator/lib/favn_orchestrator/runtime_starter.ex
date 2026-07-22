defmodule FavnOrchestrator.RuntimeStarter do
  @moduledoc false

  use GenServer

  alias FavnOrchestrator.ActiveManifestReconciler
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.RunnerHealth

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    runtime? = Keyword.get(opts, :runtime?, true)

    with :ok <- maybe_bootstrap(runtime?),
         :ok <- Lifecycle.mark_accepting(),
         :ok <- maybe_refresh(runtime?) do
      if runtime?, do: OperationalEvents.emit(:orchestrator_started, %{}, %{})
      {:ok, %{runtime?: runtime?}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  defp maybe_bootstrap(true), do: Auth.bootstrap_configured_actor()
  defp maybe_bootstrap(false), do: :ok

  defp maybe_refresh(true) do
    :ok = RunnerHealth.refresh()
    :ok = ActiveManifestReconciler.refresh()
  end

  defp maybe_refresh(false), do: :ok
end
