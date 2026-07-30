defmodule FavnOrchestrator.RuntimeBootstrap do
  @moduledoc false

  use GenServer

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.OperationalEvents

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    runtime? = Keyword.get(opts, :runtime?, true)

    with :ok <- maybe_bootstrap_local_development(runtime?),
         :ok <- Lifecycle.mark_accepting() do
      if runtime?, do: OperationalEvents.emit(:orchestrator_started, %{}, %{})
      {:ok, %{runtime?: runtime?}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  defp maybe_bootstrap_local_development(true) do
    if Application.get_env(:favn_orchestrator, :allow_automatic_admin_bootstrap, false),
      do: FavnOrchestrator.Auth.bootstrap_configured_actor(),
      else: :ok
  end

  defp maybe_bootstrap_local_development(false), do: :ok
end
