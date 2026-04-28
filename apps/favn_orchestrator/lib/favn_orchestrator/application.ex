defmodule FavnOrchestrator.Application do
  @moduledoc false

  use Application

  alias FavnOrchestrator.API.Config, as: APIConfig
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.Storage

  @impl true
  def start(_type, _args) do
    with :ok <- APIConfig.validate(),
         {:ok, storage_children} <- Storage.child_specs() do
      children =
        storage_children ++
          [
            {AuthStore, []},
            {Phoenix.PubSub, name: pubsub_name()},
            {DynamicSupervisor, strategy: :one_for_one, name: FavnOrchestrator.RunSupervisor},
            {RunManager, []}
          ] ++ scheduler_children() ++ api_children()

      with {:ok, supervisor} <-
             Supervisor.start_link(children,
               strategy: :one_for_one,
               name: FavnOrchestrator.Supervisor
             ),
           :ok <- Auth.bootstrap_configured_actor() do
        {:ok, supervisor}
      end
    end
  end

  defp scheduler_children do
    scheduler_opts = Application.get_env(:favn_orchestrator, :scheduler, [])

    if Keyword.get(scheduler_opts, :enabled, false) do
      [{SchedulerRuntime, scheduler_opts}]
    else
      []
    end
  end

  defp pubsub_name do
    Application.get_env(:favn_orchestrator, :pubsub_name, FavnOrchestrator.PubSub)
  end

  defp api_children do
    api_opts = Application.get_env(:favn_orchestrator, :api_server, [])

    if Keyword.get(api_opts, :enabled, false) do
      [
        {Plug.Cowboy,
         scheme: :http, plug: FavnOrchestrator.API.Router, options: api_server_options(api_opts)}
      ]
    else
      []
    end
  end

  defp api_server_options(api_opts) do
    api_opts
    |> Keyword.get(:bind_ip, Keyword.get(api_opts, :host))
    |> normalize_bind_ip()
    |> case do
      nil -> [port: Keyword.get(api_opts, :port, 4101)]
      bind_ip -> [port: Keyword.get(api_opts, :port, 4101), ip: bind_ip]
    end
  end

  defp normalize_bind_ip(nil), do: nil
  defp normalize_bind_ip(ip) when is_tuple(ip) and tuple_size(ip) == 4, do: ip

  defp normalize_bind_ip(host) when is_binary(host) do
    host
    |> String.split(".", parts: 4)
    |> Enum.map(&Integer.parse/1)
    |> case do
      [{a, ""}, {b, ""}, {c, ""}, {d, ""}] -> {a, b, c, d}
      _other -> nil
    end
  end

  defp normalize_bind_ip(_other), do: nil
end
