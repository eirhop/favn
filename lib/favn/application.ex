defmodule Favn.Application do
  @moduledoc """
  OTP application entrypoint for Favn.

  Startup order is intentional:

    1. load the global asset registry
    2. build the global dependency graph index
    3. load and validate configured connection definitions/runtime values
    4. validate the configured storage adapter
    5. start PubSub, connection registry, and any storage adapter child processes

  If any of the preflight steps fail, startup returns that error and Favn does
  not boot in a partially initialized state.
  """

  use Application

  alias Favn.Assets.GraphIndex
  alias Favn.Assets.Registry
  alias Favn.Connection.ConfigError
  alias Favn.Connection.Loader
  alias Favn.Connection.Registry, as: ConnectionRegistry
  alias Favn.Runtime.Manager
  alias Favn.Runtime.RunSupervisor
  alias Favn.Scheduler.Supervisor, as: SchedulerSupervisor

  @impl true
  def start(_type, _args) do
    adapter = Favn.Storage.adapter_module()
    pubsub_name = Application.get_env(:favn, :pubsub_name, Favn.PubSub)
    pubsub_child = {Phoenix.PubSub, name: pubsub_name}

    with :ok <- Registry.load(),
         :ok <- GraphIndex.load(),
         {:ok, connections} <- load_connections_or_raise(),
         :ok <- Favn.Storage.validate_adapter(adapter),
         {:ok, child_specs} <- Favn.Storage.child_specs() do
      start_supervisor(pubsub_child, child_specs, connections)
    end
  end

  defp load_connections_or_raise do
    case Loader.load() do
      {:ok, connections} ->
        {:ok, connections}

      {:error, errors} when is_list(errors) ->
        raise ConfigError, errors: errors
    end
  end

  defp start_supervisor(pubsub_child, child_specs, connections) do
    connection_registry_child = {ConnectionRegistry, connections: connections}
    runtime_children = [RunSupervisor, Manager]
    scheduler_children = if scheduler_enabled?(), do: [SchedulerSupervisor], else: []

    Supervisor.start_link(
      [pubsub_child, connection_registry_child | child_specs] ++
        runtime_children ++ scheduler_children,
      strategy: :one_for_one,
      name: Favn.Supervisor
    )
  end

  defp scheduler_enabled? do
    case Application.get_env(:favn, :scheduler, []) do
      opts when is_list(opts) -> Keyword.get(opts, :enabled, true)
      _ -> true
    end
  end
end
