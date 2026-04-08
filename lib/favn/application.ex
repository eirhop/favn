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

  @impl true
  def start(_type, _args) do
    adapter = Favn.Storage.adapter_module()
    pubsub_name = Application.get_env(:favn, :pubsub_name, Favn.PubSub)
    pubsub_child = {Phoenix.PubSub, name: pubsub_name}

    with :ok <- Favn.Assets.Registry.load(),
         :ok <- Favn.Assets.GraphIndex.load(),
         {:ok, connections} <- Favn.Connection.Loader.load(),
         :ok <- Favn.Storage.validate_adapter(adapter),
         {:ok, child_specs} <- Favn.Storage.child_specs() do
      connection_registry_child = {Favn.Connection.Registry, connections: connections}
      runtime_children = [Favn.Runtime.RunSupervisor, Favn.Runtime.Manager]
      scheduler_children = if scheduler_enabled?(), do: [Favn.Scheduler.Supervisor], else: []

      Supervisor.start_link(
        [pubsub_child, connection_registry_child | child_specs] ++
          runtime_children ++ scheduler_children,
        strategy: :one_for_one,
        name: Favn.Supervisor
      )
    else
      {:error, errors} when is_list(errors) ->
        raise Favn.Connection.ConfigError, errors: errors
    end
  end

  defp scheduler_enabled? do
    case Application.get_env(:favn, :scheduler, []) do
      opts when is_list(opts) -> Keyword.get(opts, :enabled, true)
      _ -> true
    end
  end
end
