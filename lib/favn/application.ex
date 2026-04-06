defmodule Favn.Application do
  @moduledoc """
  OTP application entrypoint for Favn.

  Startup order is intentional:

    1. load the global asset registry
    2. build the global dependency graph index
    3. validate the configured storage adapter
    4. start PubSub and any storage adapter child processes

  If any of the preflight steps fail, startup returns that error and Favn does
  not boot in a partially initialized state.
  """

  use Application

  @impl true
  def start(_type, _args) do
    adapter = Favn.Storage.adapter_module()
    pubsub_name = Application.get_env(:favn, :pubsub_name, Favn.PubSub)
    # Favn starts its own PubSub by default so it works standalone; a future
    # host-managed mode can reuse the same configurable pubsub_name boundary.
    pubsub_child = {Phoenix.PubSub, name: pubsub_name}

    with :ok <- Favn.Assets.Registry.load(),
         :ok <- Favn.Assets.GraphIndex.load(),
         :ok <- Favn.Storage.validate_adapter(adapter),
         {:ok, child_specs} <- Favn.Storage.child_specs() do
      runtime_children = [Favn.Runtime.RunSupervisor, Favn.Runtime.Manager]

      Supervisor.start_link([pubsub_child | child_specs] ++ runtime_children,
        strategy: :one_for_one,
        name: Favn.Supervisor
      )
    end
  end
end
