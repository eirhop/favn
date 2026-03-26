defmodule Flux.Application do
  @moduledoc """
  OTP application entrypoint for Flux.

  Startup order is intentional:

    1. load the global asset registry
    2. build the global dependency graph index
    3. validate the configured storage adapter
    4. start PubSub and any storage adapter child processes

  If any of the preflight steps fail, startup returns that error and Flux does
  not boot in a partially initialized state.
  """

  use Application

  @impl true
  def start(_type, _args) do
    adapter = Flux.Storage.adapter_module()
    pubsub_name = Application.get_env(:flux, :pubsub_name, Flux.PubSub)
    # Flux starts its own PubSub by default so it works standalone; a future
    # host-managed mode can reuse the same configurable pubsub_name boundary.
    pubsub_child = {Phoenix.PubSub, name: pubsub_name}

    with :ok <- Flux.Registry.load(),
         :ok <- Flux.GraphIndex.load(),
         :ok <- Flux.Storage.validate_adapter(adapter),
         {:ok, child_specs} <- Flux.Storage.child_specs() do
      Supervisor.start_link([pubsub_child | child_specs],
        strategy: :one_for_one,
        name: Flux.Supervisor
      )
    end
  end
end
