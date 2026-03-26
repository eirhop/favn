defmodule Flux.Application do
  @moduledoc false

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
