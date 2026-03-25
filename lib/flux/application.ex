defmodule Flux.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    adapter = Flux.Storage.adapter_module()

    with :ok <- Flux.Registry.load(),
         :ok <- Flux.GraphIndex.load(),
         :ok <- Flux.Storage.validate_adapter(adapter),
         {:ok, child_specs} <- Flux.Storage.child_specs() do
      Supervisor.start_link(child_specs,
        strategy: :one_for_one,
        name: Flux.Supervisor
      )
    end
  end
end
