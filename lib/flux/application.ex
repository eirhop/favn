defmodule Flux.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    with :ok <- Flux.Registry.load() do
      Supervisor.start_link([], strategy: :one_for_one, name: Flux.Supervisor)
    end
  end
end
