defmodule FavnRunner.ExtensionSupervisor do
  @moduledoc false

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    children = Keyword.fetch!(opts, :children)
    name = Keyword.get(opts, :name, __MODULE__)

    if name do
      Supervisor.start_link(__MODULE__, children, name: name)
    else
      Supervisor.start_link(__MODULE__, children)
    end
  end

  @impl true
  def init(children), do: Supervisor.init(children, strategy: :one_for_one)
end
