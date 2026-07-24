defmodule FavnLocal.Supervisor do
  @moduledoc false

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    publication = Keyword.fetch!(opts, :publication)

    lifecycle = %{
      id: FavnLocal.Lifecycle,
      start: {FavnLocal.Lifecycle, :start_link, [[config: config, publication: publication]]},
      restart: :temporary,
      significant: true
    }

    children = [
      {Task.Supervisor, name: FavnLocal.TaskSupervisor},
      lifecycle
    ]

    Supervisor.init(children, strategy: :one_for_one, auto_shutdown: :any_significant)
  end
end
