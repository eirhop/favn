defmodule FavnStoragePostgres.BackendSupervisor do
  @moduledoc false

  use Supervisor

  alias FavnStoragePostgres.NotificationListener
  alias FavnStoragePostgres.Outbox.Sequencer
  alias FavnStoragePostgres.Projections.Worker
  alias FavnStoragePostgres.Registry.ManifestCache
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.SchemaGate

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(options) when is_list(options) do
    Supervisor.start_link(__MODULE__, options, name: __MODULE__)
  end

  @impl true
  def init(options) do
    children = [
      {Repo, options},
      Supervisor.child_spec({SchemaGate, repo: Repo}, restart: :temporary),
      {ManifestCache, []},
      {Sequencer, []},
      {Worker, []},
      {NotificationListener, options}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
