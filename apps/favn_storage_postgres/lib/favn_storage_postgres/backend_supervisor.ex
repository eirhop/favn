defmodule FavnStoragePostgres.BackendSupervisor do
  @moduledoc false

  use Supervisor

  alias FavnStoragePostgres.NotificationListener
  alias FavnStoragePostgres.ConnectionConfig
  alias FavnStoragePostgres.Outbox.Sequencer
  alias FavnStoragePostgres.Projections.Worker
  alias FavnStoragePostgres.Registry.ManifestCache
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.SchemaGate

  @spec start_link({ConnectionConfig.t(), [Supervisor.child_spec()]}) :: Supervisor.on_start()
  def start_link({%ConnectionConfig{} = config, provider_children})
      when is_list(provider_children) do
    Supervisor.start_link(__MODULE__, {config, provider_children}, name: __MODULE__)
  end

  @impl true
  def init({config, provider_children}) do
    children =
      provider_children ++
        [
          {Repo, config.repo_options},
          Supervisor.child_spec({SchemaGate, repo: Repo}, restart: :temporary),
          {ManifestCache, []},
          {Sequencer, []},
          {Worker, []},
          {NotificationListener, config.notification_options}
        ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
