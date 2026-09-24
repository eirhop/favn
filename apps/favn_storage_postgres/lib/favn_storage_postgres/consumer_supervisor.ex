defmodule FavnStoragePostgres.ConsumerSupervisor do
  @moduledoc false

  use Supervisor

  alias FavnStoragePostgres.NotificationListener
  alias FavnStoragePostgres.Outbox.Sequencer
  alias FavnStoragePostgres.Projections.Worker

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(notification_options) do
    Supervisor.start_link(__MODULE__, notification_options, name: __MODULE__)
  end

  @impl true
  def init(notification_options) do
    children = [
      {Sequencer, []},
      {Worker, []},
      {NotificationListener, notification_options},
      {Task.Supervisor, name: FavnStoragePostgres.Maintenance.Tasks},
      {FavnStoragePostgres.Maintenance.Worker,
       policy: Application.get_env(:favn_orchestrator, :retention, [])}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
