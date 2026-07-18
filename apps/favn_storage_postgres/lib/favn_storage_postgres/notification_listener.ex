defmodule FavnStoragePostgres.NotificationListener do
  @moduledoc false

  use GenServer

  alias FavnOrchestrator.Events
  alias FavnOrchestrator.ExecutionAdmission.Coordinator, as: AdmissionCoordinator
  alias FavnStoragePostgres.Outbox.Sequencer
  alias FavnStoragePostgres.Projections.Worker

  @committed_channel "favn_outbox_committed"
  @published_channel "favn_outbox_published"
  @admission_channel "favn_admission_changed"

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options) when is_list(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  @impl true
  def init(options) do
    with {:ok, connection} <- Postgrex.Notifications.start_link(connection_options(options)),
         {:ok, committed_ref} <- Postgrex.Notifications.listen(connection, @committed_channel),
         {:ok, published_ref} <- Postgrex.Notifications.listen(connection, @published_channel),
         {:ok, admission_ref} <- Postgrex.Notifications.listen(connection, @admission_channel) do
      send(self(), :initial_wake)

      {:ok,
       %{
         connection: connection,
         committed_ref: committed_ref,
         published_ref: published_ref,
         admission_ref: admission_ref
       }}
    end
  end

  @impl true
  def handle_info(:initial_wake, state) do
    wake(Sequencer)
    wake(Worker)
    {:noreply, state}
  end

  def handle_info(
        {:notification, connection, ref, @committed_channel, _payload},
        %{connection: connection, committed_ref: ref} = state
      ) do
    wake(Sequencer)
    {:noreply, state}
  end

  def handle_info(
        {:notification, connection, ref, @admission_channel, _payload},
        %{connection: connection, admission_ref: ref} = state
      ) do
    AdmissionCoordinator.storage_changed()
    {:noreply, state}
  end

  def handle_info(
        {:notification, connection, ref, @published_channel, _payload},
        %{connection: connection, published_ref: ref} = state
      ) do
    wake(Worker)
    Events.broadcast_persistence_publication()
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp wake(server) do
    if Process.whereis(server), do: GenServer.cast(server, :wake)
    :ok
  end

  defp connection_options(options) do
    url_options = options |> Keyword.fetch!(:url) |> Ecto.Repo.Supervisor.parse_url()

    options
    |> Keyword.take([:ssl, :socket_options, :connect_timeout, :timeout])
    |> Keyword.merge(url_options)
    |> Keyword.put(:auto_reconnect, true)
    |> Keyword.put(:sync_connect, true)
  end
end
