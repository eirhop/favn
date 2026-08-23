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
  @projected_channel "favn_execution_group_projected"

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options) when is_list(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  @impl true
  def init(options) do
    with {:ok, connection} <- Postgrex.Notifications.start_link(options),
         {:ok, committed_ref} <- Postgrex.Notifications.listen(connection, @committed_channel),
         {:ok, published_ref} <- Postgrex.Notifications.listen(connection, @published_channel),
         {:ok, admission_ref} <- Postgrex.Notifications.listen(connection, @admission_channel),
         {:ok, projected_ref} <- Postgrex.Notifications.listen(connection, @projected_channel) do
      send(self(), :initial_wake)

      {:ok,
       %{
         connection: connection,
         committed_ref: committed_ref,
         published_ref: published_ref,
         admission_ref: admission_ref,
         projected_ref: projected_ref
       }}
    end
  end

  @impl true
  def handle_info(:initial_wake, state) do
    wake(Sequencer)
    wake(Worker)
    Events.broadcast_projection_listener_resumed()
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
        {:notification, connection, ref, @projected_channel, payload},
        %{connection: connection, projected_ref: ref} = state
      ) do
    with true <- byte_size(payload) <= 1_024,
         {:ok, decoded} <- Jason.decode(payload),
         true <- valid_projection_payload?(decoded) do
      Events.broadcast_projection(decoded)
    end

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

  defp valid_projection_payload?(payload) do
    Enum.all?(~w(workspace_id run_id root_run_id), fn key ->
      value = Map.get(payload, key)
      is_binary(value) and value != "" and byte_size(value) <= 255
    end) and is_integer(Map.get(payload, "publication_id")) and
      Map.get(payload, "publication_id") >= 0 and
      valid_repair_generation?(Map.get(payload, "repair_generation")) and
      Map.get(payload, "change") in ~w(header steps membership windows events)
  end

  defp valid_repair_generation?(nil), do: true
  defp valid_repair_generation?(generation), do: is_integer(generation) and generation > 0
end
