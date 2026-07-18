defmodule FavnStoragePostgres.Outbox.Writer do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.OutboxEvent

  @spec insert!(map()) :: OutboxEvent.t()
  def insert!(attrs) when is_map(attrs) do
    payload = Map.fetch!(attrs, :payload)
    {:ok, payload_hash} = CanonicalJSON.hash(payload)
    now = Map.fetch!(attrs, :occurred_at)

    outbox =
      %OutboxEvent{
        workspace_id: Map.fetch!(attrs, :workspace_id),
        command_id: Map.fetch!(attrs, :command_id),
        event_kind: Map.fetch!(attrs, :event_kind),
        aggregate_kind: Map.fetch!(attrs, :aggregate_kind),
        aggregate_id: Map.fetch!(attrs, :aggregate_id),
        aggregate_version: Map.fetch!(attrs, :aggregate_version),
        payload_version: 1,
        payload: payload,
        payload_hash: payload_hash,
        available_at: now,
        inserted_at: now
      }
      |> Repo.insert!()

    SQL.query!(Repo, "SELECT pg_notify('favn_outbox_committed', '')", [])
    outbox
  end
end
