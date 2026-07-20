defmodule FavnStoragePostgres.Projections.MissingRowBackfiller do
  @moduledoc false

  import Ecto.Query

  alias FavnStoragePostgres.Projections.Projector
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.OutboxEvent

  @spec backfill(atom(), String.t(), map() | nil, pos_integer()) ::
          %{count: non_neg_integer(), cursor: map() | nil}
  def backfill(projection, workspace_id, cursor, limit)
      when projection in [
             :execution_groups,
             :backfills,
             :target_statuses,
             :asset_attempts,
             :freshness
           ] do
    after_publication_id = cursor_value(cursor)

    events =
      OutboxEvent
      |> where(
        [event],
        event.workspace_id == ^workspace_id and not is_nil(event.publication_id) and
          event.publication_id > ^after_publication_id
      )
      |> event_scope(projection)
      |> order_by([event], asc: event.publication_id)
      |> limit(^limit)
      |> Repo.all()

    Enum.each(events, &Projector.rebuild_event!(projection, &1))

    %{
      count: length(events),
      cursor: publication_cursor(List.last(events))
    }
  end

  defp event_scope(query, projection)
       when projection in [:execution_groups, :target_statuses, :asset_attempts],
       do: where(query, [event], like(event.event_kind, "run.%"))

  defp event_scope(query, :backfills),
    do:
      where(
        query,
        [event],
        event.event_kind == "backfill.plan.activated" or
          like(event.event_kind, "backfill.window.%")
      )

  defp event_scope(query, :freshness),
    do: where(query, [event], event.event_kind == "materialization.succeeded")

  defp cursor_value(nil), do: 0

  defp cursor_value(cursor) when is_map(cursor) do
    Map.get(cursor, "publication_id") || Map.get(cursor, :publication_id) || 0
  end

  defp publication_cursor(nil), do: nil
  defp publication_cursor(event), do: %{"publication_id" => event.publication_id}
end
