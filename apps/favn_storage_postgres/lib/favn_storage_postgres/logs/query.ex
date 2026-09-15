defmodule FavnStoragePostgres.Logs.Query do
  @moduledoc false

  alias FavnOrchestrator.Logs.Lifecycle

  # Both sources are bounded in the same snapshot. The outer join retains the
  # publication watermark even when neither source has a matching entry.
  @spec statement(FavnOrchestrator.Persistence.Queries.PageLogs.t(), map()) ::
          {String.t(), list()}
  def statement(page, filter) do
    {stored_where, stored_params} =
      predicates(filter, :stored, [page.workspace_context.workspace_id])

    {event_where, params} = predicates(filter, :event, stored_params)
    {stored_cursor, params} = cursor(page, :stored, params)
    {event_cursor, params} = cursor(page, :event, params)
    {bound, params} = bind(params, page.limit + 1)
    watermark = if page.direction == :older && page.after, do: page.after.watermark, else: nil
    {snapshot, params} = bind(params, watermark)

    stored_order =
      order(page.direction, "e.occurred_at", "(0::integer)", "e.log_id", "e.position")

    event_order =
      order(page.direction, "e.occurred_at", "(1::integer)", "e.event_id", "(0::integer)")

    final_order =
      order(page.direction, "occurred_at", "kind", "row_id", "position", "publication_id")

    {"""
     WITH watermark AS MATERIALIZED (
       SELECT COALESCE(#{snapshot}::bigint, last_publication_id) AS value, last_publication_id AS current_value
       FROM favn_control.outbox_publication_state WHERE singleton_id = 1
     ), entries AS (
       (SELECT 0 AS kind, e.log_id AS row_id, e.occurred_at, p.publication_id,
               e.position, e.run_id, e.batch_id, e.source, e.level, e.message,
               e.metadata AS payload
        FROM favn_control.log_entries e
        JOIN favn_control.log_batches b USING (workspace_id, batch_id)
        JOIN favn_control.outbox_events p ON p.workspace_id = b.workspace_id
          AND p.outbox_event_id = b.outbox_event_id
        WHERE e.workspace_id = $1 AND p.publication_id <= (SELECT value FROM watermark)
          #{stored_where} #{stored_cursor}
        ORDER BY #{stored_order} LIMIT #{bound})
       UNION ALL
       (SELECT 1 AS kind, e.event_id AS row_id, e.occurred_at, p.publication_id,
               0 AS position, e.run_id, NULL::text AS batch_id, 'orchestrator' AS source,
               NULL::text AS level, NULL::text AS message, e.event AS payload
        FROM favn_control.run_events e
        JOIN favn_control.outbox_events p ON p.workspace_id = e.workspace_id
          AND p.outbox_event_id = e.outbox_event_id
        WHERE e.workspace_id = $1 AND e.entity_type = 'step'
          AND p.publication_id <= (SELECT value FROM watermark)
          #{event_where} #{event_cursor}
        ORDER BY #{event_order} LIMIT #{bound})
     )
     SELECT w.value, w.current_value, page.* FROM watermark w
     LEFT JOIN LATERAL (
       SELECT * FROM entries ORDER BY #{final_order} LIMIT #{bound}
     ) page ON true
     ORDER BY #{order(page.direction, "page.occurred_at", "page.kind", "page.row_id", "page.position", "page.publication_id")}
     """, params}
  end

  defp predicates(filter, kind, params) do
    Enum.reduce(filter, {"", params}, fn {key, value}, {sql, params} ->
      cond do
        value in [nil, []] ->
          {sql, params}

        key in [:node_key_hash, :asset_ref_hash] ->
          {sql, params}

        true ->
          {expression, value, operator} = predicate(kind, key, value, filter)
          {placeholder, params} = bind(params, value)
          {sql <> " AND " <> expression <> " " <> operator.(placeholder), params}
      end
    end)
  end

  defp predicate(:event, :levels, levels, _filter) do
    errors = Enum.map_join(Lifecycle.types(:error), ",", &"'#{&1}'")
    warnings = Enum.map_join(Lifecycle.types(:warning), ",", &"'#{&1}'")

    expression =
      "CASE WHEN e.event_type IN (#{errors}) THEN 'error' WHEN e.event_type IN (#{warnings}) THEN 'warning' ELSE 'info' END"

    {expression, Enum.map(levels, &to_string/1), &"= ANY(#{&1}::text[])"}
  end

  defp predicate(kind, key, value, filter) do
    expression = expression(kind, key)

    case key do
      key when key in [:levels, :sources] ->
        {expression, Enum.map(value, &to_string/1), &"= ANY(#{&1}::text[])"}

      :since ->
        {expression, value, &">= #{&1}::timestamptz"}

      :until ->
        {expression, value, &"<= #{&1}::timestamptz"}

      key when kind == :stored and key in [:node_key, :asset_ref] ->
        {expression,
         Map.fetch!(filter, if(key == :node_key, do: :node_key_hash, else: :asset_ref_hash)),
         &"= #{&1}::bytea"}

      _ ->
        {expression, to_string(value), &"= #{&1}::text"}
    end
  end

  defp expression(_, key) when key in [:since, :until], do: "e.occurred_at"
  defp expression(:event, :sources), do: "'orchestrator'"
  defp expression(:event, :stream), do: "'system'"
  defp expression(:event, :node_key), do: "e.event->'data'->>'log_node_key'"
  defp expression(:event, :asset_ref), do: "e.event->'data'->>'log_asset_ref'"
  defp expression(:event, :runner_task_id), do: "e.event->'data'->>'runner_task_id'"
  defp expression(:stored, :node_key), do: "e.node_key_hash"
  defp expression(:stored, :asset_ref), do: "e.asset_ref_hash"
  defp expression(_, :levels), do: "e.level"
  defp expression(_, :sources), do: "e.source"
  defp expression(_, key), do: "e.#{key}"

  defp cursor(%{after: nil}, _, params), do: {"", params}

  defp cursor(%{direction: :newer, after: cursor}, kind, params) do
    {publication, params} = bind(params, cursor.publication_id)
    {offset, params} = bind(params, cursor.batch_offset)
    position = if kind == :stored, do: "e.position", else: "0"
    {"AND (p.publication_id, #{position}) > (#{publication}::bigint, #{offset}::integer)", params}
  end

  defp cursor(%{direction: :older, after: cursor}, kind, params) do
    {time, params} = bind(params, cursor.occurred_at)
    {source, params} = bind(params, cursor.kind)
    {id, params} = bind(params, cursor.row_id)
    {row_kind, row_id} = if kind == :stored, do: {0, "e.log_id"}, else: {1, "e.event_id"}

    {"AND (e.occurred_at, #{row_kind}, #{row_id}) < (#{time}::timestamptz, #{source}::integer, #{id}::bigint)",
     params}
  end

  defp order(direction, time, kind, id, offset, publication \\ "p.publication_id")
  defp order(:older, time, kind, id, _, _), do: "#{time} DESC, #{kind} DESC, #{id} DESC"
  defp order(:newer, _, _, _, offset, publication), do: "#{publication} ASC, #{offset} ASC"
  defp bind(params, value), do: {"$#{length(params) + 1}", params ++ [value]}
end
