defmodule FavnStoragePostgres.Logs.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.LogStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.AppendLogBatch
  alias FavnOrchestrator.Persistence.Commands.LogEntry, as: LogEntryCommand
  alias FavnOrchestrator.Persistence.Commands.PurgeLogs
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.PageLogs
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.LogEntry, as: LogEntryResult
  alias FavnOrchestrator.Persistence.Results.PurgeResult
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Payload
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.LogBatch
  alias FavnStoragePostgres.Schemas.LogEntry

  @max_entries 1_000
  @levels [:debug, :info, :warning, :error]

  @impl true
  def append_batch(%AppendLogBatch{} = command) do
    with :ok <- validate_append(command),
         normalized <- Enum.map(command.entries, &normalize_entry/1),
         {:ok, batch_hash} <- CanonicalJSON.hash(normalized),
         {:ok, rows} <-
           Repo.transaction(fn -> append_or_replay!(command, normalized, batch_hash) end) do
      {:ok, rows}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page(%PageLogs{} = page) do
    with :ok <- validate_page(page) do
      query =
        LogEntry
        |> where([entry], entry.workspace_id == ^page.workspace_context.workspace_id)
        |> filter(page.filter_kind, page.filter_value)
        |> after_cursor(page.after, page.direction)
        |> page_order(page.direction)
        |> limit(^(page.limit + 1))

      rows = Repo.all(query)
      page_rows = Enum.take(rows, page.limit)
      items = Enum.map(page_rows, &entry_result/1)
      has_more? = length(rows) > page.limit
      last = List.last(page_rows)

      {:ok,
       %CursorPage{
         items: items,
         limit: page.limit,
         has_more?: has_more?,
         next_cursor:
           if(has_more? and last,
             do: %{occurred_at: last.occurred_at, log_id: last.log_id}
           )
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def purge(%PurgeLogs{} = command) do
    with :ok <- validate_purge(command),
         {:ok, result} <- Repo.transaction(fn -> purge!(command) end) do
      {:ok, result}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp append_or_replay!(command, normalized, batch_hash) do
    workspace_id = command.workspace_context.workspace_id

    existing =
      from(batch in LogBatch,
        where:
          batch.workspace_id == ^workspace_id and
            (batch.batch_id == ^command.batch_id or batch.command_id == ^command.command_id),
        lock: "FOR UPDATE"
      )
      |> Repo.one()

    cond do
      existing && exact_replay?(existing, command, batch_hash, length(normalized)) ->
        load_batch_entries(workspace_id, command.batch_id)

      existing ->
        Repo.rollback(Error.new(:conflict, "log batch identity has different content"))

      true ->
        insert_batch!(command, normalized, batch_hash)
    end
  end

  defp insert_batch!(command, normalized, batch_hash) do
    workspace_id = command.workspace_context.workspace_id

    outbox =
      OutboxWriter.insert!(%{
        workspace_id: workspace_id,
        command_id: command.command_id,
        event_kind: "logs.batch.appended",
        aggregate_kind: "log_batch",
        aggregate_id: command.batch_id,
        aggregate_version: 1,
        occurred_at: command.occurred_at,
        payload: %{"batch_id" => command.batch_id, "entry_count" => length(normalized)}
      })

    %LogBatch{
      workspace_id: workspace_id,
      batch_id: command.batch_id,
      command_id: command.command_id,
      batch_hash: batch_hash,
      entry_count: length(normalized),
      outbox_event_id: outbox.outbox_event_id,
      inserted_at: command.occurred_at
    }
    |> Repo.insert!()

    rows =
      normalized
      |> Enum.with_index()
      |> Enum.map(fn {entry, position} ->
        Map.merge(entry, %{
          workspace_id: workspace_id,
          batch_id: command.batch_id,
          position: position,
          inserted_at: command.occurred_at
        })
      end)

    {_count, inserted} = Repo.insert_all(LogEntry, rows, returning: true)
    inserted |> Enum.sort_by(& &1.position) |> Enum.map(&entry_result/1)
  end

  defp load_batch_entries(workspace_id, batch_id) do
    from(entry in LogEntry,
      where: entry.workspace_id == ^workspace_id and entry.batch_id == ^batch_id,
      order_by: [asc: entry.position]
    )
    |> Repo.all()
    |> Enum.map(&entry_result/1)
  end

  defp purge!(command) do
    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        WITH candidates AS (
          SELECT log_id
          FROM favn_control.log_entries
          WHERE workspace_id = $1 AND occurred_at < $2
          ORDER BY log_id
          LIMIT $3
          FOR UPDATE SKIP LOCKED
        )
        DELETE FROM favn_control.log_entries entry
        USING candidates
        WHERE entry.log_id = candidates.log_id
        RETURNING entry.log_id, entry.batch_id
        """,
        [command.workspace_context.workspace_id, command.cutoff, command.limit]
      )

    batch_ids = rows |> Enum.map(&Enum.at(&1, 1)) |> Enum.uniq()
    delete_empty_batches(command.workspace_context.workspace_id, batch_ids)
    ids = Enum.map(rows, &hd/1)

    %PurgeResult{deleted_count: length(ids), last_id: Enum.max(ids, fn -> nil end)}
  end

  defp delete_empty_batches(_workspace_id, []), do: :ok

  defp delete_empty_batches(workspace_id, batch_ids) do
    SQL.query!(
      Repo,
      """
      DELETE FROM favn_control.log_batches batch
      WHERE batch.workspace_id = $1 AND batch.batch_id = ANY($2::text[])
        AND NOT EXISTS (
          SELECT 1 FROM favn_control.log_entries entry
          WHERE entry.workspace_id = batch.workspace_id AND entry.batch_id = batch.batch_id
        )
      """,
      [workspace_id, batch_ids]
    )

    :ok
  end

  defp normalize_entry(entry) do
    redacted =
      Redaction.redact_operational_bounded(%{
        message: entry.message,
        metadata: entry.metadata
      })

    %{
      run_id: entry.run_id,
      source: String.slice(entry.source, 0, 100),
      level: Atom.to_string(entry.level),
      message: redacted.message |> to_string() |> String.slice(0, 8_192),
      metadata: JsonSafe.data(redacted.metadata),
      occurred_at: entry.occurred_at
    }
  end

  defp entry_result(entry) do
    %LogEntryResult{
      log_id: entry.log_id,
      workspace_id: entry.workspace_id,
      batch_id: entry.batch_id,
      position: entry.position,
      run_id: entry.run_id,
      source: entry.source,
      level: String.to_existing_atom(entry.level),
      message: entry.message,
      metadata: entry.metadata,
      occurred_at: entry.occurred_at
    }
  end

  defp exact_replay?(batch, command, hash, count) do
    batch.batch_id == command.batch_id and batch.command_id == command.command_id and
      batch.batch_hash == hash and batch.entry_count == count
  end

  defp filter(query, nil, nil), do: query
  defp filter(query, :run, run_id), do: where(query, [entry], entry.run_id == ^run_id)

  defp filter(query, :level, level),
    do: where(query, [entry], entry.level == ^Atom.to_string(level))

  defp after_cursor(query, nil, _direction), do: query

  defp after_cursor(query, %{log_id: log_id}, :newer) when is_integer(log_id),
    do: where(query, [entry], entry.log_id > ^log_id)

  defp after_cursor(query, %{occurred_at: occurred_at, log_id: log_id}, :older) do
    where(
      query,
      [entry],
      entry.occurred_at < ^occurred_at or
        (entry.occurred_at == ^occurred_at and entry.log_id < ^log_id)
    )
  end

  defp after_cursor(query, %{occurred_at: _occurred_at, log_id: log_id}, :newer),
    do: where(query, [entry], entry.log_id > ^log_id)

  defp page_order(query, :newer),
    do: order_by(query, [entry], asc: entry.log_id)

  defp page_order(query, :older),
    do: order_by(query, [entry], desc: entry.occurred_at, desc: entry.log_id)

  defp validate_append(command) do
    entries = command.entries

    if workspace_context?(command.workspace_context) and
         Enum.all?([command.command_id, command.batch_id], &valid_id?/1) and is_list(entries) and
         entries != [] and length(entries) <= @max_entries and Enum.all?(entries, &valid_entry?/1) and
         match?(%DateTime{}, command.occurred_at),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_page(page) do
    if workspace_context?(page.workspace_context) and valid_log_filter?(page) and
         valid_log_cursor?(page.after, page.direction) and
         page.direction in [:older, :newer] and
         valid_bound?(page.limit, 1, 500),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_log_filter?(%{filter_kind: nil, filter_value: nil}), do: true
  defp valid_log_filter?(%{filter_kind: :run, filter_value: value}), do: valid_id?(value)
  defp valid_log_filter?(%{filter_kind: :level, filter_value: value}), do: value in @levels
  defp valid_log_filter?(_page), do: false

  defp valid_log_cursor?(nil, _direction), do: true

  defp valid_log_cursor?(%{occurred_at: %DateTime{}, log_id: id}, _direction),
    do: is_integer(id)

  defp valid_log_cursor?(%{log_id: id}, :newer), do: is_integer(id) and id >= 0
  defp valid_log_cursor?(_cursor, _direction), do: false

  defp validate_purge(command) do
    if workspace_context?(command.workspace_context) and match?(%DateTime{}, command.cutoff) and
         valid_bound?(command.limit, 1, 5_000),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_entry?(%LogEntryCommand{} = entry) do
    valid_id?(entry.source) and entry.level in @levels and is_binary(entry.message) and
      byte_size(entry.message) in 1..8_192 and match?(%DateTime{}, entry.occurred_at) and
      (is_nil(entry.run_id) or valid_id?(entry.run_id)) and is_map(entry.metadata) and
      Payload.validate(entry.metadata, 32 * 1_024) == :ok
  end

  defp valid_entry?(_other), do: false

  defp workspace_context?(context), do: WorkspaceContext.valid?(context)

  defp valid_bound?(value, min, max), do: is_integer(value) and value >= min and value <= max
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
