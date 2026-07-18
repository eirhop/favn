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
  alias Favn.Log.Identity
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Payload
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.LogBatch
  alias FavnStoragePostgres.Schemas.LogEntry
  alias FavnStoragePostgres.Schemas.OutboxEvent

  @max_entries 1_000
  @levels [:debug, :info, :warning, :error]
  @sources [:orchestrator, :runner, :sql_runtime, :adapter, :user_code, :system]
  @streams [:stdout, :stderr, :system]
  @filter_keys ~w(run_id asset_step_id runner_execution_id node_key asset_ref stream levels sources since until)a

  @impl true
  def append_batch(%AppendLogBatch{} = command) do
    with :ok <- validate_append(command),
         normalized <- Enum.map(command.entries, &normalize_entry/1),
         {:ok, batch_hash} <- CanonicalJSON.hash(Enum.map(normalized, &hashable_entry/1)),
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
  def page(%PageLogs{direction: :newer} = page) do
    with :ok <- validate_page(page),
         {:ok, filter} <- prepare_filter(page.filter) do
      query =
        from(entry in LogEntry,
          join: batch in LogBatch,
          on: batch.workspace_id == entry.workspace_id and batch.batch_id == entry.batch_id,
          join: event in OutboxEvent,
          on:
            event.workspace_id == batch.workspace_id and
              event.outbox_event_id == batch.outbox_event_id,
          where:
            entry.workspace_id == ^page.workspace_context.workspace_id and
              not is_nil(event.publication_id),
          order_by: [asc: event.publication_id, asc: entry.position],
          select: {entry, event.publication_id},
          limit: ^(page.limit + 1)
        )
        |> filter(filter)
        |> after_publication(page.after)

      result_page(Repo.all(query), page.limit, :newer)
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  def page(%PageLogs{} = page) do
    with :ok <- validate_page(page),
         {:ok, filter} <- prepare_filter(page.filter) do
      query =
        from(entry in LogEntry,
          left_join: batch in LogBatch,
          on: batch.workspace_id == entry.workspace_id and batch.batch_id == entry.batch_id,
          left_join: event in OutboxEvent,
          on:
            event.workspace_id == batch.workspace_id and
              event.outbox_event_id == batch.outbox_event_id,
          where: entry.workspace_id == ^page.workspace_context.workspace_id,
          select: {entry, event.publication_id}
        )
        |> filter(filter)
        |> after_historical_cursor(page.after)
        |> order_by([entry], desc: entry.occurred_at, desc: entry.log_id)
        |> limit(^(page.limit + 1))

      result_page(Repo.all(query), page.limit, :older)
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

    metadata =
      redacted.metadata
      |> normalize_log_identity(:node_key, &Identity.node_key/1)
      |> normalize_log_identity(:asset_ref, &Identity.asset_ref/1)
      |> JsonSafe.data()

    {:ok, node_key_hash} = optional_filter_hash(Map.get(metadata, "node_key"))
    {:ok, asset_ref_hash} = optional_filter_hash(Map.get(metadata, "asset_ref"))

    %{
      run_id: entry.run_id,
      asset_step_id: optional_string(Map.get(metadata, "asset_step_id")),
      runner_execution_id: optional_string(Map.get(metadata, "runner_execution_id")),
      node_key_hash: node_key_hash,
      asset_ref_hash: asset_ref_hash,
      stream: optional_string(Map.get(metadata, "stream")),
      source: String.slice(entry.source, 0, 100),
      level: Atom.to_string(entry.level),
      message: redacted.message |> to_string() |> String.slice(0, 8_192),
      metadata: metadata,
      occurred_at: entry.occurred_at
    }
  end

  defp hashable_entry(entry) do
    entry
    |> Map.update!(:node_key_hash, &encode_optional_hash/1)
    |> Map.update!(:asset_ref_hash, &encode_optional_hash/1)
  end

  defp encode_optional_hash(nil), do: nil
  defp encode_optional_hash(hash), do: Base.encode16(hash, case: :lower)

  defp entry_result(entry, publication_id \\ nil) do
    %LogEntryResult{
      log_id: entry.log_id,
      workspace_id: entry.workspace_id,
      batch_id: entry.batch_id,
      position: entry.position,
      publication_id: publication_id,
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

  defp filter(query, filter) do
    query
    |> maybe_equal(:run_id, filter.run_id)
    |> maybe_equal(:asset_step_id, filter.asset_step_id)
    |> maybe_equal(:runner_execution_id, filter.runner_execution_id)
    |> maybe_equal(:node_key_hash, filter.node_key_hash)
    |> maybe_equal(:asset_ref_hash, filter.asset_ref_hash)
    |> maybe_equal(:stream, atom_string(filter.stream))
    |> maybe_in(:level, Enum.map(filter.levels, &Atom.to_string/1))
    |> maybe_in(:source, Enum.map(filter.sources, &Atom.to_string/1))
    |> maybe_since(filter.since)
    |> maybe_until(filter.until)
  end

  defp maybe_equal(query, _field, nil), do: query
  defp maybe_equal(query, field, value), do: where(query, [entry], field(entry, ^field) == ^value)

  defp maybe_in(query, _field, []), do: query
  defp maybe_in(query, field, values), do: where(query, [entry], field(entry, ^field) in ^values)

  defp maybe_since(query, nil), do: query
  defp maybe_since(query, since), do: where(query, [entry], entry.occurred_at >= ^since)

  defp maybe_until(query, nil), do: query
  defp maybe_until(query, until), do: where(query, [entry], entry.occurred_at <= ^until)

  defp after_historical_cursor(query, nil), do: query

  defp after_historical_cursor(query, %{occurred_at: occurred_at, log_id: log_id}) do
    where(
      query,
      [entry],
      entry.occurred_at < ^occurred_at or
        (entry.occurred_at == ^occurred_at and entry.log_id < ^log_id)
    )
  end

  defp after_publication(query, nil), do: query

  defp after_publication(query, %{publication_id: publication_id, batch_offset: batch_offset}) do
    where(
      query,
      [entry, _batch, event],
      event.publication_id > ^publication_id or
        (event.publication_id == ^publication_id and entry.position > ^batch_offset)
    )
  end

  defp result_page(rows, limit, direction) do
    page_rows = Enum.take(rows, limit)

    items =
      Enum.map(page_rows, fn {entry, publication_id} -> entry_result(entry, publication_id) end)

    has_more? = length(rows) > limit

    {:ok,
     %CursorPage{
       items: items,
       limit: limit,
       has_more?: has_more?,
       next_cursor: next_cursor(List.last(page_rows), has_more?, direction)
     }}
  end

  defp next_cursor(nil, _has_more?, _direction), do: nil
  defp next_cursor(_last, false, _direction), do: nil

  defp next_cursor({entry, publication_id}, true, :newer),
    do: %{publication_id: publication_id, batch_offset: entry.position}

  defp next_cursor({entry, _publication_id}, true, :older),
    do: %{occurred_at: entry.occurred_at, log_id: entry.log_id}

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
    if workspace_context?(page.workspace_context) and is_map(page.filter) and
         valid_log_cursor?(page.after, page.direction) and
         page.direction in [:older, :newer] and
         valid_bound?(page.limit, 1, 500),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp prepare_filter(filter) do
    with [] <- Map.keys(filter) -- @filter_keys,
         true <- optional_id?(filter.run_id),
         true <- optional_id?(filter.asset_step_id),
         true <- optional_id?(filter.runner_execution_id),
         true <- is_nil(filter.node_key) or is_binary(filter.node_key),
         true <- is_nil(filter.asset_ref) or is_binary(filter.asset_ref),
         true <- is_nil(filter.stream) or filter.stream in @streams,
         true <- is_list(filter.levels) and Enum.all?(filter.levels, &(&1 in @levels)),
         true <- is_list(filter.sources) and Enum.all?(filter.sources, &(&1 in @sources)),
         true <- is_nil(filter.since) or match?(%DateTime{}, filter.since),
         true <- is_nil(filter.until) or match?(%DateTime{}, filter.until),
         {:ok, node_key_hash} <- optional_filter_hash(filter.node_key),
         {:ok, asset_ref_hash} <- optional_filter_hash(filter.asset_ref) do
      {:ok,
       filter
       |> Map.put(:node_key_hash, node_key_hash)
       |> Map.put(:asset_ref_hash, asset_ref_hash)}
    else
      _invalid -> {:error, ErrorMapper.map(:invalid)}
    end
  end

  defp optional_filter_hash(nil), do: {:ok, nil}
  defp optional_filter_hash(value), do: CanonicalJSON.hash(value)

  defp normalize_log_identity(metadata, key, normalizer) when is_map(metadata) do
    string_key = Atom.to_string(key)
    value = Map.get(metadata, key, Map.get(metadata, string_key))
    metadata = Map.drop(metadata, [key, string_key])

    case value do
      nil ->
        metadata

      value ->
        case normalizer.(value) do
          {:ok, identity} -> Map.put(metadata, key, identity)
          {:error, _reason} -> metadata
        end
    end
  end

  defp optional_id?(nil), do: true
  defp optional_id?(value), do: valid_id?(value)
  defp optional_string(value) when is_binary(value), do: value
  defp optional_string(_value), do: nil
  defp atom_string(nil), do: nil
  defp atom_string(value) when is_atom(value), do: Atom.to_string(value)

  defp valid_log_cursor?(nil, _direction), do: true

  defp valid_log_cursor?(%{occurred_at: %DateTime{}, log_id: id}, :older),
    do: is_integer(id)

  defp valid_log_cursor?(
         %{publication_id: publication_id, batch_offset: batch_offset},
         :newer
       ),
       do:
         is_integer(publication_id) and publication_id >= 0 and is_integer(batch_offset) and
           batch_offset >= 0 and batch_offset < @max_entries

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
