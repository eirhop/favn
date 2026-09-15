defmodule FavnStoragePostgres.Logs.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.LogStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands.AppendLogBatch
  alias FavnOrchestrator.Persistence.Commands.LogEntry, as: LogEntryCommand
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.PageLogs
  alias FavnOrchestrator.Persistence.Results.LogPage
  alias FavnOrchestrator.Persistence.Results.LifecycleLog
  alias FavnStoragePostgres.Logs.Query
  alias FavnOrchestrator.Persistence.Results.LogEntry, as: LogEntryResult
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias Favn.Log.Identity
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Payload
  alias FavnStoragePostgres.Maintenance.Replay
  alias FavnStoragePostgres.Maintenance.Retention
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.LogBatch
  alias FavnStoragePostgres.Schemas.LogEntry

  @max_entries 1_000
  @max_metadata_bytes 32 * 1_024
  @levels [:debug, :info, :warning, :error]
  @sources [:orchestrator, :runner, :sql_runtime, :adapter, :user_code, :system]
  @streams [:stdout, :stderr, :system]
  @filter_keys ~w(run_id asset_step_id runner_task_id node_key asset_ref stream levels sources since until)a

  @impl true
  def append_batch(%AppendLogBatch{} = command) do
    with :ok <- validate_append(command),
         {:ok, normalized} <- normalize_entries(command.entries),
         :ok <- validate_normalized_entries(normalized),
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
  def page(%PageLogs{} = page) do
    with :ok <- validate_page(page), {:ok, filter} <- prepare_filter(page.filter) do
      Replay.read(fn ->
        floor = Replay.check!(page.workspace_context.workspace_id, ["logs", "events"], page.after)
        {sql, params} = Query.statement(page, filter, floor)
        %{rows: rows} = SQL.query!(Repo, sql, params)
        [[watermark, current | _] | _] = rows

        if page.after && page.after.publication_id > current do
          {:error, Error.new(:invalid, "cursor is ahead of published history")}
        else
          result_page(rows, page, watermark, floor)
        end
      end)
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp result_page(rows, page, watermark, floor) do
    entries = rows |> Enum.map(&Enum.drop(&1, 2)) |> Enum.reject(&(hd(&1) == nil))
    page_rows = Enum.take(entries, page.limit)

    if page.direction == :older and not is_nil(page.after) and entries == [] and floor > {0, 0},
      do: Repo.rollback(Error.new(:expired, "history cursor expired"))

    has_more? =
      length(entries) > page.limit or
        (page.direction == :older and floor > {0, 0} and entries != [])

    last = List.last(page_rows)

    replay_cursor =
      if page.direction == :newer and has_more? do
        %{publication_id: Enum.at(last, 3), batch_offset: Enum.at(last, 4)}
      else
        %{publication_id: watermark, batch_offset: 999}
      end

    {:ok,
     %LogPage{
       items: Enum.map(page_rows, &read_entry(&1, page.workspace_context.workspace_id)),
       limit: page.limit,
       has_more?: has_more?,
       next_cursor: if(has_more?, do: next_cursor(last, page.direction, watermark)),
       replay_cursor: replay_cursor
     }}
  end

  defp read_entry(
         [1, id, _at, publication, _position, _run, _batch, _source, _level, _message, event],
         workspace
       ) do
    %LifecycleLog{
      workspace_id: workspace,
      event_id: id,
      publication_id: publication,
      event: event
    }
  end

  defp read_entry(
         [0, id, at, publication, position, run, batch, source, level, message, metadata],
         workspace
       ) do
    %LogEntryResult{
      workspace_id: workspace,
      log_id: id,
      occurred_at: utc(at),
      publication_id: publication,
      position: position,
      run_id: run,
      batch_id: batch,
      source: source,
      level: String.to_existing_atom(level),
      message: message,
      metadata: metadata
    }
  end

  defp next_cursor([_, _, _, publication, offset | _], _direction, _watermark),
    do: %{publication_id: publication, batch_offset: offset}

  defp utc(%NaiveDateTime{} = at), do: DateTime.from_naive!(at, "Etc/UTC")
  defp utc(%DateTime{} = at), do: at

  defp append_or_replay!(command, normalized, batch_hash) do
    normalized
    |> Enum.map(& &1.run_id)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.each(
      &FavnStoragePostgres.RunIdentity.lock!(command.workspace_context.workspace_id, &1)
    )

    workspace_id = command.workspace_context.workspace_id
    now = Retention.now!()

    if DateTime.compare(command.occurred_at, DateTime.add(now, -604_800, :second)) == :lt or
         DateTime.compare(command.occurred_at, DateTime.add(now, 300, :second)) == :gt do
      Repo.rollback(Error.new(:invalid, "log batch is outside the replay window"))
    end

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
    now = Retention.now!()
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
      inserted_at: now
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
          inserted_at: now
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

  defp normalize_entries(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, normalized} ->
      case normalize_entry(entry) do
        {:ok, normalized_entry} -> {:cont, {:ok, [normalized_entry | normalized]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      error -> error
    end)
  end

  defp normalize_entry(entry) do
    case Redaction.redact_operational_bounded(%{
           message: entry.message,
           metadata: entry.metadata
         }) do
      %{message: message, metadata: metadata} when is_map(metadata) ->
        normalize_entry(entry, message, metadata)

      _invalid ->
        {:error, :invalid}
    end
  end

  defp normalize_entry(entry, message, metadata) do
    metadata =
      metadata
      |> normalize_log_identity(:node_key, &Identity.node_key/1)
      |> normalize_log_identity(:asset_ref, &Identity.asset_ref/1)
      |> JsonSafe.data()

    with {:ok, node_key_hash} <- optional_filter_hash(Map.get(metadata, "node_key")),
         {:ok, asset_ref_hash} <- optional_filter_hash(Map.get(metadata, "asset_ref")) do
      {:ok,
       %{
         run_id: entry.run_id,
         asset_step_id: optional_string(Map.get(metadata, "asset_step_id")),
         runner_task_id: optional_string(Map.get(metadata, "runner_task_id")),
         node_key_hash: node_key_hash,
         asset_ref_hash: asset_ref_hash,
         stream: optional_string(Map.get(metadata, "stream")) || "system",
         source: String.slice(entry.source, 0, 100),
         level: Atom.to_string(entry.level),
         message: message |> to_string() |> String.slice(0, 8_192),
         metadata: metadata,
         occurred_at: entry.occurred_at
       }}
    end
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
         true <- optional_id?(filter.runner_task_id),
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

  defp valid_log_cursor?(nil, _direction), do: true

  defp valid_log_cursor?(
         %{publication_id: publication_id, batch_offset: batch_offset},
         direction
       )
       when direction in [:newer, :older],
       do:
         is_integer(publication_id) and publication_id >= 0 and is_integer(batch_offset) and
           batch_offset >= 0 and batch_offset < @max_entries

  defp valid_log_cursor?(_cursor, _direction), do: false

  defp valid_entry?(%LogEntryCommand{} = entry) do
    valid_id?(entry.source) and entry.level in @levels and is_binary(entry.message) and
      byte_size(entry.message) in 1..8_192 and match?(%DateTime{}, entry.occurred_at) and
      (is_nil(entry.run_id) or valid_id?(entry.run_id)) and is_map(entry.metadata)
  end

  defp valid_entry?(_other), do: false

  defp validate_normalized_entries(entries) do
    if Enum.all?(entries, &valid_normalized_entry?/1),
      do: :ok,
      else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_normalized_entry?(entry),
    do: Payload.validate(entry.metadata, @max_metadata_bytes) == :ok

  defp workspace_context?(context), do: WorkspaceContext.valid?(context)

  defp valid_bound?(value, min, max), do: is_integer(value) and value >= min and value <= max
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
