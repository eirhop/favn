defmodule FavnOrchestrator.Logs do
  @moduledoc """
  Bounded lifecycle and diagnostic log history and replay.

  Pages contain `items`, `has_more?`, `next_cursor`, and `replay_cursor`.
  The initial history page supplies a publication watermark, including when empty.
  Keep its `replay_cursor` separately while walking older history pages.
  Replay continues with `replay_cursor`; drain while `has_more?` is true.
  Default limit is 200, maximum 500. Publication notifications are wakeups;
  callers read through the authorized facade rather than accepting payloads.

  """

  alias Favn.Log.Cursor
  alias Favn.Log.Entry
  alias Favn.Log.Filter
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Logs.Lifecycle
  alias FavnOrchestrator.Persistence.Results.LifecycleLog
  alias FavnOrchestrator.Persistence.Results.LogPage
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.PageLogs
  alias FavnOrchestrator.Persistence.Results.LogEntry, as: PersistedLogEntry
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @doc """
  Returns a bounded published log page under an explicit workspace authority.

  Both directions order by publication ID and position within the batch. Pass the
  returned `%{publication_id: id, batch_offset: position}` as `:after`. Event time
  remains available for display and filtering. Retention can expire a cursor;
  restart from current history after an `:expired` persistence result.
  """
  @spec page(WorkspaceContext.t(), Filter.t() | map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def page(%WorkspaceContext{} = context, filter, opts \\ []) when is_list(opts) do
    with {:ok, normalized_filter} <- normalize_filter(filter),
         :ok <- validate_page_opts(opts),
         {:ok, page} <-
           Persistence.stores().logs.page(%PageLogs{
             workspace_context: context,
             filter: normalized_filter,
             after: Keyword.get(opts, :after),
             direction: Keyword.get(opts, :direction, :older),
             limit: Keyword.get(opts, :limit, 200)
           }) do
      Enum.reduce_while(page.items, {:ok, []}, fn row, {:ok, entries} ->
        case render_entry(row) do
          {:ok, entry} -> {:cont, {:ok, [entry | entries]}}
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, entries} -> {:ok, %{page | items: Enum.reverse(entries)}}
        error -> error
      end
    end
  end

  @doc "Replays logs newer than a commit-safe publication-and-batch-offset cursor."
  @spec replay(
          WorkspaceContext.t(),
          Cursor.t() | map() | non_neg_integer(),
          Filter.t() | map(),
          keyword()
        ) ::
          {:ok, LogPage.t()} | {:error, term()}
  def replay(%WorkspaceContext{} = context, cursor, filter, opts \\ []) when is_list(opts) do
    with {:ok, publication_cursor} <- publication_cursor(cursor),
         {:ok, page} <-
           page(
             context,
             filter,
             after: publication_cursor,
             direction: :newer,
             limit: Keyword.get(opts, :limit, 200)
           ) do
      {:ok, page}
    end
  end

  @doc "Subscribes to workspace-isolated log wakeups after authorization."
  @spec subscribe_logs(WorkspaceContext.t(), term()) :: {:ok, term()} | {:error, term()}
  def subscribe_logs(%WorkspaceContext{} = context, filter) do
    with {:ok, grant} <- prepare_subscription(context, filter) do
      subscribe_prepared(grant)
    end
  end

  @doc false
  @spec prepare_subscription(WorkspaceContext.t(), term()) ::
          {:ok, map()} | {:error, term()}
  def prepare_subscription(%WorkspaceContext{} = context, filter) do
    with {:ok, normalized_filter} <- normalize_filter(filter) do
      {:ok,
       %{
         kind: :logs,
         workspace_id: context.workspace_id,
         filter: normalized_filter
       }}
    end
  end

  @doc false
  @spec subscribe_prepared(map()) :: {:ok, term()} | {:error, term()}
  def subscribe_prepared(%{
        kind: :logs,
        workspace_id: workspace_id,
        filter: normalized_filter
      })
      when is_binary(workspace_id) and workspace_id != "" and is_map(normalized_filter) do
    with {:ok, subscription} <-
           start_subscription_forwarder(self()) do
      {:ok, Map.merge(subscription, %{filter: normalized_filter})}
    end
  end

  def subscribe_prepared(_grant), do: {:error, :invalid_log_subscription}

  @spec unsubscribe_logs(term()) :: :ok | {:error, :invalid_log_subscription}
  def unsubscribe_logs(%{pid: pid, stop_ref: stop_ref})
      when is_pid(pid) and is_reference(stop_ref) do
    send(pid, {:stop, stop_ref})
    :ok
  end

  def unsubscribe_logs(_subscription), do: {:error, :invalid_log_subscription}

  defp start_subscription_forwarder(owner) do
    parent = self()
    stop_ref = make_ref()

    pid =
      spawn(fn ->
        owner_ref = Process.monitor(owner)

        case Events.subscribe_persistence_publications() do
          :ok ->
            send(parent, {__MODULE__, self(), :ready})
            subscription_loop(owner, owner_ref, stop_ref)

          {:error, reason} ->
            send(parent, {__MODULE__, self(), {:error, reason}})
        end
      end)

    receive do
      {__MODULE__, ^pid, :ready} -> {:ok, %{pid: pid, stop_ref: stop_ref}}
      {__MODULE__, ^pid, {:error, reason}} -> {:error, reason}
    after
      1_000 ->
        Process.exit(pid, :kill)
        {:error, :log_subscription_timeout}
    end
  end

  defp subscription_loop(owner, owner_ref, stop_ref) do
    receive do
      :favn_persistence_published ->
        send(owner, :favn_logs_available)
        subscription_loop(owner, owner_ref, stop_ref)

      {:DOWN, ^owner_ref, :process, _pid, _reason} ->
        :ok

      {:stop, ^stop_ref} ->
        :ok
    end
  end

  defp normalize_filter(filter) do
    normalized = filter |> Filter.normalize() |> Map.from_struct()

    with :ok <- validate_optional_binary(normalized, :run_id),
         :ok <- validate_optional_binary(normalized, :asset_step_id),
         :ok <- validate_optional_binary(normalized, :runner_task_id),
         :ok <- validate_optional_binary(normalized, :node_key),
         :ok <- validate_optional_binary(normalized, :asset_ref),
         :ok <- validate_optional_datetime(normalized, :since),
         :ok <- validate_optional_datetime(normalized, :until),
         :ok <- validate_datetime_order(normalized) do
      {:ok, normalized}
    end
  rescue
    error -> {:error, {:invalid_log_filter, error}}
  end

  defp validate_optional_binary(filter, field) do
    case Map.get(filter, field) do
      nil -> :ok
      value when is_binary(value) and value != "" and byte_size(value) <= 512 -> :ok
      value -> {:error, {:invalid_log_filter_field, field, value}}
    end
  end

  defp validate_optional_datetime(filter, field) do
    case Map.get(filter, field) do
      nil -> :ok
      %DateTime{} -> :ok
      value -> {:error, {:invalid_log_filter_field, field, value}}
    end
  end

  defp validate_datetime_order(%{since: %DateTime{} = since, until: %DateTime{} = until}) do
    if DateTime.compare(since, until) in [:lt, :eq],
      do: :ok,
      else: {:error, {:invalid_log_filter_range, since, until}}
  end

  defp validate_datetime_order(_filter), do: :ok

  defp validate_page_opts(opts) do
    unknown = Keyword.keys(opts) -- [:after, :direction, :limit]
    limit = Keyword.get(opts, :limit, 200)
    direction = Keyword.get(opts, :direction, :older)

    cond do
      unknown != [] -> {:error, {:unknown_log_page_options, unknown}}
      not is_integer(limit) or limit < 1 or limit > 500 -> {:error, :invalid_log_page_limit}
      direction not in [:older, :newer] -> {:error, :invalid_log_page_direction}
      true -> :ok
    end
  end

  defp publication_cursor(%{publication_id: id, batch_offset: offset} = cursor)
       when is_integer(id) and id >= 0 and is_integer(offset) and offset in 0..999,
       do: {:ok, cursor}

  defp publication_cursor(%Cursor{global_sequence: sequence}), do: publication_cursor(sequence)

  defp publication_cursor(0), do: {:ok, %{publication_id: 0, batch_offset: 0}}

  defp publication_cursor(sequence) when is_integer(sequence) and sequence > 0 do
    zero_based = sequence - 1

    {:ok,
     %{
       publication_id: div(zero_based, 1_000) + 1,
       batch_offset: rem(zero_based, 1_000)
     }}
  end

  defp publication_cursor(_cursor), do: {:error, :invalid_cursor}

  defp render_entry(%LifecycleLog{} = row),
    do: Lifecycle.render(row.workspace_id, row.event_id, row.publication_id, row.event)

  defp render_entry(%PersistedLogEntry{} = row), do: {:ok, public_entry(row)}

  defp public_entry(%PersistedLogEntry{} = entry) do
    metadata = entry.metadata || %{}

    Entry.normalize(%{
      id: "#{entry.workspace_id}:#{entry.log_id}",
      global_sequence: global_sequence(entry.publication_id, entry.position),
      run_id: entry.run_id,
      asset_step_id: metadata_value(metadata, :asset_step_id),
      node_key: metadata_value(metadata, :node_key),
      asset_ref: metadata_value(metadata, :asset_ref),
      runner_task_id: metadata_value(metadata, :runner_task_id),
      attempt: metadata_value(metadata, :attempt),
      producer_id: metadata_value(metadata, :producer_id),
      producer_sequence: metadata_value(metadata, :producer_sequence),
      occurred_at: entry.occurred_at,
      level: entry.level,
      source: known_source(entry.source),
      stream: known_stream(metadata_value(metadata, :stream)),
      message: entry.message,
      metadata: metadata,
      truncated: metadata_value(metadata, :truncated) == true
    })
  end

  defp global_sequence(publication_id, batch_offset)
       when is_integer(publication_id) and publication_id > 0 and is_integer(batch_offset),
       do: (publication_id - 1) * 1_000 + batch_offset + 1

  defp global_sequence(_publication_id, _batch_offset), do: nil

  defp known_source(value) when is_binary(value) do
    Enum.find(Entry.sources(), :system, &(Atom.to_string(&1) == value))
  end

  defp known_source(value)
       when value in [:orchestrator, :runner, :sql_runtime, :adapter, :user_code, :system],
       do: value

  defp known_source(_value), do: :system

  defp known_stream(value) when is_binary(value) do
    Enum.find(Entry.streams(), :system, &(Atom.to_string(&1) == value))
  end

  defp known_stream(value) when value in [:stdout, :stderr, :system], do: value
  defp known_stream(_value), do: :system

  defp metadata_value(metadata, key) when is_map(metadata),
    do: Map.get(metadata, key) || Map.get(metadata, Atom.to_string(key))
end
