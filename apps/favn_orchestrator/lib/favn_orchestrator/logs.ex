defmodule FavnOrchestrator.Logs do
  @moduledoc """
  Operator-facing PubSub topics and helpers for persisted backend logs.
  """

  require Logger

  alias Favn.Log.Cursor
  alias Favn.Log.Entry
  alias Favn.Log.Filter
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.PageLogs
  alias FavnOrchestrator.Persistence.Results.LogEntry, as: PersistedLogEntry
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @workspace_topic_prefix "favn:orchestrator:logs:workspace:"

  @doc "Returns one bounded PostgreSQL log page under an explicit workspace authority."
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
      entries = Enum.map(page.items, &public_entry/1)

      {:ok, %{page | items: entries}}
    end
  end

  @doc "Replays logs newer than a commit-safe publication-and-batch-offset cursor."
  @spec replay(
          WorkspaceContext.t(),
          Cursor.t() | non_neg_integer(),
          Filter.t() | map(),
          keyword()
        ) ::
          {:ok, [Entry.t()]} | {:error, term()}
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
      {:ok, page.items}
    end
  end

  @doc "Subscribes to workspace-isolated log wakeups after authorization."
  @spec subscribe_logs(WorkspaceContext.t(), term()) :: {:ok, term()} | {:error, term()}
  def subscribe_logs(%WorkspaceContext{} = context, filter) do
    with {:ok, normalized_filter} <- normalize_filter(filter),
         topics <- subscription_topics(context.workspace_id, normalized_filter),
         {:ok, subscription} <- start_subscription_forwarder(self(), topics, normalized_filter) do
      {:ok, Map.merge(subscription, %{topics: topics, filter: normalized_filter})}
    end
  end

  @spec unsubscribe_logs(term()) :: :ok | {:error, :invalid_log_subscription}
  def unsubscribe_logs(%{pid: pid, stop_ref: stop_ref})
      when is_pid(pid) and is_reference(stop_ref) do
    send(pid, {:stop, stop_ref})
    :ok
  end

  def unsubscribe_logs(_subscription), do: {:error, :invalid_log_subscription}

  @spec broadcast_log_entry(term()) :: :ok
  def broadcast_log_entry(entry) do
    message = {:favn_log_entry, entry}

    case field(entry, :workspace_id) do
      workspace_id when is_binary(workspace_id) and workspace_id != "" ->
        broadcast_workspace_entry(workspace_id, entry, message)

      _missing_workspace ->
        Logger.warning("refused to broadcast log entry without workspace authority")
    end

    :ok
  rescue
    error ->
      Logger.warning("failed to broadcast log entry: #{inspect(error)}")
      :ok
  end

  @spec workspace_topic(String.t()) :: String.t()
  def workspace_topic(workspace_id) when is_binary(workspace_id),
    do: @workspace_topic_prefix <> workspace_id

  @spec workspace_run_topic(String.t(), String.t()) :: String.t()
  def workspace_run_topic(workspace_id, run_id)
      when is_binary(workspace_id) and is_binary(run_id),
      do: workspace_topic(workspace_id) <> ":run:" <> run_id

  @spec workspace_asset_topic(String.t(), String.t(), String.t()) :: String.t()
  def workspace_asset_topic(workspace_id, run_id, asset_step_id)
      when is_binary(workspace_id) and is_binary(run_id) and is_binary(asset_step_id),
      do: workspace_run_topic(workspace_id, run_id) <> ":asset:" <> asset_step_id

  @spec pubsub_name() :: module()
  def pubsub_name do
    Application.get_env(:favn_orchestrator, :pubsub_name, FavnOrchestrator.PubSub)
  end

  defp subscribe_topics(topics) do
    Enum.reduce_while(topics, :ok, fn topic, :ok ->
      case Phoenix.PubSub.subscribe(pubsub_name(), topic) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp start_subscription_forwarder(owner, topics, filter) do
    parent = self()
    stop_ref = make_ref()

    pid =
      spawn(fn ->
        owner_ref = Process.monitor(owner)

        case subscribe_topics(topics) do
          :ok ->
            send(parent, {__MODULE__, self(), :ready})
            subscription_loop(owner, owner_ref, stop_ref, filter)

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

  defp subscription_loop(owner, owner_ref, stop_ref, filter) do
    receive do
      {:favn_log_entry, entry} = message ->
        if matches_filter?(entry, filter), do: send(owner, message)
        subscription_loop(owner, owner_ref, stop_ref, filter)

      {:DOWN, ^owner_ref, :process, _pid, _reason} ->
        :ok

      {:stop, ^stop_ref} ->
        :ok
    end
  end

  defp broadcast_workspace_entry(workspace_id, entry, message) do
    _ = Phoenix.PubSub.broadcast(pubsub_name(), workspace_topic(workspace_id), message)

    case entry_run_id(entry) do
      run_id when is_binary(run_id) and run_id != "" ->
        _ =
          Phoenix.PubSub.broadcast(
            pubsub_name(),
            workspace_run_topic(workspace_id, run_id),
            message
          )

        case field(entry, :asset_step_id) do
          asset_step_id when is_binary(asset_step_id) and asset_step_id != "" ->
            _ =
              Phoenix.PubSub.broadcast(
                pubsub_name(),
                workspace_asset_topic(workspace_id, run_id, asset_step_id),
                message
              )

            :ok

          _other ->
            :ok
        end

      _other ->
        :ok
    end
  end

  defp subscription_topics(workspace_id, filter) do
    run_id = Map.get(filter, :run_id)
    asset_step_id = Map.get(filter, :asset_step_id)

    cond do
      is_binary(run_id) and run_id != "" and is_binary(asset_step_id) and asset_step_id != "" ->
        [workspace_asset_topic(workspace_id, run_id, asset_step_id)]

      is_binary(run_id) and run_id != "" ->
        [workspace_run_topic(workspace_id, run_id)]

      true ->
        [workspace_topic(workspace_id)]
    end
  end

  defp matches_filter?(entry, filter) do
    Enum.all?(filter, fn
      {:levels, []} -> true
      {:levels, levels} when is_list(levels) -> field(entry, :level) in levels
      {:sources, []} -> true
      {:sources, sources} when is_list(sources) -> field(entry, :source) in sources
      {:since, %DateTime{} = since} -> DateTime.compare(field(entry, :occurred_at), since) != :lt
      {:until, %DateTime{} = until} -> DateTime.compare(field(entry, :occurred_at), until) != :gt
      {_key, nil} -> true
      {key, expected} -> field(entry, key) == expected
    end)
  end

  defp normalize_filter(filter) do
    normalized = filter |> Filter.normalize() |> Map.from_struct()

    with :ok <- validate_optional_binary(normalized, :run_id),
         :ok <- validate_optional_binary(normalized, :asset_step_id),
         :ok <- validate_optional_binary(normalized, :runner_execution_id),
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

  defp entry_run_id(entry), do: field(entry, :run_id)

  defp field(%{__struct__: _struct} = value, key), do: map_field(value, key)

  defp field(value, key) when is_map(value), do: map_field(value, key)

  defp field(_value, _key), do: nil

  defp map_field(value, key) do
    Map.get(value, key) || Map.get(value, Atom.to_string(key)) ||
      metadata_field(Map.get(value, :metadata) || Map.get(value, "metadata"), key)
  end

  defp metadata_field(metadata, key) when is_map(metadata),
    do: Map.get(metadata, key) || Map.get(metadata, Atom.to_string(key))

  defp metadata_field(_metadata, _key), do: nil

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

  defp public_entry(%PersistedLogEntry{} = entry) do
    metadata = entry.metadata || %{}

    Entry.normalize(%{
      id: "#{entry.workspace_id}:#{entry.log_id}",
      global_sequence: global_sequence(entry.publication_id, entry.position),
      run_id: entry.run_id,
      asset_step_id: metadata_value(metadata, :asset_step_id),
      node_key: metadata_value(metadata, :node_key),
      asset_ref: metadata_value(metadata, :asset_ref),
      runner_execution_id: metadata_value(metadata, :runner_execution_id),
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
