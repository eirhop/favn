defmodule FavnOrchestrator.Logs do
  @moduledoc """
  Operator-facing PubSub topics and helpers for persisted backend logs.
  """

  require Logger

  @global_topic "favn:orchestrator:logs"
  @run_topic_prefix "favn:orchestrator:logs:run:"
  @asset_topic_prefix "favn:orchestrator:logs:asset:"

  @spec subscribe_logs(term()) :: {:ok, term()} | {:error, term()}
  def subscribe_logs(filter \\ default_filter()) do
    with {:ok, normalized_filter} <- normalize_filter(filter),
         topics <- subscription_topics(normalized_filter),
         {:ok, pid} <- start_subscription_forwarder(self(), topics, normalized_filter) do
      {:ok, %{pid: pid, topics: topics, filter: normalized_filter}}
    end
  end

  @spec unsubscribe_logs(term()) :: :ok | {:error, :invalid_log_subscription}
  def unsubscribe_logs(%{pid: pid}) when is_pid(pid) do
    send(pid, :stop)
    :ok
  end

  def unsubscribe_logs(_subscription), do: {:error, :invalid_log_subscription}

  @spec broadcast_log_entry(term()) :: :ok
  def broadcast_log_entry(entry) do
    message = {:favn_log_entry, entry}

    _ = Phoenix.PubSub.broadcast(pubsub_name(), global_topic(), message)

    case entry_run_id(entry) do
      run_id when is_binary(run_id) and run_id != "" ->
        _ = Phoenix.PubSub.broadcast(pubsub_name(), run_topic(run_id), message)
        maybe_broadcast_asset_log_entry(entry, run_id, message)

      _other ->
        :ok
    end

    :ok
  rescue
    error ->
      Logger.warning("failed to broadcast log entry: #{inspect(error)}")
      :ok
  end

  @spec global_topic() :: String.t()
  def global_topic, do: @global_topic

  @spec run_topic(String.t()) :: String.t()
  def run_topic(run_id) when is_binary(run_id), do: @run_topic_prefix <> run_id

  @spec asset_topic(String.t(), String.t()) :: String.t()
  def asset_topic(run_id, asset_step_id)
      when is_binary(run_id) and is_binary(asset_step_id) do
    @asset_topic_prefix <> run_id <> ":" <> asset_step_id
  end

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

    pid =
      spawn(fn ->
        owner_ref = Process.monitor(owner)

        case subscribe_topics(topics) do
          :ok ->
            send(parent, {__MODULE__, self(), :ready})
            subscription_loop(owner, owner_ref, filter)

          {:error, reason} ->
            send(parent, {__MODULE__, self(), {:error, reason}})
        end
      end)

    receive do
      {__MODULE__, ^pid, :ready} -> {:ok, pid}
      {__MODULE__, ^pid, {:error, reason}} -> {:error, reason}
    after
      1_000 -> {:error, :log_subscription_timeout}
    end
  end

  defp subscription_loop(owner, owner_ref, filter) do
    receive do
      {:favn_log_entry, entry} = message ->
        if matches_filter?(entry, filter), do: send(owner, message)
        subscription_loop(owner, owner_ref, filter)

      {:DOWN, ^owner_ref, :process, _pid, _reason} ->
        :ok

      :stop ->
        :ok
    end
  end

  defp maybe_broadcast_asset_log_entry(entry, run_id, message) do
    case field(entry, :asset_step_id) do
      asset_step_id when is_binary(asset_step_id) and asset_step_id != "" ->
        _ = Phoenix.PubSub.broadcast(pubsub_name(), asset_topic(run_id, asset_step_id), message)
        :ok

      _other ->
        :ok
    end
  end

  defp subscription_topics(filter) do
    run_id = Map.get(filter, :run_id)
    asset_step_id = Map.get(filter, :asset_step_id)

    cond do
      is_binary(run_id) and run_id != "" and is_binary(asset_step_id) and asset_step_id != "" ->
        [asset_topic(run_id, asset_step_id)]

      is_binary(run_id) and run_id != "" ->
        [run_topic(run_id)]

      true ->
        [global_topic()]
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
    case Code.ensure_loaded(Favn.Log.Filter) do
      {:module, Favn.Log.Filter} -> {:ok, Favn.Log.Filter.normalize(filter) |> Map.from_struct()}
      _other -> {:ok, normalize_filter_map(filter)}
    end
  rescue
    error -> {:error, {:invalid_log_filter, error}}
  end

  defp normalize_filter_map(filter) when is_list(filter),
    do: filter |> Map.new() |> normalize_filter_map()

  defp normalize_filter_map(%_{} = filter),
    do: filter |> Map.from_struct() |> normalize_filter_map()

  defp normalize_filter_map(filter) when is_map(filter) do
    filter
    |> Enum.map(fn {key, value} -> {normalize_filter_key(key), value} end)
    |> Map.new()
  end

  defp normalize_filter_map(_filter), do: %{}

  defp normalize_filter_key(key) when is_atom(key), do: key

  defp normalize_filter_key(key) when is_binary(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end

  defp entry_run_id(entry), do: field(entry, :run_id)

  defp field(%{__struct__: _struct} = value, key), do: Map.get(value, key)

  defp field(value, key) when is_map(value),
    do: Map.get(value, key) || Map.get(value, Atom.to_string(key))

  defp field(_value, _key), do: nil

  defp default_filter do
    case Code.ensure_loaded(Favn.Log.Filter) do
      {:module, Favn.Log.Filter} -> struct(Favn.Log.Filter)
      _other -> %{}
    end
  end
end
