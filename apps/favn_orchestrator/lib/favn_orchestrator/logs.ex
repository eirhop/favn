defmodule FavnOrchestrator.Logs do
  @moduledoc """
  Operator-facing PubSub topics and helpers for persisted backend logs.
  """

  require Logger

  @global_topic "favn:orchestrator:logs"
  @run_topic_prefix "favn:orchestrator:logs:run:"

  @spec subscribe_logs(term()) :: {:ok, term()} | {:error, term()}
  def subscribe_logs(filter \\ default_filter()) do
    with {:ok, run_id} <- filter_run_id(filter),
         topics <- subscription_topics(run_id),
         :ok <- subscribe_topics(topics) do
      {:ok, %{topics: topics, filter: filter}}
    end
  end

  @spec unsubscribe_logs(term()) :: :ok
  def unsubscribe_logs(%{topics: topics}) when is_list(topics) do
    Enum.each(topics, &Phoenix.PubSub.unsubscribe(pubsub_name(), &1))
    :ok
  end

  def unsubscribe_logs(filter) do
    with {:ok, run_id} <- filter_run_id(filter) do
      Enum.each(subscription_topics(run_id), &Phoenix.PubSub.unsubscribe(pubsub_name(), &1))
    end

    :ok
  end

  @spec broadcast_log_entry(term()) :: :ok
  def broadcast_log_entry(entry) do
    message = {:favn_log_entry, entry}

    _ = Phoenix.PubSub.broadcast(pubsub_name(), global_topic(), message)

    case entry_run_id(entry) do
      run_id when is_binary(run_id) and run_id != "" ->
        _ = Phoenix.PubSub.broadcast(pubsub_name(), run_topic(run_id), message)

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

  defp subscription_topics(nil), do: [global_topic()]
  defp subscription_topics(run_id), do: [run_topic(run_id)]

  defp filter_run_id(filter), do: {:ok, field(filter, :run_id)}

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
