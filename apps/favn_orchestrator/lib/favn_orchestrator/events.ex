defmodule FavnOrchestrator.Events do
  @moduledoc """
  Operator-facing PubSub topics and helpers for orchestrator run events.
  """

  require Logger

  alias FavnOrchestrator.RunEvent

  @run_topic_prefix "favn:orchestrator:runs"
  @persistence_topic "favn:orchestrator:persistence:published"

  @spec subscribe_run(String.t(), String.t()) :: :ok | {:error, term()}
  def subscribe_run(workspace_id, run_id)
      when is_binary(workspace_id) and workspace_id != "" and is_binary(run_id) and run_id != "" do
    with :ok <- Phoenix.PubSub.subscribe(pubsub_name(), run_topic(workspace_id, run_id)) do
      Phoenix.PubSub.subscribe(pubsub_name(), @persistence_topic)
    end
  end

  def subscribe_run(_workspace_id, _run_id), do: {:error, :invalid_run_subscription}

  @spec unsubscribe_run(String.t(), String.t()) :: :ok
  def unsubscribe_run(workspace_id, run_id)
      when is_binary(workspace_id) and workspace_id != "" and is_binary(run_id) and run_id != "" do
    Phoenix.PubSub.unsubscribe(pubsub_name(), run_topic(workspace_id, run_id))
  end

  def unsubscribe_run(_workspace_id, _run_id), do: :ok

  @spec subscribe_runs(String.t()) :: :ok | {:error, term()}
  def subscribe_runs(workspace_id) when is_binary(workspace_id) and workspace_id != "" do
    with :ok <- Phoenix.PubSub.subscribe(pubsub_name(), runs_topic(workspace_id)) do
      Phoenix.PubSub.subscribe(pubsub_name(), @persistence_topic)
    end
  end

  def subscribe_runs(_workspace_id), do: {:error, :invalid_run_subscription}

  @spec unsubscribe_runs(String.t()) :: :ok
  def unsubscribe_runs(workspace_id) when is_binary(workspace_id) and workspace_id != "" do
    :ok = Phoenix.PubSub.unsubscribe(pubsub_name(), runs_topic(workspace_id))
    Phoenix.PubSub.unsubscribe(pubsub_name(), @persistence_topic)
  end

  def unsubscribe_runs(_workspace_id), do: :ok

  @doc "Subscribes to node-local durable-publication wake-ups."
  @spec subscribe_persistence_publications() :: :ok | {:error, term()}
  def subscribe_persistence_publications do
    Phoenix.PubSub.subscribe(pubsub_name(), @persistence_topic)
  end

  @doc "Unsubscribes from node-local durable-publication wake-ups."
  @spec unsubscribe_persistence_publications() :: :ok
  def unsubscribe_persistence_publications do
    Phoenix.PubSub.unsubscribe(pubsub_name(), @persistence_topic)
  end

  @doc "Wakes local consumers after PostgreSQL publishes durable outbox rows."
  @spec broadcast_persistence_publication() :: :ok
  def broadcast_persistence_publication do
    _ = Phoenix.PubSub.broadcast(pubsub_name(), @persistence_topic, :favn_persistence_published)
    :ok
  rescue
    _error -> :ok
  end

  @spec broadcast_run_event(String.t(), RunEvent.t()) :: :ok
  def broadcast_run_event(workspace_id, %RunEvent{} = event)
      when is_binary(workspace_id) and workspace_id != "" do
    message = {:favn_run_event, event}

    _ = Phoenix.PubSub.broadcast(pubsub_name(), run_topic(workspace_id, event.run_id), message)
    _ = Phoenix.PubSub.broadcast(pubsub_name(), runs_topic(workspace_id), message)
    :ok
  rescue
    error ->
      Logger.warning(
        "failed to broadcast run event #{inspect(event.run_id)}/#{event.sequence}: #{inspect(error)}"
      )

      :ok
  end

  @spec runs_topic(String.t()) :: String.t()
  def runs_topic(workspace_id) when is_binary(workspace_id),
    do: @run_topic_prefix <> ":workspace:" <> workspace_id

  @spec run_topic(String.t(), String.t()) :: String.t()
  def run_topic(workspace_id, run_id) when is_binary(workspace_id) and is_binary(run_id),
    do: runs_topic(workspace_id) <> ":run:" <> run_id

  @spec pubsub_name() :: module()
  def pubsub_name do
    Application.get_env(:favn_orchestrator, :pubsub_name, FavnOrchestrator.PubSub)
  end
end
