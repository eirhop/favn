defmodule FavnOrchestrator.Events do
  @moduledoc """
  Operator-facing PubSub topics and helpers for orchestrator run events.
  """

  require Logger

  alias FavnOrchestrator.RunEvent

  @run_topic_prefix "favn:orchestrator:runs"

  @spec subscribe_run(String.t()) :: :ok | {:error, term()}
  def subscribe_run(run_id) when is_binary(run_id) and run_id != "" do
    Phoenix.PubSub.subscribe(pubsub_name(), run_topic(run_id))
  end

  def subscribe_run(_run_id), do: {:error, :invalid_run_id}

  @spec unsubscribe_run(String.t()) :: :ok
  def unsubscribe_run(run_id) when is_binary(run_id) and run_id != "" do
    Phoenix.PubSub.unsubscribe(pubsub_name(), run_topic(run_id))
  end

  def unsubscribe_run(_run_id), do: :ok

  @spec subscribe_runs() :: :ok | {:error, term()}
  def subscribe_runs do
    Phoenix.PubSub.subscribe(pubsub_name(), runs_topic())
  end

  @spec unsubscribe_runs() :: :ok
  def unsubscribe_runs do
    Phoenix.PubSub.unsubscribe(pubsub_name(), runs_topic())
  end

  @spec broadcast_run_event(RunEvent.t()) :: :ok
  def broadcast_run_event(%RunEvent{} = event) do
    message = {:favn_run_event, event}

    _ = Phoenix.PubSub.broadcast(pubsub_name(), run_topic(event.run_id), message)
    _ = Phoenix.PubSub.broadcast(pubsub_name(), runs_topic(), message)
    :ok
  rescue
    error ->
      Logger.warning(
        "failed to broadcast run event #{inspect(event.run_id)}/#{event.sequence}: #{inspect(error)}"
      )

      :ok
  end

  @spec runs_topic() :: String.t()
  def runs_topic, do: @run_topic_prefix

  @spec run_topic(String.t()) :: String.t()
  def run_topic(run_id) when is_binary(run_id), do: @run_topic_prefix <> ":" <> run_id

  @spec pubsub_name() :: module()
  def pubsub_name do
    Application.get_env(:favn_orchestrator, :pubsub_name, FavnOrchestrator.PubSub)
  end
end
