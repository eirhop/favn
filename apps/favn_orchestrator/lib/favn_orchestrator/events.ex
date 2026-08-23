defmodule FavnOrchestrator.Events do
  @moduledoc """
  Operator-facing PubSub topics and helpers for orchestrator run events.
  """

  require Logger

  alias FavnOrchestrator.RunEvent

  @run_topic_prefix "favn:orchestrator:runs"
  @identity_topic_prefix "favn:orchestrator:identity"
  @persistence_topic "favn:orchestrator:persistence:published"
  @projected_topic_prefix "favn:orchestrator:projected"
  @projection_listener_topic "favn:orchestrator:projection-listener"

  @spec subscribe_run(String.t(), String.t()) :: :ok | {:error, term()}
  def subscribe_run(workspace_id, run_id)
      when is_binary(workspace_id) and workspace_id != "" and is_binary(run_id) and run_id != "" do
    Phoenix.PubSub.subscribe(pubsub_name(), projected_run_topic(workspace_id, run_id))
  end

  def subscribe_run(_workspace_id, _run_id), do: {:error, :invalid_run_subscription}

  @spec unsubscribe_run(String.t(), String.t()) :: :ok
  def unsubscribe_run(workspace_id, run_id)
      when is_binary(workspace_id) and workspace_id != "" and is_binary(run_id) and run_id != "" do
    Phoenix.PubSub.unsubscribe(pubsub_name(), projected_run_topic(workspace_id, run_id))
  end

  def unsubscribe_run(_workspace_id, _run_id), do: :ok

  @spec subscribe_execution_group(String.t(), String.t()) :: :ok | {:error, term()}
  def subscribe_execution_group(workspace_id, root_run_id)
      when is_binary(workspace_id) and workspace_id != "" and is_binary(root_run_id) and
             root_run_id != "" do
    Phoenix.PubSub.subscribe(pubsub_name(), projected_root_topic(workspace_id, root_run_id))
  end

  def subscribe_execution_group(_workspace_id, _root_run_id),
    do: {:error, :invalid_run_subscription}

  @spec unsubscribe_execution_group(String.t(), String.t()) :: :ok
  def unsubscribe_execution_group(workspace_id, root_run_id) do
    Phoenix.PubSub.unsubscribe(pubsub_name(), projected_root_topic(workspace_id, root_run_id))
  end

  @doc false
  @spec subscribe_projection_listener() :: :ok | {:error, term()}
  def subscribe_projection_listener do
    Phoenix.PubSub.subscribe(pubsub_name(), @projection_listener_topic)
  end

  @doc false
  @spec unsubscribe_projection_listener() :: :ok
  def unsubscribe_projection_listener do
    Phoenix.PubSub.unsubscribe(pubsub_name(), @projection_listener_topic)
  end

  @doc false
  @spec broadcast_projection_listener_resumed() :: :ok
  def broadcast_projection_listener_resumed do
    _ =
      Phoenix.PubSub.broadcast(
        pubsub_name(),
        @projection_listener_topic,
        :favn_projection_listener_resumed
      )

    :ok
  rescue
    _error -> :ok
  end

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

  @doc "Subscribes a browser process to persisted identity invalidations."
  @spec subscribe_identity(String.t(), String.t(), String.t()) :: :ok | {:error, term()}
  def subscribe_identity(workspace_id, actor_id, session_id)
      when is_binary(workspace_id) and workspace_id != "" and is_binary(actor_id) and
             actor_id != "" and is_binary(session_id) and session_id != "" do
    topics = [
      identity_session_topic(session_id),
      identity_actor_topic(actor_id),
      identity_workspace_actor_topic(workspace_id, actor_id)
    ]

    Enum.reduce_while(topics, :ok, fn topic, :ok ->
      case Phoenix.PubSub.subscribe(pubsub_name(), topic) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  def subscribe_identity(_workspace_id, _actor_id, _session_id),
    do: {:error, :invalid_identity_subscription}

  @doc "Broadcasts revocation of one persisted session after commit."
  @spec broadcast_session_revoked(String.t()) :: :ok
  def broadcast_session_revoked(session_id) when is_binary(session_id) and session_id != "" do
    broadcast_identity(identity_session_topic(session_id), {:favn_identity_invalidated, :session})
  end

  @doc "Broadcasts a workspace membership change after commit."
  @spec broadcast_workspace_actor_changed(String.t(), String.t()) :: :ok
  def broadcast_workspace_actor_changed(workspace_id, actor_id)
      when is_binary(workspace_id) and workspace_id != "" and is_binary(actor_id) and
             actor_id != "" do
    broadcast_identity(
      identity_workspace_actor_topic(workspace_id, actor_id),
      {:favn_identity_invalidated, :workspace_membership}
    )
  end

  @doc "Broadcasts a platform-global actor change after commit."
  @spec broadcast_actor_changed(String.t()) :: :ok
  def broadcast_actor_changed(actor_id) when is_binary(actor_id) and actor_id != "" do
    broadcast_identity(
      identity_actor_topic(actor_id),
      {:favn_identity_invalidated, :actor}
    )
  end

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

  @doc "Broadcasts one validated post-projection wake-up to exact-run and root scopes."
  @spec broadcast_projection(map()) :: :ok
  def broadcast_projection(
        %{
          "workspace_id" => workspace_id,
          "run_id" => run_id,
          "root_run_id" => root_run_id
        } = payload
      )
      when is_binary(workspace_id) and is_binary(run_id) and is_binary(root_run_id) do
    message = {:favn_run_projected, payload}

    _ =
      Phoenix.PubSub.broadcast(pubsub_name(), projected_run_topic(workspace_id, run_id), message)

    _ =
      Phoenix.PubSub.broadcast(
        pubsub_name(),
        projected_root_topic(workspace_id, root_run_id),
        message
      )

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

  @spec projected_run_topic(String.t(), String.t()) :: String.t()
  def projected_run_topic(workspace_id, run_id),
    do: @projected_topic_prefix <> ":workspace:" <> workspace_id <> ":run:" <> run_id

  @spec projected_root_topic(String.t(), String.t()) :: String.t()
  def projected_root_topic(workspace_id, root_run_id),
    do: @projected_topic_prefix <> ":workspace:" <> workspace_id <> ":root:" <> root_run_id

  defp identity_session_topic(session_id), do: @identity_topic_prefix <> ":session:" <> session_id
  defp identity_actor_topic(actor_id), do: @identity_topic_prefix <> ":actor:" <> actor_id

  defp identity_workspace_actor_topic(workspace_id, actor_id),
    do: @identity_topic_prefix <> ":workspace:" <> workspace_id <> ":actor:" <> actor_id

  defp broadcast_identity(topic, message) do
    _ = Phoenix.PubSub.broadcast(pubsub_name(), topic, message)
    :ok
  rescue
    error ->
      Logger.warning("failed to broadcast identity invalidation: #{inspect(error)}")
      :ok
  end

  @spec pubsub_name() :: module()
  def pubsub_name do
    Application.get_env(:favn_orchestrator, :pubsub_name, FavnOrchestrator.PubSub)
  end
end
