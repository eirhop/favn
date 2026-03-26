defmodule Flux.Events do
  @moduledoc """
  Run-scoped event publishing and subscription utilities.

  Flux emits structured lifecycle events over Phoenix PubSub topics keyed by
  run ID so UIs and operators can observe in-flight and completed runs.
  """

  @typedoc "Run lifecycle event type."
  @type event_type ::
          :run_started
          | :asset_started
          | :asset_finished
          | :asset_failed
          | :run_finished
          | :run_failed

  @typedoc "Structured event payload broadcast to subscribers."
  @type event :: %{
          required(:event) => event_type(),
          required(:run_id) => Flux.run_id(),
          required(:seq) => non_neg_integer(),
          required(:at) => DateTime.t(),
          optional(:ref) => Flux.asset_ref(),
          optional(:stage) => non_neg_integer(),
          optional(:payload) => map()
        }

  @doc """
  Subscribe a process to run events for `run_id`.
  """
  @spec subscribe_run(Flux.run_id()) :: :ok | {:error, term()}
  def subscribe_run(run_id) do
    Phoenix.PubSub.subscribe(pubsub_name(), run_topic(run_id))
  end

  @doc """
  Unsubscribe a process from run events for `run_id`.
  """
  @spec unsubscribe_run(Flux.run_id()) :: :ok
  def unsubscribe_run(run_id) do
    Phoenix.PubSub.unsubscribe(pubsub_name(), run_topic(run_id))
  end

  @doc """
  Publish one structured event for `run_id`.
  """
  @spec publish_run_event(Flux.run_id(), event_type(), map()) :: :ok | {:error, term()}
  def publish_run_event(run_id, event_type, attrs \\ %{})
      when is_map(attrs) and is_atom(event_type) do
    event =
      %{
        event: event_type,
        run_id: run_id,
        seq: Map.fetch!(attrs, :seq),
        at: DateTime.utc_now()
      }
      |> maybe_put(:ref, Map.get(attrs, :ref))
      |> maybe_put(:stage, Map.get(attrs, :stage))
      |> maybe_put(:payload, Map.get(attrs, :payload))

    Phoenix.PubSub.broadcast(pubsub_name(), run_topic(run_id), {:flux_run_event, event})
  end

  @doc """
  Return the Phoenix PubSub server name used by Flux events.
  """
  @spec pubsub_name() :: atom()
  def pubsub_name do
    Application.get_env(:flux, :pubsub_name, Flux.PubSub)
  end

  @doc """
  Return the canonical pubsub topic name for one run.
  """
  @spec run_topic(Flux.run_id()) :: String.t()
  def run_topic(run_id), do: "flux:run:#{run_id}"

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, :payload, payload) when payload == %{}, do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
