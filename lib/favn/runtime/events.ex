defmodule Favn.Runtime.Events do
  @moduledoc """
  Runtime run-scoped event publishing and subscription utilities.

  Favn emits structured lifecycle events over Phoenix PubSub topics keyed by
  run ID so UIs and operators can observe in-flight and completed runs.
  """

  @typedoc "Run lifecycle event type."
  @type event_type ::
          :run_created
          | :run_started
          | :run_cancel_requested
          | :run_cancelled
          | :run_failed
          | :run_finished
          | :run_timed_out
          | :step_ready
          | :step_started
          | :step_finished
          | :step_failed
          | :step_skipped
          | :step_cancelled
          | :step_timed_out
          | :asset_started
          | :asset_finished
          | :asset_failed

  @typedoc "Structured event payload broadcast to subscribers."
  @type event :: %{
          required(:event) => event_type(),
          required(:run_id) => Favn.run_id(),
          required(:seq) => non_neg_integer(),
          required(:at) => DateTime.t(),
          optional(:ref) => Favn.asset_ref(),
          optional(:stage) => non_neg_integer(),
          optional(:payload) => map()
        }

  @doc """
  Subscribe a process to run events for `run_id`.
  """
  @spec subscribe_run(Favn.run_id()) :: :ok | {:error, term()}
  def subscribe_run(run_id) do
    Phoenix.PubSub.subscribe(pubsub_name(), run_topic(run_id))
  end

  @doc """
  Unsubscribe a process from run events for `run_id`.
  """
  @spec unsubscribe_run(Favn.run_id()) :: :ok
  def unsubscribe_run(run_id) do
    Phoenix.PubSub.unsubscribe(pubsub_name(), run_topic(run_id))
  end

  @doc """
  Publish one structured event for `run_id`.
  """
  @spec publish_run_event(Favn.run_id(), event_type(), map()) :: :ok | {:error, term()}
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

    Phoenix.PubSub.broadcast(pubsub_name(), run_topic(run_id), {:favn_run_event, event})
  end

  @doc """
  Return the Phoenix PubSub server name used by Favn events.
  """
  @spec pubsub_name() :: atom()
  def pubsub_name do
    Application.get_env(:favn, :pubsub_name, Favn.PubSub)
  end

  @doc """
  Return the canonical pubsub topic name for one run.
  """
  @spec run_topic(Favn.run_id()) :: String.t()
  def run_topic(run_id), do: "favn:run:#{run_id}"

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, :payload, payload) when payload == %{}, do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
