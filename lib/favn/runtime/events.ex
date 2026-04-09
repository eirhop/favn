defmodule Favn.Runtime.Events do
  @moduledoc """
  Runtime run-scoped event publishing and subscription utilities.

  Favn emits structured lifecycle events over Phoenix PubSub topics keyed by
  run ID so UIs and operators can observe in-flight and completed runs.

  Event payloads follow a stable envelope with `schema_version`.
  """

  alias Favn.Runtime.Telemetry

  @typedoc "Run lifecycle event type."
  @type event_type ::
          :run_created
          | :run_started
          | :run_cancel_requested
          | :run_cancelled
          | :run_timeout_triggered
          | :run_failed
          | :run_finished
          | :run_timed_out
          | :step_ready
          | :step_started
          | :step_finished
          | :step_failed
          | :step_retry_scheduled
          | :step_retry_exhausted
          | :step_skipped
          | :step_cancelled
          | :step_timed_out

  @schema_version 1

  @typedoc "Stable event schema version."
  @type schema_version :: pos_integer()

  @typedoc "Event entity scope."
  @type entity :: :run | :step

  @typedoc "Internal runtime run status at emission time."
  @type run_internal_status :: Favn.Runtime.State.run_status()

  @typedoc "Internal runtime step status at emission time."
  @type step_internal_status :: Favn.Runtime.StepState.status()

  @typedoc "Internal runtime status carried by an event."
  @type event_status :: run_internal_status() | step_internal_status()

  @typedoc "Event timestamp."
  @type emitted_at :: DateTime.t()

  @typedoc "Structured event payload broadcast to subscribers."
  @type event :: %{
          required(:schema_version) => schema_version(),
          required(:event_type) => event_type(),
          required(:entity) => entity(),
          required(:run_id) => Favn.run_id(),
          required(:sequence) => non_neg_integer(),
          required(:emitted_at) => emitted_at(),
          required(:status) => event_status(),
          required(:data) => map(),
          optional(:ref) => Favn.asset_ref(),
          optional(:stage) => non_neg_integer()
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
    started = System.monotonic_time(:millisecond)
    sequence = Map.fetch!(attrs, :seq)
    emitted_at = DateTime.utc_now()
    data = Map.get(attrs, :data, %{})

    event =
      %{
        schema_version: @schema_version,
        event_type: event_type,
        entity: Map.fetch!(attrs, :entity),
        run_id: run_id,
        sequence: sequence,
        emitted_at: emitted_at,
        status: Map.fetch!(attrs, :status),
        data: data
      }
      |> maybe_put(:ref, Map.get(attrs, :ref))
      |> maybe_put(:stage, Map.get(attrs, :stage))

    result =
      try do
        Phoenix.PubSub.broadcast(pubsub_name(), run_topic(run_id), {:favn_run_event, event})
      rescue
        error ->
          {:error, {:raised, error}}
      catch
        :throw, reason ->
          {:error, {:thrown, reason}}

        :exit, reason ->
          {:error, {:exited, reason}}
      end

    emit_pubsub_telemetry(started, run_id, event_type, attrs, result)
    result
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
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp pubsub_result_status(:ok), do: :ok
  defp pubsub_result_status({:error, _}), do: :error

  defp pubsub_error_kind({:error, _}), do: :error
  defp pubsub_error_kind(_), do: nil

  defp pubsub_error_class({:error, {:raised, _}}), do: :publish_raise
  defp pubsub_error_class({:error, {:thrown, _}}), do: :publish_throw
  defp pubsub_error_class({:error, {:exited, _}}), do: :publish_exit
  defp pubsub_error_class({:error, _}), do: :publish_error
  defp pubsub_error_class(_), do: nil

  defp emit_pubsub_telemetry(started, run_id, event_type, attrs, result) do
    duration_ms = System.monotonic_time(:millisecond) - started

    _ =
      Telemetry.emit_operation(:pubsub, :publish, duration_ms, %{
        run_id: run_id,
        event_type: event_type,
        entity: Map.fetch!(attrs, :entity),
        result: pubsub_result_status(result),
        error_kind: pubsub_error_kind(result),
        error_class: pubsub_error_class(result)
      })

    :ok
  end
end
