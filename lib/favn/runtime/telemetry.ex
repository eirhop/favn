defmodule Favn.Runtime.Telemetry do
  @moduledoc """
  Internal runtime telemetry facade.

  This module is the single emission boundary for machine-oriented runtime
  telemetry signals. It keeps event naming and metadata conventions stable for
  runtime internals while remaining backend-agnostic.
  """

  @type event_name :: [atom()]
  @type measurements :: map()
  @type metadata :: map()

  @spec emit(event_name(), measurements(), metadata()) :: :ok
  def emit(event_name, measurements, metadata)
      when is_list(event_name) and is_map(measurements) and is_map(metadata) do
    :telemetry.execute(event_name, measurements, metadata)
    :ok
  end

  @spec emit_runtime_event(atom(), map(), map()) :: :ok
  def emit_runtime_event(event_type, attrs, extra_metadata \\ %{})
      when is_atom(event_type) and is_map(attrs) and is_map(extra_metadata) do
    metadata =
      attrs
      |> base_metadata()
      |> Map.merge(extra_metadata)

    emit(event_name_for(event_type, attrs), measurement_for(event_type, attrs), metadata)
  end

  @spec emit_operation(atom(), atom(), integer(), map()) :: :ok
  def emit_operation(component, operation, duration_ms, metadata)
      when is_atom(component) and is_atom(operation) and is_integer(duration_ms) and
             is_map(metadata) do
    emit([:favn, :runtime, component, operation], %{duration_ms: max(duration_ms, 0)}, metadata)
  end

  defp event_name_for(:run_started, _attrs), do: [:favn, :runtime, :run, :start]
  defp event_name_for(:run_finished, _attrs), do: [:favn, :runtime, :run, :stop]
  defp event_name_for(:run_failed, _attrs), do: [:favn, :runtime, :run, :exception]
  defp event_name_for(:step_started, _attrs), do: [:favn, :runtime, :step, :start]
  defp event_name_for(:step_finished, _attrs), do: [:favn, :runtime, :step, :stop]
  defp event_name_for(:step_failed, _attrs), do: [:favn, :runtime, :step, :exception]

  defp event_name_for(:run_cancel_requested, _attrs),
    do: [:favn, :runtime, :run, :cancel_requested]

  defp event_name_for(:run_timeout_triggered, _attrs),
    do: [:favn, :runtime, :run, :timeout_triggered]

  defp event_name_for(:run_cancelled, _attrs), do: [:favn, :runtime, :run, :cancelled]
  defp event_name_for(:run_timed_out, _attrs), do: [:favn, :runtime, :run, :timed_out]
  defp event_name_for(:step_retry_scheduled, _attrs), do: [:favn, :runtime, :step, :retry]

  defp event_name_for(:step_retry_exhausted, _attrs),
    do: [:favn, :runtime, :step, :retry_exhausted]

  defp event_name_for(:step_ready, _attrs), do: [:favn, :runtime, :step, :ready]
  defp event_name_for(:step_skipped, _attrs), do: [:favn, :runtime, :step, :skipped]
  defp event_name_for(:step_cancelled, _attrs), do: [:favn, :runtime, :step, :cancelled]
  defp event_name_for(:step_timed_out, _attrs), do: [:favn, :runtime, :step, :timed_out]
  defp event_name_for(:run_created, _attrs), do: [:favn, :runtime, :run, :created]
  defp event_name_for(other, _attrs), do: [:favn, :runtime, :event, other]

  defp measurement_for(_event_type, attrs) do
    attrs
    |> Map.get(:data, %{})
    |> Map.take([
      :duration_ms,
      :queue_wait_ms,
      :delay_ms,
      :attempt,
      :max_attempts,
      :remaining_attempts
    ])
  end

  defp base_metadata(attrs) do
    data = Map.get(attrs, :data, %{})

    %{}
    |> maybe_put(:run_id, Map.get(attrs, :run_id))
    |> maybe_put(:entity, Map.get(attrs, :entity))
    |> maybe_put(:status, Map.get(attrs, :status))
    |> maybe_put(:ref, Map.get(attrs, :ref))
    |> maybe_put(:stage, Map.get(attrs, :stage))
    |> maybe_put(:sequence, Map.get(attrs, :seq))
    |> maybe_put(:error_kind, data[:error_kind])
    |> maybe_put(:error_class, data[:error_class])
    |> maybe_put(:terminal_reason_kind, get_in(data, [:terminal_reason, :kind]))
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
