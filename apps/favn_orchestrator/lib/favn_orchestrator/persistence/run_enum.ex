defmodule FavnOrchestrator.Persistence.RunEnum do
  @moduledoc false

  @statuses %{
    "pending" => :pending,
    "running" => :running,
    "ok" => :ok,
    "partial" => :partial,
    "error" => :error,
    "cancelled" => :cancelled,
    "timed_out" => :timed_out
  }

  @submit_kinds %{
    "manual" => :manual,
    "rerun" => :rerun,
    "pipeline" => :pipeline,
    "backfill_asset" => :backfill_asset,
    "backfill_pipeline" => :backfill_pipeline
  }

  @trigger_types %{
    "manual" => :manual,
    "pipeline" => :pipeline,
    "rerun" => :rerun,
    "retry" => :retry,
    "backfill" => :backfill,
    "schedule" => :schedule,
    "resource_recovery" => :resource_recovery
  }

  @type field :: :status | :submit_kind | :trigger_type

  @spec decode!(field(), String.t()) :: atom()
  def decode!(field, value) when is_binary(value) do
    case Map.fetch(values(field), value) do
      {:ok, decoded} ->
        decoded

      :error ->
        raise ArgumentError, "invalid persisted run #{field}"
    end
  end

  def decode!(field, _value) when field in [:status, :submit_kind, :trigger_type] do
    raise ArgumentError, "invalid persisted run #{field}"
  end

  defp values(:status), do: @statuses
  defp values(:submit_kind), do: @submit_kinds
  defp values(:trigger_type), do: @trigger_types
end
