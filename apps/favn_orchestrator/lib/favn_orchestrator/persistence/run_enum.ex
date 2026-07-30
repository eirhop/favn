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

  @doc """
  Decodes one persisted enum value, or `nil` when it is not one this release knows.

  `runs.trigger_type` carries whatever kind a submission was built with and has no
  CHECK constraint behind it, so a value from another release — or a kind that
  never became an enum — must cost the row its trigger rather than cost the caller
  its whole page. Use this on reads that summarize many rows; use `decode!/2` where
  the value is part of the record's identity.
  """
  @spec decode(field(), term()) :: atom() | nil
  def decode(field, value) when is_binary(value), do: Map.get(values(field), value)
  def decode(field, _value) when field in [:status, :submit_kind, :trigger_type], do: nil

  @doc """
  Encodes one runtime enum value for persistence.

  Returns `:error` rather than raising, so a filter carrying a value the operator
  typed can be rejected as invalid input instead of crashing a read.
  """
  @spec encode(field(), term()) :: {:ok, String.t()} | :error
  def encode(field, value) when is_atom(value) and not is_nil(value) do
    Enum.find_value(values(field), :error, fn {encoded, decoded} ->
      decoded == value and {:ok, encoded}
    end)
  end

  def encode(field, _value) when field in [:status, :submit_kind, :trigger_type], do: :error

  defp values(:status), do: @statuses
  defp values(:submit_kind), do: @submit_kinds
  defp values(:trigger_type), do: @trigger_types
end
