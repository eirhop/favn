defmodule FavnOrchestrator.Storage.Backfill.CoverageBaselineCodec do
  @moduledoc false

  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Storage.JsonSafe

  @format "favn.backfill.coverage_baseline.storage.v1"

  @spec encode(CoverageBaseline.t()) :: {:ok, String.t()} | {:error, term()}
  def encode(%CoverageBaseline{} = baseline) do
    {:ok, Jason.encode!(to_dto(baseline))}
  rescue
    error -> {:error, {:coverage_baseline_encode_failed, error}}
  end

  @spec decode(String.t()) :: {:ok, CoverageBaseline.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, %{"format" => @format, "schema_version" => 1} = dto} <- Jason.decode(payload),
         {:ok, pipeline_module} <- existing_atom(Map.get(dto, "pipeline_module")),
         {:ok, coverage_start_at} <- optional_datetime(Map.get(dto, "coverage_start_at")),
         {:ok, coverage_until} <- datetime(Map.get(dto, "coverage_until")),
         {:ok, created_at} <- datetime(Map.get(dto, "created_at")),
         {:ok, updated_at} <- datetime(Map.get(dto, "updated_at")) do
      CoverageBaseline.new(%{
        baseline_id: Map.get(dto, "baseline_id"),
        pipeline_module: pipeline_module,
        source_key: Map.get(dto, "source_key"),
        segment_key_hash: Map.get(dto, "segment_key_hash"),
        segment_key_redacted: Map.get(dto, "segment_key_redacted"),
        window_kind: Map.get(dto, "window_kind"),
        timezone: Map.get(dto, "timezone"),
        coverage_start_at: coverage_start_at,
        coverage_until: coverage_until,
        created_by_run_id: Map.get(dto, "created_by_run_id"),
        manifest_version_id: Map.get(dto, "manifest_version_id"),
        status: Map.get(dto, "status"),
        errors: list(Map.get(dto, "errors")),
        metadata: map(Map.get(dto, "metadata")),
        created_at: created_at,
        updated_at: updated_at
      })
    else
      {:ok, other} -> {:error, {:invalid_coverage_baseline_dto, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp to_dto(%CoverageBaseline{} = baseline) do
    %{
      "format" => @format,
      "schema_version" => 1,
      "baseline_id" => baseline.baseline_id,
      "pipeline_module" => Atom.to_string(baseline.pipeline_module),
      "source_key" => baseline.source_key,
      "segment_key_hash" => baseline.segment_key_hash,
      "segment_key_redacted" => baseline.segment_key_redacted,
      "window_kind" => Atom.to_string(baseline.window_kind),
      "timezone" => baseline.timezone,
      "coverage_start_at" => datetime_to_dto(baseline.coverage_start_at),
      "coverage_until" => datetime_to_dto(baseline.coverage_until),
      "created_by_run_id" => baseline.created_by_run_id,
      "manifest_version_id" => baseline.manifest_version_id,
      "status" => Atom.to_string(baseline.status),
      "errors" => Enum.map(List.wrap(baseline.errors), &JsonSafe.error/1),
      "metadata" => JsonSafe.data(baseline.metadata || %{}),
      "created_at" => datetime_to_dto(baseline.created_at),
      "updated_at" => datetime_to_dto(baseline.updated_at)
    }
  end

  defp datetime_to_dto(nil), do: nil
  defp datetime_to_dto(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, reason} -> {:error, {:invalid_datetime, value, reason}}
    end
  end

  defp datetime(value), do: {:error, {:invalid_datetime, value}}

  defp optional_datetime(nil), do: {:ok, nil}
  defp optional_datetime(value), do: datetime(value)

  defp existing_atom(value) when is_binary(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> {:error, {:unknown_atom, value}}
  end

  defp existing_atom(value), do: {:error, {:invalid_atom, value}}

  defp list(value) when is_list(value), do: value
  defp list(_value), do: []

  defp map(value) when is_map(value), do: value
  defp map(_value), do: %{}
end
