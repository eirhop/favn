defmodule FavnOrchestrator.Storage.Backfill.ProgressCodec do
  @moduledoc false

  alias FavnOrchestrator.Backfill.Progress
  alias FavnOrchestrator.Storage.JsonSafe

  @format "favn.backfill.progress.storage.v1"

  @spec encode(Progress.t()) :: {:ok, String.t()} | {:error, term()}
  def encode(%Progress{} = progress) do
    {:ok, Jason.encode!(to_dto(progress))}
  rescue
    error -> {:error, {:backfill_progress_encode_failed, error}}
  end

  @spec decode(String.t()) :: {:ok, Progress.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, %{"format" => @format, "schema_version" => 1} = dto} <- Jason.decode(payload),
         {:ok, updated_at} <- datetime(Map.get(dto, "updated_at")),
         {:ok, metadata} <- map_field(dto, "metadata") do
      Progress.new(%{
        backfill_run_id: Map.get(dto, "backfill_run_id"),
        total_count: Map.get(dto, "total_count"),
        pending_count: Map.get(dto, "pending_count"),
        running_count: Map.get(dto, "running_count"),
        ok_count: Map.get(dto, "ok_count"),
        partial_count: Map.get(dto, "partial_count"),
        error_count: Map.get(dto, "error_count"),
        cancelled_count: Map.get(dto, "cancelled_count"),
        timed_out_count: Map.get(dto, "timed_out_count"),
        status: Map.get(dto, "status"),
        updated_at: updated_at,
        metadata: metadata
      })
    else
      {:ok, other} -> {:error, {:invalid_backfill_progress_dto, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp to_dto(%Progress{} = progress) do
    %{
      "format" => @format,
      "schema_version" => 1,
      "backfill_run_id" => progress.backfill_run_id,
      "total_count" => progress.total_count,
      "pending_count" => progress.pending_count,
      "running_count" => progress.running_count,
      "ok_count" => progress.ok_count,
      "partial_count" => progress.partial_count,
      "error_count" => progress.error_count,
      "cancelled_count" => progress.cancelled_count,
      "timed_out_count" => progress.timed_out_count,
      "status" => Atom.to_string(progress.status),
      "updated_at" => datetime_to_dto(progress.updated_at),
      "metadata" => JsonSafe.data(progress.metadata || %{})
    }
  end

  defp datetime_to_dto(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, reason} -> {:error, {:invalid_datetime, value, reason}}
    end
  end

  defp datetime(value), do: {:error, {:invalid_datetime, value}}

  defp map_field(dto, field) do
    case Map.fetch(dto, field) do
      {:ok, value} when is_map(value) -> {:ok, value}
      {:ok, value} -> {:error, {:invalid_dto_field, field, value}}
      :error -> {:error, {:missing_dto_field, field}}
    end
  end
end
