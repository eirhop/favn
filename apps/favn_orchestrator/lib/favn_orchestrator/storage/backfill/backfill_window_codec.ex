defmodule FavnOrchestrator.Storage.Backfill.BackfillWindowCodec do
  @moduledoc false

  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Storage.JsonSafe

  @format "favn.backfill.window.storage.v1"

  @spec encode(BackfillWindow.t()) :: {:ok, String.t()} | {:error, term()}
  def encode(%BackfillWindow{} = window) do
    {:ok, Jason.encode!(to_dto(window))}
  rescue
    error -> {:error, {:backfill_window_encode_failed, error}}
  end

  @spec decode(String.t()) :: {:ok, BackfillWindow.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, %{"format" => @format, "schema_version" => 1} = dto} <- Jason.decode(payload),
         {:ok, pipeline_module} <- existing_atom(Map.get(dto, "pipeline_module")),
         {:ok, window_start_at} <- datetime(Map.get(dto, "window_start_at")),
         {:ok, window_end_at} <- datetime(Map.get(dto, "window_end_at")),
         {:ok, started_at} <- optional_datetime(Map.get(dto, "started_at")),
         {:ok, finished_at} <- optional_datetime(Map.get(dto, "finished_at")),
         {:ok, created_at} <- optional_datetime(Map.get(dto, "created_at")),
         {:ok, updated_at} <- datetime(Map.get(dto, "updated_at")),
         {:ok, last_error} <- error_field(dto, "last_error"),
         {:ok, errors} <- list_field(dto, "errors"),
         {:ok, metadata} <- map_field(dto, "metadata") do
      BackfillWindow.new(%{
        backfill_run_id: Map.get(dto, "backfill_run_id"),
        child_run_id: Map.get(dto, "child_run_id"),
        pipeline_module: pipeline_module,
        manifest_version_id: Map.get(dto, "manifest_version_id"),
        coverage_baseline_id: Map.get(dto, "coverage_baseline_id"),
        window_kind: Map.get(dto, "window_kind"),
        window_start_at: window_start_at,
        window_end_at: window_end_at,
        timezone: Map.get(dto, "timezone"),
        window_key: Map.get(dto, "window_key"),
        status: Map.get(dto, "status"),
        attempt_count: Map.get(dto, "attempt_count", 0),
        latest_attempt_run_id: Map.get(dto, "latest_attempt_run_id"),
        last_success_run_id: Map.get(dto, "last_success_run_id"),
        last_error: last_error,
        errors: errors,
        metadata: metadata,
        started_at: started_at,
        finished_at: finished_at,
        created_at: created_at,
        updated_at: updated_at
      })
    else
      {:ok, other} -> {:error, {:invalid_backfill_window_dto, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp to_dto(%BackfillWindow{} = window) do
    %{
      "format" => @format,
      "schema_version" => 1,
      "backfill_run_id" => window.backfill_run_id,
      "child_run_id" => window.child_run_id,
      "pipeline_module" => Atom.to_string(window.pipeline_module),
      "manifest_version_id" => window.manifest_version_id,
      "coverage_baseline_id" => window.coverage_baseline_id,
      "window_kind" => Atom.to_string(window.window_kind),
      "window_start_at" => datetime_to_dto(window.window_start_at),
      "window_end_at" => datetime_to_dto(window.window_end_at),
      "timezone" => window.timezone,
      "window_key" => window.window_key,
      "status" => Atom.to_string(window.status),
      "attempt_count" => window.attempt_count,
      "latest_attempt_run_id" => window.latest_attempt_run_id,
      "last_success_run_id" => window.last_success_run_id,
      "last_error" => JsonSafe.error(window.last_error),
      "errors" => Enum.map(List.wrap(window.errors), &JsonSafe.error/1),
      "metadata" => JsonSafe.data(window.metadata || %{}),
      "started_at" => datetime_to_dto(window.started_at),
      "finished_at" => datetime_to_dto(window.finished_at),
      "created_at" => datetime_to_dto(window.created_at),
      "updated_at" => datetime_to_dto(window.updated_at)
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

  defp error_field(dto, field) do
    case Map.fetch(dto, field) do
      {:ok, value} when is_map(value) or is_nil(value) -> {:ok, value}
      {:ok, value} -> {:error, {:invalid_dto_field, field, value}}
      :error -> {:error, {:missing_dto_field, field}}
    end
  end

  defp list_field(dto, field) do
    case Map.fetch(dto, field) do
      {:ok, value} when is_list(value) -> {:ok, value}
      {:ok, value} -> {:error, {:invalid_dto_field, field, value}}
      :error -> {:error, {:missing_dto_field, field}}
    end
  end

  defp map_field(dto, field) do
    case Map.fetch(dto, field) do
      {:ok, value} when is_map(value) -> {:ok, value}
      {:ok, value} -> {:error, {:invalid_dto_field, field, value}}
      :error -> {:error, {:missing_dto_field, field}}
    end
  end
end
