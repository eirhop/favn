defmodule FavnOrchestrator.Storage.Backfill.AssetWindowStateCodec do
  @moduledoc false

  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Storage.JsonSafe

  @format "favn.backfill.asset_window_state.storage.v1"

  @spec encode(AssetWindowState.t()) :: {:ok, String.t()} | {:error, term()}
  def encode(%AssetWindowState{} = state) do
    {:ok, Jason.encode!(to_dto(state))}
  rescue
    error -> {:error, {:asset_window_state_encode_failed, error}}
  end

  @spec decode(String.t()) :: {:ok, AssetWindowState.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, %{"format" => @format, "schema_version" => 1} = dto} <- Jason.decode(payload),
         {:ok, asset_ref_module} <- existing_atom(Map.get(dto, "asset_ref_module")),
         {:ok, asset_ref_name} <- existing_atom(Map.get(dto, "asset_ref_name")),
         {:ok, pipeline_module} <- existing_atom(Map.get(dto, "pipeline_module")),
         {:ok, window_start_at} <- datetime(Map.get(dto, "window_start_at")),
         {:ok, window_end_at} <- datetime(Map.get(dto, "window_end_at")),
         {:ok, updated_at} <- datetime(Map.get(dto, "updated_at")) do
      AssetWindowState.new(%{
        asset_ref_module: asset_ref_module,
        asset_ref_name: asset_ref_name,
        pipeline_module: pipeline_module,
        manifest_version_id: Map.get(dto, "manifest_version_id"),
        window_kind: Map.get(dto, "window_kind"),
        window_start_at: window_start_at,
        window_end_at: window_end_at,
        timezone: Map.get(dto, "timezone"),
        window_key: Map.get(dto, "window_key"),
        status: Map.get(dto, "status"),
        latest_run_id: Map.get(dto, "latest_run_id"),
        latest_parent_run_id: Map.get(dto, "latest_parent_run_id"),
        latest_success_run_id: Map.get(dto, "latest_success_run_id"),
        latest_error: Map.get(dto, "latest_error"),
        errors: list(Map.get(dto, "errors")),
        rows_written: Map.get(dto, "rows_written"),
        metadata: map(Map.get(dto, "metadata")),
        updated_at: updated_at
      })
    else
      {:ok, other} -> {:error, {:invalid_asset_window_state_dto, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp to_dto(%AssetWindowState{} = state) do
    %{
      "format" => @format,
      "schema_version" => 1,
      "asset_ref_module" => Atom.to_string(state.asset_ref_module),
      "asset_ref_name" => Atom.to_string(state.asset_ref_name),
      "pipeline_module" => Atom.to_string(state.pipeline_module),
      "manifest_version_id" => state.manifest_version_id,
      "window_kind" => Atom.to_string(state.window_kind),
      "window_start_at" => datetime_to_dto(state.window_start_at),
      "window_end_at" => datetime_to_dto(state.window_end_at),
      "timezone" => state.timezone,
      "window_key" => state.window_key,
      "status" => Atom.to_string(state.status),
      "latest_run_id" => state.latest_run_id,
      "latest_parent_run_id" => state.latest_parent_run_id,
      "latest_success_run_id" => state.latest_success_run_id,
      "latest_error" => JsonSafe.error(state.latest_error),
      "errors" => Enum.map(List.wrap(state.errors), &JsonSafe.error/1),
      "rows_written" => state.rows_written,
      "metadata" => JsonSafe.data(state.metadata || %{}),
      "updated_at" => datetime_to_dto(state.updated_at)
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
