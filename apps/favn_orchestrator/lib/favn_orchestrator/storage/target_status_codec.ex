defmodule FavnOrchestrator.Storage.TargetStatusCodec do
  @moduledoc false

  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.TargetStatus

  @format "favn.target_status.storage.v1"

  @spec encode(TargetStatus.t()) :: {:ok, String.t()} | {:error, term()}
  def encode(%TargetStatus{} = status) do
    {:ok, Jason.encode!(to_dto(status))}
  rescue
    error -> {:error, {:target_status_encode_failed, error}}
  end

  @spec decode(String.t()) :: {:ok, TargetStatus.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    case Jason.decode(payload) do
      {:ok, %{"format" => @format, "schema_version" => 1} = dto} ->
        TargetStatus.new(%{
          manifest_version_id: Map.get(dto, "manifest_version_id"),
          target_kind: Map.get(dto, "target_kind"),
          target_id: Map.get(dto, "target_id"),
          target_ref_text: Map.get(dto, "target_ref_text"),
          status: Map.get(dto, "status"),
          latest_run_id: Map.get(dto, "latest_run_id"),
          latest_run_status: Map.get(dto, "latest_run_status"),
          latest_run_at: Map.get(dto, "latest_run_at"),
          latest_run_duration_ms: Map.get(dto, "latest_run_duration_ms"),
          latest_success_run_id: Map.get(dto, "latest_success_run_id"),
          latest_success_at: Map.get(dto, "latest_success_at"),
          latest_failure_run_id: Map.get(dto, "latest_failure_run_id"),
          latest_failure_at: Map.get(dto, "latest_failure_at"),
          in_flight_run_id: Map.get(dto, "in_flight_run_id"),
          freshness_status: Map.get(dto, "freshness_status"),
          freshness_key: Map.get(dto, "freshness_key"),
          updated_at: Map.get(dto, "updated_at"),
          updated_seq: Map.get(dto, "updated_seq", 0),
          payload: Map.get(dto, "payload", %{})
        })

      {:ok, other} ->
        {:error, {:invalid_target_status_dto, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp to_dto(%TargetStatus{} = status) do
    %{
      "format" => @format,
      "schema_version" => 1,
      "manifest_version_id" => status.manifest_version_id,
      "target_kind" => Atom.to_string(status.target_kind),
      "target_id" => status.target_id,
      "target_ref_text" => status.target_ref_text,
      "status" => Atom.to_string(status.status),
      "latest_run_id" => status.latest_run_id,
      "latest_run_status" => atom_to_string(status.latest_run_status),
      "latest_run_at" => datetime_to_dto(status.latest_run_at),
      "latest_run_duration_ms" => status.latest_run_duration_ms,
      "latest_success_run_id" => status.latest_success_run_id,
      "latest_success_at" => datetime_to_dto(status.latest_success_at),
      "latest_failure_run_id" => status.latest_failure_run_id,
      "latest_failure_at" => datetime_to_dto(status.latest_failure_at),
      "in_flight_run_id" => status.in_flight_run_id,
      "freshness_status" => atom_to_string(status.freshness_status),
      "freshness_key" => status.freshness_key,
      "updated_at" => datetime_to_dto(status.updated_at),
      "updated_seq" => status.updated_seq,
      "payload" => JsonSafe.data(status.payload || %{})
    }
  end

  defp atom_to_string(nil), do: nil
  defp atom_to_string(value) when is_atom(value), do: Atom.to_string(value)
  defp atom_to_string(value), do: to_string(value)

  defp datetime_to_dto(nil), do: nil
  defp datetime_to_dto(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
end
