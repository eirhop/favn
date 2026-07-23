defmodule FavnOrchestrator.Coverage.Cursor do
  @moduledoc false

  alias Favn.Window.Key

  @version 1

  @spec encode(map()) :: String.t()
  def encode(fields) when is_map(fields) do
    %{
      "v" => @version,
      "target_id" => fields.target_id,
      "manifest_version_id" => fields.manifest_version_id,
      "evidence_generation_id" => fields.evidence_generation_id,
      "active_target_generation_id" => fields.active_target_generation_id,
      "evaluated_at" => DateTime.to_iso8601(fields.evaluated_at),
      "evaluation_checksum" => fields.evaluation_checksum,
      "after_window_key" => Key.encode(fields.after_window_key)
    }
    |> Jason.encode!()
    |> Base.url_encode64(padding: false)
  end

  @spec decode(String.t()) :: {:ok, map()} | {:error, :invalid_coverage_cursor}
  def decode(value) when is_binary(value) and byte_size(value) <= 4096 do
    with {:ok, json} <- Base.url_decode64(value, padding: false),
         {:ok, payload} <- Jason.decode(json),
         %{
           "v" => @version,
           "target_id" => target_id,
           "manifest_version_id" => manifest_version_id,
           "evidence_generation_id" => evidence_generation_id,
           "active_target_generation_id" => active_target_generation_id,
           "evaluated_at" => evaluated_at,
           "evaluation_checksum" => evaluation_checksum,
           "after_window_key" => encoded_key
         } <- payload,
         true <- Enum.all?([target_id, manifest_version_id, evidence_generation_id], &valid_id?/1),
         true <- is_nil(active_target_generation_id) or valid_id?(active_target_generation_id),
         true <- valid_checksum?(evaluation_checksum),
         {:ok, evaluated_at, _offset} <- DateTime.from_iso8601(evaluated_at),
         {:ok, after_window_key} <- Key.decode(encoded_key) do
      {:ok,
       %{
         target_id: target_id,
         manifest_version_id: manifest_version_id,
         evidence_generation_id: evidence_generation_id,
         active_target_generation_id: active_target_generation_id,
         evaluated_at: evaluated_at,
         evaluation_checksum: evaluation_checksum,
         after_window_key: after_window_key
       }}
    else
      _invalid -> {:error, :invalid_coverage_cursor}
    end
  end

  def decode(_value), do: {:error, :invalid_coverage_cursor}

  defp valid_id?(value), do: is_binary(value) and byte_size(value) in 1..255

  defp valid_checksum?(value),
    do: is_binary(value) and byte_size(value) == 64 and value =~ ~r/\A[0-9a-f]+\z/
end
