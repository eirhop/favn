defmodule FavnOrchestrator.Storage.RunSnapshotCodec do
  @moduledoc false

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.PayloadCodec
  alias FavnOrchestrator.Storage.RunStateCodec

  @type run_record :: %{
          required(:run_blob) => String.t(),
          required(:manifest_version_id) => String.t()
        }

  @type manifest_record :: %{
          required(:manifest_version_id) => String.t(),
          required(:manifest_json) => String.t()
        }

  @spec decode_run(run_record(), manifest_record() | nil) ::
          {:ok, RunState.t()} | {:error, term()}
  def decode_run(%{run_blob: payload, manifest_version_id: manifest_version_id}, manifest_record)
      when is_binary(payload) and is_binary(manifest_version_id) do
    with {:ok, manifest_record} <- validate_manifest_record(manifest_record, manifest_version_id),
         {:ok, allowed_atom_strings} <- manifest_atom_strings(manifest_record),
         {:ok, decoded} <-
           PayloadCodec.decode(payload, allowed_atom_strings: allowed_atom_strings),
         %RunState{} = run_state <- decoded,
         :ok <- validate_run_manifest(run_state, manifest_version_id),
         {:ok, normalized} <- RunStateCodec.normalize(run_state) do
      {:ok, normalized}
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_run_payload, other}}
    end
  end

  def decode_run(run_record, _manifest_record), do: {:error, {:invalid_run_record, run_record}}

  defp validate_manifest_record(nil, manifest_version_id),
    do: {:error, {:missing_manifest_version, manifest_version_id}}

  defp validate_manifest_record(
         %{manifest_version_id: manifest_version_id} = record,
         manifest_version_id
       ),
       do: {:ok, record}

  defp validate_manifest_record(%{manifest_version_id: other}, manifest_version_id),
    do: {:error, {:run_manifest_mismatch, manifest_version_id, other}}

  defp validate_manifest_record(record, _manifest_version_id),
    do: {:error, {:invalid_manifest_record, record}}

  defp manifest_atom_strings(%{manifest_json: manifest_json}) when is_binary(manifest_json) do
    case JSON.decode(manifest_json) do
      {:ok, decoded} -> {:ok, decoded |> manifest_atom_strings_from_value() |> Enum.uniq()}
      {:error, reason} -> {:error, {:invalid_manifest_json, reason}}
    end
  end

  defp manifest_atom_strings(record), do: {:error, {:invalid_manifest_record, record}}

  defp manifest_atom_strings_from_value(%{} = value) do
    current =
      [Map.get(value, "module"), Map.get(value, "name")]
      |> Enum.filter(&(is_binary(&1) and &1 != ""))

    nested =
      value
      |> Map.values()
      |> Enum.flat_map(&manifest_atom_strings_from_value/1)

    current ++ nested
  end

  defp manifest_atom_strings_from_value(values) when is_list(values) do
    Enum.flat_map(values, &manifest_atom_strings_from_value/1)
  end

  defp manifest_atom_strings_from_value(_value), do: []

  defp validate_run_manifest(
         %RunState{manifest_version_id: manifest_version_id},
         manifest_version_id
       ),
       do: :ok

  defp validate_run_manifest(%RunState{manifest_version_id: other}, manifest_version_id),
    do: {:error, {:run_manifest_mismatch, manifest_version_id, other}}
end
