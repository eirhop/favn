defmodule FavnOrchestrator.Storage.RunSnapshotCodecTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.ManifestCodec
  alias FavnOrchestrator.Storage.PayloadCodec
  alias FavnOrchestrator.Storage.RunSnapshotCodec

  test "decodes run atoms allowed by the associated manifest" do
    existing_module = __MODULE__.ExistingAsset
    unknown_module = "Elixir.Favn.RunSnapshotCodecTest.RestartAsset"
    version = manifest_version("mv_run_snapshot_allowed", existing_module)
    run = run_state("run_snapshot_allowed", version, existing_module)

    assert {:ok, run_blob} = PayloadCodec.encode(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    run_record = %{
      run_blob: replace_atom_value(run_blob, Atom.to_string(existing_module), unknown_module),
      manifest_version_id: version.manifest_version_id
    }

    manifest_record = %{
      manifest_record
      | manifest_json:
          replace_string_value(
            manifest_record.manifest_json,
            Atom.to_string(existing_module),
            unknown_module
          )
    }

    assert {:ok, decoded} = RunSnapshotCodec.decode_run(run_record, manifest_record)
    assert {module, :asset} = decoded.asset_ref
    assert Atom.to_string(module) == unknown_module
  end

  test "rejects run atoms absent from the associated manifest" do
    existing_module = __MODULE__.ExistingAsset
    unknown_module = "Elixir.Favn.RunSnapshotCodecTest.UnknownAsset"
    version = manifest_version("mv_run_snapshot_rejected", existing_module)
    run = run_state("run_snapshot_rejected", version, existing_module)

    assert {:ok, run_blob} = PayloadCodec.encode(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    run_record = %{
      run_blob: replace_atom_value(run_blob, Atom.to_string(existing_module), unknown_module),
      manifest_version_id: version.manifest_version_id
    }

    assert {:error, {:payload_decode_failed, {:unknown_atom, ^unknown_module}}} =
             RunSnapshotCodec.decode_run(run_record, manifest_record)
  end

  defp manifest_version(manifest_version_id, module) do
    manifest = %Manifest{
      assets: [%Asset{ref: {module, :asset}, module: module, name: :asset}]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp run_state(run_id, version, module) do
    RunState.new(
      id: run_id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: {module, :asset}
    )
  end

  defp replace_atom_value(encoded, from, to) do
    encoded
    |> JSON.decode!()
    |> replace_atom_value_in_term(from, to)
    |> JSON.encode!()
  end

  defp replace_atom_value_in_term(%{"__type__" => "atom", "value" => value} = term, value, to) do
    %{term | "value" => to}
  end

  defp replace_atom_value_in_term(%{} = term, from, to) do
    Map.new(term, fn {key, value} -> {key, replace_atom_value_in_term(value, from, to)} end)
  end

  defp replace_atom_value_in_term(values, from, to) when is_list(values) do
    Enum.map(values, &replace_atom_value_in_term(&1, from, to))
  end

  defp replace_atom_value_in_term(value, _from, _to), do: value

  defp replace_string_value(encoded, from, to) do
    encoded
    |> JSON.decode!()
    |> replace_string_value_in_term(from, to)
    |> JSON.encode!()
  end

  defp replace_string_value_in_term(value, value, to) when is_binary(value), do: to

  defp replace_string_value_in_term(%{} = term, from, to) do
    Map.new(term, fn {key, value} -> {key, replace_string_value_in_term(value, from, to)} end)
  end

  defp replace_string_value_in_term(values, from, to) when is_list(values) do
    Enum.map(values, &replace_string_value_in_term(&1, from, to))
  end

  defp replace_string_value_in_term(value, _from, _to), do: value
end
