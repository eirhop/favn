defmodule FavnOrchestrator.Storage.RunSnapshotCodecTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Identity
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

    manifest_json =
      replace_string_value(
        manifest_record.manifest_json,
        Atom.to_string(existing_module),
        unknown_module
      )

    content_hash = manifest_content_hash!(manifest_json)

    run_record = %{
      run_record
      | run_blob: replace_string_value(run_record.run_blob, version.content_hash, content_hash)
    }

    manifest_record = %{
      manifest_record
      | manifest_json: manifest_json,
        content_hash: content_hash
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

  test "rejects stale manifest content hash before trusting manifest atoms" do
    existing_module = __MODULE__.ExistingAsset
    unknown_module = "Elixir.Favn.RunSnapshotCodecTest.RestartAsset"
    version = manifest_version("mv_run_snapshot_stale_manifest", existing_module)
    run = run_state("run_snapshot_stale_manifest", version, existing_module)

    assert {:ok, run_blob} = PayloadCodec.encode(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    manifest_json =
      replace_string_value(
        manifest_record.manifest_json,
        Atom.to_string(existing_module),
        unknown_module
      )

    run_record = %{
      run_blob: replace_atom_value(run_blob, Atom.to_string(existing_module), unknown_module),
      manifest_version_id: version.manifest_version_id
    }

    stale_record = %{manifest_record | manifest_json: manifest_json}

    assert {:error, {:manifest_content_hash_mismatch, _, _}} =
             RunSnapshotCodec.decode_run(run_record, stale_record)
  end

  test "rejects run manifest content hash mismatch" do
    existing_module = __MODULE__.ExistingAsset
    version = manifest_version("mv_run_snapshot_hash_mismatch", existing_module)
    run = run_state("run_snapshot_hash_mismatch", version, existing_module)

    assert {:ok, run_blob} = PayloadCodec.encode(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    other_hash = String.duplicate("f", 64)

    run_record = %{
      run_blob: replace_string_value(run_blob, version.content_hash, other_hash),
      manifest_version_id: version.manifest_version_id
    }

    assert {:error, {:run_manifest_content_hash_mismatch, version_hash, ^other_hash}} =
             RunSnapshotCodec.decode_run(run_record, manifest_record)

    assert version_hash == version.content_hash
  end

  test "decodes internal pipeline context atoms" do
    existing_module = __MODULE__.ExistingAsset
    version = manifest_version("mv_run_snapshot_pipeline_context", existing_module)

    internal_pipeline_atoms = ~w(
      all
      anchor_ranges
      anchor_window
      backfill_range
      config
      deps
      name
      outputs
      pipeline
      pipeline_context
      pipeline_dependencies
      pipeline_module
      pipeline_submit_ref
      pipeline_target_refs
      resolved_refs
      run_kind
      schedule
      source
      submit_ref
    )

    placeholders =
      internal_pipeline_atoms
      |> Enum.with_index()
      |> Enum.map(fn {_target, index} -> String.to_atom("snapshot_pipeline_atom_#{index}") end)

    pipeline_context = Map.new(placeholders, &{&1, &1})

    run =
      RunState.new(
        id: "run_snapshot_pipeline_context",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {existing_module, :asset},
        metadata: %{pipeline_context: pipeline_context}
      )

    assert {:ok, run_blob} = PayloadCodec.encode(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    run_blob =
      placeholders
      |> Enum.zip(internal_pipeline_atoms)
      |> Enum.reduce(run_blob, fn {placeholder, target}, payload ->
        replace_atom_value(payload, Atom.to_string(placeholder), target)
      end)

    run_record = %{run_blob: run_blob, manifest_version_id: version.manifest_version_id}

    assert {:ok, decoded} = RunSnapshotCodec.decode_run(run_record, manifest_record)

    assert decoded.metadata.pipeline_context
           |> Map.keys()
           |> Enum.map(&Atom.to_string/1)
           |> Enum.sort() == internal_pipeline_atoms
  end

  test "ignores unrelated manifest module and name fields" do
    existing_module = __MODULE__.ExistingAsset
    unknown_module = "Elixir.Favn.RunSnapshotCodecTest.MetadataModule"
    version = manifest_version("mv_run_snapshot_metadata_ignored", existing_module)
    run = run_state("run_snapshot_metadata_ignored", version, existing_module)

    assert {:ok, run_blob} = PayloadCodec.encode(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    manifest_json = put_manifest_metadata_module(manifest_record.manifest_json, unknown_module)
    content_hash = manifest_content_hash!(manifest_json)

    run_record = %{
      run_blob:
        run_blob
        |> replace_atom_value(Atom.to_string(existing_module), unknown_module)
        |> replace_string_value(version.content_hash, content_hash),
      manifest_version_id: version.manifest_version_id
    }

    manifest_record = %{
      manifest_record
      | manifest_json: manifest_json,
        content_hash: content_hash
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

  defp manifest_content_hash!(manifest_json) do
    manifest_json
    |> JSON.decode!()
    |> Identity.hash_manifest()
    |> case do
      {:ok, hash} -> hash
    end
  end

  defp put_manifest_metadata_module(manifest_json, module) do
    manifest_json
    |> JSON.decode!()
    |> put_in(["metadata", "module"], module)
    |> put_in(["metadata", "name"], "metadata_name")
    |> JSON.encode!()
  end
end
