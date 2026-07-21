defmodule Favn.Manifest.IdentityTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Identity

  test "hash is stable for key order variations" do
    manifest_a = FavnTestSupport.with_manifest_contract(%{assets: [%{name: "a"}]})

    manifest_b = %{
      assets: [%{name: "a"}],
      required_runner_release_id: manifest_a.required_runner_release_id,
      runner_contract_version: manifest_a.runner_contract_version,
      schema_version: manifest_a.schema_version
    }

    assert {:ok, hash_a} = Identity.hash_manifest(manifest_a)
    assert {:ok, hash_b} = Identity.hash_manifest(manifest_b)
    assert hash_a == hash_b
  end

  test "hash ignores compile-time-only keys" do
    base = FavnTestSupport.with_manifest_contract(%{assets: []})

    with_build_fields =
      Map.merge(base, %{
        diagnostics: [%{message: "warn"}],
        generated_at: DateTime.utc_now(),
        build_metadata: %{compiler: "x"}
      })

    assert {:ok, hash_a} = Identity.hash_manifest(base)
    assert {:ok, hash_b} = Identity.hash_manifest(with_build_fields)
    assert hash_a == hash_b
  end

  test "runner release binding participates in manifest identity" do
    primary = FavnTestSupport.with_manifest_contract(%{assets: []})

    alternate = %{
      primary
      | required_runner_release_id: FavnTestSupport.runner_release_id(:alternate)
    }

    assert {:ok, primary_hash} = Identity.hash_manifest(primary)
    assert {:ok, alternate_hash} = Identity.hash_manifest(alternate)
    refute primary_hash == alternate_hash
  end
end
