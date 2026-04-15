defmodule Favn.Manifest.IdentityTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Identity

  test "hash is stable for key order variations" do
    manifest_a = %{schema_version: 1, runner_contract_version: 1, assets: [%{name: "a"}]}
    manifest_b = %{assets: [%{name: "a"}], runner_contract_version: 1, schema_version: 1}

    assert {:ok, hash_a} = Identity.hash_manifest(manifest_a)
    assert {:ok, hash_b} = Identity.hash_manifest(manifest_b)
    assert hash_a == hash_b
  end

  test "hash ignores compile-time-only keys" do
    base = %{schema_version: 1, runner_contract_version: 1, assets: []}

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
end
