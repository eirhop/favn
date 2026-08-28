defmodule FavnOrchestrator.ManifestStoreMemoryScopeTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.Budget

  setup do
    started? = is_nil(Process.whereis(MemoryCapacity.Coordinator))

    if started? do
      start_supervised!({MemoryCapacity.Supervisor, provider_opts: [ceiling_bytes: 8 * gib()]})
    end

    :ok
  end

  test "a compiled index cannot escape a temporary scoped lease" do
    version = version()

    assert {:error, :scoped_manifest_value_escape} =
             ManifestStore.with_index(version, &{:ok, &1})
  end

  test "a closure cannot capture an index beyond a temporary scoped lease" do
    version = version()

    assert {:error, :scoped_manifest_value_escape} =
             ManifestStore.with_index(version, fn index -> fn -> index end end)
  end

  test "an ordinary struct may leave a temporary scoped lease" do
    version = version()

    assert {:ok, %URI{scheme: "https"}} =
             ManifestStore.with_index(version, fn _index ->
               {:ok, %URI{scheme: "https"}}
             end)
  end

  test "a manifest identity may leave a temporary scoped lease" do
    version = version()

    assert {:ok, %Version{manifest: nil}} =
             ManifestStore.with_index(version, fn _index ->
               {:ok, Version.identity(version)}
             end)
  end

  test "an identity-shaped version cannot hide a scoped index in another field" do
    version = version()

    assert {:error, :scoped_manifest_value_escape} =
             ManifestStore.with_index(version, fn index ->
               identity = %{Version.identity(version) | runner_releases: %{"default" => index}}
               {:ok, identity}
             end)
  end

  test "nested scoped index work reuses one owner token" do
    version = version()

    assert 1 =
             ManifestStore.with_index(version, fn _outer_index ->
               ManifestStore.with_index(version, fn _inner_index ->
                 MemoryCapacity.diagnostics().active_leases
               end)
             end)
  end

  test "a compiled index may escape only while an explicit owner token remains live" do
    version = version()
    assert {:ok, token} = MemoryCapacity.acquire(Budget.index_max(), kind: :test_retained_index)

    assert {:ok, %Favn.Manifest.Index{}} =
             ManifestStore.with_index(
               version,
               [memory_capacity_token: token],
               &{:ok, &1}
             )

    assert MemoryCapacity.diagnostics().active_leases >= 1
    assert :ok = MemoryCapacity.release(token)
  end

  test "persisted index budgets reject uncompressed content above the protocol limit" do
    assert {:ok, _budget} = Budget.persisted_index(64 * 1_024 * 1_024)

    assert {:error, :manifest_memory_budget_exceeded} =
             Budget.persisted_index(64 * 1_024 * 1_024 + 1)
  end

  defp version do
    manifest = FavnTestSupport.with_manifest_contract(%Manifest{})
    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_memory_scope")
    version
  end

  defp gib, do: 1_024 * 1_024 * 1_024
end
