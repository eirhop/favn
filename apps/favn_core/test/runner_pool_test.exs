defmodule Favn.RunnerPoolTest do
  use ExUnit.Case, async: true

  alias Favn.RunnerPool

  test "treats pool names as arbitrary opaque identifiers" do
    for pool <- [:duckdb, :pure_elixir, :gpu_a10, :private_network] do
      assert :ok = RunnerPool.validate_source(pool)
      expected = Atom.to_string(pool)
      assert {:ok, ^expected} = RunnerPool.encode(pool)
    end
  end

  test "validates runtime pool strings without creating atoms" do
    name = "tenant_pool_#{System.unique_integer([:positive])}"
    assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    assert :ok = RunnerPool.validate_runtime(name)
    assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
  end

  test "validates exact pool-to-release maps and exact lookup" do
    releases = %{
      "duckdb" => FavnTestSupport.runner_release_id(),
      "pure_elixir" => FavnTestSupport.runner_release_id(:alternate)
    }

    assert :ok = RunnerPool.validate_releases(releases)
    expected = releases["duckdb"]
    assert {:ok, ^expected} = RunnerPool.fetch_release(releases, "duckdb")

    assert {:error, {:runner_pool_release_not_found, "gpu"}} =
             RunnerPool.fetch_release(releases, "gpu")
  end

  test "test fixtures derive a default release for assets" do
    manifest =
      FavnTestSupport.with_manifest_contract(%{
        assets: [%{ref: {MyApp.Asset, :asset}, runner_pool: nil}]
      })

    assert manifest.runner_releases == %{
             "default" => FavnTestSupport.runner_release_id()
           }
  end

  test "rejects unsafe or unbounded pool names" do
    for invalid <- ["", "/capacity", "has space", String.duplicate("a", 64)] do
      assert {:error, {:invalid_runner_pool, ^invalid}} = RunnerPool.validate_runtime(invalid)
    end
  end
end
