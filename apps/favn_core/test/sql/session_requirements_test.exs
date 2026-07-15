defmodule FavnCore.SQLSessionRequirementsTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Rehydrate
  alias Favn.SQL.SessionRequirements

  doctest SessionRequirements

  test "normalizes, deduplicates, and sorts resource names" do
    assert SessionRequirements.new!([:landing_storage, "azure_extension", :landing_storage]) ==
             %SessionRequirements{
               version: 1,
               resources: ["azure_extension", "landing_storage"]
             }
  end

  test "rehydrates the versioned contract without creating resource atoms" do
    payload = %{
      "schema_version" => 5,
      "runner_contract_version" => 5,
      "assets" => [
        %{
          "ref" => %{"module" => "Elixir.Example.ResourceAsset", "name" => "asset"},
          "module" => "Elixir.Example.ResourceAsset",
          "name" => "asset",
          "type" => "sql",
          "session_requirements" => %{
            "version" => 1,
            "resources" => ["landing_storage"]
          }
        }
      ],
      "graph" => %{
        "nodes" => [%{"module" => "Elixir.Example.ResourceAsset", "name" => "asset"}],
        "edges" => [],
        "topo_order" => [%{"module" => "Elixir.Example.ResourceAsset", "name" => "asset"}]
      }
    }

    assert {:ok, manifest} = Rehydrate.manifest(payload)
    assert [asset] = manifest.assets
    assert asset.session_requirements.resources == ["landing_storage"]
  end

  test "rejects unknown contract versions and unstable names" do
    assert_raise ArgumentError, ~r/unsupported SQL session requirements version/, fn ->
      SessionRequirements.validate!(%{version: 2, resources: []})
    end

    assert_raise ArgumentError, ~r/lowercase snake_case/, fn ->
      SessionRequirements.new!(["Azure-Extension"])
    end
  end

  test "rejects unknown and duplicate contract fields" do
    assert_raise ArgumentError, ~r/unknown SQL session requirements fields/, fn ->
      SessionRequirements.validate!(%{version: 1, resources: [], resource: []})
    end

    assert_raise ArgumentError, ~r/duplicate SQL session requirements fields/, fn ->
      SessionRequirements.validate!(%{
        "version" => 1,
        version: 1,
        resources: []
      })
    end
  end
end
