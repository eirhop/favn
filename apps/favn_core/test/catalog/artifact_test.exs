Code.require_file(Path.join(:code.priv_dir(:favn_test_support), "fixtures/catalog.exs"))

defmodule Favn.Catalog.ArtifactTest do
  use ExUnit.Case, async: true
  alias Favn.Catalog.{Artifact, Projection}
  alias FavnTestSupport.CatalogFixture, as: Fixture

  test "full public graph round trips without source, execution SQL or customer module loading" do
    artifact = Fixture.manifest()
    assert {:ok, json} = Artifact.encode(artifact)
    assert {:ok, ^artifact} = Artifact.decode(json)
    refute json =~ "/private"
    refute json =~ "SELECT 1"
    refute Code.ensure_loaded?(Example.Sales)
    assert [asset] = artifact.document["assets"]
    assert asset["description"] == "Sales"
    assert length(asset["contract"]["columns"]) == 3
    assert [pipeline] = artifact.document["pipelines"]
    assert pipeline["selectors"] == [["asset", "Example.Sales.asset"]]
    assert pipeline["schedule"] == %{"ref" => "Example.Schedules.daily"}
    assert hd(artifact.document["schedules"])["missed"] == "one"
  end

  test "all asset kinds, inline schedules and typed policies are public without secret requirements" do
    artifact = Fixture.full_manifest()
    assert {:ok, json} = Artifact.encode(artifact)
    assert {:ok, ^artifact} = Artifact.decode(json)
    assert json =~ "EXAMPLE_API_HOST"
    refute json =~ "EXAMPLE_API_SECRET"

    assert Enum.sort(Enum.map(artifact.document["assets"], & &1["kind"])) == [
             "elixir",
             "source",
             "sql"
           ]

    assert {:ok, projection} = Projection.build(artifact)
    assert length(projection.tables["schedule"]) == 2
    assert length(projection.tables["edge"]) == 2
    assert map_size(artifact.document["policies"]["execution_pools"]) == 400

    [asset] = Enum.filter(artifact.document["assets"], &(&1["kind"] == "sql"))
    assert asset["policies"]["coverage"]["declared_from"]["start_at"] == "2020-01-01T00:00:00Z"

    for {policy, field, invalid} <- [
          {"retry_policy", "max_attempts", "not a number"},
          {"retry_policy", "backoff", %{"private_key" => "unexpected"}},
          {"window", "required", "yes"},
          {"coverage", "kind", "bad"},
          {"freshness", "mode", "bad"},
          {"partition_spec", "keys", ["raw SQL"]}
        ] do
      invalid_asset = put_in(asset, ["policies", policy, field], invalid)

      doc =
        Map.update!(artifact.document, "assets", fn assets ->
          Enum.map(assets, fn entry ->
            if entry["ref"] == asset["ref"], do: invalid_asset, else: entry
          end)
        end)

      assert {:error, _} = Artifact.decode(Jason.encode!(Fixture.rehash(doc)))
    end

    for policy <- ~w(environment execution_pools connection_circuits runner_releases) do
      doc = put_in(artifact.document, ["policies", policy], %{"unexpected" => "bad"})
      assert {:error, _} = Artifact.decode(Jason.encode!(Fixture.rehash(doc)))
    end
  end

  test "closed schema rejects corrupt identities, duplicate and dangling records even after rehash" do
    artifact = Fixture.manifest()
    doc = artifact.document

    bad = [
      Map.put(doc, "secret", "bad"),
      Map.put(doc, "schema_version", 2),
      Map.put(doc, "assets", doc["assets"] ++ doc["assets"]),
      put_in(doc, ["pipelines", Access.at(0), "schedule", "ref"], "missing"),
      put_in(doc, ["pipelines", Access.at(0), "selectors"], [["asset", "missing"]]),
      put_in(doc, ["assets", Access.at(0), "policies"], %{"unexpected" => true})
    ]

    for value <- bad do
      assert {:error, _} = Artifact.decode(Jason.encode!(Fixture.rehash(value)))
    end

    assert {:error, _} = Artifact.decode(Jason.encode!(Map.put(doc, "catalog_version", "mc_bad")))
    assert {:error, _} = Artifact.decode("[]")
    assert {:error, _} = Artifact.decode("invalid")
  end

  test "projection rejects an oversized relational row before opening SQL" do
    artifact = Fixture.manifest()

    sources =
      for index <- 1..1500,
          do: %{
            "kind" => "external",
            "asset_ref" => nil,
            "dataset" => String.duplicate("x", 900) <> to_string(index),
            "column" => "id"
          }

    [asset] = artifact.document["assets"]
    asset = put_in(asset, ["contract", "columns", Access.at(0), "sources"], sources)

    base =
      Map.drop(
        asset,
        ~w(description category tags policies checks runtime_requirements fingerprint)
      )

    asset = Map.put(asset, "fingerprint", Favn.Semantic.Snapshot.digest("ac_", base))
    doc = artifact.document |> Map.put("assets", [asset]) |> Fixture.rehash()
    assert {:ok, _} = Artifact.encode(%Artifact{document: doc})
    assert {:error, :projection_row_too_large} = Projection.build(%Artifact{document: doc})
  end

  test "immutable writes are reusable and reject conflicting existing contents" do
    directory = Path.join(System.tmp_dir!(), "catalog-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    artifact = Fixture.manifest()
    assert {:ok, path} = Artifact.write(artifact, directory)
    assert {:ok, ^path} = Artifact.write(artifact, directory)
    File.write!(path, "corrupt")
    assert {:error, :immutable_artifact_conflict} = Artifact.write(artifact, directory)
  end

  test "independent projections pin their own complete snapshot" do
    assert {:ok, manifest} = Projection.build(Fixture.manifest())
    assert {:ok, semantic} = Projection.build(Fixture.semantic())
    assert manifest.kind == "manifest"
    assert semantic.kind == "semantic"
    assert length(manifest.tables["pipeline"]) == 1
    assert length(semantic.tables["metric_input"]) == 2

    assert Enum.all?(semantic.tables["column"], fn [kind, version | _] ->
             kind == "semantic" and version == semantic.version
           end)

    assert [_, _, _, 1, "net", "Example.Sales.asset", "net", _] =
             hd(semantic.tables["metric_input"])
  end
end
