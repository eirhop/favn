Code.require_file(Path.join(:code.priv_dir(:favn_test_support), "fixtures/catalog.exs"))

defmodule FavnAuthoring.Deployment.CatalogExportTest do
  use ExUnit.Case, async: true
  alias FavnAuthoring.Deployment.ManifestBuilder
  alias FavnTestSupport.CatalogFixture

  setup do
    root = Path.join(System.tmp_dir!(), "catalog-export-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  test "fresh and repeated exports leave execution archive and exact bundle inventory unchanged",
       %{root: root} do
    publication = CatalogFixture.publication()
    assert {:ok, first} = ManifestBuilder.write_release(root, publication)
    original = File.read!(first.archive_path)
    bundle = File.read!(Path.join(first.dist_dir, "bundle.json"))
    assert {:ok, second} = ManifestBuilder.write_release(root, publication)
    assert second.status == :already_built
    assert first.catalog_path == second.catalog_path
    assert File.read!(second.archive_path) == original
    assert File.read!(Path.join(second.dist_dir, "bundle.json")) == bundle
    refute File.exists?(Path.join(second.dist_dir, "catalog.json"))
    assert {:ok, _} = Favn.Catalog.Artifact.read(second.catalog_path)
  end

  test "export failure preserves reusable execution output", %{root: root} do
    publication = CatalogFixture.publication()
    catalog = Path.join([root, ".favn", "dist", "catalog"])
    File.mkdir_p!(Path.dirname(catalog))
    File.write!(catalog, "blocked")
    assert {:error, _} = ManifestBuilder.write_release(root, publication)

    archive =
      Path.join([
        root,
        ".favn",
        "dist",
        "manifest",
        publication.version.manifest_version_id <> ".tar.gz"
      ])

    bytes = File.read!(archive)
    File.rm!(catalog)
    assert {:ok, result} = ManifestBuilder.write_release(root, publication)
    assert File.read!(result.archive_path) == bytes
  end
end
