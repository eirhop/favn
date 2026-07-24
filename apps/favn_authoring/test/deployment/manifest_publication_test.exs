defmodule FavnAuthoring.Deployment.ManifestPublicationTest do
  use ExUnit.Case, async: true

  alias FavnAuthoring.Deployment.ManifestPublication

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_manifest_publication_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    on_exit(fn -> File.rm_rf(root_dir) end)

    manifest_path = Path.join(root_dir, "manifest-index.json")

    File.write!(
      manifest_path,
      JSON.encode_to_iodata!(
        FavnTestSupport.with_manifest_contract(%{
          assets: [],
          pipelines: [],
          schedules: [],
          graph: %{},
          metadata: %{}
        })
      )
    )

    %{manifest_path: manifest_path}
  end

  test "raw manifests receive a stable content-derived version", context do
    assert {:ok, first} = ManifestPublication.read_version(context.manifest_path)
    assert {:ok, second} = ManifestPublication.read_version(context.manifest_path)

    assert first.manifest_version_id == second.manifest_version_id
    assert first.content_hash == second.content_hash
  end

  test "missing manifests return the structured read failure", context do
    missing_path = context.manifest_path <> ".missing"

    assert {:error, {:manifest_read_failed, ^missing_path, :enoent}} =
             ManifestPublication.read_version(missing_path)
  end
end
