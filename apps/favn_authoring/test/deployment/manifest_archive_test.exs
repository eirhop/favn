defmodule FavnAuthoring.Deployment.ManifestArchiveTest do
  use ExUnit.Case, async: true

  alias FavnAuthoring.Deployment.ManifestArchive

  setup do
    root = Path.join(System.tmp_dir!(), "favn_archive_#{System.unique_integer([:positive])}")
    bundle = Path.join(root, "bundle")
    File.mkdir_p!(Path.join(bundle, "execution-packages"))
    File.write!(Path.join(bundle, "bundle.json"), "{\"kind\":\"favn_manifest_release\"}\n")
    File.write!(Path.join(bundle, "manifest-index.json"), "{\"assets\":[]}\n")
    hash = String.duplicate("a", 64)

    File.write!(
      Path.join([bundle, "execution-packages", hash <> ".json"]),
      "{\"sql\":\"select 1\"}\n"
    )

    on_exit(fn -> File.rm_rf(root) end)
    %{root: root, bundle: bundle, hash: hash}
  end

  test "writes byte-for-byte deterministic gzip and strict ordered tar entries", context do
    first = Path.join(context.root, "first.tar.gz")
    second = Path.join(context.root, "second.tar.gz")

    assert {:ok, first_result} = ManifestArchive.write(context.bundle, first)
    File.touch!(Path.join(context.bundle, "manifest-index.json"), 2_000_000_000)
    assert {:ok, second_result} = ManifestArchive.write(context.bundle, second)

    assert first_result.sha256 == second_result.sha256
    assert File.read!(first) == File.read!(second)

    assert {:ok, files} = :erl_tar.extract({:binary, File.read!(first)}, [:compressed, :memory])

    assert Enum.map(files, &elem(&1, 0)) == [
             ~c"bundle.json",
             String.to_charlist("execution-packages/#{context.hash}.json"),
             ~c"manifest-index.json"
           ]
  end

  test "replays identical output and rejects conflicting existing bytes", context do
    path = Path.join(context.root, "manifest.tar.gz")
    assert {:ok, %{status: :built}} = ManifestArchive.write(context.bundle, path)
    assert {:ok, %{status: :already_built}} = ManifestArchive.write(context.bundle, path)

    File.write!(path, "different")
    assert {:error, :manifest_archive_conflict} = ManifestArchive.write(context.bundle, path)
  end
end
