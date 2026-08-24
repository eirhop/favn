defmodule FavnOrchestrator.API.ManifestDeploymentArchiveTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.{Asset, ExecutionPackage, Graph, Publication, SQLExecution, Version}
  alias Favn.SQL.Template
  alias FavnAuthoring.Deployment.ManifestArchive
  alias FavnAuthoring.Deployment.ManifestBuilder
  alias FavnOrchestrator.API.ManifestDeploymentArchive

  setup do
    root =
      Path.join(System.tmp_dir!(), "favn_upload_archive_#{System.unique_integer([:positive])}")

    bundle_dir = Path.join(root, "bundle")
    archive_path = Path.join(root, "manifest.tar.gz")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    manifest =
      FavnTestSupport.with_manifest_contract(
        %{
          assets: [],
          pipelines: [],
          schedules: [],
          graph: %{},
          metadata: %{}
        },
        %{}
      )

    assert {:ok, version} = Version.new(manifest)
    assert {:ok, publication} = Publication.from_parts(version, [])
    assert :ok = ManifestBuilder.write_bundle(bundle_dir, publication)
    assert {:ok, archive} = ManifestArchive.write(bundle_dir, archive_path)

    %{archive_path: archive_path, archive: archive, bundle_dir: bundle_dir, version: version}
  end

  test "streams and verifies the deterministic archive without a temporary file", context do
    state = ManifestDeploymentArchive.new(fn [] -> :ok end)

    assert {:ok, state} =
             context.archive_path
             |> File.stream!(37, [])
             |> Enum.reduce_while({:ok, state}, fn bytes, {:ok, state} ->
               case ManifestDeploymentArchive.feed(state, bytes) do
                 {:ok, state} -> {:cont, {:ok, state}}
                 error -> {:halt, error}
               end
             end)

    assert {:ok, result} = ManifestDeploymentArchive.finish(state)
    assert result.archive_sha256 == context.archive.sha256
    assert result.version.manifest_version_id == context.version.manifest_version_id
    assert result.package_count == 0
    assert result.entry_count == 2
  end

  test "persists real execution packages in bounded batches" do
    root =
      Path.join(System.tmp_dir!(), "favn_upload_packages_#{System.unique_integer([:positive])}")

    bundle_dir = Path.join(root, "bundle")
    archive_path = Path.join(root, "manifest.tar.gz")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    {assets, packages} = execution_packages(101)
    assert {:ok, graph} = Graph.build(assets)

    manifest =
      FavnTestSupport.with_manifest_contract(%{
        assets: assets,
        pipelines: [],
        schedules: [],
        graph: graph,
        metadata: %{}
      })

    assert {:ok, version} = Version.new(manifest)
    assert {:ok, publication} = Publication.from_parts(version, packages)
    assert :ok = ManifestBuilder.write_bundle(bundle_dir, publication)
    assert {:ok, _archive} = ManifestArchive.write(bundle_dir, archive_path)

    test_pid = self()

    state =
      ManifestDeploymentArchive.new(fn batch ->
        send(test_pid, {:package_batch, Enum.map(batch, & &1.content_hash)})
        :ok
      end)

    assert {:ok, state} = feed_file(state, archive_path)
    assert {:ok, result} = ManifestDeploymentArchive.finish(state)
    assert result.package_count == 101
    assert result.entry_count == 103

    assert_receive {:package_batch, first_batch}
    assert_receive {:package_batch, second_batch}
    assert length(first_batch) == 100
    assert length(second_batch) == 1
    refute_receive {:package_batch, _extra_batch}
    assert MapSet.new(first_batch ++ second_batch) == MapSet.new(packages, & &1.content_hash)
  end

  @tag :slow
  test "streams more than one thousand packages without caller chunking" do
    root =
      Path.join(System.tmp_dir!(), "favn_upload_scale_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(root) end)

    assert_package_batches(root, 1_001, List.duplicate(100, 10) ++ [1])
  end

  test "rejects a changed gzip footer", context do
    bytes = File.read!(context.archive_path)
    last = byte_size(bytes) - 1
    <<prefix::binary-size(^last), byte>> = bytes
    state = ManifestDeploymentArchive.new(fn _packages -> :ok end)

    assert {:ok, state} =
             ManifestDeploymentArchive.feed(state, prefix <> <<Bitwise.bxor(byte, 1)>>)

    assert {:error, :invalid_gzip_footer} = ManifestDeploymentArchive.finish(state)
  end

  test "rejects a non-Favn gzip header before expanding", context do
    <<_first, rest::binary>> = File.read!(context.archive_path)
    state = ManifestDeploymentArchive.new(fn _packages -> :ok end)

    assert {:error, :invalid_gzip_header} =
             ManifestDeploymentArchive.feed(state, <<0, rest::binary>>)
  end

  test "rejects concatenated gzip members", context do
    bytes = File.read!(context.archive_path)
    state = ManifestDeploymentArchive.new(fn _packages -> :ok end)

    result =
      case ManifestDeploymentArchive.feed(state, bytes <> bytes) do
        {:ok, state} -> ManifestDeploymentArchive.finish(state)
        {:error, _reason} = error -> error
      end

    assert {:error, _reason} = result
  end

  defp execution_packages(count) do
    1..count
    |> Enum.map(fn index ->
      module = Module.concat(__MODULE__, "Asset#{index}")
      ref = {module, :asset}
      sql = "SELECT #{index} AS id"

      template =
        Template.compile!(sql,
          file: "test/manifest_deployment_archive.sql",
          line: index,
          module: module,
          scope: :query,
          enforce_query_root: true
        )

      assert {:ok, package} =
               ExecutionPackage.new(ref, %SQLExecution{sql: sql, template: template})

      asset = %Asset{
        ref: ref,
        module: module,
        name: :asset,
        type: :sql,
        execution_package_hash: package.content_hash
      }

      {asset, package}
    end)
    |> Enum.unzip()
  end

  defp assert_package_batches(root, count, expected_batch_sizes) do
    bundle_dir = Path.join(root, "bundle")
    archive_path = Path.join(root, "manifest.tar.gz")
    File.mkdir_p!(root)
    {assets, packages} = execution_packages(count)
    assert {:ok, graph} = Graph.build(assets)

    manifest =
      FavnTestSupport.with_manifest_contract(%{
        assets: assets,
        pipelines: [],
        schedules: [],
        graph: graph,
        metadata: %{}
      })

    assert {:ok, version} = Version.new(manifest)
    assert {:ok, publication} = Publication.from_parts(version, packages)
    assert :ok = ManifestBuilder.write_bundle(bundle_dir, publication)
    assert {:ok, _archive} = ManifestArchive.write(bundle_dir, archive_path)

    test_pid = self()

    state =
      ManifestDeploymentArchive.new(fn batch ->
        send(test_pid, {:scale_package_batch, length(batch)})
        :ok
      end)

    assert {:ok, state} = feed_file(state, archive_path)
    assert {:ok, result} = ManifestDeploymentArchive.finish(state)
    assert result.package_count == count

    actual_batch_sizes =
      Enum.map(expected_batch_sizes, fn _expected ->
        assert_receive {:scale_package_batch, size}
        size
      end)

    assert actual_batch_sizes == expected_batch_sizes
    refute_receive {:scale_package_batch, _extra_batch}
  end

  defp feed_file(state, archive_path) do
    archive_path
    |> File.stream!(1_024 * 1_024, [])
    |> Enum.reduce_while({:ok, state}, fn bytes, {:ok, state} ->
      case ManifestDeploymentArchive.feed(state, bytes) do
        {:ok, state} -> {:cont, {:ok, state}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end
end
