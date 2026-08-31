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

    batches =
      Enum.map(List.duplicate(8, 12) ++ [5], fn expected_size ->
        assert_receive {:package_batch, batch}
        assert length(batch) == expected_size
        batch
      end)

    refute_receive {:package_batch, _extra_batch}

    assert MapSet.new(List.flatten(batches)) == MapSet.new(packages, & &1.content_hash)
  end

  test "flushes before a package would cross the byte limit" do
    root =
      Path.join(
        System.tmp_dir!(),
        "favn_upload_package_bytes_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(root) end)
    {archive_path, package_sizes} = package_archive(root, 3)
    [first, second | _rest] = Enum.sort(Map.keys(package_sizes))
    exact_limit = package_sizes[first] + package_sizes[second]

    exact_batches = parse_package_batches(archive_path, package_sizes, exact_limit)
    assert Enum.any?(exact_batches, fn batch -> batch.bytes == exact_limit end)
    assert Enum.all?(exact_batches, fn batch -> batch.bytes <= exact_limit end)

    near_limit = exact_limit - 1
    near_batches = parse_package_batches(archive_path, package_sizes, near_limit)
    assert hd(near_batches).hashes == [first]
    assert Enum.all?(near_batches, fn batch -> batch.bytes <= near_limit end)
  end

  test "rejects unknown bundle keys and executable file declarations", context do
    bundle_path = Path.join(context.bundle_dir, "bundle.json")
    bundle = bundle_path |> File.read!() |> Jason.decode!()

    assert {:error, :invalid_bundle} =
             parse_mutated_bundle(context, Map.put(bundle, "unknown", true), "unknown-key")

    [first_file | remaining_files] = bundle["files"]

    executable =
      Map.put(bundle, "files", [Map.put(first_file, "executable", true) | remaining_files])

    assert {:error, :invalid_bundle} =
             parse_mutated_bundle(context, executable, "executable")

    extra_file_key =
      Map.put(bundle, "files", [Map.put(first_file, "unknown", true) | remaining_files])

    assert {:error, :invalid_bundle} =
             parse_mutated_bundle(context, extra_file_key, "unknown-file-key")
  end

  @tag :slow
  @tag timeout: 180_000
  test "streams more than one thousand packages without caller chunking" do
    root =
      Path.join(System.tmp_dir!(), "favn_upload_scale_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(root) end)

    assert_package_batches(root, 1_001, List.duplicate(8, 125) ++ [1])
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

  test "rejects link, device, directory, sparse, and extension tar entries", context do
    tar = context.archive_path |> File.read!() |> inflate_favn_gzip()

    for type <- ["1", "2", "3", "4", "5", "6", "x", "g", "S"] do
      mutated = mutate_first_tar_header(tar, fn header -> replace_byte(header, 156, type) end)
      assert {:error, :invalid_tar_header} = parse_archive_bytes(favn_gzip(mutated))
    end
  end

  test "rejects path traversal in a regular tar entry", context do
    tar = context.archive_path |> File.read!() |> inflate_favn_gzip()

    mutated =
      mutate_first_tar_header(tar, fn header ->
        name = "../bundle.json" <> :binary.copy(<<0>>, 100 - byte_size("../bundle.json"))
        name <> binary_part(header, 100, 412)
      end)

    assert {:error, :bundle_must_be_first} = parse_archive_bytes(favn_gzip(mutated))
  end

  test "rechecks the absolute deadline on a late final body read", context do
    Process.put(:manifest_archive_clock, 0)
    bytes = File.read!(context.archive_path)
    split_at = byte_size(bytes) - 1
    <<prefix::binary-size(^split_at), final_byte::binary>> = bytes

    state =
      ManifestDeploymentArchive.new(fn _packages -> :ok end,
        clock: fn -> Process.get(:manifest_archive_clock) end,
        upload_timeout_ms: 50
      )

    assert {:ok, state} = ManifestDeploymentArchive.feed(state, prefix)
    Process.put(:manifest_archive_clock, 51)
    assert {:error, :upload_timeout} = ManifestDeploymentArchive.feed(state, final_byte)
  end

  test "rechecks the absolute deadline after package persistence" do
    root =
      Path.join(
        System.tmp_dir!(),
        "favn_upload_persistence_timeout_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(root) end)
    {archive_path, _package_sizes} = package_archive(root, 1)
    Process.put(:manifest_archive_clock, 0)

    state =
      ManifestDeploymentArchive.new(
        fn _packages ->
          Process.put(:manifest_archive_clock, 51)
          :ok
        end,
        clock: fn -> Process.get(:manifest_archive_clock) end,
        upload_timeout_ms: 50
      )

    assert {:ok, state} = feed_file(state, archive_path)
    assert {:error, :upload_timeout} = ManifestDeploymentArchive.finish(state)
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

  defp package_archive(root, count) do
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

    package_sizes =
      bundle_dir
      |> Path.join("execution-packages/*.json")
      |> Path.wildcard()
      |> Map.new(fn path -> {Path.basename(path, ".json"), File.stat!(path).size} end)

    {archive_path, package_sizes}
  end

  defp parse_package_batches(archive_path, package_sizes, byte_limit) do
    test_pid = self()
    ref = make_ref()

    state =
      ManifestDeploymentArchive.new(
        fn batch ->
          hashes = Enum.map(batch, & &1.content_hash)
          bytes = Enum.sum(Enum.map(hashes, &Map.fetch!(package_sizes, &1)))
          send(test_pid, {ref, hashes, bytes})
          :ok
        end,
        package_batch_bytes: byte_limit
      )

    assert {:ok, state} = feed_file(state, archive_path)
    assert {:ok, _result} = ManifestDeploymentArchive.finish(state)
    collect_batches(ref, [])
  end

  defp collect_batches(ref, batches) do
    receive do
      {^ref, hashes, bytes} -> collect_batches(ref, [%{hashes: hashes, bytes: bytes} | batches])
    after
      0 -> Enum.reverse(batches)
    end
  end

  defp parse_mutated_bundle(context, bundle, suffix) do
    bundle_path = Path.join(context.bundle_dir, "bundle.json")
    archive_path = Path.join(Path.dirname(context.archive_path), "mutated-#{suffix}.tar.gz")
    File.write!(bundle_path, Jason.encode!(bundle) <> "\n")
    assert {:ok, _archive} = ManifestArchive.write(context.bundle_dir, archive_path)

    state = ManifestDeploymentArchive.new(fn _packages -> :ok end)

    case feed_file(state, archive_path) do
      {:ok, state} -> ManifestDeploymentArchive.finish(state)
      {:error, _reason} = error -> error
    end
  end

  defp parse_archive_bytes(bytes) do
    state = ManifestDeploymentArchive.new(fn _packages -> :ok end)

    case ManifestDeploymentArchive.feed(state, bytes) do
      {:ok, state} -> ManifestDeploymentArchive.finish(state)
      {:error, _reason} = error -> error
    end
  end

  defp inflate_favn_gzip(bytes) do
    <<_prefix::binary-size(16), deflate_bytes::little-unsigned-64, _rest::binary>> = bytes

    <<_header::binary-size(24), deflate::binary-size(^deflate_bytes), _footer::binary-size(8)>> =
      bytes

    z = :zlib.open()

    try do
      :ok = :zlib.inflateInit(z, -15)
      expanded = z |> :zlib.inflate(deflate) |> IO.iodata_to_binary()
      :ok = :zlib.inflateEnd(z)
      expanded
    after
      :zlib.close(z)
    end
  end

  defp favn_gzip(tar) do
    z = :zlib.open()

    deflate =
      try do
        :ok = :zlib.deflateInit(z, 6, :deflated, -15, 8, :default)
        compressed = z |> :zlib.deflate(tar, :finish) |> IO.iodata_to_binary()
        :ok = :zlib.deflateEnd(z)
        compressed
      after
        :zlib.close(z)
      end

    header =
      <<0x1F, 0x8B, 8, 4, 0::little-unsigned-32, 0, 255, 12::little-unsigned-16, ?F, ?V,
        8::little-unsigned-16, byte_size(deflate)::little-unsigned-64>>

    header <>
      deflate <>
      <<:erlang.crc32(tar)::little-unsigned-32,
        Integer.mod(byte_size(tar), 4_294_967_296)::little-unsigned-32>>
  end

  defp mutate_first_tar_header(tar, mutate) do
    <<header::binary-size(512), rest::binary>> = tar
    mutated = mutate.(header)

    unsigned =
      binary_part(mutated, 0, 148) <> :binary.copy(" ", 8) <> binary_part(mutated, 156, 356)

    checksum = unsigned |> :binary.bin_to_list() |> Enum.sum()
    checksum_field = checksum |> Integer.to_string(8) |> String.pad_leading(6, "0")

    binary_part(unsigned, 0, 148) <>
      checksum_field <> <<0, 32>> <> binary_part(unsigned, 156, 356) <> rest
  end

  defp replace_byte(binary, offset, replacement) do
    binary_part(binary, 0, offset) <>
      replacement <> binary_part(binary, offset + 1, byte_size(binary) - offset - 1)
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
