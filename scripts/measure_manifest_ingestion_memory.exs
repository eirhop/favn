if Mix.env() != :test do
  raise "run the manifest ingestion measurement with MIX_ENV=test"
end

defmodule FavnManifestIngestionMeasurement do
  @moduledoc """
  Executable synthetic memory check for bounded manifest archive ingestion.

  The generator runs outside each measured parser process. The check compares
  1, 64, and 1,024 packages and fails when parser RSS growth exceeds 192 MiB.

      MIX_ENV=test mix do --app favn_orchestrator cmd mix run --no-start \
        ../../scripts/measure_manifest_ingestion_memory.exs
  """

  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Publication
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.SQL.Template
  alias FavnAuthoring.Deployment.ManifestArchive
  alias FavnAuthoring.Deployment.ManifestBuilder
  alias FavnOrchestrator.API.ManifestDeploymentArchive
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.Budget

  @counts [1, 64, 1_024]
  @maximum_growth_kib 192 * 1_024
  @large_sql_bytes 1_500_000

  def run(["--parse", archive_path, expected_count]) do
    expected_count = String.to_integer(expected_count)

    {:ok, capacity} =
      MemoryCapacity.Supervisor.start_link(
        provider_opts: [ceiling_bytes: 2 * 1_024 * 1_024 * 1_024]
      )

    {:ok, token} =
      MemoryCapacity.acquire(Budget.manifest_base(),
        kind: :manifest_measurement,
        exclusive: true
      )

    state =
      ManifestDeploymentArchive.new(fn _packages -> :ok end, capacity_token: token)

    try do
      parsed =
        archive_path
        |> File.stream!(1_024 * 1_024, [])
        |> Enum.reduce_while({:ok, state}, fn bytes, {:ok, parser} ->
          case ManifestDeploymentArchive.feed(parser, bytes) do
            {:ok, next} -> {:cont, {:ok, next}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
        |> case do
          {:ok, parser} -> ManifestDeploymentArchive.finish(parser)
          {:error, reason} -> {:error, reason}
        end

      case parsed do
        {:ok, %{package_count: ^expected_count}} ->
          IO.puts("FAVN_PARSED_PACKAGES=#{expected_count}")

        other ->
          raise "synthetic archive parse failed: #{inspect(other)}"
      end
    after
      MemoryCapacity.release(token)
      Supervisor.stop(capacity)
    end
  end

  def run([]) do
    root =
      Path.join(
        System.tmp_dir!(),
        "favn-manifest-memory-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)

    try do
      measurements =
        Enum.map(@counts, fn count ->
          {archive_path, largest_package_bytes} = build_archive(root, count)
          max_rss_kib = measure_parser(archive_path, count)

          IO.puts(
            "packages=#{count} largest_package_bytes=#{largest_package_bytes} " <>
              "max_rss_kib=#{max_rss_kib}"
          )

          {count, max_rss_kib, largest_package_bytes}
        end)

      first = measurements |> hd() |> elem(1)
      last = measurements |> List.last() |> elem(1)
      growth = max(last - first, 0)
      largest_package_sizes = measurements |> Enum.map(&elem(&1, 2)) |> Enum.uniq()

      if length(largest_package_sizes) != 1 do
        raise "measurement archives did not contain the same largest execution package"
      end

      if growth > @maximum_growth_kib do
        raise "parser RSS grew by #{growth} KiB; limit is #{@maximum_growth_kib} KiB"
      end

      IO.puts("bounded_growth_kib=#{growth} limit_kib=#{@maximum_growth_kib}")
    after
      File.rm_rf(root)
    end
  end

  def run(_args), do: raise("expected no arguments or --parse ARCHIVE COUNT")

  defp build_archive(root, count) do
    bundle_dir = Path.join(root, "bundle-#{count}")
    archive_path = Path.join(root, "manifest-#{count}.tar.gz")
    File.mkdir_p!(bundle_dir)
    {assets, packages} = execution_packages(count)
    {:ok, graph} = Graph.build(assets)

    manifest =
      FavnTestSupport.with_manifest_contract(%{
        assets: assets,
        pipelines: [],
        schedules: [],
        graph: graph,
        metadata: %{}
      })

    {:ok, version} = Version.new(manifest)
    {:ok, publication} = Publication.from_parts(version, packages)
    :ok = ManifestBuilder.write_bundle(bundle_dir, publication)
    largest_package_bytes = largest_execution_package_bytes(bundle_dir)
    {:ok, _archive} = ManifestArchive.write(bundle_dir, archive_path)
    {archive_path, largest_package_bytes}
  end

  defp largest_execution_package_bytes(bundle_dir) do
    bundle_dir
    |> Path.join("execution-packages/*.json")
    |> Path.wildcard()
    |> Enum.map(fn path -> File.stat!(path).size end)
    |> Enum.max()
  end

  defp execution_packages(count) do
    1..count
    |> Enum.map(fn index ->
      module = Module.concat(__MODULE__, "Asset#{index}")
      ref = {module, :asset}
      sql = large_first_sql(index)

      template =
        Template.compile!(sql,
          file: "scripts/measure_manifest_ingestion_memory.exs",
          line: index,
          module: module,
          scope: :query,
          enforce_query_root: true
        )

      {:ok, package} =
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

  defp large_first_sql(1),
    do: "SELECT 1 AS id /*" <> String.duplicate("x", @large_sql_bytes) <> "*/"

  defp large_first_sql(index), do: "SELECT #{index} AS id"

  defp measure_parser(archive_path, count) do
    mix = System.find_executable("mix") || raise "mix executable was not found"
    script = Path.expand(__ENV__.file)

    {output, status} =
      System.cmd(
        "/usr/bin/time",
        [
          "-f",
          "FAVN_MAX_RSS_KIB=%M",
          mix,
          "do",
          "--app",
          "favn_orchestrator",
          "cmd",
          "mix",
          "run",
          "--no-start",
          script,
          "--parse",
          archive_path,
          Integer.to_string(count)
        ],
        cd: Path.dirname(Path.dirname(script)),
        env: [{"MIX_ENV", "test"}, {"ERL_FLAGS", "+S 4:4"}],
        stderr_to_stdout: true
      )

    if status != 0 or not String.contains?(output, "FAVN_PARSED_PACKAGES=#{count}") do
      raise "measured parser failed with status #{status}:\n#{output}"
    end

    case Regex.run(~r/FAVN_MAX_RSS_KIB=(\d+)/, output, capture: :all_but_first) do
      [value] -> String.to_integer(value)
      _missing -> raise "measured parser did not report maximum RSS:\n#{output}"
    end
  end
end

FavnManifestIngestionMeasurement.run(System.argv())
