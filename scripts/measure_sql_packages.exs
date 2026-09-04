# Run from the umbrella root with:
# MIX_ENV=test mise exec -- mix do --app favn_core cmd mix run --no-start ../../scripts/measure_sql_packages.exs
# Add --baseline-ref <commit> to load that commit's parser/package implementation
# in this isolated process. No checkout or build files are changed by the script.
{opts, [], []} =
  OptionParser.parse(System.argv(), strict: [baseline_ref: :string, output: :string])

root = Path.expand("..", __DIR__)

if baseline = opts[:baseline_ref] do
  Code.compiler_options(ignore_module_conflict: true)

  for path <- ["sql/template.ex", "manifest/execution_package.ex"] do
    source_path = "apps/favn_core/lib/favn/" <> path
    {source, 0} = System.cmd("git", ["show", baseline <> ":" <> source_path], cd: root)
    Code.compile_string(source, source_path)
  end
end

Code.require_file("apps/favn_core/test/support/sql_package_fixture.exs", root)

defmodule SQLPackageMeasurement do
  alias Favn.Manifest.{ExecutionPackage, Serializer}
  alias Favn.SQL.Template.{Span, Text}
  alias FavnTestSupport.SQLPackageFixture, as: Fixture

  def measure(columns, mode, indent \\ 4) do
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        {build_us, package} = :timer.tc(fn -> Fixture.package(columns, mode, indent) end)
        :erlang.garbage_collect()
        {:memory, retained} = Process.info(self(), :memory)
        {:binary, binaries} = Process.info(self(), :binary)
        binary_bytes = Enum.sum(Enum.map(binaries, &elem(&1, 1)))
        json = Serializer.encode_manifest!(package)
        term = :erlang.term_to_binary(package, [:deterministic])
        work_term = :erlang.term_to_binary(Fixture.work(package), [:deterministic])
        {:ok, ^package} = ExecutionPackage.verify(package)
        {:ok, ^package} = json |> Jason.decode!() |> ExecutionPackage.from_published()

        timings =
          for {name, fun} <- [
                json_encode: fn -> Serializer.encode_manifest!(package) end,
                json_decode: fn -> Jason.decode!(json) end,
                term_encode: fn -> :erlang.term_to_binary(package, [:deterministic]) end,
                term_decode: fn -> :erlang.binary_to_term(term, [:safe]) end,
                verify: fn -> {:ok, ^package} = ExecutionPackage.verify(package) end,
                load_verify: fn ->
                  {:ok, ^package} = json |> Jason.decode!() |> ExecutionPackage.from_published()
                end
              ],
              into: %{},
              do: {name, median_us(fun)}

        send(
          parent,
          {:measurement, self(),
           %{
             columns: columns,
             mode: mode,
             indent: indent,
             package_schema: package.schema_version,
             source_bytes: Fixture.source_bytes(package),
             json_bytes: byte_size(json),
             term_bytes: byte_size(term),
             work_term_bytes: byte_size(work_term),
             task_overhead_bytes: byte_size(work_term) - byte_size(term),
             base64_work_bytes: byte_size(Base.encode64(work_term)),
             gzip_json_bytes: byte_size(:zlib.gzip(json)),
             text_nodes: Fixture.count(package, Text),
             spans: Fixture.count(package, Span),
             flat_term_words: :erts_debug.flat_size(package),
             retained_process_bytes: retained,
             retained_binary_bytes: binary_bytes,
             build_us: build_us,
             median_us: timings
           }}
        )
      end)

    collect(pid, ref, 0)
  end

  def crossing(mode) do
    upper = grow(mode, 10)
    {below, above} = bisect(mode, 1, upper)
    %{below: boundary(mode, below), above: boundary(mode, above)}
  end

  defp grow(mode, count) when count <= 20_000 do
    if package_bytes(mode, count) >= 1_048_576,
      do: count,
      else: grow(mode, min(count * 2, 20_001))
  end

  defp grow(_mode, _count), do: raise("1 MiB crossing not found inside the fixture bound")

  defp bisect(_mode, low, high) when high - low == 1, do: {low, high}

  defp bisect(mode, low, high) do
    midpoint = div(low + high, 2)

    if package_bytes(mode, midpoint) < 1_048_576,
      do: bisect(mode, midpoint, high),
      else: bisect(mode, low, midpoint)
  end

  defp package_bytes(mode, count) do
    bytes =
      Fixture.package(count, mode) |> :erlang.term_to_binary([:deterministic]) |> byte_size()

    :erlang.garbage_collect()
    bytes
  end

  defp boundary(mode, count) do
    package = Fixture.package(count, mode)

    %{
      columns: count,
      source_bytes: Fixture.source_bytes(package),
      term_bytes: byte_size(:erlang.term_to_binary(package, [:deterministic]))
    }
  end

  defp median_us(fun) do
    fun.()

    samples =
      for _ <- 1..5 do
        :erlang.garbage_collect()
        {elapsed, _} = :timer.tc(fun)
        elapsed
      end

    samples |> Enum.sort() |> Enum.at(2)
  end

  defp collect(pid, ref, peak) do
    receive do
      {:measurement, ^pid, result} ->
        Process.demonitor(ref, [:flush])
        Map.put(result, :sampled_peak_process_bytes, max(peak, result.retained_process_bytes))

      {:DOWN, ^ref, :process, ^pid, reason} ->
        raise "measurement process failed: #{inspect(reason)}"
    after
      2 ->
        bytes =
          case Process.info(pid, :memory) do
            {:memory, bytes} -> bytes
            nil -> 0
          end

        collect(pid, ref, max(peak, bytes))
    end
  end
end

samples =
  for {columns, mode, indent} <- [
        {10, :ordinary, 4},
        {100, :ordinary, 4},
        {1_000, :ordinary, 4},
        {5_000, :ordinary, 4},
        {100, :ordinary, 40},
        {100, :rich, 4},
        {1_000, :dynamic, 4}
      ] do
    SQLPackageMeasurement.measure(columns, mode, indent)
  end

report =
  Jason.encode!(
    %{
      baseline_ref: opts[:baseline_ref],
      samples: samples,
      one_mib_crossings: %{
        ordinary: SQLPackageMeasurement.crossing(:ordinary),
        dynamic: SQLPackageMeasurement.crossing(:dynamic)
      },
      note:
        "Generic fixtures. Source counts each SQL body once, including checks/helpers/scopes. Five warm samples per codec operation. Memory is isolated-process memory sampled every 2 ms through build and codec work, not RSS; peaks may be missed. Off-heap binaries reported separately at retained checkpoint. Flat words exclude off-heap binaries. Task fixture has modest metadata; arbitrary task metadata can be larger. No live database, dispatch, or SQL execution is measured."
    },
    pretty: true
  )

if output = opts[:output] do
  File.mkdir_p!(Path.dirname(output))
  File.write!(output, report <> "\n")
else
  IO.puts(report)
end
