fixture_path =
  Path.expand(
    "../apps/favn_core/test/support/manifest_scalability_fixture.exs",
    __DIR__
  )

measurement_path =
  Path.expand(
    "../apps/favn_core/test/support/manifest_scalability_measurement.exs",
    __DIR__
  )

Code.require_file(fixture_path)
Code.require_file(measurement_path)

alias FavnTestSupport.ManifestScalabilityFixture
alias FavnTestSupport.ManifestScalabilityMeasurement

switches = [
  assets: :string,
  sql_columns: :integer,
  contract_columns: :integer,
  sample_interval_ms: :integer,
  timeout_ms: :integer,
  help: :boolean
]

{opts, positional, invalid} = OptionParser.parse(System.argv(), strict: switches)

usage = """
Usage:
  mix run scripts/measure_manifest_scalability.exs [options]

Options:
  --assets 66,300,1000       Asset counts to measure (default shown)
  --sql-columns N            SQL projection columns per asset
  --contract-columns N       Contract columns per asset
  --sample-interval-ms N     Peak-memory sample interval (default: 100)
  --timeout-ms N             Timeout for each asset count (default: 1200000)
  --help                     Show this message

The JSON report is written to stdout; progress is written to stderr.
"""

if opts[:help] do
  IO.write(usage)
  System.halt(0)
end

if positional != [] or invalid != [] do
  IO.puts(:stderr, usage)
  raise ArgumentError, "invalid arguments: #{inspect(positional ++ invalid)}"
end

asset_counts =
  opts
  |> Keyword.get(:assets, "66,300,1000")
  |> String.split(",", trim: true)
  |> Enum.map(fn value ->
    case Integer.parse(value) do
      {count, ""} when count > 0 -> count
      _other -> raise ArgumentError, "invalid positive asset count: #{inspect(value)}"
    end
  end)

measurement_opts =
  [
    progress: true,
    sql_columns:
      Keyword.get(opts, :sql_columns, ManifestScalabilityFixture.default_sql_columns()),
    contract_columns:
      Keyword.get(
        opts,
        :contract_columns,
        ManifestScalabilityFixture.default_contract_columns()
      )
  ]
  |> then(fn measurement_opts ->
    Enum.reduce([:sample_interval_ms, :timeout_ms], measurement_opts, fn key, acc ->
      case Keyword.fetch(opts, key) do
        {:ok, value} -> Keyword.put(acc, key, value)
        :error -> acc
      end
    end)
  end)

reports =
  Enum.map(asset_counts, fn asset_count ->
    IO.puts(:stderr, "Measuring SQL-heavy manifest with #{asset_count} assets...")
    ManifestScalabilityMeasurement.measure(asset_count, measurement_opts)
  end)

output = %{
  report_version: 1,
  measurement_note:
    "One isolated sample per size; peak memory is sampled process memory, not operating-system RSS.",
  reports: reports
}

IO.puts(Jason.encode!(output, pretty: true))
