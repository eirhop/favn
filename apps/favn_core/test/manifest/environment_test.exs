defmodule Favn.Manifest.EnvironmentTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Environment
  alias Favn.Manifest.Pipeline
  alias Favn.Triggers.Schedule
  alias Favn.Window.Policy

  test "uses a pure UTC fallback and parses an ISO coverage floor" do
    assert {:ok, environment} = Environment.new(coverage_scope: [from: "2026-07-01"])
    assert environment.default_timezone == "Etc/UTC"
    assert environment.default_timezone_source == :utc_fallback
    assert environment.coverage_scope == %{from: ~D[2026-07-01]}
  end

  test "records an application timezone separately from UTC fallback" do
    assert {:ok, environment} = Environment.new(default_timezone: "Europe/Oslo")
    assert environment.default_timezone == "Europe/Oslo"
    assert environment.default_timezone_source == :application_default
  end

  test "round-trips the exact persisted environment including a coverage date" do
    environment =
      Environment.new!(
        default_timezone: "Europe/Oslo",
        coverage_scope: [from: ~D[2026-07-01]]
      )

    assert Environment.to_map(environment) == %{
             "default_timezone" => "Europe/Oslo",
             "default_timezone_source" => "application_default",
             "coverage_scope" => %{"from" => "2026-07-01"}
           }

    assert {:ok, ^environment} =
             environment |> Environment.to_map() |> Environment.from_manifest()
  end

  test "rehydration validates structs instead of trusting their fields" do
    invalid =
      malformed_environment(
        default_timezone: "Invalid/Timezone",
        default_timezone_source: :application_default
      )

    assert {:error, {:invalid_timezone, "Invalid/Timezone"}} = Environment.from_manifest(invalid)

    assert {:error, :manifest_default_timezone_source_required} =
             Environment.from_manifest(
               malformed_environment(
                 default_timezone: "Etc/UTC",
                 default_timezone_source: nil
               )
             )

    assert {:error, {:invalid_manifest_timezone_source, "local"}} =
             Environment.from_manifest(
               malformed_environment(
                 default_timezone: "Etc/UTC",
                 default_timezone_source: "local"
               )
             )

    assert {:error, {:invalid_coverage_scope_from, :today}} =
             Environment.from_manifest(
               malformed_environment(
                 default_timezone: "Etc/UTC",
                 default_timezone_source: :utc_fallback,
                 coverage_scope: %{from: :today}
               )
             )
  end

  test "rehydration reports the exact missing or invalid persisted field" do
    assert {:error, :manifest_default_timezone_required} =
             Environment.from_manifest(%{"default_timezone_source" => "utc_fallback"})

    assert {:error, {:invalid_manifest_default_timezone, 123}} =
             Environment.from_manifest(%{
               "default_timezone" => 123,
               "default_timezone_source" => "utc_fallback"
             })

    assert {:error, :manifest_default_timezone_source_required} =
             Environment.from_manifest(%{"default_timezone" => "Etc/UTC"})
  end

  test "rejects invalid timezone and coverage scope configuration" do
    assert {:error, {:invalid_timezone, "Invalid/Timezone"}} =
             Environment.new(default_timezone: "Invalid/Timezone")

    assert {:error, {:unsupported_manifest_environment_key, :through}} =
             Environment.new(through: ~D[2026-01-01])

    assert {:error, {:unsupported_coverage_scope_keys, [:through]}} =
             Environment.new(coverage_scope: [from: ~D[2026-01-01], through: :current])
  end

  test "resolves schedule and pipeline timezones independently" do
    environment = Environment.new!(default_timezone: "Europe/Oslo")

    assert {:ok, schedule} =
             Schedule.new_inline(cron: "0 2 * * *", timezone: "America/New_York")

    pipeline =
      Pipeline.from_definition(
        %{
          module: MyApp.Pipeline,
          name: :daily,
          window: Policy.new!(:monthly, lookback: 1),
          schedule: {:inline, schedule}
        },
        environment
      )

    assert pipeline.window.timezone == "Europe/Oslo"
    assert pipeline.window.timezone_source == :application_default
    assert pipeline.window.lookback == 1
    assert {:inline, manifest_schedule} = pipeline.schedule
    assert manifest_schedule.timezone == "America/New_York"
    assert manifest_schedule.timezone_source == :local
  end

  defp malformed_environment(fields), do: struct!(Environment, fields)
end
