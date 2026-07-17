Code.require_file("../support/manifest_scalability_fixture.exs", __DIR__)
Code.require_file("../support/manifest_scalability_measurement.exs", __DIR__)

defmodule Favn.ManifestScalabilityMeasurementTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias FavnTestSupport.ManifestScalabilityFixture
  alias FavnTestSupport.ManifestScalabilityMeasurement

  test "SQL-heavy fixtures are deterministic valid manifests" do
    left = ManifestScalabilityFixture.build(3, sql_columns: 4, contract_columns: 2)
    right = ManifestScalabilityFixture.build(3, sql_columns: 4, contract_columns: 2)

    assert Serializer.encode_manifest!(left) == Serializer.encode_manifest!(right)
    assert {:ok, version} = Version.new(left)
    assert version.content_hash =~ ~r/^[0-9a-f]{64}$/
  end

  test "fixture rejects unbounded or inconsistent inputs" do
    assert_raise ArgumentError, ~r/asset_count must be in/, fn ->
      ManifestScalabilityFixture.build(0)
    end

    assert_raise ArgumentError, ~r/contract_columns must be in 1..4/, fn ->
      ManifestScalabilityFixture.build(1, sql_columns: 4, contract_columns: 5)
    end

    assert_raise ArgumentError, ~r/unknown manifest scalability option/, fn ->
      ManifestScalabilityFixture.build(1, unknown: true)
    end
  end

  test "measurement reports compression, costs, and representation attribution" do
    report =
      ManifestScalabilityMeasurement.measure(3,
        sql_columns: 4,
        contract_columns: 2,
        timeout_ms: 30_000
      )

    assert report.asset_count == 3
    assert report.sample_count == 1
    assert report.sizes.canonical_json_bytes > report.sizes.gzip_bytes
    assert report.sizes.gzip_ratio > 0
    assert report.sizes.gzip_ratio < 1
    assert report.sizes.decoded_flat_heap_bytes > 0
    assert report.operations.encode.duration_us > 0
    assert report.operations.decode.duration_us > 0
    assert report.attribution.asset_field_value_json_bytes["sql_execution"] > 0
    assert report.attribution.sql_execution_field_value_json_bytes["sql"] > 0
    assert report.attribution.template_field_value_json_bytes["nodes"] > 0
    assert report.attribution.check_field_value_json_bytes["sql"] > 0
  end
end
