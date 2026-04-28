defmodule Favn.Backfill.RangeResolverTest do
  use ExUnit.Case, async: true

  alias Favn.Backfill.LookbackPolicy
  alias Favn.Backfill.RangeRequest
  alias Favn.Backfill.RangeResolver

  test "normalizes lookback policy values" do
    assert LookbackPolicy.normalize(:asset_default) == {:ok, :asset_default}
    assert LookbackPolicy.normalize("asset-default") == {:ok, :asset_default}
    assert LookbackPolicy.normalize("asset_default") == {:ok, :asset_default}
    assert LookbackPolicy.normalize(0) == {:ok, 0}
    assert LookbackPolicy.normalize(12) == {:ok, 12}
    assert LookbackPolicy.normalize("12") == {:ok, 12}

    assert LookbackPolicy.normalize(-1) == {:error, {:invalid_lookback_policy, -1}}
    assert LookbackPolicy.normalize("-1") == {:error, {:invalid_lookback_policy, "-1"}}
    assert LookbackPolicy.normalize(:bad) == {:error, {:invalid_lookback_policy, :bad}}
  end

  test "resolves explicit monthly range with inclusive request end" do
    assert {:ok, request} =
             RangeRequest.explicit(
               from: "2025-04",
               to: "2026-03",
               kind: :month,
               timezone: "Etc/UTC"
             )

    assert {:ok, resolved} = RangeResolver.resolve(request)

    assert resolved.kind == :month
    assert resolved.timezone == "Etc/UTC"
    assert resolved.requested_count == 12
    assert resolved.reference == nil
    assert length(resolved.anchors) == 12
    assert length(resolved.window_keys) == 12
    assert_same_instant(resolved.range_start_at, ~U[2025-04-01 00:00:00Z])
    assert_same_instant(resolved.range_end_at, ~U[2026-04-01 00:00:00Z])

    assert_same_instant(
      Enum.map(resolved.anchors, & &1.start_at) |> List.first(),
      ~U[2025-04-01 00:00:00Z]
    )

    assert_same_instant(
      Enum.map(resolved.anchors, & &1.start_at) |> List.last(),
      ~U[2026-03-01 00:00:00Z]
    )
  end

  test "resolves relative last 12 months from coverage_until baseline" do
    coverage_until = ~U[2026-04-15 12:30:00Z]

    assert {:ok, request} =
             RangeRequest.relative_last(
               last: {12, :month},
               baseline: %{coverage_until: coverage_until},
               timezone: "Etc/UTC"
             )

    assert {:ok, resolved} = RangeResolver.resolve(request)

    assert resolved.requested_count == 12
    assert resolved.reference == coverage_until
    assert length(resolved.anchors) == 12
    assert_same_instant(resolved.range_start_at, ~U[2025-05-01 00:00:00Z])
    assert_same_instant(resolved.range_end_at, ~U[2026-05-01 00:00:00Z])

    assert_same_instant(
      resolved.anchors |> List.first() |> Map.fetch!(:start_at),
      ~U[2025-05-01 00:00:00Z]
    )

    assert_same_instant(
      resolved.anchors |> List.last() |> Map.fetch!(:start_at),
      ~U[2026-04-01 00:00:00Z]
    )
  end

  test "relative range ending exactly on a period boundary does not include next period" do
    coverage_until = ~U[2026-04-01 00:00:00Z]

    assert {:ok, resolved} =
             RangeResolver.resolve(
               last: {2, :month},
               baseline: %{coverage_until: coverage_until},
               timezone: "Etc/UTC"
             )

    assert_same_instant(resolved.range_start_at, ~U[2026-02-01 00:00:00Z])
    assert_same_instant(resolved.range_end_at, ~U[2026-04-01 00:00:00Z])
  end

  test "validates timezone before resolving ranges" do
    assert {:error, {:invalid_timezone, "Definitely/NotAZone"}} =
             RangeRequest.explicit(
               from: "2025-04",
               to: "2026-03",
               kind: :month,
               timezone: "Definitely/NotAZone"
             )

    assert {:error, {:invalid_timezone, "Definitely/NotAZone"}} =
             RangeResolver.resolve(%{
               "from" => "2025-04",
               "to" => "2026-03",
               "kind" => :month,
               "timezone" => "Definitely/NotAZone"
             })
  end

  defp assert_same_instant(left, right) do
    assert DateTime.compare(left, right) == :eq
  end
end
