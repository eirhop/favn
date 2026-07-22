defmodule Favn.Coverage.ExpectedTest do
  use ExUnit.Case, async: true

  alias Favn.Coverage.{Effective, Expected, Spec, Summary}
  alias Favn.Window.Key
  alias Favn.Window.Spec, as: WindowSpec

  test "evaluates fixed monthly bounds and pages canonical windows" do
    coverage = effective(:month, from: ~D[2026-01-01], through: ~D[2026-05-01])

    assert {:ok, evaluation} = Expected.evaluate(coverage, ~U[2026-07-22 10:00:00Z])
    assert evaluation.expected_count == 5
    assert evaluation.first_window.start_at.month == 1
    assert evaluation.last_expected_window.start_at.month == 5
    assert byte_size(evaluation.checksum) == 64

    assert {:ok, first} = Expected.page(evaluation, nil, 2)
    assert Enum.map(first.items, & &1.start_at.month) == [1, 2]
    assert first.has_more?

    assert {:ok, second} = Expected.page(evaluation, first.next_after, 2)
    assert Enum.map(second.items, & &1.start_at.month) == [3, 4]

    assert {:ok, last} = Expected.page(evaluation, second.next_after, 2)
    assert Enum.map(last.items, & &1.start_at.month) == [5]
    refute last.has_more?
  end

  test "availability delay changes expectation only at the exact boundary" do
    coverage =
      effective(:day,
        from: ~D[2026-07-01],
        through: :latest_closed,
        availability_delay: {:hours, 6}
      )

    assert {:ok, before} = Expected.evaluate(coverage, ~U[2026-07-02 05:59:59Z])
    assert before.expected_count == 0

    for evaluated_at <- [~U[2026-07-02 06:00:00Z], ~U[2026-07-02 06:00:01Z]] do
      assert {:ok, available} = Expected.evaluate(coverage, evaluated_at)
      assert available.expected_count == 1
      assert available.last_expected_window.start_at == ~U[2026-07-01 00:00:00Z]
    end
  end

  test "rejects a page cursor that is not a canonical coverage boundary" do
    coverage = effective(:day, from: ~D[2026-07-01], through: ~D[2026-07-03])

    assert {:ok, evaluation} = Expected.evaluate(coverage, ~U[2026-07-22 10:00:00Z])

    forged_key = Key.new!(:day, ~U[2026-07-01 12:00:00Z], "Etc/UTC")

    assert {:error, :coverage_cursor_stale} = Expected.page(evaluation, forged_key, 1)
  end

  test "current includes the containing period and a scope floor may make coverage empty" do
    assert {:ok, current} =
             :month
             |> effective(from: ~D[2026-06-01], through: :current)
             |> Expected.evaluate(~U[2026-07-22 10:00:00Z])

    assert current.expected_count == 2
    assert current.last_expected_window.start_at.month == 7

    assert {:ok, empty_coverage} =
             Effective.resolve(
               Spec.new!(from: ~D[2026-01-01], through: ~D[2026-05-01]),
               WindowSpec.new!(:month, timezone: "Etc/UTC"),
               ~D[2026-07-01]
             )

    assert {:ok, empty} = Expected.evaluate(empty_coverage, ~U[2026-07-22 10:00:00Z])
    assert empty.expected_count == 0
    assert is_nil(empty.last_expected_window)
    assert {:ok, %{items: []}} = Expected.page(empty)
  end

  test "rejects evaluations above the expected-window safety limit" do
    first = ~U[2010-01-01 00:00:00Z]
    last = DateTime.add(first, Expected.max_windows(), :hour)
    coverage = effective(:hour, from: first, through: last)

    assert {:error, :coverage_window_limit_exceeded} =
             Expected.evaluate(coverage, ~U[2026-07-22 10:00:00Z])
  end

  test "summary validates complete, incomplete, and explicit unknown states" do
    base = %{
      evaluated_at: ~U[2026-07-22 10:00:00Z],
      manifest_version_id: "manifest",
      target_id: "asset:orders"
    }

    assert {:ok, %Summary{status: :unknown}} =
             Summary.new(
               Map.merge(base, %{status: :unknown, unknown_reason: :coverage_not_declared})
             )

    assert {:ok, %Summary{status: :complete}} =
             Summary.new(
               Map.merge(base, %{
                 status: :complete,
                 expected_count: 0,
                 covered_count: 0,
                 missing_count: 0,
                 evidence_generation_id: "ag_orders",
                 evaluation_checksum: String.duplicate("a", 64)
               })
             )

    assert {:error, :invalid_coverage_summary_counts} =
             Summary.new(
               Map.merge(base, %{
                 status: :incomplete,
                 expected_count: 2,
                 covered_count: 2,
                 missing_count: 1,
                 evidence_generation_id: "ag_orders",
                 evaluation_checksum: String.duplicate("a", 64)
               })
             )
  end

  defp effective(kind, opts) do
    coverage = Spec.new!(opts)
    window = WindowSpec.new!(kind, timezone: "Etc/UTC")
    {:ok, effective} = Effective.resolve(coverage, window, nil)
    effective
  end
end
