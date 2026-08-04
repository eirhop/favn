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

  test "addresses a range by instants, floored to the period and clamped to the range" do
    coverage = effective(:month, from: ~D[2026-01-01], through: ~D[2026-05-01])

    assert {:ok, evaluation} = Expected.evaluate(coverage, ~U[2026-07-22 10:00:00Z])

    # Part-way through March starts at March. A caller may hand over the first of a
    # month, or any instant in it, without checking that the month exists.
    assert {:ok, middle} =
             Expected.page_between(evaluation, ~U[2026-03-17 09:30:00Z], ~U[2026-05-01 00:00:00Z])

    assert Enum.map(middle.items, & &1.start_at.month) == [3, 4]

    # The upper bound is exclusive, so the period starting on it belongs to the next
    # range and never appears twice when a caller walks unit by unit.
    assert {:ok, one} =
             Expected.page_between(evaluation, ~U[2026-03-01 00:00:00Z], ~U[2026-04-01 00:00:00Z])

    assert Enum.map(one.items, & &1.start_at.month) == [3]

    # No upper bound reads to the end of coverage.
    assert {:ok, rest} = Expected.page_between(evaluation, ~U[2026-03-01 00:00:00Z])
    assert Enum.map(rest.items, & &1.start_at.month) == [3, 4, 5]

    # Before the range is the start of it, so a jump backwards past the beginning lands
    # on the beginning rather than on nothing.
    assert {:ok, before} =
             Expected.page_between(evaluation, ~U[2020-01-01 00:00:00Z], ~U[2026-03-01 00:00:00Z])

    assert Enum.map(before.items, & &1.start_at.month) == [1, 2]

    # Past the end is empty, so a caller stepping forward can tell it ran out.
    assert {:ok, past} = Expected.page_between(evaluation, ~U[2030-01-01 00:00:00Z])
    assert past.items == []
    refute past.has_more?
  end

  # A calendar screen is one unit, and a unit is not a count: February holds 28 days,
  # a clock change makes a day hold 23 or 25 hours. Asking for "31 days from 1
  # February" returned three days of March, which the calendar then drew under a
  # February heading and let an operator select for backfill.
  test "a bounded range holds exactly the periods inside it, whatever the unit's length" do
    coverage = effective(:day, from: ~D[2026-01-01], through: ~D[2026-12-31])
    assert {:ok, evaluation} = Expected.evaluate(coverage, ~U[2027-01-15 10:00:00Z])

    for {from, until, expected_days} <- [
          {~D[2026-02-01], ~D[2026-03-01], 28},
          {~D[2026-04-01], ~D[2026-05-01], 30},
          {~D[2026-07-01], ~D[2026-08-01], 31}
        ] do
      assert {:ok, page} =
               Expected.page_between(evaluation, utc(from), utc(until))

      assert length(page.items) == expected_days
      assert Enum.all?(page.items, &(&1.start_at.month == from.month))
    end
  end

  test "an addressed range of an empty coverage range is empty rather than an error" do
    coverage = effective(:day, from: ~D[2026-07-01], through: :latest_closed)

    assert {:ok, evaluation} = Expected.evaluate(coverage, ~U[2026-06-01 00:00:00Z])
    assert evaluation.expected_count == 0

    assert {:ok, page} =
             Expected.page_between(evaluation, ~U[2026-07-01 00:00:00Z], ~U[2026-08-01 00:00:00Z])

    assert page.items == []
  end

  test "the safety cap truncates and says so, rather than walking a whole range" do
    coverage = effective(:day, from: ~D[2026-01-01], through: ~D[2026-12-31])
    assert {:ok, evaluation} = Expected.evaluate(coverage, ~U[2027-01-15 10:00:00Z])

    assert {:ok, capped} = Expected.page_between(evaluation, utc(~D[2026-01-01]), nil, 10)
    assert length(capped.items) == 10
    assert capped.has_more?
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

  defp utc(%Date{} = date),
    do: DateTime.new!(date, ~T[00:00:00], "Etc/UTC", Favn.Timezone.database!())

  defp effective(kind, opts) do
    coverage = Spec.new!(opts)
    window = WindowSpec.new!(kind, timezone: "Etc/UTC")
    {:ok, effective} = Effective.resolve(coverage, window, nil)
    effective
  end
end
