defmodule Favn.Coverage.EffectiveTest do
  use ExUnit.Case, async: true

  alias Favn.Coverage.{Effective, Spec}
  alias Favn.Window.Spec, as: WindowSpec

  test "resolves declared and environment-effective monthly boundaries" do
    coverage = Spec.new!(from: ~D[2020-01-15], through: ~D[2020-12-20])

    window =
      WindowSpec.new!(:month, timezone: "Europe/Oslo")
      |> WindowSpec.with_declaration_source(:local)

    assert {:ok, effective} = Effective.resolve(coverage, window, ~D[2020-07-10])
    assert effective.declared_from.kind == :month
    assert effective.declared_from.start_at.month == 1
    assert effective.effective_from.start_at.month == 7
    assert effective.scope_source == :environment_floor
    assert effective.through.start_at.month == 12
    assert effective.timezone == "Europe/Oslo"
    assert effective.timezone_source == :local
  end

  test "authored hourly dates fail while the environment date floor is valid" do
    window = WindowSpec.new!(:hour, timezone: "Europe/Oslo")

    assert {:error, {:hourly_coverage_requires_datetime, ~D[2026-07-01]}} =
             Effective.resolve(Spec.new!(from: ~D[2026-07-01]), window, nil)

    coverage = Spec.new!(from: ~U[2026-06-30 00:00:00Z])
    assert {:ok, effective} = Effective.resolve(coverage, window, ~D[2026-07-01])
    assert effective.effective_from.start_at.hour == 0
    assert effective.effective_from.start_at.time_zone == "Europe/Oslo"
  end

  test "normalizes date boundaries for day and year windows" do
    daily = WindowSpec.new!(:day, timezone: "Europe/Oslo")
    yearly = WindowSpec.new!(:year, timezone: "Europe/Oslo")

    assert {:ok, spring} =
             Effective.resolve(
               Spec.new!(from: ~D[2026-03-29], through: ~D[2026-03-29]),
               daily,
               nil
             )

    assert DateTime.diff(spring.declared_from.end_at, spring.declared_from.start_at, :hour) == 23
    assert spring.through == spring.declared_from

    assert {:ok, annual} =
             Effective.resolve(
               Spec.new!(from: ~D[2026-06-01], through: ~D[2027-06-01]),
               yearly,
               nil
             )

    assert annual.declared_from.start_at.year == 2026
    assert annual.through.start_at.year == 2027
  end

  test "timezone-aware datetimes preserve distinct repeated DST hours" do
    window = WindowSpec.new!(:hour, timezone: "Europe/Oslo")

    assert {:ok, first} =
             Effective.resolve(Spec.new!(from: ~U[2026-10-25 00:30:00Z]), window, nil)

    assert {:ok, second} =
             Effective.resolve(Spec.new!(from: ~U[2026-10-25 01:30:00Z]), window, nil)

    assert first.declared_from.start_at ==
             DateTime.shift_zone!(
               ~U[2026-10-25 00:00:00Z],
               "Europe/Oslo",
               Favn.Timezone.database!()
             )

    assert second.declared_from.start_at ==
             DateTime.shift_zone!(
               ~U[2026-10-25 01:00:00Z],
               "Europe/Oslo",
               Favn.Timezone.database!()
             )

    refute first.declared_from == second.declared_from
  end

  test "environment floors earlier or equal remain declared and a later floor may empty a range" do
    window = WindowSpec.new!(:month, timezone: "Etc/UTC")
    coverage = Spec.new!(from: ~D[2026-03-15], through: ~D[2026-05-10])

    for floor <- [~D[2026-01-01], ~D[2026-03-01]] do
      assert {:ok, effective} = Effective.resolve(coverage, window, floor)
      assert effective.scope_source == :declared
      assert effective.effective_from == effective.declared_from
    end

    assert {:ok, empty} = Effective.resolve(coverage, window, ~D[2026-07-01])
    assert empty.scope_source == :environment_floor
    assert DateTime.compare(empty.effective_from.start_at, empty.through.start_at) == :gt
  end

  test "coverage requires a window" do
    assert {:error, :coverage_requires_window} =
             Effective.resolve(Spec.new!(from: ~D[2020-01-01]), nil, nil)
  end

  test "rehydration rejects noncanonical period boundaries" do
    window = WindowSpec.new!(:day, timezone: "Etc/UTC")
    assert {:ok, effective} = Effective.resolve(Spec.new!(from: ~D[2026-07-01]), window, nil)

    shifted = %{
      effective
      | declared_from: %{
          effective.declared_from
          | start_at: ~U[2026-07-01 06:00:00Z],
            end_at: ~U[2026-07-02 06:00:00Z]
        },
        effective_from: %{
          effective.effective_from
          | start_at: ~U[2026-07-01 06:00:00Z],
            end_at: ~U[2026-07-02 06:00:00Z]
        }
    }

    assert {:error, {:noncanonical_coverage_period_start, :declared_from, _start}} =
             Effective.validate(shifted)
  end
end
