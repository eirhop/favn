defmodule FavnView.RunDaysTest do
  use ExUnit.Case, async: true

  alias FavnView.RunDays, as: Subject

  doctest Subject

  defmodule RunDays do
    def layout(runs, window, now, opts \\ []) do
      FavnView.RunDays.layout(runs, window, now, Keyword.put_new(opts, :timezone, "Etc/UTC"))
    end
  end

  @now ~U[2026-07-30 14:12:00Z]

  defp run(offset_days, status \\ :ok) do
    %{
      id: "run-#{offset_days}-#{status}",
      started_at_raw: DateTime.add(@now, -offset_days * 86_400, :second),
      raw_status: status
    }
  end

  defp window(days), do: {DateTime.add(@now, -days * 86_400, :second), nil}

  test "one day needs no headers" do
    assert {:flat, [_run]} = RunDays.layout([run(0)], window(0), @now)
  end

  test "a bounded range shows every day in it, collapsing the empty stretches" do
    assert {:days, entries} = RunDays.layout([run(0), run(3)], window(4), @now)

    assert Enum.map(entries, & &1.kind) == [:day, :gap, :day, :gap]
    assert Enum.map(entries, & &1.label) == ["Today", "29 Jul to 28 Jul", "Mon 27 Jul", "26 Jul"]
    assert Enum.map(entries, &Map.get(&1, :days)) == [nil, 2, nil, 1]
  end

  test "a single empty day is named rather than described as a range" do
    assert {:days, [_today, gap, _older]} = RunDays.layout([run(0), run(2)], window(2), @now)
    assert %{kind: :gap, days: 1, label: "29 Jul"} = gap
  end

  test "an unbounded range only reports the days that hold runs" do
    assert {:days, days} = RunDays.layout([run(0), run(9)], {nil, nil}, @now)

    assert length(days) == 2
    assert Enum.all?(days, &(&1.total == 1))
  end

  test "each day says how much of it failed and how much is still going" do
    runs = [run(1, :ok), run(1, :error), run(1, :running)]

    assert {:days, days} = RunDays.layout(runs, window(2), @now)
    assert %{label: "Yesterday", total: 3, failed: 1, active: 1} = Enum.at(days, 1)
  end

  test "ascending order puts the oldest day first" do
    assert {:days, days} = RunDays.layout([run(0), run(2)], window(2), @now, order: :started_asc)
    assert Enum.map(days, & &1.label) |> List.last() == "Today"
  end

  test "an incomplete page stops at the oldest run it loaded" do
    assert {:days, entries} =
             RunDays.layout([run(0), run(2)], window(30), @now, complete?: false)

    assert Enum.map(entries, & &1.kind) == [:day, :gap, :day]
    assert Enum.map(entries, &Map.get(&1, :total)) == [1, nil, 1]
  end

  test "a very long range falls back to the days that hold runs" do
    assert {:days, days} = RunDays.layout([run(0), run(100)], window(400), @now)
    assert length(days) == 2
  end

  test "a run that never started sorts after every day that did" do
    unstarted = %{id: "queued", started_at_raw: nil, raw_status: :pending}

    assert {:days, days} = RunDays.layout([run(0), unstarted], window(1), @now)
    assert List.last(days).label == "Not started"
  end
end
