defmodule FavnView.RunsFiltersTest do
  use ExUnit.Case, async: true

  alias FavnView.RunsFilters

  doctest RunsFilters

  @now ~U[2026-07-30 14:12:00Z]

  describe "params" do
    test "an empty query string means today" do
      assert %RunsFilters{range: :today, status: :any, search: "", order: :started_desc} =
               RunsFilters.from_params(%{})
    end

    test "defaults are left out of the query string" do
      assert RunsFilters.to_params(%RunsFilters{}) == []
    end

    test "a filtered list round-trips through its own params" do
      filters = RunsFilters.from_params(%{"range" => "month", "status" => "failed", "q" => "crm"})

      assert filters |> RunsFilters.to_params() |> Map.new() |> RunsFilters.from_params() ==
               filters
    end

    test "dates only appear in the query string while the range is custom" do
      custom = RunsFilters.from_params(%{"from" => "2026-07-01", "to" => "2026-07-31"})
      assert {"from", "2026-07-01"} in RunsFilters.to_params(custom)

      week = %{custom | range: :week}
      refute Enum.any?(RunsFilters.to_params(week), &match?({"from", _}, &1))
    end

    test "a hand-edited param falls back rather than failing the page" do
      filters = RunsFilters.from_params(%{"range" => "??", "status" => "??", "limit" => "-3"})

      assert filters.range == :today
      assert filters.status == :any
      assert filters.limit == 50
    end
  end

  describe "change" do
    test "editing a date switches the range to a custom one" do
      filters = RunsFilters.from_params(%{"range" => "week"})
      changed = RunsFilters.change(filters, %{"from" => "2026-07-01"})

      assert changed.range == :custom
      assert changed.from == ~D[2026-07-01]
    end

    test "picking a named range clears the dates" do
      filters = RunsFilters.from_params(%{"from" => "2026-07-01", "to" => "2026-07-31"})
      changed = RunsFilters.change(filters, %{"range" => "today"})

      assert changed.range == :today
      assert RunsFilters.to_params(changed) == []
    end

    test "narrowing resets a page that had been grown" do
      filters = RunsFilters.from_params(%{"limit" => "200"})
      assert RunsFilters.change(filters, %{"q" => "orders"}).limit == 50
    end

    test "a search survives changing the range" do
      filters = RunsFilters.from_params(%{"q" => "orders"})
      assert RunsFilters.change(filters, %{"range" => "all"}).search == "orders"
    end
  end

  describe "window" do
    test "each named range bounds the same instant the list sorts by" do
      for {range, expected} <- [
            {"hour", ~U[2026-07-30 13:12:00Z]},
            {"today", ~U[2026-07-30 00:00:00Z]},
            {"week", ~U[2026-07-24 00:00:00Z]},
            {"month", ~U[2026-07-01 00:00:00Z]}
          ] do
        filters = RunsFilters.from_params(%{"range" => range})
        assert RunsFilters.window(filters, @now) == {expected, nil}
      end
    end

    test "a custom upper bound is exclusive, so the last day is included" do
      filters = RunsFilters.from_params(%{"from" => "2026-07-27", "to" => "2026-07-27"})

      assert RunsFilters.window(filters, @now) ==
               {~U[2026-07-27 00:00:00Z], ~U[2026-07-28 00:00:00Z]}
    end
  end

  describe "store_filters" do
    test "\"running or queued\" is one status filter, not two reads" do
      filters = RunsFilters.from_params(%{"status" => "active", "range" => "all"})

      assert Keyword.get(RunsFilters.store_filters(filters, @now), :status) == [
               :pending,
               :running
             ]
    end

    test "a blank search is not a filter" do
      filters = RunsFilters.from_params(%{"q" => "   "})
      refute Keyword.has_key?(RunsFilters.store_filters(filters, @now), :search)
    end
  end

  describe "presets" do
    test "each question is one pair of a range and a status" do
      filters = RunsFilters.from_params(%{})

      presets =
        RunsFilters.presets(filters, %{active: 2, failed_since: 1, started_since: 6, total: 9})

      assert Enum.map(presets, & &1.id) == [:running, :failed_today, :today, :all]
      assert Enum.map(presets, & &1.count) == [2, 1, 6, 9]
      assert Enum.filter(presets, & &1.active?) |> Enum.map(& &1.id) == [:today]
    end

    test "a preset keeps the current search, because it narrows rather than resets" do
      filters = RunsFilters.from_params(%{"q" => "crm"})
      running = Enum.find(RunsFilters.presets(filters, nil), &(&1.id == :running))

      assert {"q", "crm"} in running.params
      assert {"status", "active"} in running.params
    end

    test "counts are absent rather than zero when they could not be read" do
      assert RunsFilters.presets(%RunsFilters{}, nil) |> Enum.all?(&is_nil(&1.count))
    end
  end

  describe "growth" do
    test "a page grows by one step until it reaches the store's ceiling" do
      filters = %RunsFilters{limit: 450}
      assert RunsFilters.growth(filters) == 50

      grown = RunsFilters.grow(filters)
      assert grown.limit == 500
      assert RunsFilters.growth(grown) == nil
    end
  end
end
