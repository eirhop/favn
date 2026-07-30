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
      filters = RunsFilters.from_params(%{"range" => "??", "status" => "??", "order" => "??"})

      assert filters.range == :today
      assert filters.status == :any
      assert filters.order == :started_desc
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

  describe "status_filters" do
    test "the status axis is four choices, each carrying its own count" do
      filters = RunsFilters.from_params(%{})

      statuses =
        RunsFilters.status_filters(filters, %{active: 2, failed: 1, succeeded: 6, total: 9})

      assert Enum.map(statuses, & &1.id) == [:active, :failed, :succeeded, :any]
      assert Enum.map(statuses, & &1.count) == [2, 1, 6, 9]
      assert statuses |> Enum.filter(& &1.active?) |> Enum.map(& &1.id) == [:any]
    end

    test "a status keeps every other axis, because the two do not write to each other" do
      filters = RunsFilters.from_params(%{"q" => "crm", "range" => "month"})
      failed = Enum.find(RunsFilters.status_filters(filters, nil), &(&1.id == :failed))

      assert {"q", "crm"} in failed.params
      assert {"range", "month"} in failed.params
      assert {"status", "failed"} in failed.params
    end

    test "the dates survive a status change" do
      filters = RunsFilters.from_params(%{"from" => "2026-07-01", "to" => "2026-07-15"})
      active = Enum.find(RunsFilters.status_filters(filters, nil), &(&1.id == :active))

      assert {"from", "2026-07-01"} in active.params
      assert {"to", "2026-07-15"} in active.params
    end

    test "counts are absent rather than zero when they could not be read" do
      assert RunsFilters.status_filters(%RunsFilters{}, nil) |> Enum.all?(&is_nil(&1.count))
    end
  end

  describe "count_filters" do
    test "they narrow exactly as the page read does, minus the status" do
      filters = RunsFilters.from_params(%{"status" => "failed", "q" => "crm", "range" => "week"})

      count_filters = RunsFilters.count_filters(filters, @now)
      store_filters = RunsFilters.store_filters(filters, @now)

      refute Keyword.has_key?(count_filters, :status)
      refute Keyword.has_key?(count_filters, :limit)
      refute Keyword.has_key?(count_filters, :order)

      assert Keyword.get(count_filters, :search) == Keyword.get(store_filters, :search)

      assert Keyword.get(count_filters, :started_after) ==
               Keyword.get(store_filters, :started_after)
    end

    test "an unfiltered default asks for nothing but the day" do
      assert RunsFilters.count_filters(%RunsFilters{}, @now) == [
               started_after: ~U[2026-07-30 00:00:00Z]
             ]
    end
  end

  describe "adjusted?" do
    test "only the controls behind the disclosure count" do
      refute RunsFilters.adjusted?(%RunsFilters{})
      refute RunsFilters.adjusted?(%RunsFilters{status: :failed})
      assert RunsFilters.adjusted?(%RunsFilters{search: "crm"})
      assert RunsFilters.adjusted?(%RunsFilters{range: :all})
    end
  end

  describe "paging" do
    test "the next page is the last row's sort key, and it round-trips through the URL" do
      filters = RunsFilters.from_params(%{"range" => "month"})
      paged = RunsFilters.next_page(filters, ~U[2026-07-11 08:30:00Z], "run_crm_daily_9f2")

      assert RunsFilters.paged?(paged)
      assert {"after", "2026-07-11T08:30:00Z|run_crm_daily_9f2"} in RunsFilters.to_params(paged)

      assert paged
             |> RunsFilters.to_params()
             |> Map.new()
             |> RunsFilters.from_params() == paged
    end

    test "the cursor reaches the store, and one page is always one page" do
      filters =
        RunsFilters.next_page(%RunsFilters{}, ~U[2026-07-11 08:30:00Z], "run_crm_daily_9f2")

      store_filters = RunsFilters.store_filters(filters, @now)

      assert Keyword.get(store_filters, :limit) == RunsFilters.page_size()

      assert Keyword.get(store_filters, :after) == %{
               started_at: ~U[2026-07-11 08:30:00Z],
               root_run_id: "run_crm_daily_9f2"
             }
    end

    test "the counts ignore the page, because they are of the whole filtered set" do
      filters = RunsFilters.next_page(%RunsFilters{}, ~U[2026-07-11 08:30:00Z], "run_9f2")
      refute Keyword.has_key?(RunsFilters.count_filters(filters, @now), :after)
    end

    test "a row with no start instant cannot be paged from" do
      refute RunsFilters.paged?(RunsFilters.next_page(%RunsFilters{}, nil, "run_9f2"))
    end

    test "a hand-edited cursor falls back to the first page rather than failing" do
      for value <- ["", "not-a-date|run_1", "2026-07-11T08:30:00Z", "2026-07-11T08:30:00Z|"] do
        refute RunsFilters.paged?(RunsFilters.from_params(%{"after" => value}))
      end
    end

    test "every other control returns to the first page" do
      paged = RunsFilters.next_page(%RunsFilters{}, ~U[2026-07-11 08:30:00Z], "run_9f2")

      refute RunsFilters.paged?(RunsFilters.first_page(paged))
      refute RunsFilters.paged?(RunsFilters.toggle_order(paged))
      refute RunsFilters.paged?(RunsFilters.change(paged, %{"range" => "week"}))

      assert paged
             |> RunsFilters.status_filters(nil)
             |> Enum.all?(&(not Enum.any?(&1.params, fn {key, _value} -> key == "after" end)))
    end
  end
end
