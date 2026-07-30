defmodule FavnView.RunsListLiveTest do
  use ExUnit.Case, async: false

  import Phoenix.LiveViewTest

  alias FavnView.RunsListLive

  @counts %{active: 2, failed: 1, succeeded: 6, total: 148}

  setup do
    previous_page = Application.get_env(:favn_view, :page_execution_groups_fun)
    previous_counts = Application.get_env(:favn_view, :count_execution_groups_fun)

    on_exit(fn ->
      restore_env(:page_execution_groups_fun, previous_page)
      restore_env(:count_execution_groups_fun, previous_counts)
    end)

    :ok
  end

  test "projects a page of runs into rows a table can render" do
    stub_page([execution_group()])

    assigns = load(%{}).assigns

    assert {:flat, [run]} = assigns.listing
    assert run.id == "run-1"
    assert run.status == :running
    assert run.status_label == "Running"
    assert run.target == "orders"
    assert run.target_title == "MyApp.Assets.Orders.orders"
    assert run.assets == "3 / 4 assets"
    assert run.assets_failed == 1
    refute assigns.more?
  end

  # The group's own counters count runs, which is one for everything but a backfill.
  # The row reports asset steps, so a fourteen-asset pipeline run says fourteen.
  test "a row counts asset steps, not the runs that submitted them" do
    group =
      Map.merge(execution_group(), %{
        asset_counts: %{total: 14, completed: 14, failed: 0, running: 0, queued: 0}
      })

    stub_page([group])

    assert {:flat, [run]} = load(%{}).assigns.listing
    assert run.assets == "14 assets"
    assert run.assets_failed == 0
  end

  test "a run with no asset steps yet reports the plan instead of a fraction" do
    group =
      Map.merge(execution_group(), %{
        asset_counts: %{total: 0, completed: 0, failed: 0, running: 0, queued: 0}
      })

    stub_page([group])

    assert {:flat, [run]} = load(%{}).assigns.listing
    assert run.assets == nil
    assert run.target_detail == nil
  end

  test "each row carries its own date, so a page reached by paging back is readable" do
    stub_page([execution_group("run-1", ~U[2026-07-11 08:30:00Z])])

    assert [run] = load(%{"range" => "month"}).assigns.runs
    assert run.started_at == "08:30:00"
    assert run.started_on == "11 Jul"
  end

  # The row reports the duration the projection measured. It used to derive a span
  # from `last_activity_at` for multi-run groups, to work around a projection that
  # called a backfill finished the instant it was submitted; that is fixed at the
  # source now, and `apps/favn_storage_postgres` owns the test for it.
  test "a group's duration is the one the projection measured" do
    started_at = ~U[2026-07-30 09:00:00Z]

    group =
      Map.merge(execution_group(), %{
        status: :ok,
        started_at: started_at,
        finished_at: DateTime.add(started_at, 154, :second),
        duration_ms: 154_000,
        last_activity_at: DateTime.add(started_at, 154, :second)
      })

    stub_page([group])

    assert {:flat, [run]} = load(%{}).assigns.listing
    assert run.duration == "154.0 s"
  end

  test "a group still going reports elapsed rather than a duration it does not have" do
    stub_page([Map.merge(execution_group(), %{status: :running, duration_ms: nil})])

    assert {:flat, [run]} = load(%{}).assigns.listing
    assert run.duration == "elapsed"
  end

  test "a pipeline run is named by its pipeline, not by the first of its assets" do
    group = %{
      execution_group()
      | target_assets: Enum.map(1..14, &"MyApp.Assets.Landing.asset_#{&1}"),
        target_pipelines: ["MyApp.Pipelines.CrmDaily.crm_daily"]
    }

    stub_page([group])

    assert {:flat, [run]} = load(%{}).assigns.listing
    assert run.target == "crm_daily"
    assert run.target_detail == "14 assets"
  end

  test "a run with no declared target still names its column" do
    stub_page([%{execution_group() | target_assets: []}])

    assert render_component(&RunsListLive.render/1, load(%{}).assigns) =~ "No target"
  end

  test "the filters an operator can reach are the filters the store is asked for" do
    stub_page([], fn opts -> send(self(), {:filters, opts}) end)

    load(%{"status" => "failed", "q" => "orders", "range" => "all", "order" => "started_asc"})

    assert_received {:filters, opts}
    assert Keyword.get(opts, :status) == :failed
    assert Keyword.get(opts, :search) == "orders"
    assert Keyword.get(opts, :order) == :started_asc
    refute Keyword.has_key?(opts, :started_after)
  end

  test "the status buttons count the store rather than the loaded page" do
    stub_page([execution_group()])

    html = render_component(&RunsListLive.render/1, load(%{}).assigns)

    assert html =~ "Running"
    assert html =~ "Succeeded"
    assert html =~ "148"
  end

  test "the counts are narrowed the same way the list is, minus the status" do
    stub_page([])

    Application.put_env(:favn_view, :count_execution_groups_fun, fn _context, opts ->
      send(self(), {:count_filters, opts})
      {:ok, @counts}
    end)

    load(%{"status" => "failed", "q" => "orders", "range" => "week"})

    assert_received {:count_filters, opts}
    assert Keyword.get(opts, :search) == "orders"
    assert %DateTime{} = Keyword.get(opts, :started_after)
    refute Keyword.has_key?(opts, :status)
  end

  test "the filter disclosure opens and closes without reloading the list" do
    stub_page([execution_group()], fn _opts -> send(self(), :page_read) end)

    socket = load(%{})
    assert_received :page_read
    refute socket.assigns.filters_open?

    assert {:noreply, opened} = RunsListLive.handle_event("toggle_filters", %{}, socket)
    assert opened.assigns.filters_open?
    refute_received :page_read

    assert {:noreply, closed} = RunsListLive.handle_event("toggle_filters", %{}, opened)
    refute closed.assigns.filters_open?
  end

  # A row and a card of the same run were both on screen on a narrow viewport,
  # because `hidden` and the table display utility collided on one element. Each
  # list belongs inside a container that hides it, not on an element whose own
  # display class fights the one hiding it.
  test "a narrow screen renders each run once, not as a row and a card" do
    stub_page([execution_group()])

    html = render_component(&RunsListLive.render/1, load(%{}).assigns)

    assert html =~ ~r|<div class="hidden lg:block">\s*<table|
    assert html =~ ~r/class="[^"]*lg:hidden"[^>]*data-testid="runs-card-list"/
    refute html =~ ~r/<table[^>]*\bhidden\b/
  end

  test "a range covering several days grows day headers, and names the empty ones" do
    now = DateTime.utc_now()
    today = execution_group("run-today", DateTime.add(now, -3600, :second))
    older = execution_group("run-older", DateTime.add(now, -4 * 86_400, :second))

    stub_page([today, older])

    assert {:days, entries} = load(%{"range" => "week"}).assigns.listing
    assert Enum.map(entries, & &1.kind) == [:day, :gap, :day, :gap]
    assert hd(entries).label == "Today"

    html = render_component(&RunsListLive.render/1, load(%{"range" => "week"}).assigns)
    assert html =~ "no runs"
  end

  test "a truncated page does not claim the days it never reached were empty" do
    now = DateTime.utc_now()
    today = execution_group("run-today", DateTime.add(now, -3600, :second))

    Application.put_env(:favn_view, :page_execution_groups_fun, fn _context, _opts ->
      {:ok, %{items: [today], has_more?: true}}
    end)

    stub_counts()

    socket = load(%{"range" => "week"})

    assert socket.assigns.more?
    assert {:flat, [_run]} = socket.assigns.listing
  end

  test "the next page starts after the last row on this one" do
    now = DateTime.utc_now()
    older = DateTime.add(now, -7200, :second)

    stub_page([execution_group("run-newer", now), execution_group("run-older", older)])

    socket = load(%{"range" => "week"})

    assert {:noreply, socket} = RunsListLive.handle_event("next_page", %{}, socket)
    assert {:live, :patch, %{to: path}} = socket.redirected

    assert path =~ "after=" <> URI.encode_www_form("#{DateTime.to_iso8601(older)}|run-older")
  end

  test "the cursor reaches the store instead of a larger page" do
    stub_page([], fn opts -> send(self(), {:filters, opts}) end)

    load(%{"after" => "2026-07-11T08:30:00Z|run-older"})

    assert_received {:filters, opts}

    assert Keyword.get(opts, :after) == %{
             started_at: ~U[2026-07-11 08:30:00Z],
             root_run_id: "run-older"
           }

    assert Keyword.get(opts, :limit) == 50
  end

  # A later page covers part of the range at both ends, so it must not enumerate
  # days it never reached — including the days between its first row and today.
  test "a later page never claims a day outside it was empty" do
    now = DateTime.utc_now()
    older = DateTime.add(now, -5 * 86_400, :second)

    stub_page([execution_group("run-older", older)])

    socket = load(%{"range" => "month", "after" => "2026-07-11T08:30:00Z|run-newer"})

    assert {:flat, [_run]} = socket.assigns.listing
  end

  test "a backend failure renders an error state rather than an empty list" do
    Application.put_env(:favn_view, :page_execution_groups_fun, fn _context, _opts ->
      {:error, :unavailable}
    end)

    stub_counts()

    assigns = load(%{}).assigns

    assert assigns.error == "Backend unavailable"
    assert render_component(&RunsListLive.render/1, assigns) =~ "Could not load runs"
  end

  defp load(params) do
    socket = %Phoenix.LiveView.Socket{
      assigns: %{
        __changed__: %{},
        current_scope: %{operator_context: :operator_context}
      }
    }

    assert {:ok, socket} = RunsListLive.mount(%{}, %{}, socket)
    assert {:noreply, socket} = RunsListLive.handle_params(params, "/runs", socket)
    socket
  end

  defp stub_page(items, spy \\ fn _opts -> :ok end) do
    Application.put_env(:favn_view, :page_execution_groups_fun, fn _context, opts ->
      spy.(opts)
      {:ok, %{items: items, has_more?: false}}
    end)

    stub_counts()
  end

  defp stub_counts do
    Application.put_env(:favn_view, :count_execution_groups_fun, fn _context, _opts ->
      {:ok, @counts}
    end)
  end

  defp execution_group(id \\ "run-1", started_at \\ nil) do
    counts = %{total: 1, completed: 0, failed: 0, running: 1, queued: 0}

    %{
      id: id,
      root_execution_group_id: id,
      status: :running,
      health: :active,
      active?: true,
      trigger_type: :schedule,
      target_assets: ["MyApp.Assets.Orders.orders"],
      target_pipelines: [],
      root_status: :running,
      started_at: started_at || DateTime.utc_now(),
      finished_at: nil,
      duration_ms: nil,
      total_windows: 0,
      completed_windows: 0,
      failed_windows: 0,
      total_asset_attempts: 1,
      completed_asset_attempts: 0,
      failed_asset_attempts: 0,
      running_asset_attempts: 1,
      queued_asset_attempts: 0,
      failure_count: 0,
      progress: %{unit: :assets, label: "0 / 1 asset attempts", counts: counts},
      summary_totals: %{
        windows: %{total: 0, completed: 0, failed: 0},
        asset_attempts: counts
      },
      asset_counts: %{total: 4, completed: 3, failed: 1, running: 1, queued: 0},
      last_activity_at: DateTime.utc_now(),
      currently_running_asset_attempts: [],
      child_run_ids: []
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_view, key)
  defp restore_env(key, value), do: Application.put_env(:favn_view, key, value)
end
