defmodule FavnView.Dev.DesignSystem.Fixtures.RunsList do
  @moduledoc """
  Runs list sample data for the design system.

  These live under `dev/` because they are not product code. The page module used
  to carry its own `sample_*` functions inside `lib/`, which shipped invented runs
  to production so that a catalogue page could render.

  Days are anchored to a fixed instant rather than `now`, so a screenshot of the
  day-grouped table is the same image tomorrow.
  """

  alias FavnView.Components.RunsListPage
  alias FavnView.RunDays
  alias FavnView.RunsFilters

  @now ~U[2026-07-30 14:12:00Z]

  @doc "The instant every fixture is measured against."
  @spec now() :: DateTime.t()
  def now, do: @now

  @doc "Navigation items for a runs list example."
  @spec nav_items() :: list()
  def nav_items, do: RunsListPage.nav_items(:runs)

  @doc "Today's runs, flat: one day needs no day headers."
  @spec today() :: map()
  def today, do: attrs(RunsFilters.from_params(%{}), todays_runs())

  @doc """
  The failures-only status, which is the question the counts point at.

  The counts are still those of the whole day, because the status is the one axis
  they do not narrow — a count that dropped to what is on screen could never send
  the operator anywhere else.
  """
  @spec failed_today() :: map()
  def failed_today do
    filters = RunsFilters.from_params(%{"status" => "failed"})
    failures = Enum.filter(todays_runs(), &(&1.status == :failed))

    attrs(filters, failures, todays_runs())
  end

  @doc """
  A fortnight of runs for one pipeline, which is the shape that answers "did this
  run every day". The gaps are the point.
  """
  @spec month_with_gaps() :: map()
  def month_with_gaps do
    filters = RunsFilters.from_params(%{"range" => "month", "q" => "crm_daily"})
    attrs(filters, daily_runs())
  end

  @doc "A custom range, so the date inputs are on screen."
  @spec custom_range() :: map()
  def custom_range do
    filters = RunsFilters.from_params(%{"from" => "2026-07-27", "to" => "2026-07-30"})
    attrs(filters, Enum.take(daily_runs(), 4))
  end

  @doc "More runs than one page holds, so a page follows this one."
  @spec truncated() :: map()
  def truncated, do: %{today() | more?: true}

  @doc "A page reached from the one before it, with another behind it."
  @spec later_page() :: map()
  def later_page do
    filters =
      RunsFilters.next_page(
        RunsFilters.from_params(%{"range" => "month"}),
        DateTime.add(@now, -3 * 86_400, :second),
        "run_crm_daily_2003_11"
      )

    %{attrs(filters, Enum.drop(daily_runs(), 4)) | more?: true}
  end

  @doc "The narrow-screen filter disclosure, opened."
  @spec filters_open() :: map()
  def filters_open, do: %{today() | filters_open?: true}

  @doc "Nothing ran today, which is a real answer rather than an empty screen."
  @spec empty() :: map()
  def empty, do: attrs(RunsFilters.from_params(%{}), [])

  @doc "Nothing matches a search, which is a different answer from nothing ran."
  @spec no_matches() :: map()
  def no_matches, do: attrs(RunsFilters.from_params(%{"q" => "invoices"}), [])

  @doc "The store could not be read."
  @spec unavailable() :: map()
  def unavailable, do: %{empty() | error: "Backend unavailable"}

  defp attrs(filters, runs, counted \\ nil) do
    %{
      listing:
        RunDays.layout(runs, RunsFilters.window(filters, @now), @now,
          order: filters.order,
          complete?: true
        ),
      filters: filters,
      counts: counts(counted || runs),
      more?: false,
      filters_open?: false,
      error: nil,
      nav_items: nav_items()
    }
  end

  # The counts come from the same runs the example lists, so a button's number and
  # the rows below it cannot disagree in a screenshot.
  defp counts(runs) do
    by_status = Enum.frequencies_by(runs, & &1.status)
    count = &Map.get(by_status, &1, 0)

    %{
      active: count.(:running) + count.(:queued),
      failed: count.(:failed),
      succeeded: count.(:succeeded),
      total: length(runs)
    }
  end

  defp todays_runs do
    [
      run(%{
        id: "run_crm_daily_2026_07_30_a41f",
        pipeline: "crm_daily",
        assets: 14,
        status: :running,
        trigger: "Schedule",
        minutes_ago: 2,
        duration: "elapsed"
      }),
      run(%{
        id: "run_crm_reference_2026_07_30_9c02",
        pipeline: "crm_reference",
        assets: 6,
        status: :failed,
        trigger: "Manual",
        minutes_ago: 38,
        duration: "1m 12s"
      }),
      run(%{
        id: "run_backfill_crm_daily_2026_07_30_5b7d",
        pipeline: "crm_daily",
        assets: 14,
        status: :running,
        trigger: "Backfill",
        minutes_ago: 51,
        duration: "elapsed"
      }),
      run(%{
        id: "run_crm_daily_2026_07_30_3ef8",
        pipeline: "crm_daily",
        assets: 14,
        status: :succeeded,
        trigger: "Schedule",
        minutes_ago: 132,
        duration: "58s"
      }),
      run(%{
        id: "run_orders_2026_07_30_77aa",
        asset: "orders",
        status: :succeeded,
        trigger: "Manual",
        minutes_ago: 194,
        duration: "6.4 s"
      }),
      run(%{
        id: "run_crm_daily_2026_07_30_1204",
        pipeline: "crm_daily",
        assets: 14,
        status: :queued,
        trigger: "Retry",
        minutes_ago: 240,
        duration: "-"
      })
    ]
  end

  # One run a day for most of the fortnight, with two days missing on purpose.
  defp daily_runs do
    for days_ago <- 0..13, days_ago not in [3, 7] do
      run(%{
        id: "run_crm_daily_#{2000 + days_ago}_#{14 - days_ago}",
        pipeline: "crm_daily",
        assets: 14,
        status: if(days_ago == 5, do: :failed, else: :succeeded),
        trigger: "Schedule",
        minutes_ago: days_ago * 1440 + 12,
        duration: if(days_ago == 5, do: "22s", else: "1m 04s")
      })
    end
  end

  defp run(attrs) do
    started_at = DateTime.add(@now, -attrs.minutes_ago * 60, :second)
    target = target(attrs)
    assets = assets(attrs)

    %{
      id: attrs.id,
      short_id: short_id(attrs.id),
      target: target.label,
      target_title: target.title,
      target_detail: target.detail,
      assets: assets.label,
      assets_failed: assets.failed,
      status: attrs.status,
      status_label: status_label(attrs.status),
      raw_status: raw_status(attrs.status),
      trigger: attrs.trigger,
      started_at: Calendar.strftime(started_at, "%H:%M:%S"),
      started_on: Calendar.strftime(started_at, "%-d %b"),
      started_at_raw: started_at,
      started_at_title: Calendar.strftime(started_at, "%b %-d, %Y %H:%M:%S UTC"),
      duration: attrs.duration
    }
  end

  defp target(%{pipeline: pipeline, assets: assets}) do
    %{
      label: pipeline,
      title: "CrmDemo.Pipelines.#{Macro.camelize(pipeline)}.#{pipeline}",
      detail: "#{assets} assets"
    }
  end

  defp target(%{asset: asset}) do
    %{label: asset, title: "CrmDemo.Warehouse.Core.Sales.#{Macro.camelize(asset)}", detail: nil}
  end

  # What happened to the run's asset steps, which is what the row reports. A
  # queued run has none yet, so the row falls back to the plan.
  defp assets(%{status: :queued}), do: %{label: nil, failed: 0}

  defp assets(attrs) do
    total = Map.get(attrs, :assets, 1)
    failed = if attrs.status == :failed, do: min(2, total), else: 0
    completed = if attrs.status == :running, do: div(total, 2), else: total

    %{label: assets_label(completed, total), failed: failed}
  end

  defp assets_label(total, total), do: "#{total} #{word(total)}"
  defp assets_label(completed, total), do: "#{completed} / #{total} #{word(total)}"

  defp word(1), do: "asset"
  defp word(_count), do: "assets"

  defp short_id(id) when byte_size(id) > 18,
    do: binary_part(id, 0, 9) <> "..." <> binary_part(id, byte_size(id) - 6, 6)

  defp short_id(id), do: id

  defp status_label(:succeeded), do: "Succeeded"
  defp status_label(:failed), do: "Failed"
  defp status_label(:running), do: "Running"
  defp status_label(:queued), do: "Queued"

  defp raw_status(:succeeded), do: :ok
  defp raw_status(:failed), do: :error
  defp raw_status(:running), do: :running
  defp raw_status(:queued), do: :pending
end
