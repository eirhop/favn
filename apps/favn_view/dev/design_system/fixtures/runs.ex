defmodule FavnView.Dev.DesignSystem.Fixtures.Runs do
  @moduledoc """
  Run detail sample data for the design system.

  These live under `dev/` because they are not product code. Run detail carried
  780 lines of fake runs inside `lib/` for as long as Storybook needed them; the
  design system reads its own fixtures instead, so the page module no longer ships
  invented data to production.

  Times are anchored to a fixed instant rather than `now`, so a screenshot of the
  flow is the same image tomorrow.
  """

  alias FavnView.LogsViewModel
  alias FavnView.RunComparison
  alias FavnView.RunTimeline
  alias FavnView.RunWindowRail
  alias FavnView.WindowFailures

  @anchor ~U[2026-07-23 10:00:00Z]

  # The chart measures a running attempt against now, so the fixture pins now to
  # the same anchored instant its elapsed duration reports. Real time would draw
  # a running bar years long.
  @now DateTime.add(@anchor, 134, :second)

  @doc "Navigation items for a run detail example."
  @spec nav_items() :: list()
  def nav_items, do: FavnView.Components.AssetCataloguePage.nav_items(:runs)

  @doc """
  One persisted window run from a larger backfill.

  `status` picks the shape: `:running` leaves the last stage in flight,
  `:partial` fails one asset, `:ok` finishes everything.
  """
  @spec backfill(atom()) :: map()
  def backfill(status \\ :running) do
    attempts = backfill_attempts(status)

    run(%{
      id: "run_backfill_8f2c9d1",
      title: "Backfill run",
      subtitle: "Sales warehouse · Feb 2026",
      status: status,
      attempts: attempts,
      total_windows: 1,
      completed_windows: if(status == :running, do: 0, else: 1),
      failed_windows: if(status == :partial, do: 1, else: 0),
      events: events(status)
    })
  end

  @doc "A single-window daily run, which must not read as a backfill."
  @spec single_window() :: map()
  def single_window do
    attempts = [
      attempt(%{
        id: "orders-2026-07-23",
        asset: "crm.orders",
        name: "Orders",
        status: :ok,
        offset_seconds: 0,
        duration_seconds: 42
      }),
      attempt(%{
        id: "daily_revenue-2026-07-23",
        asset: "crm.daily_revenue",
        name: "Daily revenue",
        status: :ok,
        offset_seconds: 45,
        duration_seconds: 12
      })
    ]

    run(%{
      id: "run_daily_orders_2026_05_19",
      title: "Run",
      subtitle: "Daily revenue · Jul 23",
      status: :ok,
      attempts: attempts,
      total_windows: 1,
      completed_windows: 1,
      failed_windows: 0,
      events: events(:ok)
    })
  end

  @doc """
  A run wide enough to leave comfortable lane height behind.

  `lanes` picks the density the chart lands in: 40 stays comfortable, 120 is
  compact, 260 is dense with its stages collapsed.
  """
  @spec wide(pos_integer()) :: map()
  def wide(lanes \\ 120) do
    attempts =
      Enum.map(1..lanes, fn index ->
        attempt(%{
          id: "wide-#{index}",
          asset: "crm.wide_#{index}",
          name: "Wide asset #{index}",
          status: wide_status(index),
          offset_seconds: rem(index, 40) * 3,
          duration_seconds: wide_duration(index),
          stage: div(index - 1, div(lanes, 3) + 1)
        })
      end)

    run(%{
      id: "run_wide_pipeline",
      title: "Run",
      subtitle: "Wide pipeline · Jul 23",
      status: :running,
      attempts: attempts,
      total_windows: 1,
      completed_windows: 0,
      failed_windows: 1,
      events: events(:running)
    })
  end

  @doc """
  The calendar rail of a backfill's window runs.

  `layout` picks `:flat`, the strip of one cell per window run, or `:banded`,
  the coarse period band plus the selected period's cells. `:compare` is the
  flat rail with three of its windows chosen for comparison. `:combined` is the
  rail that stands down: six coverage windows executed as one run, so there is
  nothing to navigate between.
  """
  @spec rail(:flat | :banded | :compare | :combined) :: RunWindowRail.t()
  def rail(layout \\ :flat)

  def rail(:banded) do
    RunWindowRail.build(window_choices(200), "run_window_120", "Etc/UTC",
      backfill_status: :running
    )
  end

  def rail(:compare) do
    RunWindowRail.build(window_choices(8), "run_window_4", "Etc/UTC",
      backfill_status: :completed,
      compare_run_ids: ["run_window_2", "run_window_4", "run_window_6"]
    )
  end

  def rail(:flat) do
    RunWindowRail.build(window_choices(8), "run_window_4", "Etc/UTC", backfill_status: :completed)
  end

  def rail(:combined) do
    combined =
      Enum.map(window_choices(6), &%{&1 | run_id: "run_window_combined"})

    RunWindowRail.build(combined, "run_window_combined", "Etc/UTC", backfill_status: :completed)
  end

  @doc """
  A backfill parent whose windows all failed before any run existed.

  This is the shape a planning or submission failure leaves: windows counted,
  none of them navigable, and the reason held only on the ledger. The page has no
  rail, no chart and no run to open, so the failure panel is the whole reading.
  """
  @spec runless_backfill(non_neg_integer()) :: map()
  def runless_backfill(failed \\ 31) do
    backfill(:running)
    |> Map.merge(%{
      backfill_parent?: true,
      title: "Backfill parent",
      status: "Failed",
      status_tone: :error,
      raw_status: :error,
      active?: false,
      cancellable?: false,
      window: nil,
      assets: [],
      chart: nil,
      total_windows: failed,
      completed_windows: failed,
      failed_windows: failed,
      running_windows: 0,
      queued_windows: 0,
      total_asset_attempts: 0,
      completed_asset_attempts: 0,
      succeeded_asset_attempts: 0,
      failed_asset_attempts: 0,
      running_asset_attempts: 0,
      queued_asset_attempts: 0,
      planned_asset_attempts: 0
    })
  end

  @doc """
  Grouped window failures, as the run page receives them.

  `shape` picks `:single`, one cause behind every window; `:mixed`, two causes
  where one of them did reach a run; `:one`, a single window, which names its
  window rather than a span; or `:truncated`, a bounded read that reached 500 of
  900 failed windows.
  """
  @spec window_failures(:single | :mixed | :one | :truncated) :: [WindowFailures.Group.t()]
  def window_failures(shape \\ :single)

  def window_failures(:single) do
    [
      %WindowFailures.Group{
        reason: "invalid_backfill_pipeline_identity",
        window_count: 31,
        run_count: 0,
        span: "Jan 1 00:00 – Feb 1 00:00, 2026",
        attempts: 1,
        run_ids: []
      }
    ]
  end

  def window_failures(:mixed) do
    [
      %WindowFailures.Group{
        reason: "no_runner_available",
        detail: "the default runner pool was empty when the window was admitted",
        window_count: 18,
        run_count: 0,
        span: "Jan 1 00:00 – Jan 19 00:00, 2026",
        attempts: 3,
        run_ids: []
      },
      %WindowFailures.Group{
        reason: "asset_step_failed",
        window_count: 4,
        run_count: 4,
        span: "Jan 19 00:00 – Jan 23 00:00, 2026",
        attempts: 1,
        run_ids: ["run_window_19", "run_window_20", "run_window_21"]
      }
    ]
  end

  def window_failures(:truncated) do
    [
      %WindowFailures.Group{
        reason: "no_runner_available",
        window_count: 500,
        run_count: 0,
        span: "Jan 1 00:00 – May 16 00:00, 2026",
        attempts: 1,
        run_ids: []
      }
    ]
  end

  def window_failures(:one) do
    [
      %WindowFailures.Group{
        reason: "window_lease_lost",
        window_count: 1,
        run_count: 1,
        span: nil,
        first_window: "Jan 4, 2026",
        attempts: 2,
        run_ids: ["run_window_4"]
      }
    ]
  end

  @doc """
  Three window runs drawn as tracks, one of them unreadable.

  The unreadable window is deliberate: an empty track has to say which kind of
  empty it is, and that is the case a review must be able to see.
  """
  @spec comparison(keyword()) :: RunComparison.t()
  def comparison(opts \\ []) do
    # The earlier window ran an hour before the open one and never planned the
    # last asset, so both the alignment and the absent-track cases are visible.
    earlier = backfill_attempts(:ok) |> Enum.drop(-1)

    RunComparison.build(
      [
        compare_window(1, "run_window_2", :loaded, earlier, shift_seconds: -3_600),
        compare_window(2, "run_window_4", :loaded, backfill_attempts(:running), selected?: true),
        compare_window(3, "run_window_6", :unavailable, [])
      ],
      Keyword.put_new(opts, :now, @now)
    )
  end

  @doc "A full refresh: real work, no window at all."
  @spec full_refresh() :: map()
  def full_refresh do
    attempts =
      ["customers", "orders", "line_items", "inventory"]
      |> Enum.with_index()
      |> Enum.map(fn {asset, index} ->
        attempt(%{
          id: "#{asset}-full-refresh",
          asset: "crm.#{asset}",
          name: String.capitalize(String.replace(asset, "_", " ")),
          status: if(index == 3, do: :running, else: :ok),
          offset_seconds: index * 20,
          duration_seconds: if(index == 3, do: nil, else: 18)
        })
      end)

    run(%{
      id: "run_full_refresh_sales",
      title: "Full refresh run",
      subtitle: "Sales warehouse · No window",
      status: :running,
      attempts: attempts,
      total_windows: 0,
      completed_windows: 0,
      failed_windows: 0,
      events: events(:running)
    })
  end

  @doc "Rejected before any asset ran, so there is no lane to show a failure in."
  @spec admission_failure() :: map()
  def admission_failure do
    run(%{
      id: "run_backfill_unknown_pool",
      title: "Backfill run",
      subtitle: "Sales warehouse · Feb 2026",
      status: :error,
      attempts: [],
      total_windows: 1,
      completed_windows: 1,
      failed_windows: 1,
      events: events(:error)
    })
  end

  @doc "No such run. Neutral, not an error."
  @spec not_found() :: map()
  def not_found do
    %{id: "run_missing", found?: false, not_found?: true, error: "Run not found"}
  end

  @doc "The run exists but its snapshot could not be read."
  @spec unavailable() :: map()
  def unavailable do
    %{id: "run_unreadable", found?: false, not_found?: false, error: "Run could not be loaded"}
  end

  @doc "A durable submission before a run execution projection exists."
  @spec submission(:queued | :preparing | :failed) :: map()
  def submission(status) when status in [:queued, :preparing, :failed] do
    failure =
      if status == :failed do
        %{
          title: "Run preparation failed",
          message: "Favn could not inspect a physical relation required by this run.",
          remediation:
            "Check runner diagnostics, configure the DuckDB ADBC driver, and submit the run again.",
          code: "physical_inspection_unavailable"
        }
      end

    %{
      id: "run_submission_crm_reference",
      found?: false,
      submission?: true,
      initializing?: false,
      active?: status in [:queued, :preparing],
      raw_status: status,
      status: status |> Atom.to_string() |> String.capitalize(),
      status_tone: if(status == :failed, do: :error, else: :info),
      target_kind: "pipeline",
      target_id: "crm_Reference",
      attempt: if(status == :queued, do: 0, else: 1),
      enqueued_at: "Jul 31, 2026 12:00:00 UTC",
      updated_at: "Jul 31, 2026 12:00:06 UTC",
      terminal_at: if(status == :failed, do: "Jul 31, 2026 12:00:06 UTC"),
      failure: failure,
      subscribed_run_ids: []
    }
  end

  defp run(overrides) do
    attempts = Map.fetch!(overrides, :attempts)
    status = Map.fetch!(overrides, :status)
    rows = Enum.map(attempts, &asset_row/1)

    %{
      found?: true,
      active?: status in [:running, :pending],
      raw_status: status,
      status: status_label(status),
      status_tone: tone(status),
      trigger: "Schedule",
      window: Map.get(overrides, :subtitle),
      started_at: "Jul 23, 2026 10:00 UTC",
      elapsed_duration: "2m 14s",
      cancellable?: status == :running,
      cancel_label: "Cancel run",
      retry_remaining?: status in [:error, :partial],
      retry_remaining_label: "Retry 1 remaining asset",
      back_asset_href: nil,
      total_asset_attempts: length(attempts),
      succeeded_asset_attempts: count_status(attempts, [:ok]),
      skipped_asset_attempts: count_status(attempts, [:skipped_fresh]),
      failed_asset_attempts: count_status(attempts, [:error]),
      running_asset_attempts: count_status(attempts, [:running]),
      queued_asset_attempts: count_status(attempts, [:pending, :queued]),
      planned_asset_attempts: count_status(attempts, [:planned]),
      assets: rows,
      chart: RunTimeline.build(rows, now: @now),
      asset_attempts_truncated?: false,
      events: Map.get(overrides, :events, [])
    }
    |> Map.merge(Map.take(overrides, [:id, :title, :subtitle]))
    |> Map.merge(Map.take(overrides, [:total_windows, :completed_windows, :failed_windows]))
  end

  defp backfill_attempts(status) do
    [
      attempt(%{
        id: "orders-2026-02",
        asset: "crm.orders",
        name: "Orders",
        status: :ok,
        offset_seconds: 0,
        duration_seconds: 34,
        stage: 0
      }),
      attempt(%{
        id: "engagement-2026-02",
        asset: "crm.engagement",
        name: "Engagement",
        status: :ok,
        offset_seconds: 2,
        duration_seconds: 51,
        stage: 0
      }),
      attempt(%{
        id: "revenue_metrics-2026-02",
        asset: "crm.revenue_metrics",
        name: "Revenue metrics",
        status: revenue_status(status),
        offset_seconds: 60,
        duration_seconds: revenue_duration(status),
        stage: 1
      })
    ]
  end

  defp wide_status(index) when rem(index, 17) == 0, do: :error
  defp wide_status(index) when rem(index, 7) == 0, do: :running
  defp wide_status(index) when rem(index, 11) == 0, do: :pending
  defp wide_status(_index), do: :ok

  defp wide_duration(index) do
    case wide_status(index) do
      :running -> nil
      :pending -> nil
      _finished -> 4 + rem(index, 9)
    end
  end

  # The window list the rail bands over. A backfill of 200 hourly windows is
  # what makes the coarse band worth having at all.
  defp window_choices(count) do
    Enum.map(1..count, fn index ->
      start_at = DateTime.add(~U[2026-02-01 00:00:00Z], (index - 1) * 3_600, :second)

      %{
        run_id: "run_window_#{index}",
        window_start_at: start_at,
        window_end_at: DateTime.add(start_at, 3_600, :second),
        status: window_status(index),
        kind: :hour,
        timezone: "Europe/Oslo"
      }
    end)
  end

  defp window_status(index) when rem(index, 13) == 0, do: :failed
  defp window_status(index) when rem(index, 5) == 0, do: :running
  defp window_status(_index), do: :succeeded

  defp compare_window(track, run_id, state, attempts, opts \\ []) do
    shift = Keyword.get(opts, :shift_seconds, 0)

    %{
      run_id: run_id,
      track: track,
      state: state,
      label: "Feb #{track}, 2026 08:00",
      reason: if(state == :unavailable, do: :unavailable),
      selected?: Keyword.get(opts, :selected?, false),
      assets: Enum.map(attempts, &compare_row(&1, run_id, shift))
    }
  end

  defp compare_row(attempt, run_id, shift) do
    attempt
    |> asset_row()
    |> Map.merge(%{
      run_id: run_id,
      started_at: shift_at(attempt.started_at, shift),
      finished_at: shift_at(attempt.finished_at, shift)
    })
  end

  defp shift_at(nil, _seconds), do: nil
  defp shift_at(at, 0), do: at
  defp shift_at(at, seconds), do: DateTime.add(at, seconds, :second)

  defp revenue_status(:running), do: :running
  defp revenue_status(:partial), do: :error
  defp revenue_status(_status), do: :ok

  defp revenue_duration(:running), do: nil
  defp revenue_duration(:partial), do: 8
  defp revenue_duration(_status), do: 19

  defp attempt(spec) do
    started = DateTime.add(@anchor, spec.offset_seconds, :second)

    finished =
      case spec.duration_seconds do
        nil -> nil
        seconds -> DateTime.add(started, seconds, :second)
      end

    %{
      asset_step_id: spec.id,
      asset_ref: spec.asset,
      name: spec.name,
      run_id: "run_backfill_8f2c9d1",
      started_at: started,
      finished_at: finished,
      started_label: FavnView.Time.format(started, "%b %-d, %Y %H:%M %Z", "Etc/UTC"),
      finished_label:
        (finished && FavnView.Time.format(finished, "%b %-d, %Y %H:%M %Z", "Etc/UTC")) ||
          "-",
      state: spec.status,
      stage: Map.get(spec, :stage)
    }
  end

  defp events(status) do
    [
      event(1, "Run accepted", "Window run accepted", nil),
      event(2, "Step submitted", "crm.orders submitted to pool default", "crm.orders"),
      event(3, "Step succeeded", "crm.orders wrote 82,101 rows", "crm.orders"),
      event(4, event_type(status), event_summary(status), "crm.revenue_metrics")
    ]
  end

  defp event_type(:running), do: "Step started"
  defp event_type(:partial), do: "Step failed"
  defp event_type(_status), do: "Run succeeded"

  defp event_summary(:running), do: "crm.revenue_metrics started"
  defp event_summary(:partial), do: "Contract check \"revenue_not_negative\" failed"
  defp event_summary(_status), do: "All assets completed"

  defp event(sequence, type, summary, asset) do
    %{
      sequence: sequence,
      timestamp: "Jul 23, 2026 10:0#{sequence} UTC",
      event_type: type,
      raw_event_type: type,
      status: nil,
      status_tone: :neutral,
      asset: asset,
      summary: summary
    }
  end

  defp count_status(attempts, statuses), do: Enum.count(attempts, &(&1.state in statuses))

  defp asset_row(attempt) do
    %{
      id: attempt.asset_step_id,
      run_id: attempt.run_id,
      name: attempt.name,
      asset_ref: attempt.asset_ref,
      state: attempt.state,
      started_at: attempt.started_at,
      finished_at: attempt.finished_at,
      started_label: attempt.started_label,
      finished_label: attempt.finished_label,
      stage: attempt.stage
    }
  end

  # The page labels a status through `LogsViewModel`, so the fixture does too:
  # a sample that named a status differently would review a page that does not exist.
  defp status_label(status), do: LogsViewModel.status_label(status)

  defp tone(:ok), do: :success
  defp tone(:error), do: :error
  defp tone(:running), do: :info
  defp tone(:partial), do: :warning
  defp tone(:pending), do: :neutral
  defp tone(_status), do: :neutral
end
