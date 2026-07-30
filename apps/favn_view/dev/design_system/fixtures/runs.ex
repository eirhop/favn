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

  @anchor ~U[2026-07-23 10:00:00Z]

  @doc "Navigation items for a run detail example."
  @spec nav_items() :: list()
  def nav_items, do: FavnView.Components.AssetCataloguePage.nav_items(:runs)

  @doc """
  A two-stage backfill over two windows.

  `status` picks the shape: `:running` leaves the last stage in flight,
  `:partial` fails one asset, `:ok` finishes everything.
  """
  @spec backfill(atom()) :: map()
  def backfill(status \\ :running) do
    attempts = backfill_attempts(status)

    run(%{
      id: "run_backfill_8f2c9d1",
      title: "Backfill run",
      subtitle: "Sales warehouse · Feb 2026 -> Mar 2026",
      status: status,
      attempts: attempts,
      total_windows: 2,
      completed_windows: if(status == :running, do: 1, else: 2),
      failed_windows: 0,
      child_runs: [
        child_run("run_backfill_win_2026_02", "Feb 2026", :ok, 3, 0, 0, 0),
        child_run(
          "run_backfill_win_2026_03",
          "Mar 2026",
          child_status(status),
          child_succeeded(status),
          child_failed(status),
          child_running(status),
          0
        )
      ],
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
        stage: 1,
        status: :ok,
        window: "Jul 23",
        offset_seconds: 0,
        duration_seconds: 42,
        rows: 18_402
      }),
      attempt(%{
        id: "daily_revenue-2026-07-23",
        asset: "crm.daily_revenue",
        name: "Daily revenue",
        stage: 2,
        status: :ok,
        window: "Jul 23",
        offset_seconds: 45,
        duration_seconds: 12,
        rows: 1_204
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
      child_runs: [],
      events: events(:ok)
    })
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
          stage: 1,
          status: if(index == 3, do: :running, else: :ok),
          window: nil,
          offset_seconds: index * 20,
          duration_seconds: if(index == 3, do: nil, else: 18),
          rows: 4_000 + index * 311
        })
      end)

    run(%{
      id: "run_full_refresh_sales",
      title: "Full refresh run",
      subtitle: "Sales warehouse · No window",
      status: :running,
      attempts: attempts,
      total_windows: 1,
      completed_windows: 0,
      failed_windows: 0,
      child_runs: [],
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
      total_windows: 2,
      completed_windows: 2,
      failed_windows: 2,
      child_runs: [],
      events: events(:error),
      backfill_failures: [
        %{
          window_label: "Feb 2026",
          error_summary: "No runner accepted the pool \"warehouse_large\"",
          child_run_id: "run_backfill_win_2026_02"
        },
        %{
          window_label: "Mar 2026",
          error_summary: "No runner accepted the pool \"warehouse_large\"",
          child_run_id: "run_backfill_win_2026_03"
        }
      ]
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

  defp run(overrides) do
    attempts = Map.fetch!(overrides, :attempts)
    status = Map.fetch!(overrides, :status)

    %{
      found?: true,
      active?: status in [:running, :pending],
      raw_status: status,
      status: status_label(status),
      status_tone: tone(status),
      short_id: Map.fetch!(overrides, :id),
      target: "Sales warehouse",
      trigger: "Schedule",
      window: Map.get(overrides, :subtitle),
      started_at: "Jul 23, 2026 10:00 UTC",
      finished_at: if(status == :running, do: "-", else: "Jul 23, 2026 10:02 UTC"),
      duration: "2m 14s",
      elapsed_duration: "2m 14s",
      manifest_version_id: "mv_35f422a2964c16946f340eea20c5a414",
      cancellable?: status == :running,
      cancel_label: "Cancel run",
      retry_remaining?: status in [:error, :partial],
      retry_remaining_label: "Retry 1 remaining asset",
      back_asset_href: nil,
      total_asset_attempts: length(attempts),
      completed_asset_attempts: count_tone(attempts, [:success, :error]),
      succeeded_asset_attempts: count_tone(attempts, [:success]),
      failed_asset_attempts: count_tone(attempts, [:error]),
      running_asset_attempts: count_tone(attempts, [:info]),
      queued_asset_attempts: count_tone(attempts, [:neutral]),
      progress_label: "#{count_tone(attempts, [:success])} / #{length(attempts)}",
      windows: [],
      requested_windows: [],
      failures: Enum.filter(attempts, &(&1.status_tone == :error)),
      backfill_failures: Map.get(overrides, :backfill_failures, []),
      backfill_failure_count: length(Map.get(overrides, :backfill_failures, [])),
      latest_event_summary: "Run #{status_label(status)}",
      waiting_activity?: false,
      current_activity: nil,
      asset_attempts_truncated?: false,
      requested_windows_truncated?: false,
      child_runs_truncated?: false
    }
    |> Map.merge(Map.take(overrides, [:id, :title, :subtitle, :attempts, :child_runs, :events]))
    |> Map.merge(Map.take(overrides, [:total_windows, :completed_windows, :failed_windows]))
  end

  defp backfill_attempts(status) do
    [
      attempt(%{
        id: "orders-2026-02",
        asset: "crm.orders",
        name: "Orders",
        stage: 1,
        status: :ok,
        window: "Feb 2026",
        offset_seconds: 0,
        duration_seconds: 34,
        rows: 82_101
      }),
      attempt(%{
        id: "orders-2026-03",
        asset: "crm.orders",
        name: "Orders",
        stage: 1,
        status: :ok,
        window: "Mar 2026",
        offset_seconds: 36,
        duration_seconds: 29,
        rows: 88_640
      }),
      attempt(%{
        id: "engagement-2026-02",
        asset: "crm.engagement",
        name: "Engagement",
        stage: 1,
        status: :ok,
        window: "Feb 2026",
        offset_seconds: 2,
        duration_seconds: 51,
        rows: 12_900
      }),
      attempt(%{
        id: "revenue_metrics-2026-02",
        asset: "crm.revenue_metrics",
        name: "Revenue metrics",
        stage: 2,
        status: revenue_status(status),
        window: "Feb 2026",
        offset_seconds: 60,
        duration_seconds: revenue_duration(status),
        rows: if(revenue_status(status) == :ok, do: 1_204, else: nil),
        error: revenue_error(status)
      }),
      attempt(%{
        id: "revenue_metrics-2026-03",
        asset: "crm.revenue_metrics",
        name: "Revenue metrics",
        stage: 2,
        status: if(status == :running, do: :running, else: :ok),
        window: "Mar 2026",
        offset_seconds: 66,
        duration_seconds: if(status == :running, do: nil, else: 22),
        rows: 1_311
      })
    ]
  end

  defp revenue_status(:partial), do: :error
  defp revenue_status(_status), do: :ok

  defp revenue_duration(:partial), do: 8
  defp revenue_duration(_status), do: 19

  defp revenue_error(:partial),
    do: "Contract check \"revenue_not_negative\" failed on 3 rows; the write was rolled back"

  defp revenue_error(_status), do: nil

  defp attempt(spec) do
    started = DateTime.add(@anchor, spec.offset_seconds, :second)

    finished =
      case spec.duration_seconds do
        nil -> nil
        seconds -> DateTime.add(started, seconds, :second)
      end

    %{
      id: spec.id,
      asset_step_id: spec.id,
      asset_key: spec.asset,
      asset_ref: spec.asset,
      short_asset_name: spec.name,
      stage: spec.stage,
      stage_label: "Stage #{spec.stage}",
      attempt_number: 1,
      root_execution_group_id: "run_backfill_8f2c9d1",
      child_run_id: nil,
      run_id: "run_backfill_8f2c9d1",
      started_at_raw: started,
      finished_at_raw: finished,
      started_at: Calendar.strftime(started, "%b %-d, %Y %H:%M UTC"),
      finished_at: (finished && Calendar.strftime(finished, "%b %-d, %Y %H:%M UTC")) || "-",
      duration: duration_label(spec.duration_seconds),
      duration_ms: spec.duration_seconds && spec.duration_seconds * 1_000,
      status: status_label(spec.status),
      raw_status: spec.status,
      status_tone: tone(spec.status),
      window_label: spec.window || "No window",
      window_id: spec.window || "none",
      error_summary: Map.get(spec, :error),
      output_metadata: output_metadata(spec),
      logs_href: "/runs/run_backfill_8f2c9d1/assets/#{spec.id}/logs"
    }
  end

  defp output_metadata(%{rows: nil}), do: %{}

  defp output_metadata(spec) do
    %{
      "rows_written" => spec.rows,
      "relation" => "warehouse.#{String.replace(spec.asset, ".", "_")}",
      "write_outcome" => "committed",
      "quality_status" => if(Map.get(spec, :error), do: "failed", else: "passed"),
      "partition_month" => spec.window,
      "check_results" => [
        %{
          "name" => "row_count_positive",
          "origin" => "contract",
          "outcome" => "passed",
          "phase" => "post_write",
          "duration_ms" => 4
        }
      ]
    }
  end

  defp child_run(id, window_label, status, succeeded, failed, running, queued) do
    %{
      id: id,
      window_label: window_label,
      status: status_label(status),
      raw_status: status,
      status_tone: tone(status),
      progress: "#{succeeded + failed} / #{succeeded + failed + running + queued}",
      duration: "1m 07s",
      succeeded_count: succeeded,
      failed_count: failed,
      running_count: running,
      queued_count: queued,
      attempts: []
    }
  end

  defp child_status(:running), do: :running
  defp child_status(:partial), do: :error
  defp child_status(_status), do: :ok

  defp child_succeeded(:running), do: 1
  defp child_succeeded(:partial), do: 2
  defp child_succeeded(_status), do: 3

  defp child_failed(:partial), do: 1
  defp child_failed(_status), do: 0

  defp child_running(:running), do: 1
  defp child_running(_status), do: 0

  defp events(status) do
    [
      event(1, "Run accepted", "Backfill accepted for 2 windows", nil),
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

  defp count_tone(attempts, tones), do: Enum.count(attempts, &(&1.status_tone in tones))

  defp duration_label(nil), do: "-"
  defp duration_label(seconds) when seconds < 60, do: "#{seconds}s"
  defp duration_label(seconds), do: "#{div(seconds, 60)}m #{rem(seconds, 60)}s"

  defp status_label(:ok), do: "Succeeded"
  defp status_label(:error), do: "Failed"
  defp status_label(:running), do: "Running"
  defp status_label(:partial), do: "Partial"
  defp status_label(:pending), do: "Queued"
  defp status_label(status), do: status |> to_string() |> String.capitalize()

  defp tone(:ok), do: :success
  defp tone(:error), do: :error
  defp tone(:running), do: :info
  defp tone(:partial), do: :warning
  defp tone(:pending), do: :neutral
  defp tone(_status), do: :neutral
end
