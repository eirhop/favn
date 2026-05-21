defmodule FavnView.Components.RunDetailPage.Samples do
  @moduledoc false
  import FavnView.Components.RunDetailPage.Ui

  def sample_run(status \\ :running), do: sample_execution_group(status)

  def empty_run,
    do:
      sample_execution_group(:running)
      |> Map.put(:attempts, [])
      |> Map.put(:assets, [])
      |> Map.put(:matrix, %{assets: [], windows: [], rows: []})

  def sample_run_with_no_results(status), do: sample_run(status)
  def sample_run_with_long_asset_names, do: sample_run(:running)
  def sample_run_with_node_statuses, do: sample_run(:partial)
  def sample_nav_items, do: FavnView.Components.AssetCataloguePage.nav_items()

  def sample_full_refresh_run do
    assets = ["customers", "orders", "line_items", "inventory", "daily_metrics"]

    attempts =
      Enum.with_index(assets, fn asset, index ->
        status = if(asset == "daily_metrics", do: :running, else: :ok)

        sample_attempt(%{
          id: "#{asset}-full-refresh",
          asset: asset,
          window: full_refresh_window(),
          status: status,
          duration: if(status == :running, do: "2m 19s", else: "#{90 + String.length(asset)}s"),
          start_min: 44 + index,
          duration_min: 2,
          child_run_id: "run_full_refresh_sales",
          error_summary: nil
        })
      end)

    matrix = sample_matrix(assets, [full_refresh_window()], attempts)
    running = Enum.filter(attempts, &(&1.status_tone == :info))

    %{
      found?: true,
      id: "run_full_refresh_sales",
      subscribed_run_id: "run_full_refresh_sales",
      raw_status: :running,
      active?: true,
      short_id: "run_full_refresh_sales",
      title: "Full refresh run",
      subtitle: "Sales warehouse · No window",
      status: "Running",
      status_tone: :info,
      target: "Sales warehouse",
      trigger: "Manual",
      window: "No window",
      started_at: "May 19, 2026 17:10 UTC",
      finished_at: "-",
      duration: "2m 19s",
      elapsed_duration: "2m 19s",
      manifest_version_id: "mv_sales_full_refresh",
      total_windows: 1,
      completed_windows: 0,
      failed_windows: 0,
      total_asset_attempts: length(attempts),
      completed_asset_attempts: Enum.count(attempts, &(&1.status_tone == :success)),
      succeeded_asset_attempts: Enum.count(attempts, &(&1.status_tone == :success)),
      failed_asset_attempts: 0,
      running_asset_attempts: length(running),
      queued_asset_attempts: 0,
      progress_label:
        "#{Enum.count(attempts, &(&1.status_tone == :success))} / #{length(attempts)}",
      matrix: matrix,
      assets: matrix.assets,
      windows: [full_refresh_window()],
      attempts: attempts,
      legacy_asset_results: [],
      legacy_asset_text: "",
      failures: [],
      child_runs: [],
      timeline:
        Enum.map(attempts, &Map.merge(&1, %{attempt_id: &1.id, asset_name: &1.short_asset_name})),
      events: sample_events(:running),
      latest_event_summary: "Full refresh running",
      waiting_activity?: false,
      current_activity:
        case List.first(running) do
          nil ->
            nil

          attempt ->
            %{
              asset: attempt.short_asset_name,
              window: attempt.window_label,
              started_at: attempt.started_at,
              duration: attempt.duration,
              attempt: attempt
            }
        end,
      context: [],
      back_asset_href: "/assets/sales-warehouse",
      raw_run: "%{...}",
      raw_events: "[%{...}]"
    }
  end

  def sample_single_window_run do
    window = %{
      id: "2026-05-19",
      label: "May 19, 2026",
      range_label: "May 19 - May 20",
      child_run_id: nil
    }

    assets = ["raw_payments", "customer_orders_daily", "order_quality_checks"]

    attempts =
      Enum.with_index(assets, fn asset, index ->
        sample_attempt(%{
          id: "#{asset}-2026-05-19",
          asset: asset,
          window: window,
          status: :ok,
          duration: "#{80 + String.length(asset)}s",
          start_min: index,
          duration_min: 1,
          child_run_id: "run_daily_orders_2026_05_19",
          error_summary: nil
        })
      end)

    matrix = sample_matrix(assets, [window], attempts)

    %{
      found?: true,
      id: "run_daily_orders_2026_05_19",
      subscribed_run_id: "run_daily_orders_2026_05_19",
      raw_status: :ok,
      active?: false,
      short_id: "run_daily_orders_2026_05_19",
      title: "Run",
      subtitle: "daily_orders · May 19, 2026",
      status: "Succeeded",
      status_tone: :success,
      target: "daily_orders",
      trigger: "Schedule",
      window: "May 19, 2026",
      started_at: "May 19, 2026 02:00 UTC",
      finished_at: "May 19, 2026 02:03 UTC",
      duration: "3m 12s",
      elapsed_duration: "3m 12s",
      manifest_version_id: "mv_daily_orders",
      total_windows: 1,
      completed_windows: 1,
      failed_windows: 0,
      total_asset_attempts: length(attempts),
      completed_asset_attempts: length(attempts),
      succeeded_asset_attempts: length(attempts),
      failed_asset_attempts: 0,
      running_asset_attempts: 0,
      queued_asset_attempts: 0,
      progress_label: "#{length(attempts)} / #{length(attempts)}",
      matrix: matrix,
      assets: matrix.assets,
      windows: [window],
      attempts: attempts,
      legacy_asset_results: [],
      legacy_asset_text: "",
      failures: [],
      child_runs: [],
      timeline:
        Enum.map(attempts, &Map.merge(&1, %{attempt_id: &1.id, asset_name: &1.short_asset_name})),
      events: sample_events(:ok),
      latest_event_summary: "Scheduled window completed",
      waiting_activity?: false,
      current_activity: nil,
      context: [],
      back_asset_href: "/assets/daily-orders",
      raw_run: "%{...}",
      raw_events: "[%{...}]"
    }
  end

  def sample_timeline_run do
    windows = [
      %{
        id: "2026-01",
        label: "Jan 2026",
        range_label: "Jan 1 - Feb 1",
        child_run_id: "run_sales_jan"
      },
      %{
        id: "2026-02",
        label: "Feb 2026",
        range_label: "Feb 1 - Mar 1",
        child_run_id: "run_sales_feb"
      },
      %{
        id: "2026-03",
        label: "Mar 2026",
        range_label: "Mar 1 - Apr 1",
        child_run_id: "run_sales_mar"
      },
      %{
        id: "2026-04",
        label: "Apr 2026",
        range_label: "Apr 1 - May 1",
        child_run_id: "run_sales_apr"
      },
      %{
        id: "2026-05",
        label: "May 2026",
        range_label: "May 1 - Jun 1",
        child_run_id: "run_sales_may"
      }
    ]

    attempts = [
      sample_attempt(%{
        id: "daily-sales-apr",
        asset: "daily_sales",
        window: Enum.at(windows, 3),
        status: :running,
        duration: "-",
        start_min: 30,
        duration_min: 12,
        child_run_id: "run_sales_apr",
        error_summary: nil
      }),
      sample_attempt(%{
        id: "orders-jan",
        asset: "orders",
        window: Enum.at(windows, 0),
        status: :ok,
        duration: "2m 05s",
        start_min: 2,
        duration_min: 2,
        child_run_id: "run_sales_jan",
        error_summary: nil
      }),
      sample_attempt(%{
        id: "customers-jan",
        asset: "customers",
        window: Enum.at(windows, 0),
        status: :ok,
        duration: "2m 48s",
        start_min: 5,
        duration_min: 3,
        child_run_id: "run_sales_jan",
        error_summary: nil
      }),
      sample_attempt(%{
        id: "inventory-feb",
        asset: "inventory",
        window: Enum.at(windows, 1),
        status: :ok,
        duration: "3m 21s",
        start_min: 8,
        duration_min: 3,
        child_run_id: "run_sales_feb",
        error_summary: nil
      }),
      sample_attempt(%{
        id: "discounts-feb",
        asset: "discounts",
        window: Enum.at(windows, 1),
        status: :ok,
        duration: "2m 07s",
        start_min: 13,
        duration_min: 2,
        child_run_id: "run_sales_feb",
        error_summary: nil
      }),
      sample_attempt(%{
        id: "refunds-mar",
        asset: "refunds",
        window: Enum.at(windows, 2),
        status: :error,
        duration: "1m 12s",
        start_min: 17,
        duration_min: 1,
        child_run_id: "run_sales_mar",
        error_summary: "Payment API returned an invalid refund state"
      }),
      sample_attempt(%{
        id: "shipments-mar",
        asset: "shipments",
        window: Enum.at(windows, 2),
        status: :ok,
        duration: "2m 07s",
        start_min: 22,
        duration_min: 2,
        child_run_id: "run_sales_mar",
        error_summary: nil
      }),
      sample_attempt(%{
        id: "returns-mar",
        asset: "returns",
        window: Enum.at(windows, 2),
        status: :skipped_fresh,
        duration: "0 ms",
        start_min: 24,
        duration_min: 0,
        child_run_id: "run_sales_mar",
        error_summary: "existing_success"
      }),
      sample_attempt(%{
        id: "taxes-apr",
        asset: "taxes",
        window: Enum.at(windows, 3),
        status: :queued,
        duration: "-",
        child_run_id: "run_sales_apr",
        error_summary: nil
      }),
      sample_attempt(%{
        id: "stock-levels-apr",
        asset: "stock_levels",
        window: Enum.at(windows, 3),
        status: :queued,
        duration: "-",
        child_run_id: "run_sales_apr",
        error_summary: nil
      }),
      sample_attempt(%{
        id: "revenue-metrics-may",
        asset: "revenue_metrics",
        window: Enum.at(windows, 4),
        status: :queued,
        duration: "-",
        child_run_id: "run_sales_may",
        error_summary: nil
      }),
      sample_attempt(%{
        id: "margin-daily-may",
        asset: "margin_daily",
        window: Enum.at(windows, 4),
        status: :queued,
        duration: "-",
        child_run_id: "run_sales_may",
        error_summary: nil
      }),
      sample_attempt(%{
        id: "customer-segments-may",
        asset: "customer_segments",
        window: Enum.at(windows, 4),
        status: :queued,
        duration: "-",
        child_run_id: "run_sales_may",
        error_summary: nil
      })
    ]

    running = Enum.filter(attempts, &(&1.status_tone == :info))
    failures = Enum.filter(attempts, &(&1.status_tone == :error))
    matrix = sample_sparse_matrix(attempts)

    %{
      found?: true,
      id: "run_sales_backfill_timeline",
      subscribed_run_id: "run_sales_backfill_timeline",
      raw_status: :running,
      active?: true,
      short_id: "run_sales_backfill_timeline",
      title: "Backfill timeline",
      subtitle: "Sales marts · Jan 2026 -> May 2026",
      status: "Running",
      status_tone: :info,
      target: "Sales marts",
      trigger: "Manual",
      window: "Jan 2026 -> May 2026",
      started_at: sample_timestamp_label(Enum.find_value(attempts, & &1.started_at_raw)),
      finished_at: "-",
      duration: "30m",
      elapsed_duration: "30m",
      manifest_version_id: "mv_sales_backfill",
      total_windows: length(windows),
      completed_windows: 3,
      failed_windows: length(failures),
      total_asset_attempts: length(attempts),
      completed_asset_attempts: completed_sample_attempts(attempts),
      succeeded_asset_attempts: Enum.count(attempts, &(&1.status_tone == :success)),
      failed_asset_attempts: length(failures),
      running_asset_attempts: length(running),
      queued_asset_attempts: Enum.count(attempts, &(&1.status_tone == :warning)),
      progress_label: "#{completed_sample_attempts(attempts)} / #{length(attempts)}",
      matrix: matrix,
      assets: matrix.assets,
      windows: windows,
      attempts: attempts,
      legacy_asset_results: [],
      legacy_asset_text: "",
      failures: failures,
      child_runs: sample_child_runs(windows, attempts),
      timeline:
        Enum.map(attempts, &Map.merge(&1, %{attempt_id: &1.id, asset_name: &1.short_asset_name})),
      events: sample_events(:running),
      latest_event_summary: "Sales backfill running",
      waiting_activity?: false,
      current_activity: nil,
      context: [],
      back_asset_href: "/assets/sales-marts",
      raw_run: "%{...}",
      raw_events: "[%{...}]"
    }
  end

  def sample_completed_timeline_run do
    run = sample_timeline_run()

    attempts =
      Enum.map(run.attempts, fn
        %{raw_status: :running, started_at_raw: started_at} = attempt ->
          finished_at = DateTime.add(started_at, 8, :minute)

          %{
            attempt
            | raw_status: :ok,
              status: "Succeeded",
              status_tone: :success,
              finished_at_raw: finished_at,
              finished_at: sample_timestamp_label(finished_at),
              duration: "8m 00s",
              duration_ms: 480_000
          }

        attempt ->
          attempt
      end)

    matrix = sample_sparse_matrix(attempts)
    failures = Enum.filter(attempts, &(&1.status_tone == :error))

    %{
      run
      | raw_status: :ok,
        active?: false,
        status: "Succeeded",
        status_tone: :success,
        finished_at: "May 19, 2026 17:00 UTC",
        duration: "36m",
        elapsed_duration: "36m",
        completed_asset_attempts: completed_sample_attempts(attempts),
        succeeded_asset_attempts: Enum.count(attempts, &(&1.status_tone == :success)),
        failed_asset_attempts: length(failures),
        running_asset_attempts: 0,
        matrix: matrix,
        assets: matrix.assets,
        attempts: attempts,
        failures: failures,
        timeline:
          Enum.map(
            attempts,
            &Map.merge(&1, %{attempt_id: &1.id, asset_name: &1.short_asset_name})
          )
    }
  end

  def sample_admission_failed_backfill do
    run = sample_execution_group(:error)
    window = List.first(run.windows)

    failure = %{
      id: "run_backfill_unknown_pool_jan",
      child_run_id: "run_backfill_unknown_pool_jan",
      asset_ref: nil,
      short_asset_name: "Window run",
      window: window,
      window_id: window.id,
      window_label: window.label,
      status: "Failed",
      raw_status: :error,
      status_tone: :error,
      error_summary: "{:unknown_execution_pool, :partner_api}",
      attempt_count: 1,
      started_at: "May 19, 2026 16:26 UTC",
      finished_at: "May 19, 2026 16:26 UTC",
      duration: "0.0 s"
    }

    Map.merge(run, %{
      raw_status: :error,
      active?: false,
      status: "Failed",
      status_tone: :error,
      failed_windows: 1,
      total_asset_attempts: 0,
      completed_asset_attempts: 0,
      succeeded_asset_attempts: 0,
      failed_asset_attempts: 0,
      running_asset_attempts: 0,
      queued_asset_attempts: 0,
      progress_label: "0 / 0",
      matrix: %{assets: [], windows: [window], rows: []},
      assets: [],
      windows: [window],
      attempts: [],
      failures: [],
      backfill_failures: [failure],
      backfill_failure_count: 1,
      child_runs: [
        %{
          id: "run_backfill_unknown_pool_jan",
          window: window,
          window_label: window.label,
          status: "Failed",
          raw_status: :error,
          status_tone: :error,
          progress: "0 / 0",
          started_at: "May 19, 2026 16:26 UTC",
          finished_at: "May 19, 2026 16:26 UTC",
          duration: "0.0 s",
          succeeded_count: 0,
          failed_count: 0,
          running_count: 0,
          queued_count: 0,
          attempts: []
        }
      ],
      timeline: [],
      latest_event_summary: "Backfill failed"
    })
  end

  def not_found_run do
    %{
      found?: false,
      id: "run_missing",
      error: "Run not found",
      status: nil,
      status_tone: :neutral
    }
  end

  def sample_execution_group(status \\ :running) do
    windows = [
      %{
        id: "2026-01",
        label: "Jan 2026",
        range_label: "Jan 1 - Feb 1",
        child_run_id: "run_backfill_jan"
      },
      %{
        id: "2026-02",
        label: "Feb 2026",
        range_label: "Feb 1 - Mar 1",
        child_run_id: "run_backfill_feb"
      },
      %{
        id: "2026-03",
        label: "Mar 2026",
        range_label: "Mar 1 - Apr 1",
        child_run_id: "run_backfill_mar"
      },
      %{
        id: "2026-04",
        label: "Apr 2026",
        range_label: "Apr 1 - May 1",
        child_run_id: "run_backfill_apr"
      },
      %{
        id: "2026-05",
        label: "May 2026",
        range_label: "May 1 - Jun 1",
        child_run_id: "run_backfill_may"
      }
    ]

    assets = [
      "orders",
      "order_items",
      "customers",
      "inventory",
      "discounts",
      "daily_sales",
      "revenue_metrics"
    ]

    attempts =
      for {asset, asset_index} <- Enum.with_index(assets),
          {window, window_index} <- Enum.with_index(windows) do
        status = sample_attempt_status(status, asset, window_index)

        sample_attempt(%{
          id: "#{asset}-#{window.id}",
          asset: asset,
          window: window,
          status: status,
          duration:
            if(status == :queued,
              do: "-",
              else: "#{1 + rem(asset_index + window_index, 4)}m #{5 + asset_index}s"
            ),
          start_min: asset_index * 2 + window_index * 5,
          duration_min: 1 + rem(asset_index + window_index, 4),
          child_run_id: window.child_run_id,
          error_summary:
            if(status == :error, do: "DuckDB ADBC connection bootstrap failed at attach_mart")
        })
      end

    matrix = sample_matrix(assets, windows, attempts)
    failures = Enum.filter(attempts, &(&1.status_tone == :error))
    running = Enum.filter(attempts, &(&1.status_tone == :info))

    %{
      found?: true,
      id: "run_backfill_8f2c9d1",
      subscribed_run_id: "run_backfill_8f2c9d1",
      raw_status: status,
      active?: status in [:pending, :running],
      short_id: "run_backfill_8f2c9d1",
      title: "Backfill run",
      subtitle: "Sales marts · Jan 2026 -> May 2026",
      status: status_label(status),
      status_tone: status_tone(status),
      target: "Sales marts",
      trigger: "Manual",
      window: "Jan 2026 -> May 2026",
      started_at: "May 19, 2026 16:26 UTC",
      finished_at: "-",
      duration: "5m 41s",
      elapsed_duration: "5m 41s",
      manifest_version_id: "mv_daily_sales",
      total_windows: 5,
      completed_windows: 4,
      failed_windows: length(failures),
      total_asset_attempts: length(attempts),
      completed_asset_attempts: Enum.count(attempts, &(&1.status_tone in [:success, :error])),
      succeeded_asset_attempts: Enum.count(attempts, &(&1.status_tone == :success)),
      failed_asset_attempts: length(failures),
      running_asset_attempts: length(running),
      queued_asset_attempts: Enum.count(attempts, &(&1.status_tone == :warning)),
      progress_label:
        "#{Enum.count(attempts, &(&1.status_tone in [:success, :error]))} / #{length(attempts)}",
      matrix: matrix,
      assets: matrix.assets,
      windows: windows,
      attempts: attempts,
      legacy_asset_results: [],
      legacy_asset_text: "",
      failures: failures,
      child_runs: sample_child_runs(windows, attempts),
      timeline:
        Enum.take(attempts, 12)
        |> Enum.map(&Map.merge(&1, %{attempt_id: &1.id, asset_name: &1.short_asset_name})),
      events: sample_events(status),
      latest_event_summary: "Window run scheduled",
      waiting_activity?: false,
      current_activity:
        case List.first(running) do
          nil ->
            nil

          attempt ->
            %{
              asset: attempt.short_asset_name,
              window: attempt.window_label,
              started_at: attempt.started_at,
              duration: attempt.duration,
              attempt: attempt
            }
        end,
      context: [],
      back_asset_href: "/assets/reporting-baseline-adjustment",
      raw_run: "%{...}",
      raw_events: "[%{...}]"
    }
  end

  defp sample_attempt_status(:error, _asset, 2), do: :error
  defp sample_attempt_status(:partial, "revenue_metrics", 1), do: :error
  defp sample_attempt_status(:running, "daily_sales", 3), do: :running
  defp sample_attempt_status(:running, _asset, window_index) when window_index >= 3, do: :queued
  defp sample_attempt_status(_status, _asset, _window_index), do: :ok

  defp sample_attempt(attrs) do
    status = attrs.status
    base = DateTime.add(DateTime.utc_now(), -50, :minute)

    started_at_raw =
      if(status in [:queued, :pending],
        do: nil,
        else: DateTime.add(base, attrs[:start_min] || 0, :minute)
      )

    duration_ms = (attrs[:duration_min] || 2) * 60_000

    finished_at_raw =
      if(status in [:queued, :pending, :running] or is_nil(started_at_raw),
        do: nil,
        else: DateTime.add(started_at_raw, duration_ms, :millisecond)
      )

    %{
      id: attrs.id,
      root_execution_group_id: "run_backfill_8f2c9d1",
      child_run_id: attrs.child_run_id,
      run_id: attrs.child_run_id,
      asset_key: "Favn.Examples.Sales.#{attrs.asset}",
      asset_ref: "Favn.Examples.Sales.#{attrs.asset}",
      short_asset_name: attrs.asset,
      stage: 0,
      stage_label: "Stage 0",
      attempt_number: 1,
      started_at_raw: started_at_raw,
      finished_at_raw: finished_at_raw,
      duration_ms: duration_ms,
      started_at: sample_timestamp_label(started_at_raw),
      finished_at: sample_timestamp_label(finished_at_raw),
      duration: attrs.duration,
      status: status_label(status),
      raw_status: status,
      status_tone: status_tone(status),
      error_summary: attrs.error_summary,
      window: attrs.window,
      window_id: attrs.window.id,
      window_label: attrs.window.label,
      logs_href: "/runs/#{attrs.child_run_id}/assets/#{attrs.id}/logs"
    }
  end

  defp full_refresh_window do
    %{id: "full-refresh", label: "Full refresh", range_label: "No window", child_run_id: nil}
  end

  defp sample_timestamp_label(nil), do: "-"

  defp sample_timestamp_label(datetime),
    do: Calendar.strftime(datetime, "%b %d, %Y %H:%M UTC")

  defp sample_matrix(assets, windows, attempts) do
    by_cell = Map.new(attempts, &{{&1.short_asset_name, &1.window_id}, &1})
    asset_rows = Enum.map(assets, &%{key: &1, name: &1, stage: "Stage 0"})

    rows =
      Enum.map(asset_rows, fn asset ->
        cells = Enum.map(windows, &Map.fetch!(by_cell, {asset.name, &1.id}))
        Map.put(asset, :cells, cells)
      end)

    %{assets: asset_rows, windows: windows, rows: rows}
  end

  defp sample_sparse_matrix(attempts) do
    asset_rows =
      Enum.map(attempts, fn attempt ->
        %{
          key: attempt.asset_key,
          name: attempt.short_asset_name,
          stage: attempt.stage_label,
          cells: [attempt]
        }
      end)

    %{assets: asset_rows, windows: [], rows: asset_rows}
  end

  defp sample_child_runs(windows, attempts) do
    Enum.map(windows, fn window ->
      child_attempts = Enum.filter(attempts, &(&1.window_id == window.id))

      %{
        id: window.child_run_id,
        window: window,
        window_label: window.label,
        status: child_status(child_attempts),
        status_tone: child_tone(child_attempts),
        progress: "#{completed_sample_attempts(child_attempts)} / #{length(child_attempts)}",
        started_at: "May 19, 2026 16:26 UTC",
        finished_at: "-",
        duration: "5m 41s",
        succeeded_count: Enum.count(child_attempts, &(&1.status_tone == :success)),
        failed_count: Enum.count(child_attempts, &(&1.status_tone == :error)),
        running_count: Enum.count(child_attempts, &(&1.status_tone == :info)),
        queued_count: Enum.count(child_attempts, &(&1.status_tone == :warning)),
        attempts: child_attempts
      }
    end)
  end

  defp child_status(attempts) do
    cond do
      Enum.any?(attempts, &(&1.status_tone == :error)) -> "Failed"
      Enum.any?(attempts, &(&1.status_tone == :info)) -> "Running"
      Enum.any?(attempts, &(&1.status_tone == :warning)) -> "Queued"
      true -> "Succeeded"
    end
  end

  defp child_tone(attempts) do
    cond do
      Enum.any?(attempts, &(&1.status_tone == :error)) -> :error
      Enum.any?(attempts, &(&1.status_tone == :info)) -> :info
      Enum.any?(attempts, &(&1.status_tone == :warning)) -> :warning
      true -> :success
    end
  end

  defp completed_sample_attempts(attempts) do
    Enum.count(attempts, &(&1.status_tone not in [:info, :warning]))
  end

  defp sample_events(status) do
    [
      %{
        sequence: 1,
        timestamp: "May 19, 2026 16:26 UTC",
        event_type: "Run started",
        status: "Running",
        status_tone: :info,
        asset: nil,
        summary: "Backfill accepted"
      },
      %{
        sequence: 2,
        timestamp: "May 19, 2026 16:31 UTC",
        event_type: "Run updated",
        status: status_label(status),
        status_tone: status_tone(status),
        asset: nil,
        summary: "Window run updated"
      }
    ]
  end
end
