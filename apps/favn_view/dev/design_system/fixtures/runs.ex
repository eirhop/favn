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
      assets: Enum.map(attempts, &asset_row/1),
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
        duration_seconds: 34
      }),
      attempt(%{
        id: "engagement-2026-02",
        asset: "crm.engagement",
        name: "Engagement",
        status: :ok,
        offset_seconds: 2,
        duration_seconds: 51
      }),
      attempt(%{
        id: "revenue_metrics-2026-02",
        asset: "crm.revenue_metrics",
        name: "Revenue metrics",
        status: revenue_status(status),
        offset_seconds: 60,
        duration_seconds: revenue_duration(status)
      })
    ]
  end

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
      started_at: FavnView.Time.format(started, "%b %-d, %Y %H:%M %Z", "Etc/UTC"),
      finished_at:
        (finished && FavnView.Time.format(finished, "%b %-d, %Y %H:%M %Z", "Etc/UTC")) ||
          "-",
      state: spec.status
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
      detail?: true
    }
  end

  defp status_label(:ok), do: "Succeeded"
  defp status_label(:error), do: "Failed"
  defp status_label(:running), do: "Running"
  defp status_label(:skipped_fresh), do: "Already fresh"
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
