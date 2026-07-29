defmodule FavnView.Dev.DesignSystem.Fixtures.Logs do
  @moduledoc """
  Log entries and log-page view models.

  The log viewer is judged on things only real content exposes: a multi-line SQL
  statement, a stacktrace, a line long enough to overflow, and a persisted
  truncation marker. Each of those is a named fixture rather than a paragraph of
  lorem, because the wrapping decision is the thing under test.

  `Favn.Log.Entry` is a public log struct, so building one here does not reach
  past the orchestrator facade.
  """

  alias Favn.Log.Entry
  alias Favn.Log.Filter
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.LogsViewModel

  @occurred_at ~U[2026-06-12 09:41:12Z]

  @doc """
  One log entry.

  ## Options

    * `:run_id`, `:asset_step_id` — scope the entry to a run or asset step
    * `:truncated` — mark the entry as persisted-truncated
  """
  @spec entry(pos_integer(), atom(), atom(), String.t(), keyword()) :: Entry.t()
  def entry(sequence, level, source, message, opts \\ []) do
    %Entry{
      id: "design-system-log-#{sequence}",
      global_sequence: sequence,
      run_id: Keyword.get(opts, :run_id),
      asset_step_id: Keyword.get(opts, :asset_step_id),
      occurred_at: DateTime.add(@occurred_at, sequence * 8, :second),
      level: level,
      source: source,
      message: message,
      metadata: %{fixture: true},
      truncated: Keyword.get(opts, :truncated, false)
    }
  end

  @doc """
  A run's worth of entries across every level and source.
  """
  @spec mixed() :: [Entry.t()]
  def mixed do
    [
      entry(1, :info, :runner, "Starting asset customer_orders_daily"),
      entry(2, :info, :adapter, sql()),
      entry(3, :warning, :adapter, "Query exceeded warehouse threshold"),
      entry(4, :error, :adapter, "Warehouse timeout\nquery_id=01bc...")
    ]
  end

  @doc """
  Entries scoped to a run.
  """
  @spec for_run(String.t()) :: [Entry.t()]
  def for_run(run_id) do
    Enum.map(mixed(), &%{&1 | run_id: run_id})
  end

  @doc """
  Entries scoped to one asset step of a run.
  """
  @spec for_asset_step(String.t(), String.t()) :: [Entry.t()]
  def for_asset_step(run_id, asset_step_id) do
    Enum.map(mixed(), &%{&1 | run_id: run_id, asset_step_id: asset_step_id})
  end

  @doc """
  Entries the orchestrator emits, with no run scope.
  """
  @spec global() :: [Entry.t()]
  def global do
    [
      entry(1, :info, :orchestrator, "Run run_2026_06_12 accepted"),
      entry(2, :info, :runner, "Starting asset customer_orders_daily"),
      entry(3, :warning, :adapter, "Query exceeded warehouse threshold")
    ]
  end

  @doc """
  A single multi-line SQL entry.
  """
  @spec sql_only() :: [Entry.t()]
  def sql_only, do: [entry(1, :info, :adapter, sql())]

  @doc """
  A single entry carrying a stacktrace.
  """
  @spec stacktrace() :: [Entry.t()]
  def stacktrace do
    [
      entry(
        1,
        :error,
        :user_code,
        "Warehouse timeout\nquery_id=01bc...\n    at Favn.Asset.run/2\n    at FavnRunner.Worker.perform/1"
      )
    ]
  end

  @doc """
  A single entry long enough to overflow the viewport.
  """
  @spec long() :: [Entry.t()]
  def long do
    message =
      String.duplicate(
        "customer_orders_daily emitted a very long diagnostic message with many columns and partitions ",
        4
      )

    [entry(1, :info, :runner, message)]
  end

  @doc """
  A single entry that hit the persisted log size limit.
  """
  @spec truncated() :: [Entry.t()]
  def truncated do
    [
      entry(1, :warning, :system, "Output exceeded maximum persisted log size\n[TRUNCATED]",
        truncated: true
      )
    ]
  end

  @doc """
  Assigns for `FavnView.Components.LogViewer.log_viewer/1`.

  Accepts `:logs` as raw entries and converts them, so callers state content
  rather than view-model shapes.
  """
  @spec viewer_attrs(map()) :: map()
  def viewer_attrs(overrides \\ %{}) do
    logs = Map.get(overrides, :logs, mixed())

    %{
      visible_logs: LogsViewModel.entries(logs),
      scope: :global,
      status: :ready,
      live?: true,
      live_tail?: true,
      wrap?: true,
      search_query: "",
      selected_level: "all",
      selected_source: "all",
      empty_state: "No logs yet."
    }
    |> Map.merge(Map.delete(overrides, :logs))
  end

  @doc """
  Assigns shared by every log page.
  """
  @spec page_attrs(map()) :: map()
  def page_attrs(overrides \\ %{}) do
    logs = Map.get(overrides, :logs, mixed())

    %{
      logs: logs,
      visible_logs: LogsViewModel.entries(logs),
      logs_status: :ready,
      live?: true,
      live_tail?: true,
      wrap?: true,
      search_query: "",
      selected_level: "all",
      selected_source: "all",
      next_cursor: nil,
      stream_warning: nil,
      context_note: nil,
      output_metadata: nil,
      output_status: nil
    }
    |> Map.merge(overrides)
    |> then(&Map.put(&1, :visible_logs, LogsViewModel.entries(&1.logs)))
  end

  @doc """
  Assigns for the global logs page.
  """
  @spec global_page_attrs() :: map()
  def global_page_attrs do
    page_attrs(%{
      logs: global(),
      title: "Logs",
      subtitle: "Live system and run logs",
      nav_items: AssetCataloguePage.nav_items(:logs),
      scope: :global,
      filter: %Filter{},
      empty_state: "No logs yet."
    })
  end

  @doc """
  Assigns for the run logs page, including the step strip: one step failed, so
  the strip must say so before the operator reads a single log line.
  """
  @spec run_page_attrs() :: map()
  def run_page_attrs do
    page_attrs(%{
      logs: for_run("run_2026_06_12"),
      title: "Run logs",
      subtitle: "run_2026_06_12 · customer_orders_daily",
      status: "Failed",
      status_tone: :error,
      nav_items: AssetCataloguePage.nav_items(:runs),
      back_href: "/runs/run_2026_06_12",
      back_label: "Back to run",
      facts: Enum.take(facts(), 2),
      scope: :run,
      run_id: "run_2026_06_12",
      run_steps: run_steps(),
      filter: %Filter{run_id: "run_2026_06_12"},
      empty_state: "No logs recorded for this run yet."
    })
  end

  @doc """
  The per-step results the run logs page links to.
  """
  @spec run_steps() :: [map()]
  def run_steps do
    [
      %{
        id: "01",
        display_name: "stg_orders",
        status: "Succeeded",
        status_tone: :success,
        duration: "48 s"
      },
      %{
        id: "02",
        display_name: "stg_customers",
        status: "Succeeded",
        status_tone: :success,
        duration: "31 s"
      },
      %{
        id: "03",
        display_name: "customer_orders_daily",
        status: "Failed",
        status_tone: :error,
        duration: "2.1 s"
      }
    ]
  end

  @doc """
  Assigns for the asset-step logs page.
  """
  @spec asset_page_attrs(map()) :: map()
  def asset_page_attrs(overrides \\ %{}) do
    page_attrs(
      Map.merge(
        %{
          logs: for_asset_step("run_2026_06_12", "03"),
          title: "customer_orders_daily",
          subtitle: "Run run_2026_06_12 · Asset step 03",
          status: "Running",
          status_tone: :info,
          nav_items: AssetCataloguePage.nav_items(:runs),
          back_href: "/runs/run_2026_06_12",
          back_label: "Back to run",
          facts: facts(),
          scope: :asset,
          filter: %Filter{run_id: "run_2026_06_12", asset_step_id: "03"},
          empty_state: "No logs recorded for this asset step yet."
        },
        overrides
      )
    )
  end

  @doc """
  The output metadata a successful asset step persists.
  """
  @spec output_metadata() :: map()
  def output_metadata do
    %{
      rows_written: 1_820,
      rows_read: 1_820,
      relation: "analytics.customer_orders_daily",
      mode: :monthly_replace,
      partition_month: "2026-06",
      loaded_at: ~U[2026-06-12 09:43:26Z]
    }
  end

  @doc """
  The toolbar facts a run-scoped log page shows.
  """
  @spec facts() :: [map()]
  def facts do
    [
      %{label: "Started", value: "09:41:12"},
      %{label: "Duration", value: "02:14"},
      %{label: "Attempt", value: "1/3"}
    ]
  end

  defp sql do
    """
    Running SQL:
    SELECT customer_id, order_id, total_amount
    FROM raw.orders
    WHERE order_date >= '2026-06-12'\
    """
  end
end
