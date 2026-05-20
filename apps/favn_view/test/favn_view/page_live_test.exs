defmodule FavnView.PageLiveTest do
  use FavnView.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias Favn.Log.Entry
  alias Favn.Manifest
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Version
  alias Favn.Run.AssetResult
  alias Favn.Run.NodeResult
  alias Favn.Window.Policy
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnView.Components.AssetDetailPage
  alias FavnView.Components.RunDetailPage.Timeline
  alias FavnView.Auth.BrowserSessionStore
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup %{conn: conn} do
    ensure_auth_store_started()
    :ok = AuthStore.reset()
    :ok = BrowserSessionStore.reset()
    Memory.reset()

    version = manifest_version()
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)
    assert :ok = Storage.put_run(run_state(:customer_orders_daily, :ok, -600))
    assert :ok = Storage.put_run(run_state(:raw_payments, :running, -30))
    assert :ok = Storage.put_run(pipeline_run_state())

    assert :ok =
             Storage.put_run(empty_run_state(:stg_payments, :running, "run_empty_running"))

    assert :ok =
             Storage.put_run(failed_run_state(:stg_payments, "run_failed_customer_orders"))

    assert :ok =
             Storage.put_run(
               failed_run_state(:customer_orders_daily, "run_failed_customer_daily")
             )

    assert :ok =
             Storage.put_run(empty_run_state(:stg_payments, :error, "run_failed_empty"))

    assert :ok = Storage.put_run(node_results_run_state())
    seed_freshness_states!()

    seed_run_events!("run_customer_orders_daily")
    seed_run_events!("run_failed_empty", :error)

    {:ok, conn: authenticate_conn(conn)}
  end

  test "redirects home to the asset catalogue", %{conn: conn} do
    assert {:error, {:redirect, %{to: "/assets"}}} = live(conn, ~p"/")
  end

  test "renders the asset catalogue", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/assets")

    assert html =~ "Asset catalogue"
    assert html =~ "Browse and monitor all assets"
    assert has_element?(view, ~s([data-testid="asset-table"]))
    assert has_element?(view, ~s([data-testid="asset-card-list"]))

    assert has_element?(view, "a", "customer_orders_daily")

    assert html =~ "Healthy"
    assert html =~ "10m ago"

    assert has_element?(view, ~s([aria-label="View modes"]))
    assert has_element?(view, ~s([aria-label="Connection filter"]))
    assert has_element?(view, ~s([aria-label="Catalogue filter"]))
  end

  test "renders the runs list", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/runs")

    assert html =~ "Runs"
    assert html =~ "Backfills and runs"
    assert has_element?(view, ~s([data-testid="execution-groups-table"]))
    assert has_element?(view, ~s([data-testid="execution-group-card-list"]))
    assert has_element?(view, ~s([data-testid="runs-summary-band"]))

    assert has_element?(view, ~s(a[href="/runs/run_customer_orders_daily"]), "run_custo..._daily")
    assert html =~ "customer_orders_daily"
    assert html =~ "Succeeded"
    assert html =~ "1 / 1 attempts"
    assert html =~ "2 / 3 attempts"
    assert html =~ "+2"
  end

  test "renders execution groups without child runs as equal top-level rows", %{conn: conn} do
    %{parent_id: parent_id, child_ids: [child_id | _]} = seed_backfill_overview_group!()

    {:ok, view, _html} = live(conn, ~p"/runs")

    assert has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="#{parent_id}"])
           )

    refute has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="#{child_id}"])
           )

    view
    |> element(
      ~s([data-testid="execution-group-row"][data-group-id="#{parent_id}"] [data-testid="toggle-execution-group"])
    )
    |> render_click()

    assert has_element?(view, ~s([data-testid="child-runs-table"]))
    assert has_element?(view, ~s([data-testid="child-run-row"][data-run-id="#{child_id}"]))

    assert has_element?(
             view,
             ~s(a[href="/runs/#{parent_id}?view=windows&child_run_id=#{child_id}"]),
             "Open window run"
           )
  end

  test "execution group health reflects failed and running children", %{conn: conn} do
    %{parent_id: parent_id} = seed_backfill_overview_group!()

    {:ok, view, _html} = live(conn, ~p"/runs")

    assert has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="#{parent_id}"]),
             "Failed"
           )

    assert has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="#{parent_id}"]),
             "2 / 3 attempts"
           )

    assert has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="#{parent_id}"]),
             "May 2026"
           )
  end

  test "filters execution groups by failed running incomplete and search", %{conn: conn} do
    %{parent_id: parent_id} = seed_backfill_overview_group!()

    {:ok, view, _html} = live(conn, ~p"/runs")

    view
    |> element(~s([data-testid="execution-group-filters"]))
    |> render_change(%{"filters" => %{"only_failed" => "true"}})

    assert has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="#{parent_id}"])
           )

    refute has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="run_customer_orders_daily"])
           )

    view
    |> element(~s([data-testid="execution-group-filters"]))
    |> render_change(%{"filters" => %{"only_failed" => "false", "only_running" => "true"}})

    assert has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="run_empty_running"])
           )

    refute has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="run_customer_orders_daily"])
           )

    view
    |> element(~s([data-testid="execution-group-filters"]))
    |> render_change(%{"filters" => %{"only_running" => "false", "only_incomplete" => "true"}})

    assert has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="run_empty_running"])
           )

    view
    |> element(~s([data-testid="execution-group-filters"]))
    |> render_change(%{
      "filters" => %{"only_incomplete" => "false", "search" => "overview_backfill"}
    })

    assert has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="#{parent_id}"])
           )

    refute has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="run_customer_orders_daily"])
           )
  end

  test "runs overview shows empty state", %{conn: conn} do
    Memory.reset()
    conn = authenticate_conn(conn)

    {:ok, view, _html} = live(conn, ~p"/runs")

    assert has_element?(view, ~s([data-testid="runs-empty-state"]), "No runs yet")
  end

  test "runs overview shows filtered empty state for unmatched search", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs")

    view
    |> element(~s([data-testid="execution-group-filters"]))
    |> render_change(%{"filters" => %{"search" => "not_real"}})

    assert has_element?(
             view,
             ~s([data-testid="runs-filtered-empty-state"]),
             "No runs found"
           )
  end

  test "renders the pipelines list", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/pipelines")

    assert html =~ "Pipelines"
    assert html =~ "Monitor active manifest pipelines"
    assert has_element?(view, ~s([data-testid="pipelines-table"]))
    assert has_element?(view, ~s([data-testid="pipeline-card-list"]))

    assert html =~ "daily_orders"
    assert has_element?(view, ~s(a[href="#{pipeline_detail_path()}"]), "daily_orders")
    assert html =~ "customer_orders_daily"
    assert html =~ "Include deps"
    assert html =~ "Day Europe/Oslo"
    assert html =~ "Healthy"
    assert html =~ "5.0 s"
  end

  test "renders the pipeline detail page with run history and actions", %{conn: conn} do
    {:ok, view, html} = live(conn, pipeline_detail_path())

    assert html =~ "daily_orders"
    assert has_element?(view, ~s([data-testid="pipeline-summary-panel"]), "customer_orders_daily")
    assert has_element?(view, ~s([data-testid="pipeline-actions-panel"]), "Run pipeline")
    assert has_element?(view, ~s([data-testid="pipeline-backfill-form"]))
    assert has_element?(view, ~s([data-testid="run-pipeline-button"][disabled]))
    assert has_element?(view, ~s([data-testid="pipeline-run-disabled-help"]), "explicit window")
    assert has_element?(view, ~s([data-testid="pipeline-backfill-defaults"]), "day")
    assert has_element?(view, ~s([data-testid="pipeline-backfill-defaults"]), "Europe/Oslo")
    assert has_element?(view, ~s([data-testid="pipeline-runs-table"]), "run_daily_orders")
  end

  test "pipeline detail does not invent an implicit window for normal pipeline runs", %{
    conn: conn
  } do
    {:ok, view, _html} = live(conn, pipeline_detail_path())

    html =
      view
      |> element(~s([data-testid="run-pipeline-form"]))
      |> render_submit()

    refute html =~ "Pipeline run submitted"
    refute html =~ "pipeline-run-error"
  end

  test "pipeline detail submits a pipeline run and navigates to run detail", %{conn: conn} do
    {:ok, view, _html} = live(conn, pipeline_detail_path("full_refresh"))

    view
    |> element(~s([data-testid="run-pipeline-form"]))
    |> render_submit()

    assert {run_path, %{"info" => "Pipeline run submitted"}} = assert_redirect(view)
    assert String.starts_with?(run_path, "/runs/run_")

    run_id = String.replace_prefix(run_path, "/runs/", "")
    assert {:ok, run} = Storage.get_run(run_id)
    assert run.submit_kind == :pipeline
    assert run.metadata.pipeline_submit_ref == __MODULE__.Pipelines.FullRefresh
    assert run.metadata.refresh_policy == %{mode: :auto, refs: [], include_upstream?: false}
  end

  test "viewer cannot submit a pipeline run with a forged LiveView event", %{conn: _conn} do
    conn = authenticate_conn(build_conn(), :viewer)
    {:ok, view, _html} = live(conn, pipeline_detail_path("full_refresh"))

    html =
      view
      |> element(~s([data-testid="run-pipeline-form"]))
      |> render_submit()

    assert html =~ "Operator role required"
  end

  test "pipeline detail disables backfill for non-windowed pipelines", %{conn: conn} do
    {:ok, view, _html} = live(conn, pipeline_detail_path("full_refresh"))

    assert has_element?(view, ~s([data-testid="submit-backfill-button"][disabled]))

    assert has_element?(
             view,
             ~s([data-testid="pipeline-backfill-disabled-help"]),
             "windowed pipeline"
           )

    refute has_element?(view, ~s([data-testid="pipeline-backfill-defaults"]))
  end

  test "pipeline detail submits a backfill using pipeline window defaults", %{conn: conn} do
    {:ok, view, _html} = live(conn, pipeline_detail_path())

    view
    |> element(~s([data-testid="pipeline-backfill-form"]))
    |> render_submit(%{
      "backfill" => %{
        "from" => "2026-01-01",
        "to" => "2026-01-02",
        "kind" => "day",
        "timezone" => "Europe/Oslo",
        "refresh" => "force"
      }
    })

    assert {run_path, %{"info" => "Pipeline backfill submitted"}} = assert_redirect(view)
    run_id = String.replace_prefix(run_path, "/runs/", "")
    assert {:ok, run} = Storage.get_run(run_id)
    assert run.submit_kind == :backfill_pipeline
    assert run.metadata.backfill.kind == :day
    assert run.metadata.backfill.timezone == "Europe/Oslo"

    assert {:ok, runs} = Storage.list_runs()

    assert Enum.any?(runs, fn child ->
             child.parent_run_id == run_id and
               child.metadata.refresh_policy == %{
                 mode: :force,
                 refs: [],
                 include_upstream?: false
               }
           end)
  end

  test "viewer cannot submit a pipeline backfill with a forged LiveView event", %{conn: _conn} do
    conn = authenticate_conn(build_conn(), :viewer)
    {:ok, view, _html} = live(conn, pipeline_detail_path())

    html =
      view
      |> element(~s([data-testid="pipeline-backfill-form"]))
      |> render_submit(%{
        "backfill" => %{
          "from" => "2026-01-01",
          "to" => "2026-01-02",
          "kind" => "day",
          "timezone" => "Europe/Oslo"
        }
      })

    assert html =~ "Operator role required"
  end

  test "runs list refreshes active runs", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs")

    assert has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="run_empty_running"]),
             "Running"
           )

    {:ok, active_run} = Storage.get_run("run_empty_running")

    terminal_run =
      RunState.transition(active_run,
        status: :ok,
        result: %{asset_results: terminal_asset_results(:stg_payments)}
      )

    assert :ok =
             Storage.persist_run_transition(terminal_run, %{
               run_id: terminal_run.id,
               sequence: 3,
               event_type: :run_finished,
               occurred_at: DateTime.utc_now(),
               status: :ok,
               data: %{message: "Run finished"}
             })

    send(view.pid, :refresh_runs)

    assert has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="run_empty_running"]),
             "Succeeded"
           )

    refute has_element?(
             view,
             ~s([data-testid="execution-group-row"][data-group-id="run_empty_running"]),
             "Running"
           )
  end

  test "filters assets by search, connection, and catalogue", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/assets")

    view
    |> element(~s(form[phx-change="filter_assets"]))
    |> render_change(%{
      "filters" => %{"search" => "payments", "connection" => "s3", "catalogue" => "finance"}
    })

    assert has_element?(view, "a", "raw_payments")
    refute has_element?(view, ~s(a[href*="stg_payments"]), "stg_payments")
  end

  test "shows empty state when filters do not match", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/assets")

    view
    |> element(~s(form[phx-change="filter_assets"]))
    |> render_change(%{
      "filters" => %{"search" => "not_real", "connection" => "all", "catalogue" => "all"}
    })

    assert has_element?(view, ~s([data-testid="asset-empty-state"]), "No assets found")
  end

  test "renders the selected asset detail page", %{conn: conn} do
    {:ok, view, html} = live(conn, detail_path(:customer_orders_daily))

    assert html =~ "customer_orders_daily"
    assert has_element?(view, ~s([data-testid="asset-freshness-summary"]), "Stale")
    assert has_element?(view, ~s([data-testid="asset-freshness-summary"]), "daily Europe/Oslo")
    assert html =~ "Refresh timeline"
    assert has_element?(view, ~s([data-testid="window-timeline-panel"]))
    assert has_element?(view, ~s([aria-label="View modes"]))
  end

  test "asset detail renders stale freshness explanation in details mode", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Details"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="asset-freshness-detail-panel"]), "Stale")
    assert has_element?(view, ~s([data-testid="asset-freshness-reasons"]), "raw_payments")
    assert has_element?(view, ~s([data-testid="asset-freshness-reasons"]), "raw:v1")
    assert has_element?(view, ~s([data-testid="asset-freshness-reasons"]), "raw:v2")
  end

  test "asset detail renders unknown freshness explanation", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:stg_payments))

    assert has_element?(view, ~s([data-testid="asset-freshness-summary"]), "Unknown")
    assert has_element?(view, ~s([data-testid="asset-freshness-summary"]), "No freshness policy")

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Details"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="asset-freshness-reasons"]), "No freshness policy")
  end

  test "asset detail renders always-run freshness explanation", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:always_refresh))

    assert has_element?(view, ~s([data-testid="asset-freshness-summary"]), "Always run")
    assert has_element?(view, ~s([data-testid="asset-freshness-summary"]), "always run")

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Details"]))
    |> render_click()

    assert has_element?(
             view,
             ~s([data-testid="asset-freshness-reasons"]),
             "Manifest policy is always run"
           )
  end

  test "asset detail component tolerates missing or partial freshness detail" do
    attrs = %{
      title: "partial_freshness_asset",
      status: "Unknown",
      status_tone: :neutral,
      window_range: "No windows",
      nav_items: AssetDetailPage.sample_nav_items(),
      timeline: AssetDetailPage.sample_timeline(),
      selected_window: AssetDetailPage.selected_sample_window(),
      active_mode: :timeline,
      freshness: nil
    }

    assert render_component(&AssetDetailPage.asset_detail_page/1, attrs) =~ "Refresh timeline"

    html =
      render_component(&AssetDetailPage.asset_detail_page/1, %{
        attrs
        | active_mode: :details,
          freshness: %{state: :unknown, explanation: "Backend returned partial freshness detail."}
      })

    assert html =~ "Backend returned partial freshness detail."
    assert html =~ "policy unavailable"
  end

  test "asset detail timeline renders backend-provided window labels" do
    yearly_window = %{
      id: "window:year:2026",
      label: "2026",
      date_label: "2026",
      range_label: "2026",
      status: :muted,
      run_enabled?: true,
      run_disabled_reason: nil,
      run_label: "Run this window"
    }

    html =
      render_component(&AssetDetailPage.asset_detail_page/1, %{
        title: "yearly_asset",
        status: "Unknown",
        status_tone: :neutral,
        window_kind_label: "Yearly windows",
        refresh_cadence_label: "Yearly refresh periods",
        window_range: "1997 - 2026",
        nav_items: AssetDetailPage.sample_nav_items(),
        timeline: [yearly_window],
        selected_window: yearly_window,
        active_mode: :timeline,
        freshness: nil
      })

    assert html =~ "Yearly refresh periods"
    assert html =~ "2026"
    refute html =~ "Jan"
  end

  test "asset detail defaults to timeline mode", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    assert has_element?(view, ~s([data-testid="window-timeline-panel"]), "Refresh timeline")

    assert has_element?(
             view,
             ~s([data-testid="selected-window-actions"]),
             "No timeline context selected"
           )

    assert has_element?(view, "[data-testid='run-selected-window']:not([disabled])")
    refute has_element?(view, ~s([data-testid="create-backfill"]))
    refute has_element?(view, ~s([data-testid="asset-mode-placeholder"]))
  end

  test "run selected window opens run config panel with defaults", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    view
    |> element(~s([data-testid="run-selected-window"]), "Run asset")
    |> render_click()

    assert has_element?(view, ~s([data-testid="run-config-panel"]), "Plan scope / dependencies")
    assert has_element?(view, ~s(input[name="run_config[dependencies]"][value="all"][checked]))
    assert has_element?(view, ~s(input[name="run_config[refresh]"][value="auto"][checked]))
    assert has_element?(view, ~s([data-testid="run-config-window-kind"]), "Day")
    assert has_element?(view, ~s([data-testid="run-config-window-value"]))
    assert has_element?(view, ~s(input[name="run_config[source]"][value="refresh_timeline"]))
  end

  test "selected failed timeline item prepopulates failed run config", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))
    failed_date = Date.utc_today() |> Date.add(-1) |> Date.to_iso8601()
    window_id = "refresh:day:#{failed_date}"

    view
    |> element(~s([data-testid="timeline-window-#{window_id}"]))
    |> render_click()

    open_run_config(view)

    assert has_element?(view, ~s(input[name="run_config[dependencies]"][value="none"][checked]))
    assert has_element?(view, ~s(input[name="run_config[refresh]"][value="force_all"][checked]))
  end

  test "run selected window submits default auto config and navigates to run detail", %{
    conn: conn
  } do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    open_run_config(view)

    view
    |> element(~s([data-testid="run-config-form"]))
    |> render_submit(%{"run_config" => %{"dependencies" => "all", "refresh" => "auto"}})

    assert {run_path, %{"info" => "Run submitted"}} = assert_redirect(view)
    assert String.starts_with?(run_path, "/runs/run_")
    assert_submitted_refresh(run_path, :all, %{mode: :auto, refs: [], include_upstream?: false})

    {:ok, run_view, html} = live(conn, run_path)

    assert html =~ "run_"
    assert has_element?(run_view, ~s([data-testid="run-overview-panel"]))
  end

  test "viewer cannot submit an asset run with a forged LiveView event", %{conn: _conn} do
    conn = authenticate_conn(build_conn(), :viewer)
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    html = render_submit(view, "run_selected_window", %{"run_config" => %{}})

    assert html =~ "Operator role required"
  end

  test "run selected window submits missing refresh config", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    open_run_config(view)

    view
    |> element(~s([data-testid="run-config-form"]))
    |> render_submit(%{"run_config" => %{"dependencies" => "all", "refresh" => "missing"}})

    assert {run_path, _flash} = assert_redirect(view)

    assert_submitted_refresh(run_path, :all, %{mode: :missing, refs: [], include_upstream?: false})
  end

  test "run selected window submits force selected refresh config", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    open_run_config(view)

    view
    |> element(~s([data-testid="run-config-form"]))
    |> render_submit(%{"run_config" => %{"dependencies" => "all", "refresh" => "force_selected"}})

    assert {run_path, _flash} = assert_redirect(view)

    assert_submitted_refresh(run_path, :all, %{
      mode: :force_assets,
      refs: [{__MODULE__.Assets, :customer_orders_daily}],
      include_upstream?: false
    })
  end

  test "run selected window submits force selected plus upstream refresh config", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    open_run_config(view)

    view
    |> element(~s([data-testid="run-config-form"]))
    |> render_submit(%{
      "run_config" => %{"dependencies" => "all", "refresh" => "force_selected_upstream"}
    })

    assert {run_path, _flash} = assert_redirect(view)

    assert_submitted_refresh(run_path, :all, %{
      mode: :force_assets,
      refs: [{__MODULE__.Assets, :customer_orders_daily}],
      include_upstream?: true
    })
  end

  test "run asset range defaults visible refresh and child runs to missing", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    open_run_config(view)

    assert has_element?(view, ~s(input[name="run_config[refresh]"][value="auto"][checked]))

    assert has_element?(
             view,
             ~s([data-testid="run-config-panel"]),
             "Range backfills default to missing refresh"
           )

    view
    |> element(~s([data-testid="run-config-form"]))
    |> render_change(%{
      "run_config" => %{
        "dependencies" => "all",
        "refresh" => "auto",
        "source" => "refresh_timeline",
        "kind" => "day",
        "value" => "2026-06-12",
        "to" => "2026-06-13",
        "timezone" => "Etc/UTC"
      }
    })

    assert has_element?(view, ~s(input[name="run_config[refresh]"][value="missing"][checked]))

    view
    |> element(~s([data-testid="run-config-form"]))
    |> render_submit(%{
      "run_config" => %{
        "dependencies" => "all",
        "source" => "refresh_timeline",
        "kind" => "day",
        "value" => "2026-06-12",
        "to" => "2026-06-13",
        "timezone" => "Etc/UTC"
      }
    })

    assert {run_path, %{"info" => "Asset backfill submitted"}} = assert_redirect(view)
    parent_run_id = String.replace_prefix(run_path, "/runs/", "")

    assert {:ok, runs} = Storage.list_runs()

    children = Enum.filter(runs, &(&1.parent_run_id == parent_run_id))
    assert length(children) == 2
    assert Enum.all?(children, &(&1.metadata[:refresh_policy].mode == :missing))
  end

  test "run asset form submits an inclusive forced backfill range", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    open_run_config(view)

    view
    |> element(~s([data-testid="run-config-form"]))
    |> render_submit(%{
      "run_config" => %{
        "dependencies" => "all",
        "refresh" => "force_all",
        "source" => "refresh_timeline",
        "kind" => "day",
        "value" => "2026-06-12",
        "to" => "2026-06-13",
        "timezone" => "Etc/UTC"
      }
    })

    assert {run_path, %{"info" => "Asset backfill submitted"}} = assert_redirect(view)
    parent_run_id = String.replace_prefix(run_path, "/runs/", "")

    assert {:ok, parent} = Storage.get_run(parent_run_id)
    assert parent.submit_kind == :backfill_asset

    assert {:ok, runs} = Storage.list_runs()

    range_runs =
      Enum.filter(runs, fn run ->
        run.parent_run_id == parent_run_id and
          run.asset_ref == {__MODULE__.Assets, :customer_orders_daily} and
          run.metadata[:refresh_policy] == %{mode: :force, refs: [], include_upstream?: false} and
          get_in(run.metadata, [:selected_window, :kind]) == :day and
          get_in(run.metadata, [:selected_window, :start_at]) in [
            ~U[2026-06-12 00:00:00Z],
            ~U[2026-06-13 00:00:00Z]
          ]
      end)

    assert length(range_runs) == 2

    assert {:ok, page} = Storage.list_backfill_windows(backfill_run_id: parent_run_id)
    assert length(page.items) == 2
  end

  test "full-refresh asset can run without data coverage windows", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:stg_payments))

    assert has_element?(view, ~s([data-testid="run-selected-window"]), "Run asset")
    refute has_element?(view, ~s([data-testid="data-coverage-timeline-toggle"]))
    refute has_element?(view, ~s([data-testid="create-backfill"]))

    assert has_element?(
             view,
             ~s([data-testid="selected-window-actions"]),
             "No timeline context selected"
           )

    render_click(view, "open_run_config", %{})
    assert has_element?(view, ~s([data-testid="run-config-panel"]))
    refute has_element?(view, ~s([data-testid="run-config-panel"]), "Timeline context")
  end

  test "asset detail mode rail changes the central panel", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Runs"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="asset-mode-placeholder"]), "Runs coming soon")
    refute has_element?(view, ~s([data-testid="window-timeline-panel"]))
    assert has_element?(view, ~s(button[aria-label="Runs"][aria-pressed="true"]))
  end

  test "clicking a timeline window changes the selected window", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))
    window_id = "refresh:day:#{Date.utc_today() |> Date.add(-29) |> Date.to_iso8601()}"
    window_label = Date.utc_today() |> Date.add(-29) |> Calendar.strftime("%b %-d, %Y")

    view
    |> element(~s([data-testid="timeline-window-#{window_id}"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="selected-window-actions"]), window_label)

    assert has_element?(
             view,
             ~s([data-testid="timeline-window-#{window_id}"][aria-pressed="true"])
           )

    assert has_element?(
             view,
             ~s([data-testid="timeline-window-#{window_id}"][aria-label*="unknown"])
           )

    view
    |> element(~s([data-testid="timeline-window-#{window_id}"]))
    |> render_click()

    assert has_element?(
             view,
             ~s([data-testid="selected-window-actions"]),
             "No timeline context selected"
           )

    assert has_element?(
             view,
             ~s([data-testid="timeline-window-#{window_id}"][aria-pressed="false"])
           )
  end

  test "asset detail ignores invalid mode and window events", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    assert render_click(view, "set_mode", %{"mode" => "not_real"}) =~ "Refresh timeline"

    assert has_element?(
             view,
             ~s([data-testid="selected-window-actions"]),
             "No timeline context selected"
           )

    assert render_click(view, "select_window", %{"window-id" => "not-real"}) =~ today_label()
    assert has_element?(view, ~s([data-testid="window-timeline-panel"]))
  end

  test "shows not-found state for unknown asset ids", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/assets/not_real")

    assert html =~ "Asset not found"
    assert has_element?(view, ~s([data-testid="asset-not-found-state"]))
  end

  test "renders existing run detail overview", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/runs/run_customer_orders_daily")

    assert html =~ "run_customer_order"
    assert has_element?(view, ~s([data-testid="run-overview-panel"]))
    assert has_element?(view, ~s([data-testid="run-overview-panel"]), "Succeeded")
    refute html =~ "Healthy"

    assert has_element?(view, ~s([data-testid="execution-group-header"]))
    refute has_element?(view, ~s([data-testid="run-detail-tabs"]))
  end

  test "run detail renders events mode when present", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily")

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Events"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="run-event-timeline"]), "Run started")
    assert has_element?(view, ~s([data-testid="run-event-timeline"]), "Run finished")
  end

  test "run detail renders asset results when present", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily")
    asset_step_id = asset_step_id("run_customer_orders_daily", :customer_orders_daily)

    assert has_element?(view, ~s([data-testid="run-asset-results"]), "customer_orders_daily")
    assert has_element?(view, ~s([data-testid="run-asset-results"]), "Stage 0")
    assert has_element?(view, ~s([data-testid="run-asset-result-row"][data-asset-step-id]))

    view
    |> element(~s(a[data-testid="run-asset-result-row"][data-asset-step-id="#{asset_step_id}"]))
    |> render_click()

    assert {path, _flash} = assert_redirect(view)
    assert path == ~p"/runs/run_customer_orders_daily/assets/#{asset_step_id}/logs"
  end

  test "run detail prefers node results for execution rows", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_node_results")

    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Skipped fresh")
    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Retrying")
    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Blocked")
    assert has_element?(view, ~s([data-testid="run-asset-result-row"][data-asset-step-id]))

    refute has_element?(
             view,
             ~s([data-testid="run-asset-result-row"]),
             "asset aggregate fallback"
           )
  end

  test "active run refreshes from running to terminal", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_empty_running")

    assert has_element?(view, ~s([data-testid="run-overview-panel"][data-run-active="true"]))
    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Waiting")

    {:ok, active_run} = Storage.get_run("run_empty_running")

    terminal_run =
      RunState.transition(active_run,
        status: :ok,
        result: %{asset_results: terminal_asset_results(:stg_payments)}
      )

    assert :ok =
             Storage.persist_run_transition(terminal_run, %{
               run_id: terminal_run.id,
               sequence: 3,
               event_type: :run_finished,
               occurred_at: DateTime.utc_now(),
               status: :ok,
               data: %{message: "Run finished"}
             })

    send(view.pid, :refresh_run)

    assert render(view) =~ "Succeeded"
    assert has_element?(view, ~s([data-testid="run-overview-panel"][data-run-active="false"]))
  end

  test "run detail reloads from live run events", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_empty_running")

    assert has_element?(view, ~s([data-testid="run-overview-panel"][data-run-active="true"]))

    {:ok, active_run} = Storage.get_run("run_empty_running")

    terminal_run =
      RunState.transition(active_run,
        status: :ok,
        result: %{asset_results: terminal_asset_results(:stg_payments)}
      )

    event = %{
      run_id: terminal_run.id,
      sequence: 3,
      event_type: :run_finished,
      occurred_at: DateTime.utc_now(),
      status: :ok,
      data: %{message: "Run finished"}
    }

    assert :ok = Storage.persist_run_transition(terminal_run, event)
    send(view.pid, {:favn_run_event, RunEvent.from_map(event)})

    assert render(view) =~ "Succeeded"
    assert has_element?(view, ~s([data-testid="run-overview-panel"][data-run-active="false"]))
  end

  test "terminal run does not render as refreshing", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily")

    assert has_element?(view, ~s([data-testid="run-overview-panel"][data-run-active="false"]))
  end

  test "running run with no asset results shows waiting state and no fake rows", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_empty_running")

    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Waiting")
    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "stg_payments")

    assert has_element?(
             view,
             ~s([data-testid="run-current-activity"]),
             "Waiting for first execution event"
           )
  end

  test "run detail hides nil latest event activity", %{conn: conn} do
    assert :ok =
             Storage.append_run_event("run_empty_running", %{
               run_id: "run_empty_running",
               sequence: 3,
               event_type: :run_started,
               occurred_at: DateTime.utc_now(),
               status: nil,
               asset_ref: "nil",
               data: %{}
             })

    {:ok, view, _html} = live(conn, ~p"/runs/run_empty_running")

    refute has_element?(view, ~s([data-testid="run-current-activity"]))
    refute render(view) =~ "Latest event: nil"
    refute render(view) =~ "Asset nil"
  end

  test "run detail derives active asset rows from step events before results persist", %{
    conn: conn
  } do
    step_id = "run_empty_running-stg-payments"
    ref = {__MODULE__.Assets, :stg_payments}

    assert :ok =
             Storage.append_run_event("run_empty_running", %{
               run_id: "run_empty_running",
               sequence: 3,
               event_type: :step_started,
               occurred_at: DateTime.utc_now(),
               status: :running,
               data: %{
                 asset_ref: ref,
                 asset_step_id: step_id,
                 stage: 0,
                 attempt: 1,
                 runner_execution_id: "exec_step_active"
               }
             })

    {:ok, view, _html} = live(conn, ~p"/runs/run_empty_running")

    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Running")
    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Stage 0")
    refute has_element?(view, ~s([data-testid="run-asset-result-row"]), "Waiting")
    refute has_element?(view, ~s([data-testid="run-asset-results-empty"]))
  end

  test "run detail does not duplicate event and persisted rows for same asset", %{conn: conn} do
    ref = {__MODULE__.Assets, :customer_orders_daily}

    assert :ok =
             Storage.append_run_event("run_customer_orders_daily", %{
               run_id: "run_customer_orders_daily",
               sequence: 3,
               event_type: :step_started,
               occurred_at: DateTime.add(DateTime.utc_now(), -5, :second),
               status: :running,
               data: %{
                 asset_ref: ref,
                 asset_step_id: "different-live-step-id",
                 stage: 0,
                 attempt: 1
               }
             })

    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily")
    html = render(view)

    assert asset_result_row_count(html) == 1
    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "customer_orders_daily")
    refute has_element?(view, ~s([data-testid="run-asset-result-row"]), "Running")
  end

  test "terminal run detail suppresses stale event rows for duplicate asset refs", %{conn: conn} do
    ref = {__MODULE__.Assets, :stg_payments}

    assert :ok = Storage.put_run(same_asset_node_results_run_state())

    assert :ok =
             Storage.append_run_event("run_same_asset_nodes", %{
               run_id: "run_same_asset_nodes",
               sequence: 3,
               event_type: :step_started,
               occurred_at: DateTime.add(DateTime.utc_now(), -2, :second),
               status: :running,
               data: %{
                 asset_ref: ref,
                 asset_step_id: "live-window-a",
                 stage: 0,
                 attempt: 1
               }
             })

    assert :ok =
             Storage.append_run_event("run_same_asset_nodes", %{
               run_id: "run_same_asset_nodes",
               sequence: 4,
               event_type: :step_started,
               occurred_at: DateTime.add(DateTime.utc_now(), -1, :second),
               status: :running,
               data: %{
                 asset_ref: ref,
                 asset_step_id: "live-window-b",
                 stage: 1,
                 attempt: 1
               }
             })

    {:ok, view, _html} = live(conn, ~p"/runs/run_same_asset_nodes")
    html = render(view)

    assert asset_result_row_count(html) == 2
    assert html =~ "window:day:2026-06-12"
    assert html =~ "window:day:2026-06-13"
    refute has_element?(view, ~s([data-testid="run-asset-result-row"]), "Running")
  end

  test "run detail renders retrying step event before results persist", %{conn: conn} do
    step_id = "run_empty_running-stg-payments"
    ref = {__MODULE__.Assets, :stg_payments}

    assert :ok =
             Storage.append_run_event("run_empty_running", %{
               run_id: "run_empty_running",
               sequence: 3,
               event_type: :step_started,
               occurred_at: DateTime.add(DateTime.utc_now(), -1, :second),
               status: :running,
               data: %{
                 asset_ref: ref,
                 asset_step_id: step_id,
                 stage: 0,
                 attempt: 1
               }
             })

    assert :ok =
             Storage.append_run_event("run_empty_running", %{
               run_id: "run_empty_running",
               sequence: 4,
               event_type: :step_retry_scheduled,
               occurred_at: DateTime.utc_now(),
               status: :retrying,
               data: %{
                 asset_ref: ref,
                 asset_step_id: step_id,
                 stage: 0,
                 attempt: 1
               }
             })

    {:ok, view, _html} = live(conn, ~p"/runs/run_empty_running")

    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Retrying")

    assert has_element?(
             view,
             ~s([data-testid="run-asset-result-row"]),
             "Retry has been scheduled"
           )

    refute has_element?(view, ~s([data-testid="run-asset-result-row"]), "Waiting")
  end

  test "run detail shows failed step event error before final results persist", %{conn: conn} do
    step_id = "run_empty_running-stg-payments"
    ref = {__MODULE__.Assets, :stg_payments}

    assert :ok =
             Storage.append_run_event("run_empty_running", %{
               run_id: "run_empty_running",
               sequence: 3,
               event_type: :step_started,
               occurred_at: DateTime.add(DateTime.utc_now(), -1, :second),
               status: :running,
               data: %{
                 asset_ref: ref,
                 asset_step_id: step_id,
                 stage: 0,
                 attempt: 1
               }
             })

    assert :ok =
             Storage.append_run_event("run_empty_running", %{
               run_id: "run_empty_running",
               sequence: 4,
               event_type: :step_failed,
               occurred_at: DateTime.utc_now(),
               status: :error,
               data: %{
                 "error" => %{"message" => "Warehouse timeout"},
                 asset_ref: ref,
                 asset_step_id: step_id,
                 stage: 0,
                 attempt: 1
               }
             })

    {:ok, view, _html} = live(conn, ~p"/runs/run_empty_running")

    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Failed")
    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Warehouse timeout")
    assert has_element?(view, ~s([data-testid="asset-error-copy-button"]), "Copy error")
  end

  test "run detail marks drain failures separately from root failures", %{conn: conn} do
    root_ref = {__MODULE__.Assets, :stg_payments}
    cascade_ref = {__MODULE__.Assets, :customer_orders_daily}

    events = [
      %{
        run_id: "run_empty_running",
        sequence: 3,
        event_type: :step_started,
        occurred_at: DateTime.add(DateTime.utc_now(), -4, :second),
        status: :running,
        data: %{
          asset_ref: root_ref,
          asset_step_id: "root-step",
          runner_execution_id: "rx_root",
          stage: 0,
          attempt: 1
        }
      },
      %{
        run_id: "run_empty_running",
        sequence: 4,
        event_type: :step_started,
        occurred_at: DateTime.add(DateTime.utc_now(), -4, :second),
        status: :running,
        data: %{
          asset_ref: cascade_ref,
          asset_step_id: "cascade-step",
          runner_execution_id: "rx_cascade",
          stage: 0,
          attempt: 1
        }
      },
      %{
        run_id: "run_empty_running",
        sequence: 5,
        event_type: :step_failed,
        occurred_at: DateTime.add(DateTime.utc_now(), -3, :second),
        status: :error,
        data: %{
          asset_ref: root_ref,
          asset_step_id: "root-step",
          error: %{message: "Postgres pool exhausted"},
          stage: 0,
          attempt: 1
        }
      },
      %{
        run_id: "run_empty_running",
        sequence: 6,
        event_type: :stage_draining_after_failure,
        occurred_at: DateTime.add(DateTime.utc_now(), -2, :second),
        status: :error,
        data: %{
          failed_asset_ref: root_ref,
          pending_execution_ids: ["rx_cascade"],
          stage: 0,
          attempt: 1
        }
      },
      %{
        run_id: "run_empty_running",
        sequence: 7,
        event_type: :step_failed,
        occurred_at: DateTime.add(DateTime.utc_now(), -1, :second),
        status: :error,
        data: %{
          asset_ref: cascade_ref,
          asset_step_id: "cascade-step",
          error: %{message: "query failed after pool exhaustion"},
          stage: 0,
          attempt: 1
        }
      }
    ]

    Enum.each(events, fn event ->
      assert :ok = Storage.append_run_event("run_empty_running", event)
    end)

    {:ok, view, _html} = live(conn, ~p"/runs/run_empty_running")

    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Failed")
    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Cascade failed")
    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "draining in-flight work")
  end

  test "failed run surfaces failed asset and error in overview", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_failed_customer_orders")

    assert has_element?(view, ~s([data-testid="run-overview-panel"]), "Failed")
    assert has_element?(view, ~s([data-testid="run-failure-summary"]), "1 of 2 assets failed")
    assert has_element?(view, ~s([data-testid="run-failure-summary"]), "stg_payments")
    assert has_element?(view, ~s([data-testid="run-failure-summary"]), "Warehouse timeout")
    assert has_element?(view, ~s([data-testid="run-asset-result-row"]), "Failed")
  end

  test "failed run without asset results surfaces latest error event", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_failed_empty")

    assert has_element?(
             view,
             ~s([data-testid="run-asset-results-empty"]),
             "Run failed before asset results"
           )

    assert has_element?(view, ~s([data-testid="run-failure-summary"]), "Warehouse timeout")
    refute has_element?(view, ~s([data-testid="run-asset-result-row"]))
  end

  test "backfill parent surfaces failed child window context", %{conn: conn} do
    parent_id = "run_backfill_parent_failed_child"
    child_id = "run_backfill_child_failed_asset"
    failed_ref = {__MODULE__.Assets, :inventory_by_day}
    started_at = ~U[2026-05-01 00:00:00Z]
    finished_at = DateTime.add(started_at, 2, :second)
    window_key = "day:2026-05-01"

    parent =
      RunState.new(
        id: parent_id,
        manifest_version_id: "mv_view_assets",
        manifest_content_hash: "hash_view_assets",
        asset_ref: failed_ref,
        target_refs: [failed_ref],
        submit_kind: :backfill_pipeline,
        trigger: %{kind: :backfill, pipeline_module: __MODULE__.Pipelines.DailyOrders},
        metadata: %{pipeline_submit_ref: __MODULE__.Pipelines.DailyOrders}
      )
      |> RunState.transition(status: :error, error: :failed, result: %{status: :error})

    child =
      RunState.new(
        id: child_id,
        manifest_version_id: "mv_view_assets",
        manifest_content_hash: "hash_view_assets",
        asset_ref: failed_ref,
        target_refs: [failed_ref],
        submit_kind: :pipeline,
        parent_run_id: parent_id,
        root_run_id: parent_id,
        lineage_depth: 1,
        trigger: %{
          kind: :backfill,
          backfill_run_id: parent_id,
          pipeline_module: __MODULE__.Pipelines.DailyOrders,
          window_key: window_key
        },
        metadata: %{pipeline_submit_ref: __MODULE__.Pipelines.DailyOrders}
      )
      |> RunState.transition(
        status: :error,
        error: :failed,
        result: %{
          node_results: [
            NodeResult.new(%{
              node_key: {failed_ref, window_key},
              ref: failed_ref,
              stage: 0,
              status: :error,
              started_at: started_at,
              finished_at: finished_at,
              duration_ms: 2_000,
              error: %{message: "DuckDB ADBC connection bootstrap failed at attach_mart"},
              asset_step_id: "failed-inventory-step"
            })
          ]
        }
      )

    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: parent_id,
        child_run_id: child_id,
        latest_attempt_run_id: child_id,
        pipeline_module: __MODULE__.Pipelines.DailyOrders,
        manifest_version_id: "mv_view_assets",
        window_kind: :day,
        window_start_at: started_at,
        window_end_at: DateTime.add(started_at, 1, :day),
        timezone: "Etc/UTC",
        window_key: window_key,
        status: :error,
        attempt_count: 1,
        last_error: :failed,
        started_at: started_at,
        finished_at: finished_at,
        updated_at: finished_at
      })

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_run(child)
    assert :ok = Storage.put_backfill_window(window)

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    assert has_element?(view, ~s([data-testid="run-failure-summary"]), "1 backfill window failed")
    assert has_element?(view, ~s([data-testid="backfill-failure-list"]), "Failed backfill window")
    assert has_element?(view, ~s([data-testid="backfill-failure-list"]), "Showing 1 of 1")
    assert has_element?(view, ~s([data-testid="backfill-failure-row"]), "inventory_by_day")

    assert has_element?(
             view,
             ~s([data-testid="backfill-failure-row"]),
             "DuckDB ADBC connection bootstrap failed at attach_mart"
           )

    assert has_element?(
             view,
             ~s(a[href="/runs/#{parent_id}?view=windows&child_run_id=#{child_id}"]),
             "Open window run"
           )
  end

  test "backfill parent surfaces child admission failure without asset results", %{conn: conn} do
    parent_id = "run_backfill_parent_unknown_pool"
    child_id = "run_backfill_child_unknown_pool"
    failed_ref = {__MODULE__.Assets, :inventory_by_day}
    started_at = ~U[2026-05-01 00:00:00Z]
    window_key = "day:2026-05-01"
    error = {:unknown_execution_pool, :mercatus_api}

    parent =
      RunState.new(
        id: parent_id,
        manifest_version_id: "mv_view_assets",
        manifest_content_hash: "hash_view_assets",
        asset_ref: failed_ref,
        target_refs: [failed_ref],
        submit_kind: :backfill_pipeline,
        trigger: %{kind: :backfill, pipeline_module: __MODULE__.Pipelines.DailyOrders},
        metadata: %{pipeline_submit_ref: __MODULE__.Pipelines.DailyOrders}
      )
      |> RunState.transition(status: :error, error: :failed, result: %{status: :error})

    child =
      RunState.new(
        id: child_id,
        manifest_version_id: "mv_view_assets",
        manifest_content_hash: "hash_view_assets",
        asset_ref: failed_ref,
        target_refs: [failed_ref],
        submit_kind: :pipeline,
        parent_run_id: parent_id,
        root_run_id: parent_id,
        lineage_depth: 1,
        trigger: %{
          kind: :backfill,
          backfill_run_id: parent_id,
          pipeline_module: __MODULE__.Pipelines.DailyOrders,
          window_key: window_key
        },
        metadata: %{pipeline_submit_ref: __MODULE__.Pipelines.DailyOrders}
      )
      |> RunState.transition(status: :error, error: error, result: %{status: :error})

    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: parent_id,
        child_run_id: child_id,
        latest_attempt_run_id: child_id,
        pipeline_module: __MODULE__.Pipelines.DailyOrders,
        manifest_version_id: "mv_view_assets",
        window_kind: :day,
        window_start_at: started_at,
        window_end_at: DateTime.add(started_at, 1, :day),
        timezone: "Etc/UTC",
        window_key: window_key,
        status: :error,
        attempt_count: 1,
        last_error: error,
        started_at: started_at,
        finished_at: started_at,
        updated_at: started_at
      })

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_run(child)
    assert :ok = Storage.put_backfill_window(window)

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    assert has_element?(view, ~s([data-testid="run-failure-summary"]), "1 backfill window failed")
    assert has_element?(view, ~s([data-testid="backfill-failure-row"]), "unknown_execution_pool")
    assert has_element?(view, ~s([data-testid="backfill-failure-row"]), "mercatus_api")

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Failures"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="window-failure-row"]), "unknown_execution_pool")
    assert has_element?(view, ~s([data-testid="window-failure-row"]), "mercatus_api")
  end

  test "run detail renders execution group header for backfill", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    assert has_element?(view, ~s([data-testid="execution-group-header"]), "Windows")
    assert has_element?(view, ~s([data-testid="execution-group-id"]), parent_id)
    assert has_element?(view, ~s([data-testid="execution-group-stat-cards"]), "Asset attempts")
    assert has_element?(view, ~s([data-testid="execution-group-stat-cards"]), "Running")
  end

  test "run detail renders asset window matrix with multiple windows", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    assert has_element?(view, ~s([data-testid="asset-window-matrix"]), "Jan 2026")
    assert has_element?(view, ~s([data-testid="asset-window-matrix"]), "Feb 2026")
    assert has_element?(view, ~s([data-testid="asset-window-matrix"]), "Mar 2026")
    assert has_element?(view, ~s([data-testid="asset-window-matrix"]), "customer_orders_daily")
    assert has_element?(view, ~s([data-testid="asset-window-matrix"]), "stg_payments")
  end

  test "run detail matrix exposes a window for every attempt", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, _view, html} = live(conn, ~p"/runs/#{parent_id}")

    assert Regex.scan(~r/data-testid="asset-window-cell"/, html) |> length() == 6
    refute html =~ ~s(data-window-id="none")
  end

  test "run detail matrix renders failed running and queued statuses", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    assert has_element?(
             view,
             ~s([data-testid="asset-window-cell"][data-status="error"]),
             "Failed"
           )

    assert has_element?(
             view,
             ~s([data-testid="asset-window-cell"][data-status="running"]),
             "Running"
           )

    assert has_element?(
             view,
             ~s([data-testid="asset-window-cell"][data-status="pending"]),
             "Queued"
           )
  end

  test "run detail failures view focuses failed attempts", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Failures"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="failures-view"]), "Failed asset attempts")
    assert has_element?(view, ~s([data-testid="failure-row"]), "customer_orders_daily")
    assert has_element?(view, ~s([data-testid="failure-row"]), "detail group failed")
  end

  test "run detail window runs view shows child runs", %{conn: conn} do
    %{parent_id: parent_id, child_ids: child_ids} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Window runs"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="window-runs-view"]), "Window runs")

    for child_id <- child_ids do
      assert has_element?(view, ~s([data-testid="window-run-row"][data-run-id="#{child_id}"]))
    end
  end

  test "run detail deep links child run ids to highlighted window runs", %{conn: conn} do
    %{parent_id: parent_id, child_ids: [child_id | _]} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}?view=windows&child_run_id=#{child_id}")

    assert has_element?(
             view,
             ~s([data-testid="window-run-row"][data-run-id="#{child_id}"][data-selected="true"])
           )
  end

  test "run detail timeline groups attempts into running ran and queued", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Timeline"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="timeline-view"]), "Timeline")

    assert has_element?(
             view,
             ~s([data-testid="timeline-section"][data-section="running"]),
             "Running"
           )

    assert has_element?(view, ~s([data-testid="timeline-section"][data-section="ran"]), "Ran")

    assert has_element?(
             view,
             ~s([data-testid="timeline-section"][data-section="queued"]),
             "Queued"
           )
  end

  test "run detail timeline sorts started groups by start time", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    html = render_click(view, "set_mode", %{"mode" => "timeline"})

    assert html =~
             ~r/data-section="ran".*customer_orders_daily.*Jan 2026.*stg_payments.*Jan 2026.*customer_orders_daily.*Feb 2026/s
  end

  test "run detail timeline shows window labels on every row", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    html = render_click(view, "set_mode", %{"mode" => "timeline"})
    rows = Regex.scan(~r/data-testid="timeline-row"[^>]+data-window-label="([^"]+)"/, html)

    assert length(rows) == 6

    assert Enum.all?(rows, fn [_match, label] -> label in ["Jan 2026", "Feb 2026", "Mar 2026"] end)
  end

  test "run detail timeline puts failed terminal attempts in ran", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    html = render_click(view, "set_mode", %{"mode" => "timeline"})

    assert html =~ ~r/data-section="ran".*customer_orders_daily.*Feb 2026.*Failed/s
  end

  test "run detail timeline puts blocked skipped fresh and partial attempts in ran" do
    run = %{
      active?: false,
      windows: [],
      matrix: %{
        rows: [
          %{
            cells: [
              timeline_attempt(:blocked),
              timeline_attempt(:skipped_fresh),
              timeline_attempt(:partial)
            ]
          }
        ]
      }
    }

    html =
      render_component(&Timeline.timeline_panel/1,
        run: run,
        timeline_hook?: false,
        timeline_state: %{
          mode: :fit,
          zoom: "full",
          live_follow?: false,
          search: "",
          status: "all",
          window: "all",
          failed_only?: false,
          running_only?: false
        }
      )

    assert html =~ ~r/data-section="ran".*Blocked.*Skipped fresh.*Partial/s
    refute html =~ ~r/data-section="queued".*Blocked/s
  end

  test "run detail timeline prefers authoritative attempts over matrix placeholders" do
    run = %{
      active?: false,
      windows: [],
      attempts: [timeline_attempt(:ok)],
      matrix: %{
        rows: [
          %{
            cells: [
              %{
                timeline_attempt(:pending)
                | id: nil,
                  attempt_id: nil,
                  raw_status: :pending,
                  status: "Queued",
                  started_at_raw: nil,
                  finished_at_raw: nil
              }
            ]
          }
        ]
      }
    }

    html =
      render_component(&Timeline.timeline_panel/1,
        run: run,
        timeline_hook?: false,
        timeline_state: %{
          mode: :fit,
          zoom: "full",
          live_follow?: false,
          search: "",
          status: "all",
          window: "all",
          failed_only?: false,
          running_only?: false
        }
      )

    assert html =~ ~r/data-section="ran".*Ok/s
    refute html =~ ~r/data-testid="timeline-row"[^>]*data-section="queued"/s
  end

  test "run detail timeline uses started to now bars for running attempts", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    html = render_click(view, "set_mode", %{"mode" => "timeline"})

    assert html =~ ~r/data-section="running"[^>]+data-time-mode="started-to-now"/s
    assert html =~ ~s(data-testid="timeline-now-marker")
    assert html =~ ~s(data-testid="timeline-bar")
  end

  test "run detail timeline exposes live mode for active runs", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}?view=timeline")

    assert has_element?(view, ~s([data-testid="timeline-mode-indicator"][data-mode="live"]))
    assert has_element?(view, ~s([data-testid="timeline-now-marker"]))
    assert has_element?(view, ~s([data-testid="timeline-scroll-region"][data-live-follow="true"]))
  end

  test "run detail timeline defaults completed runs to fit behavior", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!(completed?: true)

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}?view=timeline")

    assert has_element?(view, ~s([data-testid="timeline-mode-indicator"][data-mode="fit"]))
    assert has_element?(view, ~s([data-testid="timeline-fit-run"].btn-warning))
    refute has_element?(view, ~s([data-testid="timeline-now-marker"]))
  end

  test "run detail timeline zoom controls update rendered state", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}?view=timeline")

    view
    |> element(~s([data-testid="timeline-zoom-1h"]))
    |> render_click()

    path = assert_patch(view)
    assert path =~ "/runs/#{parent_id}"
    assert path =~ "view=timeline"
    assert path =~ "timeline_zoom=1h"
    assert path =~ "timeline_mode=manual"
    assert path =~ "timeline_follow=0"
    assert has_element?(view, ~s([data-testid="timeline-mode-indicator"][data-mode="manual"]))
    assert has_element?(view, ~s([data-testid="timeline-zoom-1h"].btn-info))
    assert has_element?(view, ~s([data-testid="timeline-jump-now"]), "Jump to now")
  end

  test "run detail timeline focus switches fit view to manual zoom", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!(completed?: true)

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}?view=timeline")

    assert has_element?(view, ~s([data-testid="timeline-mode-indicator"][data-mode="fit"]))

    render_hook(view, "timeline_focus", %{"ratio" => 0.5})

    path = assert_patch(view)
    assert path =~ "view=timeline"
    assert path =~ "timeline_mode=manual"
    assert path =~ "timeline_follow=0"
    assert has_element?(view, ~s([data-testid="timeline-mode-indicator"][data-mode="manual"]))
    assert has_element?(view, ~s([data-testid="timeline-zoom-30m"].btn-info))
  end

  test "run detail timeline filters remain active after zoom changes", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}?view=timeline")

    view
    |> form(~s([data-testid="timeline-controls"]), %{
      "timeline" => %{"search" => "payments", "status" => "succeeded", "window" => "Jan 2026"}
    })
    |> render_change()

    path = assert_patch(view)
    assert path =~ "/runs/#{parent_id}"
    assert path =~ "timeline_search=payments"
    assert path =~ "timeline_status=succeeded"
    assert path =~ "timeline_window=Jan+2026"

    view
    |> element(~s([data-testid="timeline-zoom-15m"]))
    |> render_click()

    path = assert_patch(view)
    assert path =~ "timeline_zoom=15m"
    assert path =~ "timeline_mode=manual"
    assert path =~ "timeline_follow=0"
    assert path =~ "timeline_search=payments"
    assert path =~ "timeline_status=succeeded"
    assert path =~ "timeline_window=Jan+2026"

    assert has_element?(view, ~s([data-testid="timeline-row"]), "stg_payments")
    refute has_element?(view, ~s([data-testid="timeline-row"]), "customer_orders_daily")
  end

  test "run detail timeline shows queued rows without fake starts", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    html = render_click(view, "set_mode", %{"mode" => "timeline"})

    assert html =~ ~r/data-section="queued"[^>]+data-time-mode="no-start"/s
    assert html =~ ~s(data-testid="timeline-queued-placeholder")
    assert html =~ "No scheduled start"
  end

  test "run detail opens attempt drawer from timeline row", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    render_click(view, "set_mode", %{"mode" => "timeline"})

    view
    |> element(~s([data-testid="timeline-row"][data-section="ran"][data-attempt-id]), "Failed")
    |> render_click()

    assert has_element?(view, ~s([data-testid="asset-attempt-drawer"]), "customer_orders_daily")
    assert has_element?(view, ~s([data-testid="asset-attempt-drawer"]), "Feb 2026")
  end

  test "run detail opens attempt drawer from matrix cell", %{conn: conn} do
    %{parent_id: parent_id} = seed_run_detail_group!()

    {:ok, view, _html} = live(conn, ~p"/runs/#{parent_id}")

    view
    |> element(~s([data-testid="asset-window-cell"][data-status="error"]), "Failed")
    |> render_click()

    assert has_element?(view, ~s([data-testid="asset-attempt-drawer"]), "customer_orders_daily")
    assert has_element?(view, ~s([data-testid="asset-attempt-drawer"]), "Feb 2026")
    assert has_element?(view, ~s([data-testid="attempt-logs-link"]), "Open logs/events")
  end

  test "run detail mode rail changes to events mode", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily")

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Events"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="run-event-timeline"]), "Events")
    refute has_element?(view, ~s([data-testid="run-overview-panel"]))
  end

  test "run detail overview rail button returns from another mode", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily?view=timeline")

    refute has_element?(view, ~s([data-testid="run-overview-panel"]))

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Overview"]))
    |> render_click()

    path = assert_patch(view)
    assert path == ~p"/runs/run_customer_orders_daily"
    assert has_element?(view, ~s([data-testid="run-overview-panel"]))
  end

  test "run detail right rail exposes expected modes", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily")

    for label <- ["Overview", "Timeline", "Failures", "Window runs", "Events"] do
      assert has_element?(view, ~s([data-testid="view-mode-rail"] button[aria-label="#{label}"]))
    end

    refute has_element?(view, ~s([data-testid="view-mode-rail"] button[aria-label="Outputs"]))
    refute has_element?(view, ~s([data-testid="view-mode-rail"] button[aria-label="Context"]))
    refute has_element?(view, ~s([data-testid="view-mode-rail"] button[aria-label="Debug"]))
    refute has_element?(view, ~s([data-testid="view-mode-rail"] button[aria-label="Assets"]))
  end

  test "run detail ignores invalid mode events", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily")

    assert render_click(view, "set_mode", %{"mode" => "not_real"}) =~ "Run status"
    assert has_element?(view, ~s([data-testid="run-overview-panel"]))
  end

  test "shows not-found state for unknown run ids", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/runs/run_missing")

    assert html =~ "Run not found"
    assert has_element?(view, ~s([data-testid="run-not-found-state"]), "run_missing")
  end

  test "/logs renders the global log viewer", %{conn: conn} do
    seed_log!("global boot", source: :system, occurred_at: ~U[2026-05-19 19:24:56Z])

    {:ok, view, html} = live(conn, ~p"/logs")

    assert html =~ "Live system and run logs"
    assert has_element?(view, ~s([data-testid="log-viewer"][data-log-scope="global"]))
    assert has_element?(view, ~s([data-testid="log-row"]), "global boot")
    assert has_element?(view, ~s([data-testid="log-row"]), "May 19 19:24:56")
  end

  test "/logs loads newest entries when more than one backend page exists", %{conn: conn} do
    for index <- 1..510 do
      seed_log!("paged log #{index}", producer_id: "paged", producer_sequence: index)
    end

    {:ok, view, _html} = live(conn, ~p"/logs")

    assert has_element?(view, ~s([data-testid="log-row"]), "paged log 510")
    assert has_element?(view, ~s([data-testid="log-row"]), "paged log 311")
    refute has_element?(view, ~s([data-testid="log-row"]), "paged log 310")
    refute has_element?(view, ~s([data-testid="log-row"]), "paged log 1")
  end

  test "/runs/:run_id/logs renders only run-scoped logs", %{conn: conn} do
    seed_log!("visible run log", run_id: "run_customer_orders_daily")
    seed_log!("other run log", run_id: "run_failed_empty")

    {:ok, view, html} = live(conn, ~p"/runs/run_customer_orders_daily/logs")

    assert html =~ "run_customer_orders_daily"
    assert has_element?(view, ~s([data-testid="log-viewer"][data-log-scope="run"]))
    assert has_element?(view, ~s([data-testid="log-row"]), "visible run log")
    refute has_element?(view, ~s([data-testid="log-row"]), "other run log")
  end

  test "/runs/:run_id/assets/:asset_step_id/logs renders only asset-scoped logs", %{conn: conn} do
    asset_step_id = asset_step_id("run_customer_orders_daily", :customer_orders_daily)

    seed_log!("visible asset log",
      run_id: "run_customer_orders_daily",
      asset_step_id: asset_step_id
    )

    seed_log!("run-only log", run_id: "run_customer_orders_daily")

    {:ok, view, html} =
      live(conn, ~p"/runs/run_customer_orders_daily/assets/#{asset_step_id}/logs")

    assert html =~ "customer_orders_daily"
    assert has_element?(view, ~s([data-testid="log-viewer"][data-log-scope="asset"]))
    assert has_element?(view, ~s([data-testid="log-row"]), "visible asset log")
    refute has_element?(view, ~s([data-testid="log-row"]), "run-only log")
  end

  test "asset logs show fallback when asset context is missing", %{conn: conn} do
    seed_log!("orphan asset log",
      run_id: "run_customer_orders_daily",
      asset_step_id: "missing-step"
    )

    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily/assets/missing-step/logs")

    assert has_element?(
             view,
             ~s([data-testid="log-context-note"]),
             "Asset step context not found"
           )

    assert has_element?(view, ~s([data-testid="log-row"]), "orphan asset log")
  end

  test "multi-line log messages preserve newlines", %{conn: conn} do
    seed_log!("Running SQL:\nSELECT customer_id\nFROM raw.orders",
      run_id: "run_customer_orders_daily"
    )

    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily/logs")

    assert render(view) =~ "Running SQL:\nSELECT customer_id\nFROM raw.orders"
  end

  test "logs expose execution context details", %{conn: conn} do
    seed_log!("asset execution started",
      run_id: "run_customer_orders_daily",
      asset_step_id: "customer-step",
      asset_ref: {__MODULE__.Assets, :customer_orders_daily},
      runner_execution_id: "runner_exec_123456789",
      attempt: 2
    )

    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily/logs")

    assert has_element?(view, ~s([data-testid="log-row"]), "asset execution started")
    assert has_element?(view, ~s([data-testid="log-detail-chip"]), "asset")
    assert has_element?(view, ~s([data-testid="log-detail-chip"]), "customer_orders_daily")
    assert has_element?(view, ~s([data-testid="log-detail-chip"]), "#2")
  end

  test "error logs expose full details and copy action", %{conn: conn} do
    seed_log!("asset execution failed",
      run_id: "run_customer_orders_daily",
      asset_step_id: "customer-step",
      asset_ref: {__MODULE__.Assets, :customer_orders_daily},
      level: :error,
      metadata: %{error: %{message: "Warehouse timeout", reason: "connection failed"}}
    )

    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily/logs")

    assert has_element?(view, ~s([data-testid="log-row"]), "asset execution failed")
    assert has_element?(view, ~s([data-testid="log-details-panel"]), "Warehouse timeout")
    assert has_element?(view, ~s([data-testid="log-error-copy-button"]), "Copy")
  end

  test "level source and search filters affect rendered logs", %{conn: conn} do
    seed_log!("warehouse threshold", level: :warning, source: :adapter)
    seed_log!("runner heartbeat", level: :info, source: :runner)

    {:ok, view, _html} = live(conn, ~p"/logs")

    view
    |> element(~s(form[phx-change="filter_logs"]))
    |> render_change(%{
      "filters" => %{"search" => "warehouse", "level" => "warning", "source" => "adapter"}
    })

    assert has_element?(view, ~s([data-testid="log-row"]), "warehouse threshold")
    refute has_element?(view, ~s([data-testid="log-row"]), "runner heartbeat")
  end

  test "live log messages append and duplicate global sequences are ignored", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/logs")

    entry = log_entry("live append", id: "manual-live", global_sequence: 4242)
    send(view.pid, {:favn_log_entry, entry})
    send(view.pid, {:favn_log_entry, entry})

    html = render(view)
    assert html =~ "live append"
    assert Regex.scan(~r/data-testid="log-row"/, html) |> length() == 1
  end

  test "subscription failure shows non-fatal warning", %{conn: conn} do
    original = Application.get_env(:favn_view, :log_subscribe_fun)
    Application.put_env(:favn_view, :log_subscribe_fun, fn _filter -> {:error, :unavailable} end)

    try do
      {:ok, view, _html} = live(conn, ~p"/logs")

      assert has_element?(
               view,
               ~s([data-testid="log-stream-warning"]),
               "live streaming is unavailable"
             )
    after
      if original do
        Application.put_env(:favn_view, :log_subscribe_fun, original)
      else
        Application.delete_env(:favn_view, :log_subscribe_fun)
      end
    end
  end

  test "copy text formatting preserves multi-line logs" do
    text =
      [log_entry("first\nsecond", global_sequence: 1)]
      |> FavnView.LogsViewModel.entries()
      |> FavnView.LogsViewModel.plain_text()

    assert text =~ "first\nsecond"
  end

  test "favn_view lib does not call storage adapters directly" do
    files = Path.wildcard(Path.expand("../../lib/favn_view/**/*.ex", __DIR__))

    for file <- files do
      source = File.read!(file)
      refute source =~ "Storage.Adapter"
      refute source =~ "FavnOrchestrator.Storage"
      refute source =~ "FavnOrchestrator.Auth"
      refute source =~ "Auth.Store"
      refute source =~ "service_token"
      refute source =~ "Scheduler.Runtime"
      refute source =~ "RunnerClient"
    end
  end

  test "catalogue links open the detail route", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/assets")
    detail_path = detail_path(:customer_orders_daily)

    {:ok, detail_view, html} =
      view
      |> element(
        ~s(tr[data-testid="asset-row"] a[href="#{detail_path}"]),
        "customer_orders_daily"
      )
      |> render_click()
      |> follow_redirect(conn, detail_path)

    assert html =~ "customer_orders_daily"
    assert has_element?(detail_view, ~s([data-testid="window-timeline-panel"]))
  end

  test "ignores invalid mode events", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/assets")

    assert render_click(view, "set_mode", %{"mode" => "not_real"}) =~ "Asset catalogue"
  end

  defp manifest_version do
    manifest = %Manifest{
      assets: [
        asset(:raw_payments, :s3, "finance", :source,
          freshness: Favn.Freshness.Policy.from_value!(max_age: {:hours, 24})
        ),
        asset(:customer_orders_daily, :snowflake, "sales", :sql,
          window: WindowSpec.new!(:day),
          freshness: Favn.Freshness.Policy.from_value!({:daily, timezone: "Europe/Oslo"}),
          depends_on: [{__MODULE__.Assets, :raw_payments}]
        ),
        asset(:stg_payments, :postgres, "finance", :sql),
        asset(:always_refresh, :snowflake, "sales", :sql,
          freshness: Favn.Freshness.Policy.from_value!(:always)
        )
      ],
      pipelines: [
        %Pipeline{
          module: __MODULE__.Pipelines.DailyOrders,
          name: :daily_orders,
          selectors: [{:asset, {__MODULE__.Assets, :customer_orders_daily}}],
          deps: :all,
          window: Policy.new!(:daily, timezone: "Europe/Oslo")
        },
        %Pipeline{
          module: __MODULE__.Pipelines.FullRefresh,
          name: :full_refresh,
          selectors: [{:asset, {__MODULE__.Assets, :customer_orders_daily}}],
          deps: :all,
          window: nil
        }
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_view_assets")
    version
  end

  defp authenticate_conn(conn, role \\ :operator) do
    username = "#{role}-#{System.unique_integer([:positive])}"

    assert {:ok, _actor} =
             Auth.create_actor(username, "operator-password-long", "Test Operator", [role])

    conn
    |> post(~p"/login", %{
      "operator" => %{"username" => username, "password" => "operator-password-long"}
    })
    |> recycle()
  end

  defp ensure_auth_store_started do
    case Process.whereis(AuthStore) do
      nil -> start_supervised!({AuthStore, []})
      _pid -> :ok
    end
  end

  defp asset(name, connection, catalog, type, opts \\ []) do
    %Favn.Manifest.Asset{
      ref: {__MODULE__.Assets, name},
      module: __MODULE__.Assets,
      name: name,
      type: type,
      window: Keyword.get(opts, :window),
      freshness: Keyword.get(opts, :freshness),
      depends_on: Keyword.get(opts, :depends_on, []),
      relation: %{connection: connection, catalog: catalog, name: Atom.to_string(name)}
    }
  end

  defp seed_backfill_overview_group! do
    parent_id = "run_overview_backfill_parent"

    child_ids = [
      "run_overview_backfill_jan",
      "run_overview_backfill_apr",
      "run_overview_backfill_may"
    ]

    ref = {__MODULE__.Assets, :customer_orders_daily}
    base = ~U[2026-01-01 00:00:00Z]

    parent =
      RunState.new(
        id: parent_id,
        manifest_version_id: "mv_view_assets",
        manifest_content_hash: "hash_view_assets",
        asset_ref: ref,
        target_refs: [ref],
        submit_kind: :backfill_pipeline,
        trigger: %{kind: :backfill, pipeline_module: __MODULE__.Pipelines.DailyOrders},
        metadata: %{pipeline_submit_ref: __MODULE__.Pipelines.DailyOrders}
      )
      |> RunState.transition(status: :running, result: %{status: :running})
      |> Map.put(:inserted_at, base)
      |> Map.put(:updated_at, DateTime.add(base, 5, :minute))
      |> RunState.with_snapshot_hash()

    assert :ok = Storage.put_run(parent)

    windows = [
      {Enum.at(child_ids, 0), :ok, "Jan 2026", 0, 1_000},
      {Enum.at(child_ids, 1), :error, "Apr 2026", 90, 1_000},
      {Enum.at(child_ids, 2), :running, "May 2026", 120, nil}
    ]

    Enum.each(windows, fn {child_id, status, label, day_offset, duration_ms} ->
      started_at = DateTime.add(base, day_offset, :day)
      finished_at = if(duration_ms, do: DateTime.add(started_at, duration_ms, :millisecond))
      window_key = "month:#{label}"

      child =
        RunState.new(
          id: child_id,
          manifest_version_id: "mv_view_assets",
          manifest_content_hash: "hash_view_assets",
          asset_ref: ref,
          target_refs: [ref],
          submit_kind: :pipeline,
          parent_run_id: parent_id,
          root_run_id: parent_id,
          lineage_depth: 1,
          trigger: %{
            kind: :backfill,
            backfill_run_id: parent_id,
            pipeline_module: __MODULE__.Pipelines.DailyOrders,
            window_key: window_key
          },
          metadata: %{pipeline_submit_ref: __MODULE__.Pipelines.DailyOrders}
        )
        |> RunState.transition(
          status: status,
          result: %{
            node_results: [
              NodeResult.new(%{
                node_key: {ref, window_key},
                ref: ref,
                window: %{id: window_key},
                stage: 0,
                status: status,
                started_at: started_at,
                finished_at: finished_at,
                duration_ms: duration_ms,
                error: if(status == :error, do: %{message: "overview child failed"}),
                asset_step_id: "#{child_id}-step"
              })
            ]
          }
        )
        |> Map.put(:inserted_at, started_at)
        |> Map.put(:updated_at, finished_at || started_at)
        |> RunState.with_snapshot_hash()

      assert :ok = Storage.put_run(child)

      {:ok, window} =
        BackfillWindow.new(%{
          backfill_run_id: parent_id,
          child_run_id: child_id,
          latest_attempt_run_id: child_id,
          pipeline_module: __MODULE__.Pipelines.DailyOrders,
          manifest_version_id: "mv_view_assets",
          window_kind: :month,
          window_start_at: started_at,
          window_end_at: DateTime.add(started_at, 31, :day),
          timezone: "Etc/UTC",
          window_key: window_key,
          status: status,
          attempt_count: 1,
          last_error: if(status == :error, do: :failed),
          started_at: started_at,
          finished_at: finished_at,
          updated_at: finished_at || started_at
        })

      assert :ok = Storage.put_backfill_window(window)
    end)

    %{parent_id: parent_id, child_ids: child_ids}
  end

  defp seed_run_detail_group!(opts \\ []) do
    parent_id = "run_detail_backfill_parent"
    child_ids = ["run_detail_backfill_jan", "run_detail_backfill_feb", "run_detail_backfill_mar"]
    refs = [{__MODULE__.Assets, :customer_orders_daily}, {__MODULE__.Assets, :stg_payments}]
    base = ~U[2026-01-01 00:00:00Z]
    completed? = Keyword.get(opts, :completed?, false)
    parent_status = if(completed?, do: :ok, else: :running)

    parent =
      RunState.new(
        id: parent_id,
        manifest_version_id: "mv_view_assets",
        manifest_content_hash: "hash_view_assets",
        asset_ref: List.first(refs),
        target_refs: refs,
        submit_kind: :backfill_pipeline,
        trigger: %{kind: :backfill, pipeline_module: __MODULE__.Pipelines.DailyOrders},
        metadata: %{pipeline_submit_ref: __MODULE__.Pipelines.DailyOrders}
      )
      |> RunState.transition(status: parent_status, result: %{status: parent_status})
      |> Map.put(:inserted_at, base)
      |> Map.put(:updated_at, DateTime.add(base, 5, :minute))
      |> RunState.with_snapshot_hash()

    assert :ok = Storage.put_run(parent)

    windows =
      if completed? do
        [
          {Enum.at(child_ids, 0), :ok, "Jan 2026", 0, [:ok, :ok]},
          {Enum.at(child_ids, 1), :ok, "Feb 2026", 31, [:ok, :ok]},
          {Enum.at(child_ids, 2), :ok, "Mar 2026", 59, [:ok, :ok]}
        ]
      else
        [
          {Enum.at(child_ids, 0), :ok, "Jan 2026", 0, [:ok, :ok]},
          {Enum.at(child_ids, 1), :error, "Feb 2026", 31, [:error, :ok]},
          {Enum.at(child_ids, 2), :running, "Mar 2026", 59, [:running, nil]}
        ]
      end

    Enum.each(windows, fn {child_id, run_status, label, day_offset, statuses} ->
      started_at = DateTime.add(base, day_offset, :day)
      finished_at = if(run_status in [:ok, :error], do: DateTime.add(started_at, 90, :second))
      window_key = "month:#{label}"

      node_results =
        refs
        |> Enum.zip(statuses)
        |> Enum.reject(fn {_ref, status} -> is_nil(status) end)
        |> Enum.map(fn {ref, status} ->
          NodeResult.new(%{
            node_key: {ref, window_key},
            ref: ref,
            window: %{
              key: window_key,
              label: label,
              start_at: started_at,
              end_at: DateTime.add(started_at, 31, :day)
            },
            stage: 0,
            status: status,
            started_at: started_at,
            finished_at: if(status in [:ok, :error], do: DateTime.add(started_at, 90, :second)),
            duration_ms: if(status in [:ok, :error], do: 90_000),
            error: if(status == :error, do: %{message: "detail group failed"}),
            asset_step_id: "#{child_id}-#{elem(ref, 1)}"
          })
        end)

      child =
        RunState.new(
          id: child_id,
          manifest_version_id: "mv_view_assets",
          manifest_content_hash: "hash_view_assets",
          asset_ref: List.first(refs),
          target_refs: refs,
          submit_kind: :pipeline,
          parent_run_id: parent_id,
          root_run_id: parent_id,
          lineage_depth: 1,
          trigger: %{
            kind: :backfill,
            backfill_run_id: parent_id,
            pipeline_module: __MODULE__.Pipelines.DailyOrders,
            window_key: window_key
          },
          metadata: %{pipeline_submit_ref: __MODULE__.Pipelines.DailyOrders}
        )
        |> RunState.transition(status: run_status, result: %{node_results: node_results})
        |> Map.put(:inserted_at, started_at)
        |> Map.put(:updated_at, finished_at || started_at)
        |> RunState.with_snapshot_hash()

      assert :ok = Storage.put_run(child)

      {:ok, window} =
        BackfillWindow.new(%{
          backfill_run_id: parent_id,
          child_run_id: child_id,
          latest_attempt_run_id: child_id,
          pipeline_module: __MODULE__.Pipelines.DailyOrders,
          manifest_version_id: "mv_view_assets",
          window_kind: :month,
          window_start_at: started_at,
          window_end_at: DateTime.add(started_at, 31, :day),
          timezone: "Etc/UTC",
          window_key: window_key,
          status: run_status,
          attempt_count: 1,
          last_error: if(run_status == :error, do: :failed),
          started_at: started_at,
          finished_at: finished_at,
          updated_at: finished_at || started_at
        })

      assert :ok = Storage.put_backfill_window(window)
    end)

    %{parent_id: parent_id, child_ids: child_ids}
  end

  defp timeline_attempt(status) do
    label =
      status
      |> to_string()
      |> String.replace("_", " ")
      |> String.capitalize()

    %{
      id: "attempt-#{status}",
      attempt_id: "attempt-#{status}",
      asset_key: "asset_#{status}",
      asset_ref: "asset_#{status}",
      asset_name: "asset_#{status}",
      short_asset_name: "asset_#{status}",
      window_label: "May 2026",
      raw_status: status,
      status: label,
      status_tone: if(status == :blocked, do: :error, else: :neutral),
      started_at_raw: ~U[2026-05-01 00:00:00Z],
      finished_at_raw: ~U[2026-05-01 00:00:10Z],
      started_at: "May 1, 2026 00:00 UTC",
      duration: "10s",
      error_summary: nil
    }
  end

  defp run_state(name, status, seconds_from_now, run_id \\ nil) do
    ref = {__MODULE__.Assets, name}
    finished_at = DateTime.add(DateTime.utc_now(), seconds_from_now, :second)
    started_at = DateTime.add(finished_at, -1, :second)

    RunState.new(
      id: run_id || "run_#{name}",
      manifest_version_id: "mv_view_assets",
      manifest_content_hash: "hash_view_assets",
      asset_ref: ref,
      target_refs: [ref]
    )
    |> RunState.transition(
      status: status,
      result: %{
        asset_results: [
          %AssetResult{
            ref: ref,
            stage: 0,
            status: status,
            started_at: started_at,
            finished_at: if(status in [:pending, :running], do: nil, else: finished_at),
            duration_ms: 1
          }
        ]
      }
    )
    |> Map.put(:inserted_at, started_at)
    |> Map.put(:updated_at, finished_at)
    |> RunState.with_snapshot_hash()
  end

  defp pipeline_run_state do
    ref = {__MODULE__.Assets, :customer_orders_daily}
    finished_at = DateTime.add(DateTime.utc_now(), -900, :second)
    started_at = DateTime.add(finished_at, -5, :second)

    RunState.new(
      id: "run_daily_orders",
      manifest_version_id: "mv_view_assets",
      manifest_content_hash: "hash_view_assets",
      asset_ref: ref,
      target_refs: [ref],
      submit_kind: :pipeline,
      metadata: %{
        pipeline_submit_ref: __MODULE__.Pipelines.DailyOrders,
        pipeline_target_refs: [ref],
        pipeline_dependencies: :all
      }
    )
    |> RunState.transition(
      status: :ok,
      result: %{asset_results: terminal_asset_results(:customer_orders_daily)}
    )
    |> Map.put(:inserted_at, started_at)
    |> Map.put(:updated_at, finished_at)
    |> RunState.with_snapshot_hash()
  end

  defp empty_run_state(name, status, run_id) do
    ref = {__MODULE__.Assets, name}
    now = DateTime.utc_now()

    RunState.new(
      id: run_id,
      manifest_version_id: "mv_view_assets",
      manifest_content_hash: "hash_view_assets",
      asset_ref: ref,
      target_refs: [ref]
    )
    |> RunState.transition(status: status, result: %{asset_results: []})
    |> Map.put(:inserted_at, now)
    |> Map.put(:updated_at, now)
    |> RunState.with_snapshot_hash()
  end

  defp failed_run_state(name, run_id) do
    ref = {__MODULE__.Assets, name}
    upstream_ref = {__MODULE__.Assets, :raw_payments}
    finished_at = DateTime.add(DateTime.utc_now(), -1_200, :second)
    started_at = DateTime.add(finished_at, -2, :second)

    RunState.new(
      id: run_id,
      manifest_version_id: "mv_view_assets",
      manifest_content_hash: "hash_view_assets",
      asset_ref: ref,
      target_refs: [upstream_ref, ref]
    )
    |> RunState.transition(
      status: :error,
      result: %{
        asset_results: [
          %AssetResult{
            ref: upstream_ref,
            stage: 0,
            status: :ok,
            started_at: started_at,
            finished_at: DateTime.add(started_at, 1, :second),
            duration_ms: 1_000
          },
          %AssetResult{
            ref: ref,
            stage: 1,
            status: :error,
            started_at: DateTime.add(started_at, 1, :second),
            finished_at: finished_at,
            duration_ms: 1_000,
            error: %{message: "Warehouse timeout"}
          }
        ]
      }
    )
    |> Map.put(:inserted_at, started_at)
    |> Map.put(:updated_at, finished_at)
    |> Map.put(:metadata, %{
      asset_dependencies: :none,
      refresh_policy: %{mode: :force, refs: [], include_upstream?: false}
    })
    |> RunState.with_snapshot_hash()
  end

  defp node_results_run_state do
    finished_at = DateTime.add(DateTime.utc_now(), -1_200, :second)
    started_at = DateTime.add(finished_at, -2, :second)
    raw_ref = {__MODULE__.Assets, :raw_payments}
    stg_ref = {__MODULE__.Assets, :stg_payments}
    customer_ref = {__MODULE__.Assets, :customer_orders_daily}

    RunState.new(
      id: "run_node_results",
      manifest_version_id: "mv_view_assets",
      manifest_content_hash: "hash_view_assets",
      asset_ref: customer_ref,
      target_refs: [raw_ref, stg_ref, customer_ref]
    )
    |> RunState.transition(
      status: :partial,
      result: %{
        asset_results: [
          %AssetResult{
            ref: customer_ref,
            stage: 99,
            status: :ok,
            started_at: started_at,
            finished_at: finished_at,
            duration_ms: 1,
            meta: %{note: "asset aggregate fallback"}
          }
        ],
        node_results: [
          NodeResult.new(%{
            node_key: {raw_ref, "window:day:2026-06-12"},
            ref: raw_ref,
            window: %{id: "window:day:2026-06-12"},
            stage: 0,
            status: :skipped_fresh,
            reason: :fresh,
            started_at: started_at,
            finished_at: DateTime.add(started_at, 1, :second),
            duration_ms: 1_000
          }),
          NodeResult.new(%{
            node_key: {stg_ref, "window:day:2026-06-12"},
            ref: stg_ref,
            window: %{id: "window:day:2026-06-12"},
            stage: 1,
            status: :retrying,
            started_at: DateTime.add(started_at, 1, :second),
            duration_ms: 500
          }),
          NodeResult.new(%{
            node_key: {customer_ref, "window:day:2026-06-12"},
            ref: customer_ref,
            window: %{id: "window:day:2026-06-12"},
            stage: 2,
            status: :blocked,
            reason: :upstream_failed,
            started_at: DateTime.add(started_at, 2, :second),
            finished_at: finished_at,
            duration_ms: 1,
            error: %{reason: :upstream_failed}
          })
        ]
      }
    )
    |> Map.put(:inserted_at, started_at)
    |> Map.put(:updated_at, finished_at)
    |> RunState.with_snapshot_hash()
  end

  defp same_asset_node_results_run_state do
    finished_at = DateTime.add(DateTime.utc_now(), -1_200, :second)
    started_at = DateTime.add(finished_at, -2, :second)
    stg_ref = {__MODULE__.Assets, :stg_payments}

    RunState.new(
      id: "run_same_asset_nodes",
      manifest_version_id: "mv_view_assets",
      manifest_content_hash: "hash_view_assets",
      asset_ref: stg_ref,
      target_refs: [stg_ref]
    )
    |> RunState.transition(
      status: :partial,
      result: %{
        node_results: [
          NodeResult.new(%{
            node_key: {stg_ref, "window:day:2026-06-12"},
            ref: stg_ref,
            window: %{id: "window:day:2026-06-12"},
            stage: 0,
            status: :ok,
            started_at: started_at,
            finished_at: DateTime.add(started_at, 1, :second),
            duration_ms: 1_000
          }),
          NodeResult.new(%{
            node_key: {stg_ref, "window:day:2026-06-13"},
            ref: stg_ref,
            window: %{id: "window:day:2026-06-13"},
            stage: 1,
            status: :ok,
            started_at: DateTime.add(started_at, 1, :second),
            finished_at: finished_at,
            duration_ms: 1_000
          })
        ]
      }
    )
    |> Map.put(:inserted_at, started_at)
    |> Map.put(:updated_at, finished_at)
    |> RunState.with_snapshot_hash()
  end

  defp seed_freshness_states! do
    now = DateTime.utc_now()
    customer_at = DateTime.add(now, -600, :second)

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state(:raw_payments, "raw:v2", now, run_id: "run_raw_payments")
             )

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state(:customer_orders_daily, "customer:v1", customer_at,
                 run_id: "run_customer_orders_daily",
                 freshness_key: current_daily_freshness_key(now),
                 input_versions: [
                   %{
                     upstream_ref: {__MODULE__.Assets, :raw_payments},
                     upstream_node_key: {{__MODULE__.Assets, :raw_payments}, nil},
                     freshness_version: "raw:v1",
                     success_run_id: "run_raw_old"
                   }
                 ]
               )
             )

    failed_date = Date.utc_today() |> Date.add(-1) |> Date.to_iso8601()

    assert :ok =
             Storage.put_asset_freshness_state(
               freshness_state(
                 :customer_orders_daily,
                 "customer:failed",
                 DateTime.add(now, -1_200, :second),
                 run_id: "run_failed_customer_daily",
                 freshness_key: "calendar:day:Etc/UTC:#{failed_date}",
                 status: :error
               )
             )
  end

  defp freshness_state(name, version, at, opts) do
    run_id = Keyword.fetch!(opts, :run_id)

    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: __MODULE__.Assets,
        asset_ref_name: name,
        freshness_key: Keyword.get(opts, :freshness_key, Favn.Freshness.Key.latest()),
        status: :ok,
        freshness_version: version,
        latest_success_run_id: run_id,
        latest_success_node_key: {{__MODULE__.Assets, name}, nil},
        latest_success_at: at,
        latest_attempt_run_id: run_id,
        latest_attempt_status: Keyword.get(opts, :status, :ok),
        latest_attempt_at: at,
        manifest_version_id: "mv_view_assets",
        input_versions: Keyword.get(opts, :input_versions, []),
        updated_at: at
      })

    state
  end

  defp current_daily_freshness_key(now) do
    {:ok, period} = Favn.TimePeriod.current(:day, now, "Europe/Oslo")
    Favn.Freshness.Key.calendar!(:day, "Europe/Oslo", period.start_at)
  end

  defp terminal_asset_results(name) do
    ref = {__MODULE__.Assets, name}
    finished_at = DateTime.utc_now()
    started_at = DateTime.add(finished_at, -1, :second)

    [
      %AssetResult{
        ref: ref,
        stage: 0,
        status: :ok,
        started_at: started_at,
        finished_at: finished_at,
        duration_ms: 1_000
      }
    ]
  end

  defp seed_run_events!(run_id, status \\ :ok) do
    now = DateTime.utc_now()

    assert :ok =
             Storage.append_run_event(run_id, %{
               run_id: run_id,
               sequence: 1,
               event_type: :run_started,
               occurred_at: DateTime.add(now, -1, :second),
               status: :running,
               data: %{message: "Run started"}
             })

    assert :ok =
             Storage.append_run_event(run_id, %{
               run_id: run_id,
               sequence: 2,
               event_type: if(status == :error, do: :run_failed, else: :run_finished),
               occurred_at: now,
               status: status,
               data: %{
                 message: if(status == :error, do: "Warehouse timeout", else: "Run finished")
               }
             })
  end

  defp seed_log!(message, opts) do
    assert {:ok, [_entry]} = FavnOrchestrator.emit_log(log_entry(message, opts))
  end

  defp log_entry(message, opts) do
    %Entry{
      id: Keyword.get(opts, :id),
      global_sequence: Keyword.get(opts, :global_sequence),
      run_id: Keyword.get(opts, :run_id),
      asset_step_id: Keyword.get(opts, :asset_step_id),
      asset_ref: Keyword.get(opts, :asset_ref),
      runner_execution_id: Keyword.get(opts, :runner_execution_id),
      attempt: Keyword.get(opts, :attempt),
      producer_id: Keyword.get(opts, :producer_id),
      producer_sequence: Keyword.get(opts, :producer_sequence),
      occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now()),
      level: Keyword.get(opts, :level, :info),
      source: Keyword.get(opts, :source, :runner),
      message: message,
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  defp asset_step_id(run_id, name) do
    {:ok, detail} = FavnOrchestrator.get_run_detail(run_id)

    detail.steps
    |> Enum.find(&String.ends_with?(&1.asset_ref, ".#{name}"))
    |> Map.fetch!(:id)
  end

  defp open_run_config(view) do
    view
    |> element(~s([data-testid="run-selected-window"]), "Run asset")
    |> render_click()
  end

  defp assert_submitted_refresh(run_path, dependencies, refresh_policy) do
    run_id = String.replace_prefix(run_path, "/runs/", "")
    assert {:ok, run} = Storage.get_run(run_id)
    assert run.metadata.asset_dependencies == dependencies
    assert run.metadata.refresh_policy == refresh_policy
  end

  defp target_id(name) do
    {:ok, entries} = FavnOrchestrator.active_asset_catalogue()

    entries
    |> Enum.find(&(get_in(&1, [:relation, :name]) == Atom.to_string(name)))
    |> Map.fetch!(:target_id)
  end

  defp detail_path(name) do
    ~p"/assets/#{FavnView.AssetRoute.to_param(target_id(name))}"
  end

  defp pipeline_target_id(name) do
    {:ok, entries} = FavnOrchestrator.active_pipeline_catalogue()

    entries
    |> Enum.find(&(&1.name == name))
    |> Map.fetch!(:target_id)
  end

  defp pipeline_detail_path(name \\ "daily_orders") do
    ~p"/pipelines/#{FavnView.AssetRoute.to_param(pipeline_target_id(name))}"
  end

  defp asset_result_row_count(html) do
    ~r/data-testid="run-asset-result-row"/
    |> Regex.scan(html)
    |> length()
  end

  defp today_label do
    Date.utc_today() |> Calendar.strftime("%b %-d, %Y")
  end
end
