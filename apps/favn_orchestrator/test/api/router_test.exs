defmodule FavnOrchestrator.API.RouterTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Version
  alias Favn.RuntimeConfig.Ref
  alias Favn.Window.Anchor
  alias Favn.Window.Key, as: WindowKey
  alias Favn.Window.Policy
  alias FavnOrchestrator
  alias FavnOrchestrator.API.Router
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  @opts Router.init([])

  defmodule RunnerClientStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, _opts), do: {:ok, "exec_#{work.run_id}"}

    @impl true
    def await_result(_execution_id, _timeout, _opts) do
      {:ok, %RunnerResult{status: :ok, asset_results: [], metadata: %{}}}
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    @impl true
    def inspect_relation(request, _opts) do
      {:ok,
       %Favn.Contracts.RelationInspectionResult{
         asset_ref: request.asset_ref,
         relation_ref: %Favn.RelationRef{connection: :warehouse, schema: "raw", name: "orders"},
         relation: %Favn.SQL.Relation{schema: "raw", name: "orders", type: :table},
         columns: [%Favn.SQL.Column{name: "id", position: 1, data_type: "INTEGER"}],
         row_count: 2,
         sample: %{limit: request.sample_limit, columns: ["id"], rows: [%{"id" => 1}]},
         inspected_at: ~U[2026-01-01 00:00:00Z]
       }}
    end
  end

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_client_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)
    previous_username = Application.get_env(:favn_orchestrator, :auth_bootstrap_username)
    previous_password = Application.get_env(:favn_orchestrator, :auth_bootstrap_password)
    previous_display = Application.get_env(:favn_orchestrator, :auth_bootstrap_display_name)
    previous_roles = Application.get_env(:favn_orchestrator, :auth_bootstrap_roles)

    Application.put_env(:favn_orchestrator, :api_service_tokens, ["test-service-token"])
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])
    Application.put_env(:favn_orchestrator, :auth_bootstrap_username, "admin")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_password, "admin-password")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_display_name, "Admin User")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_roles, [:admin])

    auth_start = ensure_auth_store_started()
    :ok = AuthStore.reset()
    Memory.reset()
    :ok = Auth.bootstrap_configured_actor()

    on_exit(fn ->
      restore_env(:favn_orchestrator, :api_service_tokens, previous_tokens)
      restore_env(:favn_orchestrator, :runner_client, previous_client)
      restore_env(:favn_orchestrator, :runner_client_opts, previous_client_opts)
      restore_env(:favn_orchestrator, :auth_bootstrap_username, previous_username)
      restore_env(:favn_orchestrator, :auth_bootstrap_password, previous_password)
      restore_env(:favn_orchestrator, :auth_bootstrap_display_name, previous_display)
      restore_env(:favn_orchestrator, :auth_bootstrap_roles, previous_roles)
      maybe_stop_auth_store(auth_start)
    end)

    :ok
  end

  test "liveness and readiness endpoints expose health diagnostics without auth" do
    live_conn =
      conn(:get, "/api/orchestrator/v1/health/live")
      |> Router.call(@opts)

    assert live_conn.status == 200
    assert %{"data" => %{"status" => "ok"}} = Jason.decode!(live_conn.resp_body)

    ready_conn =
      conn(:get, "/api/orchestrator/v1/health/ready")
      |> Router.call(@opts)

    assert ready_conn.status == 200

    assert %{"data" => %{"status" => "ready", "checks" => checks}} =
             Jason.decode!(ready_conn.resp_body)

    assert Enum.any?(checks, &(&1["name"] == "storage" and &1["status"] == "ok"))
  end

  test "password session login and me endpoint" do
    login_conn =
      conn(:post, "/api/orchestrator/v1/auth/password/sessions", %{
        username: "admin",
        password: "admin-password"
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> Router.call(@opts)

    assert login_conn.status == 201

    assert %{"data" => %{"session" => session, "actor" => actor}} =
             Jason.decode!(login_conn.resp_body)

    assert actor["username"] == "admin"

    me_conn =
      conn(:get, "/api/orchestrator/v1/me")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor["id"])
      |> put_req_header("x-favn-session-id", session["id"])
      |> Router.call(@opts)

    assert me_conn.status == 200
    assert %{"data" => %{"actor" => me_actor}} = Jason.decode!(me_conn.resp_body)
    assert me_actor["id"] == actor["id"]
    assert me_actor["roles"] == ["admin"]
  end

  test "rejects requests missing service credentials" do
    response =
      conn(:get, "/api/orchestrator/v1/me")
      |> Router.call(@opts)

    assert response.status == 401
    assert %{"error" => %{"code" => "service_unauthorized"}} = Jason.decode!(response.resp_body)
  end

  test "SSE global stream returns baseline ready event" do
    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:get, "/api/orchestrator/v1/streams/runs")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> put_req_header("last-event-id", "runs:cursor_1")
      |> Router.call(@opts)

    assert response.status == 200
    assert ["text/event-stream; charset=utf-8"] = get_resp_header(response, "content-type")
    assert response.resp_body =~ "event: stream.ready"
    assert response.resp_body =~ "id: runs:cursor_1"
    assert response.resp_body =~ "stream\":\"runs\""
  end

  test "SSE run stream replays persisted events after run cursor" do
    seed_run_events!("run_stream_b", [1, 2, 3])

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:get, "/api/orchestrator/v1/streams/runs/run_stream_b")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> put_req_header("last-event-id", "run:run_stream_b:2")
      |> Router.call(@opts)

    assert response.status == 200
    assert response.resp_body =~ "id: run:run_stream_b:3"
    assert response.resp_body =~ "event: run_updated"
    assert response.resp_body =~ "event: stream.ready"
  end

  test "SSE stream rejects invalid last-event-id header" do
    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:get, "/api/orchestrator/v1/streams/runs")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> put_req_header("last-event-id", "bad id with spaces")
      |> Router.call(@opts)

    assert response.status == 422
    assert %{"error" => %{"code" => "validation_failed"}} = Jason.decode!(response.resp_body)
  end

  test "password login returns invalid credentials without rate-limiting branch" do
    response =
      conn(:post, "/api/orchestrator/v1/auth/password/sessions", %{
        username: "admin",
        password: "wrong-password"
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> Router.call(@opts)

    assert response.status == 401
    assert %{"error" => %{"code" => "unauthenticated"}} = Jason.decode!(response.resp_body)
  end

  test "bootstrap roles can create operator-scoped local actor" do
    :ok = AuthStore.reset()
    Application.put_env(:favn_orchestrator, :auth_bootstrap_username, "operator")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_password, "operator-password")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_display_name, "Operator User")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_roles, ["operator"])

    assert :ok = Auth.bootstrap_configured_actor()
    assert {:ok, _session, actor} = Auth.password_login("operator", "operator-password")
    assert actor.roles == [:operator]
  end

  test "lists schedules from active manifest when scheduler runtime is not running" do
    version = schedule_manifest_version("mv_schedule_router")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:get, "/api/orchestrator/v1/schedules")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 200
    assert %{"data" => %{"items" => [schedule]}} = Jason.decode!(response.resp_body)
    assert schedule["id"] == "schedule:Elixir.MyApp.Pipelines.DailyOrders:daily"
    assert schedule["schedule_id"] == "daily"
    assert schedule["pipeline_module"] == "Elixir.MyApp.Pipelines.DailyOrders"
    assert schedule["manifest_version_id"] == "mv_schedule_router"
  end

  test "returns one schedule detail by schedule id" do
    version = schedule_manifest_version("mv_schedule_detail")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(
        :get,
        "/api/orchestrator/v1/schedules/schedule:Elixir.MyApp.Pipelines.DailyOrders:daily"
      )
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 200
    assert %{"data" => %{"schedule" => schedule}} = Jason.decode!(response.resp_body)
    assert schedule["id"] == "schedule:Elixir.MyApp.Pipelines.DailyOrders:daily"
    assert schedule["active"] == true
  end

  test "activates manifest" do
    version = schedule_manifest_version("mv_activate_router")
    assert :ok = FavnOrchestrator.register_manifest(version)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:post, "/api/orchestrator/v1/manifests/mv_activate_router/activate")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 200
    assert %{"data" => %{"activated" => true}} = Jason.decode!(response.resp_body)
  end

  test "service token can activate manifest without actor headers" do
    version = schedule_manifest_version("mv_activate_service")
    assert :ok = FavnOrchestrator.register_manifest(version)

    response =
      conn(:post, "/api/orchestrator/v1/manifests/mv_activate_service/activate")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> Router.call(@opts)

    assert response.status == 200
    assert %{"data" => %{"activated" => true}} = Jason.decode!(response.resp_body)
  end

  test "active manifest target payload exposes pipeline window policy" do
    version = schedule_manifest_version("mv_active_window_targets")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:get, "/api/orchestrator/v1/manifests/active")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 200

    assert %{"data" => %{"targets" => %{"pipelines" => [pipeline]}}} =
             Jason.decode!(response.resp_body)

    assert pipeline["target_id"] == "pipeline:Elixir.MyApp.Pipelines.DailyOrders"

    assert pipeline["window"] == %{
             "kind" => "day",
             "anchor" => "previous_complete_period",
             "timezone" => nil,
             "allow_full_load" => false
           }
  end

  test "active manifest asset targets expose relation and runtime metadata" do
    version = dependency_manifest_version("mv_asset_target_metadata")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:get, "/api/orchestrator/v1/manifests/active")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 200

    assert %{"data" => %{"targets" => %{"assets" => assets}}} =
             Jason.decode!(response.resp_body)

    gold = Enum.find(assets, &(&1["target_id"] == "asset:Elixir.MyApp.Assets.Gold:asset"))
    assert gold["asset_ref"] == "Elixir.MyApp.Assets.Gold:asset"
    assert gold["type"] == "sql"

    assert gold["relation"] == %{
             "connection" => "warehouse",
             "catalog" => nil,
             "schema" => "gold",
             "name" => "orders"
           }

    assert gold["depends_on"] == ["Elixir.MyApp.Assets.Raw:asset"]
    assert %{"source_system" => %{"segment_id" => segment_id}} = gold["runtime_config"]
    assert segment_id["provider"] == "env"
    assert segment_id["secret"] == false
  end

  test "run detail exposes stored per-asset result metadata" do
    now = DateTime.utc_now()

    run_state =
      RunState.new(
        id: "run_detail_metadata",
        manifest_version_id: "mv_detail_metadata",
        manifest_content_hash: "hash_detail_metadata",
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Gold, :asset}],
        metadata: %{source: :test},
        params: %{limit: 1},
        trigger: %{type: :manual}
      )
      |> RunState.transition(
        status: :ok,
        result: %{
          status: :ok,
          asset_results: [
            %Favn.Run.AssetResult{
              ref: {MyApp.Assets.Gold, :asset},
              stage: 0,
              status: :ok,
              started_at: now,
              finished_at: now,
              duration_ms: 10,
              meta: %{rows_written: 2, relation: "gold.orders"},
              error: nil,
              attempt_count: 1,
              max_attempts: 1,
              attempts: []
            }
          ],
          metadata: %{runner: true}
        }
      )

    assert :ok = Storage.put_run(run_state)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:get, "/api/orchestrator/v1/runs/run_detail_metadata")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 200
    assert %{"data" => %{"run" => run}} = Jason.decode!(response.resp_body)
    assert run["metadata"] == %{"source" => "test"}

    assert [%{"asset_ref" => "Elixir.MyApp.Assets.Gold:asset", "meta" => meta}] =
             run["asset_results"]

    assert meta == %{"rows_written" => 2, "relation" => "gold.orders"}
    assert run["params"] == %{"limit" => 1}
    assert run["trigger"] == %{"type" => "manual"}
  end

  test "run list exposes per-asset metadata for asset catalog summaries" do
    now = DateTime.utc_now()

    run_state =
      RunState.new(
        id: "run_list_metadata",
        manifest_version_id: "mv_list_metadata",
        manifest_content_hash: "hash_list_metadata",
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Gold, :asset}]
      )
      |> RunState.transition(
        status: :ok,
        result: %{
          status: :ok,
          asset_results: [
            %Favn.Run.AssetResult{
              ref: {MyApp.Assets.Gold, :asset},
              stage: 0,
              status: :ok,
              started_at: now,
              finished_at: now,
              duration_ms: 10,
              meta: %{rows_written: 3, relation: "gold.orders"},
              error: nil,
              attempt_count: 1,
              max_attempts: 1,
              attempts: []
            }
          ]
        }
      )

    assert :ok = Storage.put_run(run_state)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:get, "/api/orchestrator/v1/runs")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 200
    assert %{"data" => %{"items" => runs}} = Jason.decode!(response.resp_body)
    run = Enum.find(runs, &(&1["id"] == "run_list_metadata"))
    assert run["target_refs"] == ["Elixir.MyApp.Assets.Gold:asset"]

    assert [%{"asset_ref" => "Elixir.MyApp.Assets.Gold:asset", "meta" => meta}] =
             run["asset_results"]

    assert meta == %{"rows_written" => 3, "relation" => "gold.orders"}
  end

  test "inspection endpoint dispatches to runner and caps sample limit" do
    version = dependency_manifest_version("mv_inspection")
    assert :ok = FavnOrchestrator.register_manifest(version)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(
        :get,
        "/api/orchestrator/v1/manifests/mv_inspection/assets/asset:Elixir.MyApp.Assets.Gold:asset/inspection?limit=100"
      )
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 200
    assert %{"data" => %{"inspection" => inspection}} = Jason.decode!(response.resp_body)
    assert inspection["asset_ref"] == "Elixir.MyApp.Assets.Gold:asset"
    assert inspection["row_count"] == 2
    assert inspection["sample"]["limit"] == 20
    assert [%{"name" => "id", "data_type" => "INTEGER"}] = inspection["columns"]
  end

  test "lists in-flight run ids for reload guard" do
    seed_run_events!("run_in_flight_a", [1])

    response =
      conn(:get, "/api/orchestrator/v1/runs/in-flight")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> Router.call(@opts)

    assert response.status == 200

    assert %{"data" => %{"run_ids" => run_ids, "count" => count}} =
             Jason.decode!(response.resp_body)

    assert "run_in_flight_a" in run_ids
    assert count >= 1
  end

  test "run submission without actor context returns unauthenticated" do
    response =
      conn(:post, "/api/orchestrator/v1/runs", %{
        target: %{type: "pipeline", id: "pipeline:Elixir.MyApp.Pipelines.DailyOrders"},
        manifest_selection: %{mode: "active"}
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> Router.call(@opts)

    assert response.status == 401
    assert %{"error" => %{"code" => "unauthenticated"}} = Jason.decode!(response.resp_body)
  end

  test "read endpoints return unauthenticated for invalid forwarded actor sessions" do
    response =
      conn(:get, "/api/orchestrator/v1/runs")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", "actor_stale")
      |> put_req_header("x-favn-session-id", "session_stale")
      |> Router.call(@opts)

    assert response.status == 401
    assert %{"error" => %{"code" => "unauthenticated"}} = Jason.decode!(response.resp_body)
  end

  test "run submission accepts explicit asset dependency mode" do
    version = dependency_manifest_version("mv_dependency_scope")
    assert :ok = FavnOrchestrator.register_manifest(version)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:post, "/api/orchestrator/v1/runs", %{
        target: %{type: "asset", id: "asset:Elixir.MyApp.Assets.Gold:asset"},
        manifest_selection: %{mode: "version", manifest_version_id: version.manifest_version_id},
        dependencies: "none"
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 201
    assert %{"data" => %{"run" => %{"id" => run_id}}} = Jason.decode!(response.resp_body)
    assert {:ok, run} = FavnOrchestrator.get_run(run_id)
    assert run.manifest_version_id == "mv_dependency_scope"
    assert run.plan.topo_order == [{MyApp.Assets.Gold, :asset}]
  end

  test "run submission rejects invalid dependency mode" do
    version = dependency_manifest_version("mv_invalid_dependency_scope")
    assert :ok = FavnOrchestrator.register_manifest(version)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:post, "/api/orchestrator/v1/runs", %{
        target: %{type: "asset", id: "asset:Elixir.MyApp.Assets.Gold:asset"},
        manifest_selection: %{mode: "version", manifest_version_id: version.manifest_version_id},
        dependencies: "downstream"
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 422
    assert %{"error" => %{"code" => "validation_failed"}} = Jason.decode!(response.resp_body)
  end

  test "run submission rejects request-level dependency mode for pipeline targets" do
    version = schedule_manifest_version("mv_pipeline_dependency_rejected")
    assert :ok = FavnOrchestrator.register_manifest(version)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:post, "/api/orchestrator/v1/runs", %{
        target: %{type: "pipeline", id: "pipeline:Elixir.MyApp.Pipelines.DailyOrders"},
        manifest_selection: %{mode: "version", manifest_version_id: version.manifest_version_id},
        dependencies: "none"
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 422
    assert %{"error" => %{"code" => "validation_failed"}} = Jason.decode!(response.resp_body)
  end

  test "run submission reports missing pipeline window request clearly" do
    version = schedule_manifest_version("mv_pipeline_window_missing")
    assert :ok = FavnOrchestrator.register_manifest(version)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:post, "/api/orchestrator/v1/runs", %{
        target: %{type: "pipeline", id: "pipeline:Elixir.MyApp.Pipelines.DailyOrders"},
        manifest_selection: %{mode: "version", manifest_version_id: version.manifest_version_id}
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 422

    assert %{
             "error" => %{
               "code" => "validation_failed",
               "message" => "Pipeline requires an explicit day window"
             }
           } = Jason.decode!(response.resp_body)
  end

  test "run submission reports pipeline window kind mismatch clearly" do
    version = schedule_manifest_version("mv_pipeline_window_mismatch")
    assert :ok = FavnOrchestrator.register_manifest(version)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:post, "/api/orchestrator/v1/runs", %{
        target: %{type: "pipeline", id: "pipeline:Elixir.MyApp.Pipelines.DailyOrders"},
        manifest_selection: %{mode: "version", manifest_version_id: version.manifest_version_id},
        window: %{mode: "single", kind: "month", value: "2026-03"}
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 422

    assert %{
             "error" => %{
               "code" => "validation_failed",
               "message" => "Window kind month does not match pipeline policy day"
             }
           } = Jason.decode!(response.resp_body)
  end

  test "submits pipeline backfill and lists requested windows" do
    version = schedule_manifest_version("mv_backfill_submit_http")
    assert :ok = FavnOrchestrator.register_manifest(version)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    submit_response =
      conn(:post, "/api/orchestrator/v1/backfills", %{
        "target" => %{"type" => "pipeline", "id" => "pipeline:Elixir.MyApp.Pipelines.DailyOrders"},
        "manifest_selection" => %{
          "mode" => "version",
          "manifest_version_id" => version.manifest_version_id
        },
        "range" => %{
          "from" => "2026-01-01",
          "to" => "2026-01-02",
          "kind" => "day",
          "timezone" => "Etc/UTC"
        }
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert submit_response.status == 201

    assert %{
             "data" => %{
               "run" => %{"id" => backfill_run_id, "submit_kind" => "backfill_pipeline"}
             }
           } =
             Jason.decode!(submit_response.resp_body)

    list_response =
      conn(:get, "/api/orchestrator/v1/backfills/#{backfill_run_id}/windows")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert list_response.status == 200
    assert %{"data" => %{"items" => windows}} = Jason.decode!(list_response.resp_body)
    assert length(windows) == 2
    assert Enum.all?(windows, &(&1["backfill_run_id"] == backfill_run_id))
    assert Enum.all?(windows, &(&1["pipeline_module"] == "Elixir.MyApp.Pipelines.DailyOrders"))
  end

  test "backfill submit endpoint rejects oversized ranges with validation details" do
    version = schedule_manifest_version("mv_backfill_too_large_http")
    assert :ok = FavnOrchestrator.register_manifest(version)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:post, "/api/orchestrator/v1/backfills", %{
        "target" => %{"type" => "pipeline", "id" => "pipeline:Elixir.MyApp.Pipelines.DailyOrders"},
        "manifest_selection" => %{
          "mode" => "version",
          "manifest_version_id" => version.manifest_version_id
        },
        "range" => %{
          "from" => "2024-01-01",
          "to" => "2026-01-01",
          "kind" => "day",
          "timezone" => "Etc/UTC"
        }
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 422

    assert %{
             "error" => %{
               "code" => "validation_failed",
               "details" => %{"requested" => requested, "max" => 500}
             }
           } = Jason.decode!(response.resp_body)

    assert requested > 500
  end

  for option <- ["lookback", "lookback_policy"] do
    @option option

    test "backfill submit endpoint rejects unsupported #{@option} input" do
      version = schedule_manifest_version("mv_backfill_#{@option}_rejected_http")
      assert :ok = FavnOrchestrator.register_manifest(version)

      {:ok, session, actor} = Auth.password_login("admin", "admin-password")

      payload =
        Map.put(
          %{
            "target" => %{
              "type" => "pipeline",
              "id" => "pipeline:Elixir.MyApp.Pipelines.DailyOrders"
            },
            "manifest_selection" => %{
              "mode" => "version",
              "manifest_version_id" => version.manifest_version_id
            },
            "range" => %{
              "from" => "2026-01-01",
              "to" => "2026-01-02",
              "kind" => "day",
              "timezone" => "Etc/UTC"
            }
          },
          @option,
          %{"days" => 7}
        )

      response =
        conn(:post, "/api/orchestrator/v1/backfills", payload)
        |> put_req_header("authorization", "Bearer test-service-token")
        |> put_req_header("x-favn-actor-id", actor.id)
        |> put_req_header("x-favn-session-id", session.id)
        |> Router.call(@opts)

      assert response.status == 422

      assert %{
               "error" => %{
                 "code" => "validation_failed",
                 "message" => "Unsupported backfill option",
                 "details" => %{"option" => @option}
               }
             } = Jason.decode!(response.resp_body)
    end
  end

  test "backfill submit endpoint reports missing coverage baseline clearly" do
    version = schedule_manifest_version("mv_backfill_missing_baseline_http")
    assert :ok = FavnOrchestrator.register_manifest(version)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:post, "/api/orchestrator/v1/backfills", %{
        "target" => %{"type" => "pipeline", "id" => "pipeline:Elixir.MyApp.Pipelines.DailyOrders"},
        "manifest_selection" => %{
          "mode" => "version",
          "manifest_version_id" => version.manifest_version_id
        },
        "range" => %{
          "from" => "2026-01-01",
          "to" => "2026-01-01",
          "kind" => "day",
          "timezone" => "Etc/UTC"
        },
        "coverage_baseline_id" => "missing_baseline"
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 422

    assert %{
             "error" => %{
               "code" => "validation_failed",
               "message" => "Coverage baseline was not found",
               "details" => %{"coverage_baseline_id" => "missing_baseline"}
             }
           } = Jason.decode!(response.resp_body)
  end

  test "lists backfill coverage baselines and asset window states" do
    %{window_key: window_key} = seed_backfill_http_state!()

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    baseline_response =
      conn(
        :get,
        "/api/orchestrator/v1/backfills/coverage-baselines?pipeline_module=MyApp.Pipelines.DailyOrders&status=ok"
      )
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert baseline_response.status == 200

    assert %{
             "data" => %{
               "items" => [%{"baseline_id" => "baseline_http"}],
               "pagination" => %{
                 "limit" => 100,
                 "offset" => 0,
                 "has_more" => false,
                 "next_offset" => nil
               }
             }
           } =
             Jason.decode!(baseline_response.resp_body)

    state_response =
      conn(
        :get,
        "/api/orchestrator/v1/assets/window-states?asset_ref_module=MyApp.Assets.DailyOrders&asset_ref_name=asset"
      )
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert state_response.status == 200

    assert %{
             "data" => %{
               "items" => [%{"window_key" => ^window_key, "status" => "error"}],
               "pagination" => %{"limit" => 100, "offset" => 0}
             }
           } =
             Jason.decode!(state_response.resp_body)
  end

  test "backfill read endpoints reject invalid pagination" do
    seed_backfill_http_state!()
    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:get, "/api/orchestrator/v1/backfills/coverage-baselines?limit=501")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 422

    assert %{"error" => %{"message" => "Invalid pagination parameters"}} =
             Jason.decode!(response.resp_body)
  end

  test "backfill read endpoints reject invalid filters" do
    seed_backfill_http_state!()

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    headers = fn conn ->
      conn
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
    end

    assert conn(:get, "/api/orchestrator/v1/backfills/backfill_http/windows?status=bad")
           |> headers.()
           |> Router.call(@opts)
           |> Map.fetch!(:status) == 422

    assert conn(:get, "/api/orchestrator/v1/backfills/backfill_http/windows?pipeline_module=Typo")
           |> headers.()
           |> Router.call(@opts)
           |> Map.fetch!(:status) == 422

    assert conn(:get, "/api/orchestrator/v1/backfills/coverage-baselines?pipeline_module=Typo")
           |> headers.()
           |> Router.call(@opts)
           |> Map.fetch!(:status) == 422

    assert conn(
             :get,
             "/api/orchestrator/v1/backfills/coverage-baselines?pipeline_module=Elixir.String"
           )
           |> headers.()
           |> Router.call(@opts)
           |> Map.fetch!(:status) == 422

    assert conn(
             :get,
             "/api/orchestrator/v1/assets/window-states?asset_ref_module=Typo&asset_ref_name=asset"
           )
           |> headers.()
           |> Router.call(@opts)
           |> Map.fetch!(:status) == 422

    assert conn(
             :get,
             "/api/orchestrator/v1/assets/window-states?asset_ref_module=Elixir.String&asset_ref_name=asset"
           )
           |> headers.()
           |> Router.call(@opts)
           |> Map.fetch!(:status) == 422
  end

  test "backfill read filter lookup failures are operational errors" do
    {:ok, session, actor} = Auth.password_login("admin", "admin-password")
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)

    on_exit(fn -> restore_env(:favn_orchestrator, :storage_adapter, previous_adapter) end)

    Application.put_env(:favn_orchestrator, :storage_adapter, String)

    response =
      conn(
        :get,
        "/api/orchestrator/v1/backfills/coverage-baselines?pipeline_module=MyApp.Pipelines.DailyOrders"
      )
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 400

    assert %{"error" => %{"code" => "bad_request", "message" => "Request failed"}} =
             Jason.decode!(response.resp_body)
  end

  test "reruns failed backfill window from latest attempt" do
    version = schedule_manifest_version("mv_backfill_rerun_http")
    assert :ok = FavnOrchestrator.register_manifest(version)
    %{window_key: window_key} = seed_backfill_http_state!(version.manifest_version_id)

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:post, "/api/orchestrator/v1/backfills/backfill_http/windows/rerun", %{
        "window_key" => window_key
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 201

    assert %{"data" => %{"run" => %{"id" => rerun_id, "submit_kind" => "rerun"}}} =
             Jason.decode!(response.resp_body)

    assert {:ok, rerun} = Storage.get_run(rerun_id)
    assert rerun.parent_run_id == "backfill_http"
    assert rerun.trigger.kind == :backfill
    assert rerun.trigger.window_key == window_key
  end

  test "service token can cancel run without actor headers" do
    seed_run_events!("run_cancel_service", [1])

    response =
      conn(:post, "/api/orchestrator/v1/runs/run_cancel_service/cancel")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> Router.call(@opts)

    assert response.status == 200

    assert %{"data" => %{"cancelled" => true, "run_id" => "run_cancel_service"}} =
             Jason.decode!(response.resp_body)
  end

  test "generic cancel and rerun return conflict for backfill parent runs" do
    parent =
      RunState.new(
        id: "backfill_parent_http",
        manifest_version_id: "mv_backfill_parent_http",
        manifest_content_hash: "hash_backfill_parent_http",
        asset_ref: {MyApp.Assets.DailyOrders, :asset},
        target_refs: [{MyApp.Assets.DailyOrders, :asset}],
        submit_kind: :backfill_pipeline
      )
      |> Map.put(:status, :running)
      |> RunState.with_snapshot_hash()

    assert :ok = Storage.put_run(parent)
    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    cancel_response =
      conn(:post, "/api/orchestrator/v1/runs/backfill_parent_http/cancel")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert cancel_response.status == 409
    assert %{"error" => %{"code" => "conflict"}} = Jason.decode!(cancel_response.resp_body)

    rerun_response =
      conn(:post, "/api/orchestrator/v1/runs/backfill_parent_http/rerun")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert rerun_response.status == 409
    assert %{"error" => %{"code" => "conflict"}} = Jason.decode!(rerun_response.resp_body)
  end

  test "registers manifest through private API" do
    version = schedule_manifest_version("mv_register_router")

    response =
      conn(:post, "/api/orchestrator/v1/manifests", %{
        manifest_version_id: version.manifest_version_id,
        content_hash: version.content_hash,
        schema_version: version.schema_version,
        runner_contract_version: version.runner_contract_version,
        serialization_format: version.serialization_format,
        manifest: version.manifest
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> Router.call(@opts)

    assert response.status == 201

    assert %{"data" => %{"manifest" => manifest}} = Jason.decode!(response.resp_body)
    assert manifest["manifest_version_id"] == "mv_register_router"
  end

  test "returns conflict when manifest version id changes content" do
    version = schedule_manifest_version("mv_register_conflict")
    assert :ok = FavnOrchestrator.register_manifest(version)

    conflicting_manifest =
      version.manifest
      |> Map.from_struct()
      |> Map.put(:metadata, %{changed: true})

    assert {:ok, conflicting_version} =
             Version.new(conflicting_manifest,
               manifest_version_id: version.manifest_version_id
             )

    response =
      conn(:post, "/api/orchestrator/v1/manifests", %{
        manifest_version_id: conflicting_version.manifest_version_id,
        content_hash: conflicting_version.content_hash,
        schema_version: conflicting_version.schema_version,
        runner_contract_version: conflicting_version.runner_contract_version,
        serialization_format: conflicting_version.serialization_format,
        manifest: conflicting_manifest
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> Router.call(@opts)

    assert response.status == 409
    assert %{"error" => %{"code" => "manifest_conflict"}} = Jason.decode!(response.resp_body)
  end

  test "rejects manifest publication when supplied hash does not match payload" do
    version = schedule_manifest_version("mv_register_hash_mismatch")

    response =
      conn(:post, "/api/orchestrator/v1/manifests", %{
        manifest_version_id: version.manifest_version_id,
        content_hash: String.duplicate("0", 64),
        schema_version: version.schema_version,
        runner_contract_version: version.runner_contract_version,
        serialization_format: version.serialization_format,
        manifest: version.manifest
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> Router.call(@opts)

    assert response.status == 422
    assert %{"error" => %{"code" => "validation_failed"}} = Jason.decode!(response.resp_body)
  end

  test "forbids non-operator manifest activation" do
    version = schedule_manifest_version("mv_activate_forbidden")
    assert :ok = FavnOrchestrator.register_manifest(version)

    {:ok, _viewer} =
      AuthStore.create_actor("viewer_user", "viewer-pass-1", "Viewer User", [:viewer])

    {:ok, session, actor} = Auth.password_login("viewer_user", "viewer-pass-1")

    response =
      conn(:post, "/api/orchestrator/v1/manifests/mv_activate_forbidden/activate")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 403
    assert %{"error" => %{"code" => "forbidden"}} = Jason.decode!(response.resp_body)
  end

  test "admin can list actors but viewer cannot" do
    {:ok, _viewer} = AuthStore.create_actor("viewer2", "viewer-pass-2", "Viewer Two", [:viewer])

    {:ok, admin_session, admin_actor} = Auth.password_login("admin", "admin-password")

    admin_response =
      conn(:get, "/api/orchestrator/v1/actors")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", admin_actor.id)
      |> put_req_header("x-favn-session-id", admin_session.id)
      |> Router.call(@opts)

    assert admin_response.status == 200
    assert %{"data" => %{"items" => actors}} = Jason.decode!(admin_response.resp_body)
    assert Enum.any?(actors, &(&1["username"] == "admin"))
    assert Enum.any?(actors, &(&1["username"] == "viewer2"))

    {:ok, viewer_session, viewer_actor} = Auth.password_login("viewer2", "viewer-pass-2")

    viewer_response =
      conn(:get, "/api/orchestrator/v1/actors")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", viewer_actor.id)
      |> put_req_header("x-favn-session-id", viewer_session.id)
      |> Router.call(@opts)

    assert viewer_response.status == 403
    assert %{"error" => %{"code" => "forbidden"}} = Jason.decode!(viewer_response.resp_body)
  end

  test "admin can get one actor by id" do
    {:ok, viewer} =
      AuthStore.create_actor("viewer_lookup", "viewer-pass-3", "Viewer Lookup", [:viewer])

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:get, "/api/orchestrator/v1/actors/#{viewer.id}")
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 200
    assert %{"data" => %{"actor" => actor_payload}} = Jason.decode!(response.resp_body)
    assert actor_payload["id"] == viewer.id
    assert actor_payload["username"] == "viewer_lookup"
  end

  test "admin can create actor via API" do
    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:post, "/api/orchestrator/v1/actors", %{
        username: "operator_user",
        password: "operator-pass-1",
        display_name: "Operator User",
        roles: ["operator"]
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 201
    assert %{"data" => %{"actor" => payload_actor}} = Jason.decode!(response.resp_body)
    assert payload_actor["username"] == "operator_user"
    assert payload_actor["roles"] == ["operator"]
  end

  test "admin can update actor roles via API" do
    {:ok, managed_actor} =
      AuthStore.create_actor("managed_role", "managed-pass-1", "Managed", [:viewer])

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    response =
      conn(:put, "/api/orchestrator/v1/actors/#{managed_actor.id}/roles", %{
        roles: ["admin"]
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert response.status == 200
    assert %{"data" => %{"actor" => payload_actor}} = Jason.decode!(response.resp_body)
    assert payload_actor["roles"] == ["admin"]
  end

  test "admin can reset actor password and old password is revoked" do
    {:ok, managed_actor} =
      AuthStore.create_actor("managed_password", "managed-pass-1", "Managed Password", [:viewer])

    {:ok, session, actor} = Auth.password_login("admin", "admin-password")

    reset_response =
      conn(:put, "/api/orchestrator/v1/actors/#{managed_actor.id}/password", %{
        password: "managed-pass-2"
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert reset_response.status == 200
    assert %{"data" => %{"updated" => true}} = Jason.decode!(reset_response.resp_body)

    assert {:error, :invalid_credentials} =
             Auth.password_login("managed_password", "managed-pass-1")

    assert {:ok, _new_session, _managed_actor} =
             Auth.password_login("managed_password", "managed-pass-2")
  end

  test "viewer is forbidden from actor management commands" do
    {:ok, _viewer} =
      AuthStore.create_actor("viewer_cmd", "viewer-pass-4", "Viewer Cmd", [:viewer])

    {:ok, session, actor} = Auth.password_login("viewer_cmd", "viewer-pass-4")

    create_response =
      conn(:post, "/api/orchestrator/v1/actors", %{
        username: "blocked_create",
        password: "blocked-pass-1",
        roles: ["viewer"]
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert create_response.status == 403

    password_response =
      conn(:put, "/api/orchestrator/v1/actors/#{actor.id}/password", %{
        password: "viewer-pass-5"
      })
      |> put_req_header("authorization", "Bearer test-service-token")
      |> put_req_header("x-favn-actor-id", actor.id)
      |> put_req_header("x-favn-session-id", session.id)
      |> Router.call(@opts)

    assert password_response.status == 403
  end

  defp ensure_auth_store_started do
    case Process.whereis(AuthStore) do
      nil ->
        start_supervised!({AuthStore, []})
        :started

      _pid ->
        :existing
    end
  end

  defp maybe_stop_auth_store(:existing), do: :ok

  defp maybe_stop_auth_store(:started) do
    case Process.whereis(AuthStore) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal)
    end
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)

  defp schedule_manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{
          ref: {MyApp.Assets.DailyOrders, :asset},
          module: MyApp.Assets.DailyOrders,
          name: :asset
        }
      ],
      schedules: [
        %Schedule{
          module: MyApp.Schedules,
          name: :daily,
          ref: {MyApp.Schedules, :daily},
          cron: "0 * * * *",
          timezone: "Etc/UTC",
          missed: :skip,
          overlap: :forbid,
          active: true
        }
      ],
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.DailyOrders,
          name: :daily_orders,
          selectors: [{:asset, {MyApp.Assets.DailyOrders, :asset}}],
          deps: :all,
          schedule: {:ref, {MyApp.Schedules, :daily}},
          window: Policy.new!(:day),
          source: :dsl,
          outputs: [:asset],
          config: %{},
          metadata: %{}
        }
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp dependency_manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{
          ref: {MyApp.Assets.Raw, :asset},
          module: MyApp.Assets.Raw,
          name: :asset,
          type: :elixir,
          relation: %{connection: :warehouse, schema: "raw", name: "orders"}
        },
        %Favn.Manifest.Asset{
          ref: {MyApp.Assets.Gold, :asset},
          module: MyApp.Assets.Gold,
          name: :asset,
          type: :sql,
          depends_on: [{MyApp.Assets.Raw, :asset}],
          relation: %{connection: :warehouse, schema: "gold", name: "orders"},
          runtime_config: %{source_system: %{segment_id: Ref.env!("SOURCE_SYSTEM_SEGMENT_ID")}},
          metadata: %{owner: "analytics"}
        }
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp seed_backfill_http_state!(manifest_version_id \\ "mv_backfill_http") do
    version = schedule_manifest_version(manifest_version_id)
    assert :ok = FavnOrchestrator.register_manifest(version)

    now = DateTime.utc_now()
    start_at = ~U[2026-01-01 00:00:00Z]
    end_at = ~U[2026-01-02 00:00:00Z]
    {:ok, anchor} = Anchor.new(:day, start_at, end_at, timezone: "Etc/UTC")
    window_key = WindowKey.encode(anchor.key)

    source_run =
      RunState.new(
        id: "backfill_child_http",
        manifest_version_id: manifest_version_id,
        manifest_content_hash: "hash_backfill_http",
        asset_ref: {MyApp.Assets.DailyOrders, :asset},
        target_refs: [{MyApp.Assets.DailyOrders, :asset}],
        trigger: %{
          kind: :backfill,
          backfill_run_id: "backfill_http",
          window_key: window_key
        },
        metadata: %{
          submit_kind: :pipeline,
          pipeline_submit_ref: MyApp.Pipelines.DailyOrders,
          pipeline_target_refs: [{MyApp.Assets.DailyOrders, :asset}],
          pipeline_dependencies: :all
        },
        submit_kind: :pipeline,
        parent_run_id: "backfill_http",
        root_run_id: "backfill_http",
        lineage_depth: 1
      )
      |> Map.put(:status, :error)
      |> Map.put(:error, :seeded_failure)
      |> Map.put(:updated_at, now)
      |> RunState.with_snapshot_hash()

    assert :ok = Storage.put_run(source_run)

    {:ok, baseline} =
      CoverageBaseline.new(%{
        baseline_id: "baseline_http",
        pipeline_module: MyApp.Pipelines.DailyOrders,
        source_key: "orders",
        segment_key_hash: "hash_segment",
        window_kind: :day,
        timezone: "Etc/UTC",
        coverage_until: start_at,
        created_by_run_id: "baseline_run_http",
        manifest_version_id: manifest_version_id,
        status: :ok,
        created_at: now,
        updated_at: now
      })

    assert :ok = Storage.put_coverage_baseline(baseline)

    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: "backfill_http",
        child_run_id: "backfill_child_http",
        pipeline_module: MyApp.Pipelines.DailyOrders,
        manifest_version_id: manifest_version_id,
        coverage_baseline_id: "baseline_http",
        window_kind: :day,
        window_start_at: start_at,
        window_end_at: end_at,
        timezone: "Etc/UTC",
        window_key: window_key,
        status: :error,
        attempt_count: 1,
        latest_attempt_run_id: "backfill_child_http",
        last_error: :seeded_failure,
        created_at: now,
        updated_at: now
      })

    assert :ok = Storage.put_backfill_window(window)

    {:ok, state} =
      AssetWindowState.new(%{
        asset_ref_module: MyApp.Assets.DailyOrders,
        asset_ref_name: :asset,
        pipeline_module: MyApp.Pipelines.DailyOrders,
        manifest_version_id: manifest_version_id,
        window_kind: :day,
        window_start_at: start_at,
        window_end_at: end_at,
        timezone: "Etc/UTC",
        window_key: window_key,
        status: :error,
        latest_run_id: "backfill_child_http",
        latest_parent_run_id: "backfill_http",
        latest_error: :seeded_failure,
        updated_at: now
      })

    assert :ok = Storage.put_asset_window_state(state)

    %{window_key: window_key}
  end

  defp seed_run_events!(run_id, sequences) when is_binary(run_id) and is_list(sequences) do
    now = DateTime.utc_now()

    run_state =
      RunState.new(
        id: run_id,
        manifest_version_id: "mv_stream",
        manifest_content_hash: "hash_stream",
        asset_ref: {MyApp.Assets.Streamed, :asset},
        target_refs: [{MyApp.Assets.Streamed, :asset}]
      )
      |> Map.put(:event_seq, Enum.max(sequences))
      |> Map.put(:status, :running)
      |> Map.put(:updated_at, now)
      |> RunState.with_snapshot_hash()

    assert :ok = Storage.put_run(run_state)

    Enum.each(sequences, fn sequence ->
      assert :ok =
               Storage.append_run_event(run_id, %{
                 schema_version: 1,
                 run_id: run_id,
                 sequence: sequence,
                 event_type: :run_updated,
                 entity: :run,
                 occurred_at: DateTime.add(now, sequence, :second),
                 status: :running,
                 manifest_version_id: "mv_stream",
                 manifest_content_hash: "hash_stream",
                 asset_ref: nil,
                 stage: nil,
                 data: %{}
               })
    end)
  end
end
