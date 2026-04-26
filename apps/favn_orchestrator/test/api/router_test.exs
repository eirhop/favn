defmodule FavnOrchestrator.API.RouterTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Favn.Manifest
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Version
  alias FavnOrchestrator
  alias FavnOrchestrator.API.Router
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  @opts Router.init([])

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)
    previous_username = Application.get_env(:favn_orchestrator, :auth_bootstrap_username)
    previous_password = Application.get_env(:favn_orchestrator, :auth_bootstrap_password)
    previous_display = Application.get_env(:favn_orchestrator, :auth_bootstrap_display_name)

    Application.put_env(:favn_orchestrator, :api_service_tokens, ["test-service-token"])
    Application.put_env(:favn_orchestrator, :auth_bootstrap_username, "admin")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_password, "admin-password")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_display_name, "Admin User")

    auth_start = ensure_auth_store_started()
    :ok = AuthStore.reset()
    Memory.reset()
    :ok = Auth.bootstrap_admin()

    on_exit(fn ->
      restore_env(:favn_orchestrator, :api_service_tokens, previous_tokens)
      restore_env(:favn_orchestrator, :auth_bootstrap_username, previous_username)
      restore_env(:favn_orchestrator, :auth_bootstrap_password, previous_password)
      restore_env(:favn_orchestrator, :auth_bootstrap_display_name, previous_display)
      maybe_stop_auth_store(auth_start)
    end)

    :ok
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
          selectors: [{MyApp.Assets.DailyOrders, :asset}],
          deps: :all,
          schedule: {:ref, {MyApp.Schedules, :daily}},
          window: :day,
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
