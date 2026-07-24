defmodule Favn.CLI.OrchestratorClientTest do
  use ExUnit.Case, async: false

  alias Favn.CLI.OrchestratorClient
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Publication
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.SQL.Template

  test "in_flight_runs/3 parses run ids" do
    {:ok, base_url, _server} = start_server(~s({"data":{"run_ids":["run_a","run_b"]}}), 200)

    assert {:ok, ["run_a", "run_b"]} =
             OrchestratorClient.in_flight_runs(base_url, "token", %{
               "workspace_id" => "local-dev"
             })
  end

  test "runner replacement methods carry the opaque maintenance lease" do
    parent = self()
    runner_release_id = "rr_" <> String.duplicate("a", 64)

    {:ok, base_url, _server} =
      start_server_sequence(
        [
          {~s({"data":{"maintenance_token":"lease-token"}}), 200, 0},
          {~s({"data":{"maintenance?":true,"maintenance_kind":"runner_replacement","active_admissions":0}}),
           200, 0},
          {JSON.encode!(%{data: %{runner_release_id: runner_release_id, ready?: true}}), 200, 0},
          {~s({"data":{"status":"accepting"}}), 200, 0}
        ],
        parent: parent
      )

    assert {:ok, "lease-token"} =
             OrchestratorClient.begin_runner_replacement(
               base_url,
               "service-token",
               "lease-token"
             )

    assert_receive {:request_path, "/api/orchestrator/v1/maintenance/runner-replacement"}
    assert_receive {:request_headers, begin_headers}
    assert begin_headers["authorization"] == "Bearer service-token"
    assert begin_headers["x-favn-maintenance-token"] == "lease-token"
    assert_receive {:request_body, "{}"}

    assert {:ok, %{"active_admissions" => 0}} =
             OrchestratorClient.runner_replacement_status(base_url, "service-token")

    assert_receive {:request_path, "/api/orchestrator/v1/maintenance/runner-replacement"}
    assert_receive {:request_headers, _status_headers}
    assert_receive {:request_body, ""}

    assert {:ok, %{"runner_release_id" => ^runner_release_id}} =
             OrchestratorClient.verify_replacement_runner(
               base_url,
               "service-token",
               "lease-token",
               runner_release_id
             )

    assert_receive {:request_path,
                    "/api/orchestrator/v1/maintenance/runner-replacement/verify-runner"}

    assert_receive {:request_headers, verify_headers}
    assert verify_headers["x-favn-maintenance-token"] == "lease-token"
    assert_receive {:request_body, verify_body}
    assert JSON.decode!(verify_body)["runner_release_id"] == runner_release_id

    assert :ok =
             OrchestratorClient.finish_runner_replacement(
               base_url,
               "service-token",
               "lease-token"
             )

    assert_receive {:request_path, "/api/orchestrator/v1/maintenance/runner-replacement"}
    assert_receive {:request_headers, finish_headers}
    assert finish_headers["x-favn-maintenance-token"] == "lease-token"
    assert_receive {:request_body, ""}
  end

  test "in_flight_runs/3 returns operation context on non-2xx response" do
    {:ok, base_url, _server} = start_server(~s({"error":{"code":"bad_request"}}), 400)

    assert {:error,
            %{
              operation: :list_in_flight_runs,
              method: :get,
              url: url,
              reason: {:http_error, 400, _decoded}
            }} =
             OrchestratorClient.in_flight_runs(base_url, "token", %{
               "workspace_id" => "local-dev"
             })

    assert url == base_url <> "/api/orchestrator/v1/runs/in-flight"
  end

  test "connect failures are structured and include operation context" do
    base_url = "http://127.0.0.1:#{unused_port()}"

    assert {:error,
            %{
              operation: :list_in_flight_runs,
              method: :get,
              url: url,
              reason: {:connect_failed, _reason}
            }} =
             OrchestratorClient.in_flight_runs(base_url, "token", %{
               "workspace_id" => "local-dev"
             })

    assert url == base_url <> "/api/orchestrator/v1/runs/in-flight"
  end

  test "operation errors never echo URL credentials or query values" do
    assert {:error, error} =
             OrchestratorClient.health(
               "https://operator:embedded-secret@control.internal?token=query-secret"
             )

    rendered = inspect(error)
    refute rendered =~ "embedded-secret"
    refute rendered =~ "query-secret"
    assert error.url == "https://control.internal"
  end

  test "publish_manifest/3 serializes manifest structs before JSON encoding" do
    parent = self()

    {:ok, base_url, _server} =
      start_server_sequence(
        [
          {missing_response([]), 200, 0},
          {~s({"data":{"ok":true}}), 200, 0}
        ],
        parent: parent
      )

    manifest =
      FavnTestSupport.with_manifest_contract(%{
        assets: [],
        pipelines: [],
        schedules: [],
        graph: %{},
        metadata: %{}
      })

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_orchestrator_client_test")
    {:ok, publication} = Publication.from_parts(version, [])

    assert {:ok, %{"data" => %{"ok" => true}}} =
             OrchestratorClient.publish_manifest(base_url, "token", publication)

    assert_receive {:request_path, "/api/orchestrator/v1/execution-packages/missing"}
    assert_receive {:request_headers, headers}
    assert headers["content-encoding"] == "gzip"
    assert headers["content-type"] == "application/json"

    assert_receive {:request_body, missing_body}
    assert JSON.decode!(:zlib.gunzip(missing_body)) == %{"hashes" => []}

    assert_receive {:request_path, "/api/orchestrator/v1/manifests"}
    assert_receive {:request_headers, publish_headers}
    assert publish_headers["content-encoding"] == "gzip"
    assert publish_headers["content-type"] == "application/json"

    assert_receive {:request_body, compressed_body}
    body = :zlib.gunzip(compressed_body)
    assert body =~ ~s("manifest_version_id":"mv_orchestrator_client_test")
    assert body =~ ~s("manifest":{"assets":[])
    refute body =~ ~s("__struct__")
  end

  test "publish_manifest/3 allows a response beyond the generic client timeout" do
    {:ok, base_url, _server} =
      start_server_sequence([
        {missing_response([]), 200, 0},
        {~s({"data":{"ok":true}}), 200, 5_100}
      ])

    manifest =
      FavnTestSupport.with_manifest_contract(%{
        assets: [],
        pipelines: [],
        schedules: [],
        graph: %{},
        metadata: %{}
      })

    {:ok, version} = Version.new(manifest)
    {:ok, publication} = Publication.from_parts(version, [])

    assert {:ok, %{"data" => %{"ok" => true}}} =
             OrchestratorClient.publish_manifest(base_url, "token", publication)
  end

  test "publish_manifest/3 rejects a missing hash outside the publication" do
    unknown_hash = String.duplicate("0", 64)

    {:ok, base_url, _server} =
      start_server(missing_response([unknown_hash]), 200)

    manifest =
      FavnTestSupport.with_manifest_contract(%{
        assets: [],
        pipelines: [],
        schedules: [],
        graph: %{},
        metadata: %{}
      })

    {:ok, version} = Version.new(manifest)
    {:ok, publication} = Publication.from_parts(version, [])

    assert {:error, {:unexpected_missing_execution_package_hash, ^unknown_hash}} =
             OrchestratorClient.publish_manifest(base_url, "token", publication)
  end

  test "publish_manifest/3 honors server-advertised package batch limits" do
    publication = packaged_publication(2)
    hashes = Publication.required_package_hashes(publication.version)
    parent = self()

    {:ok, base_url, _server} =
      start_server_sequence(
        [
          {missing_response(hashes, max_packages: 1), 200, 0},
          {~s({"data":{"stored":1}}), 201, 0},
          {~s({"data":{"stored":1}}), 201, 0},
          {~s({"data":{"ok":true}}), 201, 0}
        ],
        parent: parent
      )

    assert {:ok, %{"data" => %{"ok" => true}}} =
             OrchestratorClient.publish_manifest(base_url, "token", publication)

    assert_receive {:request_path, "/api/orchestrator/v1/execution-packages/missing"}
    assert_receive {:request_headers, _headers}
    assert_receive {:request_body, _body}
    assert_receive {:request_path, "/api/orchestrator/v1/execution-packages"}
    assert_receive {:request_headers, _headers}
    assert_receive {:request_body, _body}
    assert_receive {:request_path, "/api/orchestrator/v1/execution-packages"}
    assert_receive {:request_headers, _headers}
    assert_receive {:request_body, _body}
    assert_receive {:request_path, "/api/orchestrator/v1/manifests"}
  end

  test "verify_service_token/2 checks bootstrap service-token endpoint" do
    parent = self()

    {:ok, base_url, _server} =
      start_server(~s({"data":{"authenticated":true,"service_tokens":{"redacted":true}}}), 200,
        parent: parent
      )

    assert :ok = OrchestratorClient.verify_service_token(base_url, "token")
    assert_receive {:request_path, "/api/orchestrator/v1/bootstrap/service-token"}
  end

  test "health/1 checks the orchestrator health endpoint" do
    parent = self()
    {:ok, base_url, _server} = start_server(~s({"data":{"status":"ok"}}), 200, parent: parent)

    assert :ok = OrchestratorClient.health(base_url)
    assert_receive {:request_path, "/api/orchestrator/v1/health"}
  end

  test "register_runner/4 sends the workspace actor context" do
    parent = self()

    {:ok, base_url, _server} =
      start_server(~s({"data":{"registration":{"manifest_version_id":"mv_1"}}}), 200,
        parent: parent
      )

    assert {:ok, %{"data" => %{"registration" => %{"manifest_version_id" => "mv_1"}}}} =
             OrchestratorClient.register_runner(
               base_url,
               "token",
               session_context(),
               %{manifest_version_id: "mv_1"}
             )

    assert_receive {:request_path, "/api/orchestrator/v1/manifests/mv_1/runner/register"}
    assert_receive {:request_body, body}
    assert body == "{}"
    assert_receive {:request_headers, headers}
    assert headers["x-favn-workspace-id"] == "workspace-1"
  end

  test "activate_manifest_service/4 sends workspace authority without actor credentials" do
    parent = self()

    {:ok, base_url, _server} =
      start_server(~s({"data":{"activated":true}}), 200, parent: parent)

    assert {:ok, %{"data" => %{"activated" => true}}} =
             OrchestratorClient.activate_manifest_service(
               base_url,
               "service-token",
               "mv_service",
               "workspace-service"
             )

    assert_receive {:request_path, "/api/orchestrator/v1/manifests/mv_service/activate"}
    assert_receive {:request_body, _body}
    assert_receive {:request_headers, headers}
    assert headers["x-favn-workspace-id"] == "workspace-service"
    assert headers["authorization"] == "Bearer service-token"
    refute Map.has_key?(headers, "x-favn-actor-id")
    refute Map.has_key?(headers, "x-favn-session-token")
    assert is_binary(headers["idempotency-key"])
  end

  test "bootstrap_active_manifest/3 reads the workspace active manifest" do
    parent = self()

    {:ok, base_url, _server} =
      start_server(~s({"data":{"manifest":{"manifest_version_id":"mv_1"}}}), 200, parent: parent)

    assert {:ok, %{"manifest" => %{"manifest_version_id" => "mv_1"}}} =
             OrchestratorClient.bootstrap_active_manifest(base_url, "token", session_context())

    assert_receive {:request_path, "/api/orchestrator/v1/bootstrap/active-manifest"}
  end

  test "password_login/5 selects a workspace and returns forwarded session context" do
    body =
      ~s({"data":{"session":{"id":"sess_1"},"session_token":"raw_session_token_1","actor":{"id":"act_1"}}})

    {:ok, base_url, _server} = start_server(body, 201)

    assert {:ok,
            %{
              "actor_id" => "act_1",
              "session_id" => "sess_1",
              "session_token" => "raw_session_token_1",
              "workspace_id" => "workspace-1"
            }} =
             OrchestratorClient.password_login(
               base_url,
               "token",
               "workspace-1",
               "user",
               "password-1"
             )
  end

  test "submit_run/4 sends forwarded actor and raw session token headers" do
    parent = self()
    body = ~s({"data":{"run":{"id":"run_1","status":"running"}}})
    {:ok, base_url, _server} = start_server(body, 201, parent: parent)

    assert {:ok, %{"id" => "run_1"}} =
             OrchestratorClient.submit_run(
               base_url,
               "token",
               %{
                 "actor_id" => "act_1",
                 "session_id" => "sess_1",
                 "session_token" => "raw_session_token_1"
               },
               %{target: %{type: "pipeline", id: "pipeline:Elixir.MyApp.Pipeline"}}
             )

    assert_receive {:request_headers, headers}
    assert headers["x-favn-actor-id"] == "act_1"
    assert headers["x-favn-session-token"] == "raw_session_token_1"
    refute Map.has_key?(headers, "x-favn-session-id")
    assert_idempotency_header(headers)
  end

  test "submit_run/4 sends workspace context without session headers" do
    parent = self()
    body = ~s({"data":{"run":{"id":"run_1","status":"running"}}})
    {:ok, base_url, _server} = start_server(body, 201, parent: parent)

    assert {:ok, %{"id" => "run_1"}} =
             OrchestratorClient.submit_run(
               base_url,
               "token",
               %{"workspace_id" => "local-dev"},
               %{target: %{type: "pipeline", id: "pipeline:Elixir.MyApp.Pipeline"}}
             )

    assert_receive {:request_headers, headers}
    assert headers["x-favn-workspace-id"] == "local-dev"
    refute Map.has_key?(headers, "x-favn-session-token")
    refute Map.has_key?(headers, "x-favn-actor-id")
  end

  test "submit_run/5 uses explicit idempotency key override" do
    parent = self()
    body = ~s({"data":{"run":{"id":"run_1","status":"running"}}})
    {:ok, base_url, _server} = start_server(body, 201, parent: parent)

    assert {:ok, %{"id" => "run_1"}} =
             OrchestratorClient.submit_run(
               base_url,
               "token",
               %{"workspace_id" => "local-dev"},
               %{target: %{type: "pipeline", id: "pipeline:Elixir.MyApp.Pipeline"}},
               idempotency_key: "manual-key-297"
             )

    assert_receive {:request_headers, headers}
    assert headers["idempotency-key"] == "manual-key-297"
  end

  test "separate submit_run invocations use fresh command identities" do
    parent = self()
    body = ~s({"data":{"run":{"id":"run_1","status":"running"}}})
    session = %{"workspace_id" => "local-dev"}
    payload = %{target: %{type: "pipeline", id: "pipeline:Elixir.MyApp.Pipeline"}}

    {:ok, first_url, _server} = start_server(body, 201, parent: parent)

    assert {:ok, %{"id" => "run_1"}} =
             OrchestratorClient.submit_run(first_url, "token", session, payload)

    assert_receive {:request_headers, first_headers}

    {:ok, second_url, _server} = start_server(body, 201, parent: parent)

    assert {:ok, %{"id" => "run_1"}} =
             OrchestratorClient.submit_run(second_url, "token", session, payload)

    assert_receive {:request_headers, second_headers}

    refute first_headers["idempotency-key"] == second_headers["idempotency-key"]
  end

  test "mutating command helpers send idempotency keys without secrets" do
    session_context = %{
      "workspace_id" => "workspace_1",
      "actor_id" => "act_1",
      "session_id" => "sess_1",
      "session_token" => "raw_session_token_1"
    }

    assert_mutating_header(fn base_url, token ->
      OrchestratorClient.activate_manifest(base_url, token, "mv_1", session_context)
    end)

    assert_mutating_header(fn base_url, token ->
      OrchestratorClient.cancel_run(base_url, token, "run_1", session_context)
    end)

    assert_mutating_header(fn base_url, token ->
      OrchestratorClient.submit_backfill(base_url, token, session_context, %{
        target: %{type: "pipeline", id: "pipeline:Elixir.MyApp.Pipeline"},
        range: %{from: "2026-01-01", to: "2026-01-02", kind: "day"}
      })
    end)

    assert_mutating_header(fn base_url, token ->
      OrchestratorClient.rerun_backfill_window(
        base_url,
        token,
        session_context,
        "backfill_1",
        "day:2026-01-01:Etc/UTC"
      )
    end)
  end

  test "missing coverage helpers preserve the reviewed plan and command identity" do
    parent = self()
    plan = %{"plan_id" => "coverage_plan_1", "plan_hash" => String.duplicate("a", 64)}

    {:ok, base_url, _server} =
      start_server_sequence(
        [
          {JSON.encode!(%{data: %{plan: plan}}), 200, 0},
          {JSON.encode!(%{data: %{run_id: "run_coverage_1"}}), 202, 0}
        ],
        parent: parent
      )

    context = session_context()
    target_id = "asset:Elixir.MyApp.Orders:orders"

    assert {:ok, ^plan} =
             OrchestratorClient.plan_missing_coverage_backfill(
               base_url,
               "token",
               context,
               target_id,
               limit: 250
             )

    encoded_target = URI.encode(target_id)
    plan_path = "/api/orchestrator/v1/coverage/assets/#{encoded_target}/backfill/plan"

    assert_receive {:request_path, ^plan_path}

    assert_receive {:request_headers, _plan_headers}
    assert_receive {:request_body, plan_body}
    assert JSON.decode!(plan_body) == %{"limit" => 250}

    assert {:ok, "run_coverage_1"} =
             OrchestratorClient.submit_missing_coverage_backfill(
               base_url,
               "token",
               context,
               target_id,
               plan
             )

    submit_path = "/api/orchestrator/v1/coverage/assets/#{encoded_target}/backfill"
    assert_receive {:request_path, ^submit_path}

    assert_receive {:request_headers, submit_headers}
    assert_idempotency_header(submit_headers)
    assert_receive {:request_body, submit_body}
    assert JSON.decode!(submit_body) == %{"plan" => plan}
  end

  test "rebuild helpers preserve explicit plan approval and mutation identity" do
    parent = self()
    plan_hash = String.duplicate("a", 64)
    plan = %{"plan_id" => "rebuild-plan-1", "plan_hash" => plan_hash}
    rebuild = %{"operation_id" => "rebuild-plan-1", "plan_hash" => plan_hash, "state" => "queued"}

    {:ok, base_url, _server} =
      start_server_sequence(
        [
          {JSON.encode!(%{data: %{plan: plan}}), 201, 0},
          {JSON.encode!(%{data: %{rebuild: rebuild}}), 202, 0},
          {JSON.encode!(%{data: %{rebuild: rebuild}}), 200, 0},
          {JSON.encode!(%{data: %{rebuild: %{rebuild | "state" => "cancelling"}}}), 202, 0},
          {JSON.encode!(%{data: %{rebuild: rebuild}}), 202, 0},
          {JSON.encode!(%{data: %{rebuild: %{rebuild | "state" => "reconciling"}}}), 202, 0}
        ],
        parent: parent
      )

    context = session_context()

    assert {:ok, ^plan} =
             OrchestratorClient.plan_rebuild(
               base_url,
               "token",
               context,
               "asset:orders",
               "schema changed"
             )

    assert_receive {:request_path, "/api/orchestrator/v1/rebuilds/plan"}
    assert_receive {:request_headers, plan_headers}
    assert_idempotency_header(plan_headers)
    assert_receive {:request_body, plan_body}

    assert JSON.decode!(plan_body) == %{
             "target_id" => "asset:orders",
             "reason" => "schema changed"
           }

    assert {:ok, ^rebuild} =
             OrchestratorClient.start_rebuild(
               base_url,
               "token",
               context,
               "rebuild-plan-1",
               plan_hash
             )

    assert_receive {:request_path, "/api/orchestrator/v1/rebuilds"}
    assert_receive {:request_headers, start_headers}
    assert_idempotency_header(start_headers)
    assert_receive {:request_body, start_body}

    assert JSON.decode!(start_body) == %{
             "approved" => true,
             "plan_hash" => plan_hash,
             "plan_id" => "rebuild-plan-1"
           }

    assert {:ok, ^rebuild} =
             OrchestratorClient.get_rebuild(base_url, "token", context, "rebuild-plan-1")

    assert_receive {:request_path, "/api/orchestrator/v1/rebuilds/rebuild-plan-1"}
    assert_receive {:request_headers, _get_headers}
    assert_receive {:request_body, ""}

    assert {:ok, %{"state" => "cancelling"}} =
             OrchestratorClient.cancel_rebuild(
               base_url,
               "token",
               context,
               "rebuild-plan-1",
               "operator request"
             )

    assert_receive {:request_path, "/api/orchestrator/v1/rebuilds/rebuild-plan-1/cancel"}
    assert_receive {:request_headers, cancel_headers}
    assert_idempotency_header(cancel_headers)
    assert_receive {:request_body, cancel_body}
    assert JSON.decode!(cancel_body) == %{"reason" => "operator request"}

    assert {:ok, ^rebuild} =
             OrchestratorClient.retry_rebuild(
               base_url,
               "token",
               context,
               "rebuild-plan-1",
               plan_hash
             )

    assert_receive {:request_path, "/api/orchestrator/v1/rebuilds/rebuild-plan-1/retry"}
    assert_receive {:request_headers, retry_headers}
    assert_idempotency_header(retry_headers)
    assert_receive {:request_body, retry_body}
    assert JSON.decode!(retry_body) == %{"plan_hash" => plan_hash}

    assert {:ok, %{"state" => "reconciling"}} =
             OrchestratorClient.reconcile_rebuild(
               base_url,
               "token",
               context,
               "rebuild-plan-1"
             )

    assert_receive {:request_path, "/api/orchestrator/v1/rebuilds/rebuild-plan-1/reconcile"}
    assert_receive {:request_headers, reconcile_headers}
    assert_idempotency_header(reconcile_headers)
    assert_receive {:request_body, "{}"}
  end

  test "repeatable rebuild attempts use fresh command identities" do
    parent = self()
    plan_hash = String.duplicate("a", 64)
    plan = %{data: %{plan: %{plan_id: "plan-1", plan_hash: plan_hash}}}
    rebuild = %{data: %{rebuild: %{operation_id: "plan-1", plan_hash: plan_hash}}}

    {:ok, base_url, _server} =
      start_server_sequence(
        [
          {JSON.encode!(plan), 201, 0},
          {JSON.encode!(plan), 201, 0},
          {JSON.encode!(rebuild), 202, 0},
          {JSON.encode!(rebuild), 202, 0}
        ],
        parent: parent
      )

    context = session_context()

    assert {:ok, _plan} =
             OrchestratorClient.plan_rebuild(
               base_url,
               "token",
               context,
               "asset:orders",
               "schema changed"
             )

    assert_receive {:request_path, "/api/orchestrator/v1/rebuilds/plan"}
    assert_receive {:request_headers, first_plan_headers}
    assert_receive {:request_body, _body}

    assert {:ok, _plan} =
             OrchestratorClient.plan_rebuild(
               base_url,
               "token",
               context,
               "asset:orders",
               "schema changed"
             )

    assert_receive {:request_path, "/api/orchestrator/v1/rebuilds/plan"}
    assert_receive {:request_headers, second_plan_headers}
    assert_receive {:request_body, _body}

    assert idempotency_header(first_plan_headers) != idempotency_header(second_plan_headers)

    assert {:ok, _rebuild} =
             OrchestratorClient.retry_rebuild(base_url, "token", context, "plan-1", plan_hash)

    assert_receive {:request_path, "/api/orchestrator/v1/rebuilds/plan-1/retry"}
    assert_receive {:request_headers, first_retry_headers}
    assert_receive {:request_body, _body}

    assert {:ok, _rebuild} =
             OrchestratorClient.retry_rebuild(base_url, "token", context, "plan-1", plan_hash)

    assert_receive {:request_path, "/api/orchestrator/v1/rebuilds/plan-1/retry"}
    assert_receive {:request_headers, second_retry_headers}
    assert_receive {:request_body, _body}

    assert idempotency_header(first_retry_headers) != idempotency_header(second_retry_headers)
  end

  test "separate activation invocations use fresh command identities across renewed sessions" do
    parent = self()

    session = %{
      "workspace_id" => "workspace_1",
      "actor_id" => "act_1",
      "session_token" => "raw_session_token_1"
    }

    {:ok, first_url, _server} =
      start_server(~s({"data":{"activated":true}}), 200, parent: parent)

    assert {:ok, _response} =
             OrchestratorClient.activate_manifest(
               first_url,
               "token",
               "mv_1",
               Map.put(session, "session_id", "sess_1")
             )

    assert_receive {:request_headers, first_headers}

    {:ok, second_url, _server} =
      start_server(~s({"data":{"activated":true}}), 200, parent: parent)

    assert {:ok, _response} =
             OrchestratorClient.activate_manifest(
               second_url,
               "token",
               "mv_1",
               session
               |> Map.put("session_id", "sess_2")
               |> Map.put("session_token", "raw_session_token_2")
             )

    assert_receive {:request_headers, second_headers}

    refute first_headers["idempotency-key"] == second_headers["idempotency-key"]
  end

  test "a persisted maintenance token keeps activation retries idempotent" do
    parent = self()
    maintenance_token = String.duplicate("m", 43)

    {:ok, first_url, _server} =
      start_server(~s({"data":{"activated":true}}), 200, parent: parent)

    assert {:ok, _response} =
             OrchestratorClient.activate_manifest_service(
               first_url,
               "token",
               "mv_rollback",
               "workspace_1",
               maintenance_token: maintenance_token
             )

    assert_receive {:request_headers, first_headers}

    {:ok, second_url, _server} =
      start_server(~s({"data":{"activated":true}}), 200, parent: parent)

    assert {:ok, _response} =
             OrchestratorClient.activate_manifest_service(
               second_url,
               "token",
               "mv_rollback",
               "workspace_1",
               maintenance_token: maintenance_token
             )

    assert_receive {:request_headers, second_headers}
    assert first_headers["idempotency-key"] == second_headers["idempotency-key"]
  end

  test "cancel_run/4 parses cancel response and encodes run id" do
    parent = self()

    {:ok, base_url, _server} =
      start_server(~s({"data":{"cancelled":true,"run_id":"run 1"}}), 200, parent: parent)

    assert {:ok, %{"cancelled" => true, "run_id" => "run 1"}} =
             OrchestratorClient.cancel_run(
               base_url,
               "token",
               "run 1",
               %{"workspace_id" => "local-dev"}
             )

    assert_receive {:request_path, "/api/orchestrator/v1/runs/run%201/cancel"}
    assert_receive {:request_headers, headers}
    assert headers["x-favn-workspace-id"] == "local-dev"
    assert_idempotency_header(headers)
  end

  test "backfill list helpers parse item responses and encode filters" do
    parent = self()

    {:ok, base_url, _server} =
      start_server(
        ~s({"data":{"items":[{"baseline_id":"base_1"}],"pagination":{"limit":100,"offset":0,"has_more":false,"next_offset":null}}}),
        200,
        parent: parent
      )

    assert {:ok, %{"items" => [%{"baseline_id" => "base_1"}]}} =
             OrchestratorClient.list_coverage_baselines(
               base_url,
               "token",
               %{
                 "actor_id" => "act_1",
                 "session_id" => "sess_1",
                 "session_token" => "raw_session_token_1"
               },
               pipeline_module: "MyApp.Pipeline",
               status: "ok"
             )

    assert_receive {:request_path,
                    "/api/orchestrator/v1/backfills/coverage-baselines?pipeline_module=MyApp.Pipeline&status=ok"}
  end

  test "asset window states helper parses item responses" do
    parent = self()

    {:ok, base_url, _server} =
      start_server(
        ~s({"data":{"items":[{"window_key":"day:2026-01-01:Etc/UTC"}],"pagination":{"limit":100,"offset":0,"has_more":false,"next_offset":null}}}),
        200,
        parent: parent
      )

    assert {:ok, %{"items" => [%{"window_key" => "day:2026-01-01:Etc/UTC"}]}} =
             OrchestratorClient.list_asset_window_states(
               base_url,
               "token",
               %{
                 "actor_id" => "act_1",
                 "session_id" => "sess_1",
                 "session_token" => "raw_session_token_1"
               },
               asset_ref_module: "MyApp.Asset",
               asset_ref_name: "asset"
             )

    assert_receive {:request_path,
                    "/api/orchestrator/v1/assets/window-states?asset_ref_module=MyApp.Asset&asset_ref_name=asset"}
  end

  test "list_runs/4 parses runs and encodes filters" do
    parent = self()

    {:ok, base_url, _server} =
      start_server(~s({"data":{"items":[{"id":"run_1","status":"error"}]}}), 200, parent: parent)

    assert {:ok, [%{"id" => "run_1", "status" => "error"}]} =
             OrchestratorClient.list_runs(
               base_url,
               "token",
               %{"workspace_id" => "local-dev"},
               status: "error",
               limit: 5
             )

    assert_receive {:request_path, "/api/orchestrator/v1/runs?status=error&limit=5"}
  end

  test "list_run_events/5 parses run event responses" do
    parent = self()

    {:ok, base_url, _server} =
      start_server(~s({"data":{"items":[{"sequence":1,"event_type":"run_started"}]}}), 200,
        parent: parent
      )

    assert {:ok, [%{"sequence" => 1, "event_type" => "run_started"}]} =
             OrchestratorClient.list_run_events(
               base_url,
               "token",
               %{"workspace_id" => "local-dev"},
               "run_1",
               limit: 20
             )

    assert_receive {:request_path, "/api/orchestrator/v1/runs/run_1/events?limit=20"}
  end

  defp session_context do
    %{
      "workspace_id" => "workspace-1",
      "actor_id" => "act_1",
      "session_id" => "sess_1",
      "session_token" => "raw_session_token_1"
    }
  end

  defp start_server(body, status, opts \\ []) when is_binary(body) and is_integer(status) do
    response_delay_ms = Keyword.get(opts, :response_delay_ms, 0)
    start_server_sequence([{body, status, response_delay_ms}], opts)
  end

  defp missing_response(missing, opts \\ []) do
    JSON.encode!(%{
      data: %{
        missing: missing,
        publication_limits: %{
          max_packages: Keyword.get(opts, :max_packages, 100),
          compressed_limit_bytes: Keyword.get(opts, :compressed_limit_bytes, 8 * 1024 * 1024),
          decompressed_limit_bytes: Keyword.get(opts, :decompressed_limit_bytes, 32 * 1024 * 1024)
        }
      }
    })
  end

  defp packaged_publication(count) do
    pairs =
      Enum.map(1..count, fn index ->
        ref = {Module.concat([MyApp.OrchestratorClient, "Asset#{index}"]), :asset}
        sql = "SELECT #{index} AS id"

        template =
          Template.compile!(sql,
            file: "test/orchestrator_client_package.sql",
            line: index,
            module: __MODULE__,
            scope: :query,
            enforce_query_root: true
          )

        {:ok, package} =
          ExecutionPackage.new(ref, %SQLExecution{sql: sql, template: template})

        {ref, package}
      end)

    assets =
      Enum.map(pairs, fn {{module, name} = ref, package} ->
        %Asset{
          ref: ref,
          module: module,
          name: name,
          type: :sql,
          execution_package_hash: package.content_hash
        }
      end)

    refs = Enum.map(pairs, &elem(&1, 0))
    packages = Enum.map(pairs, &elem(&1, 1))

    {:ok, version} =
      Version.new(
        FavnTestSupport.with_manifest_contract(%Manifest{
          assets: assets,
          graph: %Graph{nodes: refs, topo_order: refs}
        }),
        manifest_version_id: "mv_orchestrator_client_packages"
      )

    {:ok, publication} = Publication.from_parts(version, packages)
    publication
  end

  defp start_server_sequence(responses, opts \\ []) when is_list(responses) do
    parent = Keyword.get(opts, :parent)
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_addr, port}} = :inet.sockname(listen_socket)

    server =
      spawn_link(fn ->
        Enum.each(responses, fn {body, status, response_delay_ms} ->
          {:ok, socket} = :gen_tcp.accept(listen_socket)
          request = receive_request(socket, "")
          Process.sleep(response_delay_ms)
          :ok = :gen_tcp.send(socket, response(status, body))
          :ok = :gen_tcp.close(socket)

          if parent do
            send(parent, {:request_path, request_path(request)})
            send(parent, {:request_headers, request_headers(request)})
            send(parent, {:request_body, request_body(request)})
          end
        end)

        :ok = :gen_tcp.close(listen_socket)
      end)

    {:ok, "http://127.0.0.1:#{port}", server}
  end

  defp receive_request(socket, acc) do
    {:ok, chunk} = :gen_tcp.recv(socket, 0, 2_000)
    acc = acc <> chunk

    if request_complete?(acc) do
      acc
    else
      receive_request(socket, acc)
    end
  end

  defp request_complete?(request) do
    case String.split(request, "\r\n\r\n", parts: 2) do
      [headers, body] -> byte_size(body) >= content_length(headers)
      _other -> false
    end
  end

  defp content_length(headers) do
    headers
    |> String.split("\r\n")
    |> Enum.find_value(0, fn line ->
      case String.split(line, ":", parts: 2) do
        [key, value] ->
          if String.downcase(key) == "content-length" do
            value |> String.trim() |> String.to_integer()
          end

        _other ->
          nil
      end
    end)
  end

  defp request_path(request) do
    request
    |> String.split("\r\n", parts: 2)
    |> hd()
    |> String.split(" ")
    |> Enum.at(1)
  end

  defp request_body(request) do
    case String.split(request, "\r\n\r\n", parts: 2) do
      [_headers, body] -> body
      _other -> ""
    end
  end

  defp request_headers(request) do
    request
    |> String.split("\r\n\r\n", parts: 2)
    |> hd()
    |> String.split("\r\n")
    |> Enum.drop(1)
    |> Enum.reduce(%{}, fn line, acc ->
      case String.split(line, ":", parts: 2) do
        [key, value] -> Map.put(acc, String.downcase(key), String.trim(value))
        _other -> acc
      end
    end)
  end

  defp response(status, body) do
    [
      "HTTP/1.1 #{status} #{reason(status)}\r\n",
      "content-type: application/json\r\n",
      "content-length: #{byte_size(body)}\r\n",
      "connection: close\r\n",
      "\r\n",
      body
    ]
  end

  defp reason(status) when status in 200..299, do: "OK"
  defp reason(_status), do: "Error"

  defp assert_mutating_header(fun) when is_function(fun, 2) do
    parent = self()

    {:ok, base_url, _server} =
      start_server(~s({"data":{"run":{"id":"run_1"}}}), 200, parent: parent)

    _ = fun.(base_url, "service-token-secret")

    assert_receive {:request_headers, headers}
    assert_idempotency_header(headers)
  end

  defp assert_idempotency_header(headers) do
    assert "favn-local-" <> _ = key = headers["idempotency-key"]
    refute key =~ "service-token-secret"
    refute key =~ "sess_1"
    refute key =~ "raw_session_token_1"
  end

  defp idempotency_header(headers) do
    assert_idempotency_header(headers)
    Map.fetch!(headers, "idempotency-key")
  end

  defp unused_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_addr, port}} = :inet.sockname(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
