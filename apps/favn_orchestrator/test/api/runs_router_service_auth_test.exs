defmodule FavnOrchestrator.API.RunsRouterServiceAuthTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias FavnOrchestrator.API.Router
  alias FavnOrchestrator.API.RunsRouter
  alias FavnOrchestrator.Auth.Session
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Persistence.{Runtime, Stores}
  alias FavnOrchestrator.Persistence.Results.Actor
  alias FavnOrchestrator.Persistence.Results.Session, as: SessionResult

  @token "runs-router-service-token-with-32-bytes"

  defmodule EmptyRunsStore do
    alias FavnOrchestrator.Persistence.Error
    alias FavnOrchestrator.Persistence.Queries.GetActor
    alias FavnOrchestrator.Persistence.Queries.GetSession
    alias FavnOrchestrator.Persistence.Results.CursorPage
    alias FavnOrchestrator.Persistence.Selectors.ActorById
    alias FavnOrchestrator.Persistence.Selectors.SessionByTokenHash

    def page_run_summaries(query) do
      case Process.get(:runs_router_list_error) do
        nil ->
          {:ok, %CursorPage{items: [], limit: query.limit, has_more?: false, next_cursor: nil}}

        error ->
          {:error, error}
      end
    end

    def page_events(query) do
      case Process.get(:runs_router_events_error) do
        nil ->
          {:ok, %CursorPage{items: [], limit: query.limit, has_more?: false, next_cursor: nil}}

        error ->
          {:error, error}
      end
    end

    def request_cancellation(_command) do
      {:error,
       Error.new(:conflict, "run is already terminal", details: %{reason: :run_already_terminal})}
    end

    def get_run(_query) do
      case Process.get(:runs_router_terminal_run) do
        nil -> {:error, Error.new(:not_found, "run not found")}
        run -> {:ok, run}
      end
    end

    def get_by_run_id(_query) do
      case Process.get(:runs_router_submission) do
        nil -> {:error, Error.new(:not_found, "run submission not found")}
        submission -> {:ok, submission}
      end
    end

    def page(_query),
      do: {:ok, %{items: List.wrap(Process.get(:runs_router_submission)), next: nil}}

    def stats(_query), do: {:ok, Process.get(:runs_router_submission_stats)}

    def get_runtime_state(_query),
      do: {:error, Process.get(:runs_router_runtime_state_error, :active_manifest_not_set)}

    def record_audit(command) do
      send(Process.get(:runs_router_test_pid), {:record_audit, command})
      :ok
    end

    def get_session(%GetSession{
          workspace_context: context,
          selector: %SessionByTokenHash{token_hash: token_hash}
        }) do
      fetch(:runs_router_sessions, {context.workspace_id, token_hash})
    end

    def get_actor(%GetActor{
          workspace_context: context,
          selector: %ActorById{actor_id: actor_id}
        }) do
      fetch(:runs_router_actors, {context.workspace_id, actor_id})
    end

    defp fetch(key, id) do
      case Map.fetch(Process.get(key, %{}), id) do
        {:ok, value} -> {:ok, value}
        :error -> {:error, Error.new(:not_found, "identity not found")}
      end
    end
  end

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "runs_router_test",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true,
        platform_roles: [:platform_operator]
      ]
    ])

    Process.put(:runs_router_actors, %{})
    Process.put(:runs_router_sessions, %{})
    Process.put(:runs_router_terminal_run, %{status: :ok})
    Process.put(:runs_router_submission, nil)
    Process.put(:runs_router_submission_stats, submission_stats_fixture())
    Process.put(:runs_router_test_pid, self())
    Process.delete(:runs_router_list_error)
    Process.delete(:runs_router_events_error)
    Process.delete(:runs_router_runtime_state_error)

    stores = %Stores{
      registry: EmptyRunsStore,
      runs: EmptyRunsStore,
      run_submissions: EmptyRunsStore,
      runner_tasks: FavnOrchestrator.TestRunnerTaskStore,
      run_ownership: EmptyRunsStore,
      scheduler: EmptyRunsStore,
      admission: EmptyRunsStore,
      resource_circuits: EmptyRunsStore,
      target_generations: EmptyRunsStore,
      target_recovery: EmptyRunsStore,
      rebuilds: EmptyRunsStore,
      target_operation_locks: EmptyRunsStore,
      materialization: EmptyRunsStore,
      backfills: EmptyRunsStore,
      operator_reads: EmptyRunsStore,
      logs: EmptyRunsStore,
      identity: EmptyRunsStore,
      maintenance: EmptyRunsStore
    }

    assert {:ok, runtime} =
             Runtime.start_link(%Runtime{backend: __MODULE__, options: [], stores: stores})

    on_exit(fn ->
      restore_env(:api_service_tokens, previous_tokens)
      Process.delete(:runs_router_actors)
      Process.delete(:runs_router_sessions)
      Process.delete(:runs_router_terminal_run)
      Process.delete(:runs_router_submission)
      Process.delete(:runs_router_submission_stats)
      Process.delete(:runs_router_test_pid)
      Process.delete(:runs_router_list_error)
      Process.delete(:runs_router_events_error)
      Process.delete(:runs_router_runtime_state_error)
      if Process.alive?(runtime), do: GenServer.stop(runtime)
    end)

    :ok
  end

  test "platform operator service token can read runs without actor headers" do
    response = list_request()

    assert response.status == 200
    assert %{"data" => %{"items" => []}} = Jason.decode!(response.resp_body)
  end

  test "run storage failures are reported as unavailable rather than bad requests" do
    Process.put(
      :runs_router_list_error,
      FavnOrchestrator.Persistence.Error.new(:internal, "internal persistence failure")
    )

    response = list_request()

    assert response.status == 500
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "runs_unavailable"
  end

  test "run event storage failures are reported as unavailable rather than bad requests" do
    Process.put(
      :runs_router_events_error,
      FavnOrchestrator.Persistence.Error.new(:internal, "internal persistence failure")
    )

    response = events_request("run-1")

    assert response.status == 500

    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) ==
             "run_events_unavailable"
  end

  test "viewer endpoints expose queued submissions and run detail during admission delay" do
    Process.put(:runs_router_terminal_run, nil)
    Process.put(:runs_router_submission, submission_fixture())

    detail = authenticated_get("/queued-run") |> decoded_data()
    assert detail["run"]["status"] == "accepted"
    assert detail["submission"]["status"] == "queued"

    submission = authenticated_get("/submissions/queued-run") |> decoded_data()
    assert submission["submission"]["run_id"] == "queued-run"

    page = authenticated_get("/submissions?status=queued&limit=10") |> decoded_data()
    assert [%{"run_id" => "queued-run", "status" => "queued"}] = page["items"]

    stats = authenticated_get("/submissions/stats") |> decoded_data()
    assert stats["stats"]["queued_depth"] == 1
    assert stats["stats"]["oldest_queued_age_ms"] == 5_000
  end

  test "submission listing rejects malformed filters" do
    response = authenticated_get("/submissions?status=not-a-state")

    assert response.status == 422
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "validation_failed"
  end

  test "platform operator service token reaches run submission without actor headers" do
    response = submit_request()

    assert response.status == 404
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "not_found"
  end

  test "operator actor session reaches run submission with a concrete workspace context" do
    actor = put_actor("workspace-a", "operator", [:customer_operator])
    response = submit_request(actor)

    assert response.status == 404
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "not_found"
  end

  test "shared API parser rejects malformed rerun JSON before route dispatch" do
    assert_raise Plug.Parsers.ParseError, fn ->
      :post
      |> conn("/api/orchestrator/v1/runs/run-a/rerun", "{")
      |> put_req_header("content-type", "application/json")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")
      |> Router.call(Router.init([]))
    end
  end

  test "unmapped run submission failures return a redacted stable response" do
    Process.put(:runs_router_runtime_state_error, :future_submission_failure)

    response = submit_request()

    assert response.status == 400

    assert %{
             "error" => %{
               "code" => "bad_request",
               "message" => "Request failed",
               "details" => %{}
             }
           } = Jason.decode!(response.resp_body)
  end

  test "platform admin service token does not imply run submission access" do
    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "runs_router_admin",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true,
        platform_roles: [:platform_admin]
      ]
    ])

    response = submit_request()

    assert response.status == 403
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "forbidden"
  end

  test "underprivileged actor session does not fall back to operator service authority" do
    actor = put_actor("workspace-a", "viewer", [:customer_reader])
    response = submit_request(actor)

    assert response.status == 403
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "forbidden"
  end

  test "invalid actor session does not fall back to service authority" do
    response =
      submit_request(%{
        id: "missing-actor",
        token: "invalid-forwarded-session-token"
      })

    assert response.status == 401
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "unauthenticated"
  end

  test "partial actor headers do not fall back to service authority" do
    actor_only =
      submit_request_with_headers([{"x-favn-actor-id", "actor-without-session"}])

    session_only =
      submit_request_with_headers([{"x-favn-session-token", "session-without-actor"}])

    assert actor_only.status == 401
    assert session_only.status == 401
    assert get_in(Jason.decode!(actor_only.resp_body), ["error", "code"]) == "unauthenticated"
    assert get_in(Jason.decode!(session_only.resp_body), ["error", "code"]) == "unauthenticated"
  end

  test "mismatched actor session does not fall back to service authority" do
    actor = put_actor("workspace-a", "expected", [:customer_operator])

    response =
      submit_request(%{actor | id: "different-actor"})

    assert response.status == 401
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "unauthenticated"
  end

  test "expired actor session does not fall back to service authority" do
    actor = put_actor("workspace-a", "expired", [:customer_operator], expires_in: -1)
    response = submit_request(actor)

    assert response.status == 401
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "unauthenticated"
  end

  test "in-flight reads treat explicit invalid actor headers as authoritative" do
    response =
      :get
      |> conn("/in-flight")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")
      |> put_req_header("x-favn-actor-id", "missing-actor")
      |> put_req_header("x-favn-session-token", "invalid-forwarded-session-token")
      |> RunsRouter.call(RunsRouter.init([]))

    assert response.status == 401
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "unauthenticated"
  end

  test "cancelling a successful run preserves and reports its terminal result" do
    response = cancel_request("successful-run")

    assert response.status == 200

    assert %{
             "data" => %{
               "cancelled" => false,
               "outcome" => "already_terminal",
               "run_id" => "successful-run",
               "status" => "ok"
             }
           } = Jason.decode!(response.resp_body)

    assert_received {:record_audit, audit}
    assert audit.detail.outcome == "already_terminal"
    assert audit.detail.idempotency.outcome == "already_terminal"
  end

  test "repeating cancellation for a cancelled run reports the persisted outcome" do
    Process.put(:runs_router_terminal_run, %{status: :cancelled})
    response = cancel_request("cancelled-run")

    assert response.status == 200

    assert %{
             "data" => %{
               "cancelled" => true,
               "outcome" => "already_terminal",
               "run_id" => "cancelled-run",
               "status" => "cancelled"
             }
           } = Jason.decode!(response.resp_body)
  end

  defp list_request do
    :get
    |> conn("/")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> put_req_header("x-favn-workspace-id", "workspace-a")
    |> RunsRouter.call(RunsRouter.init([]))
  end

  defp authenticated_get(path) do
    :get
    |> conn(path)
    |> fetch_query_params()
    |> put_req_header("authorization", "Bearer #{@token}")
    |> put_req_header("x-favn-workspace-id", "workspace-a")
    |> RunsRouter.call(RunsRouter.init([]))
  end

  defp decoded_data(response) do
    assert response.status == 200
    get_in(Jason.decode!(response.resp_body), ["data"])
  end

  defp submit_request(actor \\ nil) do
    submit_request_with_headers(actor_headers(actor))
  end

  defp events_request(run_id) do
    :get
    |> conn("/#{run_id}/events")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> put_req_header("x-favn-workspace-id", "workspace-a")
    |> RunsRouter.call(RunsRouter.init([]))
  end

  defp submit_request_with_headers(headers) do
    :post
    |> conn("/", "")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> put_req_header("x-favn-workspace-id", "workspace-a")
    |> put_req_header("idempotency-key", "runs-router-service-submit")
    |> put_headers(headers)
    |> Map.put(:body_params, %{})
    |> RunsRouter.call(RunsRouter.init([]))
  end

  defp cancel_request(run_id) do
    :post
    |> conn("/#{run_id}/cancel", "")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> put_req_header("x-favn-workspace-id", "workspace-a")
    |> put_req_header("idempotency-key", "runs-router-cancel-#{run_id}")
    |> Map.put(:body_params, %{})
    |> RunsRouter.call(RunsRouter.init([]))
  end

  defp put_actor(workspace_id, name, roles, opts \\ []) do
    actor_id = "actor-#{name}"
    token = Session.raw_token()
    now = DateTime.utc_now()

    actor = %Actor{
      actor_id: actor_id,
      username: name,
      display_name: name,
      status: :active,
      workspace_id: workspace_id,
      membership_status: :active,
      roles: roles,
      access_version: 1,
      version: 1
    }

    session = %SessionResult{
      session_id: "session-#{name}",
      actor_id: actor_id,
      workspace_id: workspace_id,
      provider: "password_local",
      issued_at: now,
      status: if(Keyword.get(opts, :expires_in, 3_600) > 0, do: :active, else: :expired),
      expires_at: DateTime.add(now, Keyword.get(opts, :expires_in, 3_600), :second)
    }

    Process.put(
      :runs_router_actors,
      Map.put(Process.get(:runs_router_actors), {workspace_id, actor_id}, actor)
    )

    Process.put(
      :runs_router_sessions,
      Map.put(
        Process.get(:runs_router_sessions),
        {workspace_id, Session.token_hash(token)},
        session
      )
    )

    %{id: actor_id, workspace_id: workspace_id, token: token}
  end

  defp actor_headers(nil), do: []

  defp actor_headers(actor),
    do: [{"x-favn-actor-id", actor.id}, {"x-favn-session-token", actor.token}]

  defp put_headers(conn, headers),
    do: Enum.reduce(headers, conn, fn {key, value}, conn -> put_req_header(conn, key, value) end)

  defp submission_fixture do
    now = ~U[2026-07-26 12:00:00Z]

    %{
      submission_id: "submission-queued",
      run_id: "queued-run",
      source: :api,
      deployment_id: "deployment",
      manifest_version_id: "manifest",
      target_kind: "asset",
      target_id: "asset",
      status: :queued,
      attempt: 0,
      failure_kind: nil,
      error: nil,
      cancellation_requested_at: nil,
      retry_root_id: "submission-queued",
      retry_of_submission_id: nil,
      superseded_by_submission_id: nil,
      enqueued_at: now,
      available_at: now,
      preparing_at: nil,
      admitting_at: nil,
      terminal_at: nil,
      updated_at: now
    }
  end

  defp submission_stats_fixture do
    %{
      total: 1,
      counts: %{queued: 1},
      failure_counts: %{},
      queued_depth: 1,
      active_depth: 0,
      retrying_depth: 0,
      cancellation_requested_depth: 0,
      oldest_queued_at: ~U[2026-07-26 12:00:00Z],
      oldest_queued_age_ms: 5_000,
      observed_at: ~U[2026-07-26 12:00:05Z]
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
