defmodule FavnOrchestrator.API.RunsRouterServiceAuthTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

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
      {:ok, %CursorPage{items: [], limit: query.limit, has_more?: false, next_cursor: nil}}
    end

    def get_runtime_state(_query), do: {:error, :active_manifest_not_set}

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

    stores = %Stores{
      registry: EmptyRunsStore,
      runs: EmptyRunsStore,
      run_ownership: EmptyRunsStore,
      scheduler: EmptyRunsStore,
      admission: EmptyRunsStore,
      resource_circuits: EmptyRunsStore,
      target_generations: EmptyRunsStore,
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
      if Process.alive?(runtime), do: GenServer.stop(runtime)
    end)

    :ok
  end

  test "platform operator service token can read runs without actor headers" do
    response = list_request()

    assert response.status == 200
    assert %{"data" => %{"items" => []}} = Jason.decode!(response.resp_body)
  end

  test "platform operator service token reaches run submission without actor headers" do
    response = submit_request()

    assert response.status == 404
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "not_found"
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

  defp list_request do
    :get
    |> conn("/")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> put_req_header("x-favn-workspace-id", "workspace-a")
    |> RunsRouter.call(RunsRouter.init([]))
  end

  defp submit_request(actor \\ nil) do
    :post
    |> conn("/", "")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> put_req_header("x-favn-workspace-id", "workspace-a")
    |> put_req_header("idempotency-key", "runs-router-service-submit")
    |> maybe_put_actor(actor)
    |> Map.put(:body_params, %{})
    |> RunsRouter.call(RunsRouter.init([]))
  end

  defp put_actor(workspace_id, name, roles) do
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
      provider: "password_local",
      issued_at: now,
      status: :active,
      expires_at: DateTime.add(now, 3_600, :second)
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

  defp maybe_put_actor(conn, nil), do: conn

  defp maybe_put_actor(conn, actor) do
    conn
    |> put_req_header("x-favn-actor-id", actor.id)
    |> put_req_header("x-favn-session-token", actor.token)
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
