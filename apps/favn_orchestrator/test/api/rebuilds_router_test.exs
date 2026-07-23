defmodule FavnOrchestrator.API.RebuildsRouterTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias FavnOrchestrator.API.RebuildsRouter
  alias FavnOrchestrator.API.IdempotentCommand
  alias FavnOrchestrator.Auth.Session
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Idempotency
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.Actor
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Persistence.Results.RebuildTimestamps
  alias FavnOrchestrator.Persistence.Results.Session, as: SessionResult
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores

  @token "rebuild-router-test-token-with-32-bytes"

  defmodule Store do
    alias FavnOrchestrator.Idempotency
    alias FavnOrchestrator.Persistence.Error
    alias FavnOrchestrator.Persistence.Queries.GetActor
    alias FavnOrchestrator.Persistence.Queries.GetRebuild
    alias FavnOrchestrator.Persistence.Queries.GetSession
    alias FavnOrchestrator.Persistence.Queries.PageRebuildOperations
    alias FavnOrchestrator.Persistence.Results.CursorPage
    alias FavnOrchestrator.Persistence.Results.RebuildOperation
    alias FavnOrchestrator.Persistence.Results.RebuildTimestamps
    alias FavnOrchestrator.Persistence.Selectors.ActorById
    alias FavnOrchestrator.Persistence.Selectors.SessionByTokenHash

    def get_session(%GetSession{
          workspace_context: context,
          selector: %SessionByTokenHash{token_hash: token_hash}
        }) do
      fetch(:router_sessions, {context.workspace_id, token_hash}, "session")
    end

    def get_actor(%GetActor{
          workspace_context: context,
          selector: %ActorById{actor_id: actor_id}
        }) do
      fetch(:router_actors, {context.workspace_id, actor_id}, "actor")
    end

    def get(%GetRebuild{workspace_context: context, operation_id: "rebuild_api_" <> _ = id}) do
      now = DateTime.utc_now()

      {:ok,
       %RebuildOperation{
         workspace_id: context.workspace_id,
         operation_id: id,
         root_target_id: "asset:orders",
         manifest_version_id: "manifest-api-test",
         candidate_generation_id: Ecto.UUID.generate(),
         plan_hash: digest(id),
         plan_version: 1,
         plan_payload: %{expires_at: DateTime.add(now, 3_600, :second)},
         actor_id: context.principal_id,
         reason: "schema changed",
         idempotency_key: Idempotency.key_hash("shared-key"),
         evaluated_at: now,
         action_count: 1,
         window_count: 1,
         state: :planned,
         phase: :planned,
         cleanup_state: :not_started,
         cancel_requested: false,
         version: 1,
         timestamps: %RebuildTimestamps{inserted_at: now, updated_at: now}
       }}
    end

    def get(%GetRebuild{workspace_context: context, operation_id: operation_id}) do
      fetch(:router_rebuilds, {context.workspace_id, operation_id}, "rebuild")
    end

    def page_operations(%PageRebuildOperations{workspace_context: context, limit: limit}) do
      items =
        :router_rebuilds
        |> values()
        |> Enum.filter(&(&1.workspace_id == context.workspace_id))

      {:ok, %CursorPage{items: items, limit: limit, has_more?: false}}
    end

    def request_cancellation(command) do
      operation_key = {command.workspace_context.workspace_id, command.operation_id}

      with {:ok, operation} <- fetch(:router_rebuilds, operation_key, "rebuild") do
        idempotency = command.idempotency

        replay_key =
          {
            command.workspace_context.workspace_id,
            idempotency.principal_kind,
            idempotency.principal_id,
            idempotency.operation,
            idempotency.key_hash
          }

        case Process.get({:router_cancel, replay_key}) do
          nil ->
            cancelled = %{
              operation
              | state: :cancelling,
                phase: :cancelling,
                cancel_requested: true,
                version: operation.version + 1,
                idempotency_replay?: false,
                timestamps: %{operation.timestamps | updated_at: command.occurred_at}
            }

            put_value(:router_rebuilds, operation_key, cancelled)
            Process.put({:router_cancel, replay_key}, cancelled)
            {:ok, cancelled}

          replayed ->
            {:ok, %{replayed | idempotency_replay?: true}}
        end
      end
    end

    def record_audit(command) do
      put_value(:router_audits, command.command_id, command)
      :ok
    end

    defp fetch(key, identity, label) do
      case Process.get(key, %{}) do
        %{^identity => value} -> {:ok, value}
        %{} -> {:error, Error.new(:not_found, "#{label} not found")}
      end
    end

    defp values(key), do: Process.get(key, %{}) |> Map.values()

    defp put_value(key, identity, value) do
      Process.put(key, Map.put(Process.get(key, %{}), identity, value))
    end

    defp digest(value),
      do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
  end

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "rebuild_router_test",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true,
        platform_roles: []
      ]
    ])

    stores =
      Stores.__struct__()
      |> Map.from_struct()
      |> Map.new(fn {name, _module} -> {name, Store} end)
      |> then(&struct!(Stores, &1))

    assert {:ok, runtime} =
             Runtime.start_link(%Runtime{backend: __MODULE__, options: [], stores: stores})

    Process.put(:router_sessions, %{})
    Process.put(:router_actors, %{})
    Process.put(:router_rebuilds, %{})
    Process.put(:router_audits, %{})

    on_exit(fn ->
      if Process.alive?(runtime), do: GenServer.stop(runtime)
      restore_env(:api_service_tokens, previous_tokens)
    end)

    :ok
  end

  test "enforces the viewer, operator, and administrator role matrix" do
    viewer = put_actor("workspace-a", "viewer", [:customer_reader])
    operator = put_actor("workspace-a", "operator", [:customer_operator])
    admin = put_actor("workspace-a", "admin", [:workspace_admin])
    put_rebuild("workspace-a", "rebuild-existing")

    assert actor_request(viewer, :get, "/").status == 200
    assert actor_request(operator, :get, "/").status == 200
    assert actor_request(admin, :get, "/").status == 200

    assert actor_request(
             viewer,
             :post,
             "/plan",
             %{"target_id" => "asset:orders", "reason" => "schema changed"},
             "shared-key"
           ).status == 403

    assert actor_request(
             operator,
             :post,
             "/plan",
             %{"target_id" => "asset:orders", "reason" => "schema changed"},
             "shared-key"
           ).status == 201

    for actor <- [viewer, operator] do
      assert actor_request(
               actor,
               :post,
               "/rebuild-existing/cancel",
               %{"reason" => "operator request"},
               "cancel-shared"
             ).status == 403
    end

    assert actor_request(
             admin,
             :post,
             "/rebuild-existing/cancel",
             %{"reason" => "operator request"},
             "cancel-admin"
           ).status == 202
  end

  test "scopes plan identity and deterministic audit identity to the actor" do
    first = put_actor("workspace-a", "operator-one", [:customer_operator])
    second = put_actor("workspace-a", "operator-two", [:customer_operator])
    body = %{"target_id" => "asset:orders", "reason" => "schema changed"}

    first_response = actor_request(first, :post, "/plan", body, "shared-key")
    replay_response = actor_request(first, :post, "/plan", body, "shared-key")
    second_response = actor_request(second, :post, "/plan", body, "shared-key")

    assert first_response.status == 201
    assert replay_response.status == 201
    assert second_response.status == 201

    first_plan_id = get_in(json(first_response), ["data", "plan", "plan_id"])
    replay_plan_id = get_in(json(replay_response), ["data", "plan", "plan_id"])
    second_plan_id = get_in(json(second_response), ["data", "plan", "plan_id"])

    assert first_plan_id == replay_plan_id
    refute first_plan_id == second_plan_id

    plan_audits = Enum.filter(audits(), &(&1.detail.action == "rebuild.plan"))
    assert MapSet.new(plan_audits, & &1.detail.actor_id) == MapSet.new([first.id, second.id])
    assert MapSet.size(MapSet.new(plan_audits, & &1.command_id)) == 2
  end

  test "persists accepted, replayed, and rejected mutation audit evidence" do
    admin = put_actor("workspace-a", "admin", [:workspace_admin])
    put_rebuild("workspace-a", "rebuild-existing")

    request = fn operation_id, key ->
      actor_request(
        admin,
        :post,
        "/#{operation_id}/cancel",
        %{"reason" => "operator request"},
        key
      )
    end

    assert request.("rebuild-existing", "cancel-replay").status == 202
    assert request.("rebuild-existing", "cancel-replay").status == 202
    assert request.("rebuild-missing", "cancel-rejected").status == 404

    cancel_audits = Enum.filter(audits(), &(&1.detail.action == "rebuild.cancel"))

    assert Enum.any?(cancel_audits, fn audit ->
             audit.detail.outcome == "accepted" and
               audit.detail.idempotency.replayed == false
           end)

    assert Enum.any?(cancel_audits, fn audit ->
             audit.detail.outcome == "accepted" and audit.detail.idempotency.replayed == true
           end)

    assert Enum.any?(cancel_audits, &(&1.detail.outcome == "rejected"))
  end

  test "never reads a rebuild from another workspace" do
    workspace_a = put_actor("workspace-a", "viewer-a", [:customer_reader])
    workspace_b = put_actor("workspace-b", "viewer-b", [:customer_reader])
    put_rebuild("workspace-a", "rebuild-private")

    assert actor_request(workspace_a, :get, "/rebuild-private").status == 200
    assert actor_request(workspace_b, :get, "/rebuild-private").status == 404

    assert get_in(json(actor_request(workspace_b, :get, "/")), ["data", "items"]) == []
  end

  test "requires explicit approval and the exact immutable plan inputs" do
    response = request(:post, "/", %{"plan_id" => "plan-1", "plan_hash" => hash()})

    assert response.status == 422
    assert error_code(response) == "rebuild_approval_required"

    response = request(:post, "/", %{"approved" => true, "plan_id" => "plan-1"})
    assert response.status == 422
    assert error_code(response) == "invalid_rebuild_plan_hash"
  end

  test "rejects invalid operation and item pagination before persistence" do
    response = request(:get, "/?limit=201")
    assert response.status == 422
    assert error_code(response) == "invalid_rebuild_page"

    response = request(:get, "/rebuild-1/items?cursor=not-a-cursor")
    assert response.status == 422
    assert error_code(response) == "invalid_rebuild_cursor"

    response = request(:get, "/?state=not-a-state")
    assert response.status == 422
    assert error_code(response) == "invalid_rebuild_page"
  end

  test "requires reasons for plans and cancellation" do
    response = request(:post, "/plan", %{"target_id" => "asset-a"})
    assert response.status == 422
    assert error_code(response) == "rebuild_reason_required"

    response = request(:post, "/plan", %{"target_id" => "asset-a", "reason" => "   "})
    assert response.status == 422
    assert error_code(response) == "rebuild_reason_required"

    response = request(:post, "/rebuild-1/cancel", %{})
    assert response.status == 422
    assert error_code(response) == "rebuild_reason_required"
  end

  test "maps storage outcomes to stable HTTP semantics" do
    assert {404, "not_found", _message, %{}} =
             RebuildsRouter.error_response(Error.new(:not_found, "missing"))

    assert {409, "rebuild_plan_stale", "stale", %{}} =
             RebuildsRouter.error_response(
               Error.new(:conflict, "stale", details: %{reason_code: "rebuild_plan_stale"})
             )

    assert {403, "forbidden", _message, %{}} =
             RebuildsRouter.error_response(Error.new(:forbidden, "forbidden"))

    assert {409, "coverage_window_limit_exceeded", _message, %{}} =
             RebuildsRouter.error_response(:coverage_window_limit_exceeded)

    assert {409, "rebuild_not_supported", _message, %{}} =
             RebuildsRouter.error_response(:rebuild_target_not_supported)
  end

  test "audit metadata distinguishes initial execution from replay" do
    idempotency = %{operation: "rebuild.start", key_hash: "hashed-key"}

    refute IdempotentCommand.audit_metadata(idempotency, "accepted", false).idempotency.replayed
    assert IdempotentCommand.audit_metadata(idempotency, "accepted", true).idempotency.replayed
  end

  defp request(method, path, params \\ nil) do
    method
    |> conn(path, "")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> fetch_query_params()
    |> maybe_put_params(params)
    |> RebuildsRouter.call(RebuildsRouter.init([]))
  end

  defp actor_request(actor, method, path, params \\ nil, idempotency_key \\ nil) do
    method
    |> conn(path, "")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> put_req_header("x-favn-workspace-id", actor.workspace_id)
    |> put_req_header("x-favn-actor-id", actor.id)
    |> put_req_header("x-favn-session-token", actor.token)
    |> maybe_put_header("idempotency-key", idempotency_key)
    |> fetch_query_params()
    |> maybe_put_params(params)
    |> RebuildsRouter.call(RebuildsRouter.init([]))
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
      :router_actors,
      Map.put(Process.get(:router_actors), {workspace_id, actor_id}, actor)
    )

    Process.put(
      :router_sessions,
      Map.put(Process.get(:router_sessions), {workspace_id, Session.token_hash(token)}, session)
    )

    %{id: actor_id, workspace_id: workspace_id, token: token}
  end

  defp put_rebuild(workspace_id, operation_id) do
    now = DateTime.utc_now()

    operation = %RebuildOperation{
      workspace_id: workspace_id,
      operation_id: operation_id,
      root_target_id: "asset:orders",
      manifest_version_id: "manifest-api-test",
      candidate_generation_id: Ecto.UUID.generate(),
      plan_hash: hash(),
      plan_version: 1,
      plan_payload: %{expires_at: DateTime.add(now, 3_600, :second)},
      actor_id: "actor-admin",
      reason: "schema changed",
      idempotency_key: Idempotency.key_hash("seed"),
      evaluated_at: now,
      action_count: 1,
      window_count: 1,
      state: :queued,
      phase: :queued,
      cleanup_state: :not_started,
      cancel_requested: false,
      version: 1,
      timestamps: %RebuildTimestamps{inserted_at: now, updated_at: now}
    }

    Process.put(
      :router_rebuilds,
      Map.put(Process.get(:router_rebuilds), {workspace_id, operation_id}, operation)
    )
  end

  defp audits, do: Process.get(:router_audits) |> Map.values()
  defp json(response), do: Jason.decode!(response.resp_body)
  defp maybe_put_header(conn, _name, nil), do: conn
  defp maybe_put_header(conn, name, value), do: put_req_header(conn, name, value)

  defp maybe_put_params(conn, nil), do: conn
  defp maybe_put_params(conn, params), do: Map.put(conn, :body_params, params)
  defp error_code(response), do: get_in(Jason.decode!(response.resp_body), ["error", "code"])
  defp hash, do: String.duplicate("a", 64)
  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
