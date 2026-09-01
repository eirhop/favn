defmodule FavnOrchestrator.API.AuthRouterContractTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias FavnOrchestrator.API.AuthRouter
  alias FavnOrchestrator.Auth.Credentials
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Auth.Store
  alias FavnOrchestrator.Persistence.Results.Actor
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores

  @service_token "auth-router-service-token-with-32-bytes"
  @username "operator@example.test"
  @password "correct horse battery staple"

  defmodule IdentityStore do
    alias FavnOrchestrator.Persistence.Commands.CreateSession
    alias FavnOrchestrator.Persistence.Error
    alias FavnOrchestrator.Persistence.Queries.GetActor
    alias FavnOrchestrator.Persistence.Results.Session
    alias FavnOrchestrator.Persistence.Selectors.ActorByUsername

    def get_actor(%GetActor{
          workspace_context: context,
          selector: %ActorByUsername{username: username}
        }) do
      case Process.get(:auth_router_actor) do
        %{workspace_id: workspace_id, username: ^username} = actor
        when workspace_id == context.workspace_id ->
          {:ok, actor}

        _missing ->
          {:error, Error.new(:not_found, "actor not found")}
      end
    end

    def create_session(%CreateSession{} = command) do
      {:ok,
       %Session{
         session_id: command.session_id,
         actor_id: command.actor_id,
         workspace_id: command.workspace_context.workspace_id,
         provider: command.provider,
         issued_at: command.occurred_at,
         status: :active,
         expires_at: command.expires_at
       }}
    end

    def record_audit(_command), do: :ok
  end

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "auth_router_test",
        token_hash: ServiceTokens.hash_token(@service_token),
        enabled: true,
        platform_roles: []
      ]
    ])

    %{password_hash: password_hash} = Credentials.hash_password(@password)

    Process.put(:auth_router_actor, %Actor{
      actor_id: "actor-auth-router",
      username: @username,
      display_name: "Auth Router Operator",
      status: :active,
      workspace_id: "workspace-auth",
      membership_status: :active,
      roles: [:customer_operator],
      credential_hash: password_hash,
      credential_version: 1,
      access_version: 1,
      version: 1
    })

    stores = %Stores{
      registry: IdentityStore,
      runs: IdentityStore,
      run_submissions: IdentityStore,
      runner_tasks: IdentityStore,
      run_ownership: IdentityStore,
      scheduler: IdentityStore,
      admission: IdentityStore,
      resource_circuits: IdentityStore,
      target_generations: IdentityStore,
      target_recovery: IdentityStore,
      rebuilds: IdentityStore,
      target_operation_locks: IdentityStore,
      materialization: IdentityStore,
      backfills: IdentityStore,
      operator_reads: IdentityStore,
      logs: IdentityStore,
      identity: IdentityStore,
      maintenance: IdentityStore
    }

    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})
    start_supervised!(Store)

    on_exit(fn ->
      restore_env(:api_service_tokens, previous_tokens)
      Process.delete(:auth_router_actor)
    end)

    :ok
  end

  test "public facade preserves password-login success and invalid-credential contracts" do
    assert {:ok, session, actor} =
             FavnOrchestrator.operator_password_login(
               "workspace-auth",
               @username,
               @password,
               remote_identity: "127.0.0.1"
             )

    assert session.workspace_id == "workspace-auth"
    assert actor.id == "actor-auth-router"
    assert actor.roles == [:operator]

    assert {:error, :invalid_credentials} =
             FavnOrchestrator.operator_password_login(
               "workspace-auth",
               @username,
               "wrong password value",
               remote_identity: "127.0.0.2"
             )
  end

  test "password session endpoint returns the persisted session and actor" do
    response = password_request(%{"username" => @username, "password" => @password})

    assert response.status == 201

    assert %{
             "data" => %{
               "actor" => %{"id" => "actor-auth-router", "roles" => ["operator"]},
               "session" => %{"actor_id" => "actor-auth-router", "provider" => "password_local"},
               "session_token" => token
             }
           } = Jason.decode!(response.resp_body)

    assert is_binary(token) and token != ""
  end

  test "password session endpoint preserves validation and credential failures" do
    missing = password_request(%{"username" => @username})
    denied = password_request(%{"username" => @username, "password" => "wrong password value"})

    assert missing.status == 422
    assert get_in(Jason.decode!(missing.resp_body), ["error", "code"]) == "validation_failed"
    assert denied.status == 401
    assert get_in(Jason.decode!(denied.resp_body), ["error", "code"]) == "unauthenticated"
  end

  test "password session endpoint rejects missing service credentials before login" do
    response =
      :post
      |> conn("/password/sessions", "")
      |> put_req_header("x-favn-workspace-id", "workspace-auth")
      |> Map.put(:body_params, %{"username" => @username, "password" => @password})
      |> AuthRouter.call(AuthRouter.init([]))

    assert response.status == 401

    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) ==
             "service_unauthorized"
  end

  defp password_request(params) do
    :post
    |> conn("/password/sessions", "")
    |> put_req_header("authorization", "Bearer #{@service_token}")
    |> put_req_header("x-favn-workspace-id", "workspace-auth")
    |> Map.put(:body_params, params)
    |> AuthRouter.call(AuthRouter.init([]))
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
