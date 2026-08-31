defmodule FavnOrchestrator.API.AuthenticationServiceContextTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.Router
  alias FavnOrchestrator.Auth.ServiceTokens

  @token "service-context-test-token-with-32-bytes"

  setup do
    previous = Application.get_env(:favn_orchestrator, :api_service_tokens)

    previous_deployer_tokens =
      Application.get_env(:favn_orchestrator, :manifest_deployer_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "deployment_cli",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true,
        platform_roles: [:platform_operator]
      ]
    ])

    Application.put_env(:favn_orchestrator, :manifest_deployer_tokens, [])

    on_exit(fn ->
      restore_env(:api_service_tokens, previous)
      restore_env(:manifest_deployer_tokens, previous_deployer_tokens)
    end)

    :ok
  end

  test "generated request IDs are available to manifest deployment authentication" do
    conn =
      :put
      |> conn("/api/orchestrator/v1/manifest-deployments/request-id-test")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")
      |> Router.call(Router.init([]))

    assert conn.status == 422
    assert [_request_id] = get_resp_header(conn, "x-request-id")

    assert Jason.decode!(conn.resp_body)["error"]["message"] ==
             "X-Favn-Archive-Sha256 is required"
  end

  test "builds bounded workspace-admin authority for a platform operator service" do
    conn =
      :post
      |> conn("/activate")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")

    assert {:ok, session, actor, context} = Authentication.service_workspace_context(conn)
    assert session.id == "api-service:deployment_cli"
    assert actor.id == "service:deployment_cli"
    assert context.workspace_id == "workspace-a"
    assert context.roles == [:workspace_admin]
  end

  test "service-only local clients use one workspace contract for every operator level" do
    conn =
      :post
      |> conn("/command")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")

    for role <- [:viewer, :operator, :admin] do
      assert {:ok, session, actor, context} =
               Authentication.workspace_or_service_context(conn, role)

      assert session.id == "api-service:deployment_cli"
      assert actor.id == "service:deployment_cli"
      assert context.workspace_id == "workspace-a"
      assert context.roles == [:workspace_admin]
    end
  end

  test "rejects a service without platform operator authority" do
    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "reader",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true,
        platform_roles: [:platform_reader]
      ]
    ])

    conn =
      :post
      |> conn("/activate")
      |> put_req_header("authorization", "Bearer #{@token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")

    assert {:error, :forbidden} = Authentication.service_workspace_context(conn)
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
