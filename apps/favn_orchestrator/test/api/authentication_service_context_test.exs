defmodule FavnOrchestrator.API.AuthenticationServiceContextTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.Auth.ServiceTokens

  @token "service-context-test-token-with-32-bytes"

  setup do
    previous = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "deployment_cli",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true,
        platform_roles: [:platform_operator]
      ]
    ])

    on_exit(fn -> restore_env(:api_service_tokens, previous) end)
    :ok
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
