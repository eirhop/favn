defmodule FavnOrchestrator.Auth.TrustedLocalDevelopmentTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Auth.Session

  setup do
    previous = Application.get_env(:favn_orchestrator, :trusted_local_development_auth)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:favn_orchestrator, :trusted_local_development_auth)
        value -> Application.put_env(:favn_orchestrator, :trusted_local_development_auth, value)
      end
    end)

    :ok
  end

  test "trusted local sessions use an explicit persisted provider" do
    assert {:ok, session} =
             Session.issue("actor-local", provider: "trusted_local_dev", ttl_seconds: 60)

    assert session.provider == "trusted_local_dev"
  end

  test "facade fails closed when trusted local development is disabled" do
    Application.delete_env(:favn_orchestrator, :trusted_local_development_auth)

    assert {:error, :trusted_local_development_unavailable} =
             FavnOrchestrator.trusted_local_development_login(
               "workspace",
               "admin",
               "capability"
             )
  end

  test "facade rejects mismatched workspace, username, and capability before persistence" do
    capability = "local-capability"

    Application.put_env(:favn_orchestrator, :trusted_local_development_auth, %{
      workspace_id: "workspace",
      username: "admin",
      capability_hash: ServiceTokens.hash_token(capability)
    })

    for {workspace_id, username, provided} <- [
          {"other-workspace", "admin", capability},
          {"workspace", "other-user", capability},
          {"workspace", "admin", "wrong-capability"}
        ] do
      assert {:error, :trusted_local_development_unavailable} =
               FavnOrchestrator.trusted_local_development_login(
                 workspace_id,
                 username,
                 provided
               )
    end
  end
end
