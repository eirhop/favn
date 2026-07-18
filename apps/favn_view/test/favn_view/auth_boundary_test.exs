defmodule FavnView.AuthBoundaryTest do
  use FavnView.ConnCase, async: true

  alias FavnView.Auth
  alias FavnView.Auth.Scope

  test "protected routes redirect anonymous requests to a local login path", %{conn: conn} do
    conn = get(conn, ~p"/assets")

    assert redirected_to(conn) == "/login?return_to=%2Fassets"
  end

  test "return paths accept only local absolute paths" do
    assert Auth.safe_return_to("/pipelines?status=failed") == "/pipelines?status=failed"

    for unsafe <- [
          nil,
          "pipelines",
          "//evil.example/assets",
          "https://evil.example/assets",
          "javascript:alert(1)"
        ] do
      assert Auth.safe_return_to(unsafe) == nil
    end
  end

  test "browser scope retains only public actor and session fields" do
    actor = %{
      id: "actor-1",
      username: "operator",
      display_name: "Operator",
      roles: [:operator, :unknown],
      credential_hash: "must-not-escape"
    }

    session = %{
      id: "session-1",
      actor_id: actor.id,
      provider: "password_local",
      issued_at: DateTime.utc_now(),
      expires_at: DateTime.add(DateTime.utc_now(), 3_600),
      revoked_at: nil,
      token: "must-not-escape",
      token_hash: "must-not-escape"
    }

    scope = Scope.new("workspace-1", actor, session)

    assert scope.roles == [:operator]
    assert Scope.has_role?(scope, :viewer)
    assert Scope.has_role?(scope, :operator)
    refute Scope.has_role?(scope, :admin)
    refute Map.has_key?(scope.actor, :credential_hash)
    refute Map.has_key?(scope.session, :token)
    refute Map.has_key?(scope.session, :token_hash)
  end

  test "view code depends only on the public orchestrator boundary" do
    files = Path.wildcard(Path.expand("../../lib/favn_view/**/*.{ex,heex}", __DIR__))

    forbidden = [
      "FavnOrchestrator.Persistence",
      "FavnOrchestrator.Storage",
      "FavnStoragePostgres",
      "FavnOrchestrator.Auth.Store",
      "FavnOrchestrator.Scheduler.PersistenceRuntime",
      "FavnOrchestrator.RunManager"
    ]

    for file <- files, term <- forbidden do
      refute File.read!(file) =~ term, "#{file} bypasses the orchestrator facade with #{term}"
    end
  end
end
