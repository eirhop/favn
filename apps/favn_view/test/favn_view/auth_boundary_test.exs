defmodule FavnView.AuthBoundaryTest do
  use FavnView.ConnCase, async: true

  alias FavnView.Auth
  alias FavnView.Auth.Scope

  test "protected routes redirect anonymous requests to a local login path", %{conn: conn} do
    conn = get(conn, ~p"/assets")

    assert redirected_to(conn) == "/login?return_to=%2Fassets"
  end

  test "browser responses include the production security headers", %{conn: conn} do
    conn = get(conn, ~p"/login")

    assert [policy] = get_resp_header(conn, "content-security-policy")
    assert policy =~ "default-src 'self'"
    assert policy =~ "script-src 'self'"
    assert policy =~ "connect-src 'self'"
    refute policy =~ " ws:"
    refute policy =~ " wss:"
    refute policy =~ "unsafe-eval"
    refute policy =~ "script-src 'self' 'unsafe-inline'"

    assert get_resp_header(conn, "permissions-policy") == [
             "camera=(), geolocation=(), microphone=()"
           ]

    assert get_resp_header(conn, "referrer-policy") == ["no-referrer"]
    assert get_resp_header(conn, "x-content-type-options") == ["nosniff"]
    assert get_resp_header(conn, "x-frame-options") == ["DENY"]
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

  test "browser scope accepts public atom-key actor and session DTOs" do
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

    assert scope.actor.id == actor.id
    assert scope.session.id == session.id
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
      "FavnOrchestrator.ControlPlaneRuntimeConfig",
      "FavnStoragePostgres",
      "FavnOrchestrator.Auth.Store",
      "FavnOrchestrator.Scheduler.PersistenceRuntime",
      "FavnOrchestrator.RunManager"
    ]

    for file <- files, term <- forbidden do
      refute File.read!(file) =~ term, "#{file} bypasses the orchestrator facade with #{term}"
    end
  end

  test "view code never mints a durable run or backfill id" do
    files = Path.wildcard(Path.expand("../../lib/favn_view/**/*.{ex,heex}", __DIR__))

    # A run id is a permanent record and the handle a resubmission is recognised by, so
    # the orchestrator derives it from the operator command. A view that builds one is
    # deciding an identity whose uniqueness and stability it cannot answer for, and the
    # facade would then have to choose between its own id and the caller's.
    minting = [
      ~r/"(run|bf|bfw)_[a-z_]*"\s*<>/,
      ~r/"(run|bf|bfw)_[a-z_]*\#\{/
    ]

    for file <- files, pattern <- minting do
      refute File.read!(file) =~ pattern,
             "#{file} builds an orchestrator id; ask the facade for one instead"
    end
  end
end
