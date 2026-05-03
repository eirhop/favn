defmodule FavnOrchestrator.Auth.StoreTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.Storage

  setup do
    auth_start = ensure_auth_store_started()
    :ok = AuthStore.reset()

    on_exit(fn -> maybe_stop_auth_store(auth_start) end)

    :ok
  end

  test "persists actors credentials sessions and audits through storage" do
    assert {:ok, actor} = Auth.create_actor("admin", "admin-password", "Admin", [:admin])

    assert {:ok, %{password_hash: "$argon2id$" <> _} = credential} =
             Storage.get_auth_credential(actor.id)

    assert Argon2.verify_pass("admin-password", credential.password_hash)
    refute inspect(credential) =~ "admin-password"

    assert {:ok, session, ^actor} = Auth.password_login("admin", "admin-password")
    assert is_binary(session.token)
    assert {:ok, _session, ^actor} = Auth.introspect_session(session.token)

    :ok = Auth.put_audit(%{action: "auth.test", actor_id: actor.id, password: "secret"})

    assert [%{action: "auth.test", password: "[REDACTED]"}] = Auth.list_audit(limit: 10)

    refute inspect(Auth.list_actors()) =~ "admin-password"
    refute inspect(Auth.list_audit()) =~ "secret"
  end

  test "rejects weak passwords and authenticates without user enumeration details" do
    assert {:error, :password_too_short} =
             Auth.create_actor("short", "too-short", "Short", [:admin])

    assert {:error, :password_blank} = Auth.create_actor("blank", "   ", "Blank", [:admin])

    assert {:ok, _actor} = Auth.create_actor("admin", "admin-password", "Admin", [:admin])
    assert {:error, :invalid_credentials} = Auth.password_login("admin", "wrong-password")
    assert {:error, :invalid_credentials} = Auth.password_login("missing", "wrong-password")
  end

  test "password change revokes active sessions" do
    assert {:ok, actor} = Auth.create_actor("admin", "admin-password", "Admin", [:admin])
    assert {:ok, session, ^actor} = Auth.password_login("admin", "admin-password")

    assert :ok = Auth.set_actor_password(actor.id, "new-admin-password")
    assert {:error, :invalid_session} = Auth.introspect_session(session.token)
    assert {:ok, _new_session, updated_actor} = Auth.password_login("admin", "new-admin-password")
    assert updated_actor.id == actor.id
  end

  test "session ttl is explicit and expired sessions fail" do
    assert {:ok, actor} = Auth.create_actor("admin", "admin-password", "Admin", [:admin])
    assert {:ok, session} = AuthStore.issue_session(actor.id, ttl_seconds: 30)
    assert DateTime.diff(session.expires_at, session.issued_at, :second) == 30
    assert {:error, :invalid_session_ttl} = AuthStore.issue_session(actor.id, ttl_seconds: 0)

    now = DateTime.utc_now()
    expired_token = "expired-session-token"

    expired_session = %{
      id: "ses_expired",
      actor_id: actor.id,
      provider: "password_local",
      issued_at: DateTime.add(now, -120, :second),
      expires_at: DateTime.add(now, -60, :second),
      revoked_at: nil,
      token_hash: token_hash(expired_token)
    }

    assert :ok = Storage.put_auth_session(expired_session)
    assert {:error, :invalid_session} = Auth.introspect_session(expired_token)
  end

  test "revoked sessions stay invalid" do
    assert {:ok, actor} = Auth.create_actor("admin", "admin-password", "Admin", [:admin])
    assert {:ok, session, ^actor} = Auth.password_login("admin", "admin-password")

    assert :ok = Auth.revoke_session(session.id)
    assert {:error, :invalid_session} = Auth.introspect_session(session.token)
  end

  test "revoking a missing session returns an error without corrupting memory storage" do
    assert {:error, :not_found} = Auth.revoke_session("ses_missing")
    assert Auth.list_audit(limit: 10) == []
  end

  defp ensure_auth_store_started do
    case Process.whereis(AuthStore) do
      nil ->
        start_supervised!({AuthStore, []})
        :started

      _pid ->
        :existing
    end
  end

  defp maybe_stop_auth_store(:existing), do: :ok

  defp maybe_stop_auth_store(:started) do
    case Process.whereis(AuthStore) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal)
    end
  end

  defp token_hash(token) do
    :sha256
    |> :crypto.hash(token)
    |> Base.url_encode64(padding: false)
  end
end
