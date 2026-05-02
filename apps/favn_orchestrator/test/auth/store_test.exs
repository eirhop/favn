defmodule FavnOrchestrator.Auth.StoreTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore

  setup do
    auth_start = ensure_auth_store_started()
    :ok = AuthStore.reset()

    on_exit(fn -> maybe_stop_auth_store(auth_start) end)

    :ok
  end

  test "persists actors credentials sessions and audits through storage" do
    assert {:ok, actor} = Auth.create_actor("admin", "admin-password", "Admin", [:admin])
    assert {:ok, session, ^actor} = Auth.password_login("admin", "admin-password")
    assert is_binary(session.token)
    assert {:ok, _session, ^actor} = Auth.introspect_session(session.token)

    :ok = Auth.put_audit(%{action: "auth.test", actor_id: actor.id, password: "secret"})

    assert [%{action: "auth.test", password: "[REDACTED]"}] = Auth.list_audit(limit: 10)

    refute inspect(Auth.list_actors()) =~ "admin-password"
    refute inspect(Auth.list_audit()) =~ "secret"
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
end
