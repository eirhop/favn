defmodule FavnView.AdminLiveTest do
  use FavnView.ConnCase, async: false

  import Phoenix.LiveViewTest

  @env_keys [
    :introspect_operator_session_fun,
    :list_operator_workspaces_fun,
    :subscribe_operator_identity_fun,
    :page_operator_actors_fun,
    :page_operator_sessions_fun,
    :page_operator_audit_fun,
    :create_operator_actor_fun,
    :attach_operator_actor_fun,
    :update_operator_actor_membership_fun,
    :revoke_operator_managed_session_fun,
    :change_operator_password_fun
  ]

  setup do
    previous = Map.new(@env_keys, &{&1, Application.get_env(:favn_view, &1)})

    on_exit(fn ->
      Enum.each(previous, fn
        {key, nil} -> Application.delete_env(:favn_view, key)
        {key, value} -> Application.put_env(:favn_view, key, value)
      end)
    end)

    :ok
  end

  test "non-admin operators cannot open workspace administration", %{conn: conn} do
    put_identity_boundary(:viewer)
    put_admin_pages()

    assert {:error, {:redirect, %{to: "/"}}} =
             conn |> authenticated_conn() |> live(~p"/admin")
  end

  test "admin reads stay current-workspace scoped and audit detail is not rendered", %{conn: conn} do
    put_identity_boundary(:admin)
    test_pid = self()

    Application.put_env(:favn_view, :page_operator_actors_fun, fn context, opts ->
      send(test_pid, {:actors_context, context.workspace_id, opts})
      {:ok, page([managed_actor()])}
    end)

    Application.put_env(:favn_view, :page_operator_sessions_fun, fn context, opts ->
      send(test_pid, {:sessions_context, context.workspace_id, opts})
      {:ok, page([managed_session()])}
    end)

    Application.put_env(:favn_view, :page_operator_audit_fun, fn context, opts ->
      send(test_pid, {:audit_context, context.workspace_id, opts})

      {:ok,
       page([
         %{
           action: "session.revoked",
           subject_kind: "session",
           subject_id: "session-other",
           principal_id: "actor-admin",
           occurred_at: DateTime.utc_now(),
           detail: %{"must_not_render" => "secret-looking-value"}
         }
       ])}
    end)

    assert {:ok, _view, html} = conn |> authenticated_conn() |> live(~p"/admin")
    assert html =~ ~s(data-testid="workspace-admin")
    assert html =~ "session.revoked"
    refute html =~ "secret-looking-value"
    assert_receive {:actors_context, "workspace-one", [limit: 50]}
    assert_receive {:sessions_context, "workspace-one", [limit: 50]}
    assert_receive {:audit_context, "workspace-one", [limit: 50]}
  end

  test "membership and session handlers block self-lockout even with crafted events", %{
    conn: conn
  } do
    put_identity_boundary(:admin)
    put_admin_pages()
    test_pid = self()

    Application.put_env(:favn_view, :update_operator_actor_membership_fun, fn _, _, _, _ ->
      send(test_pid, :membership_called)
      {:ok, managed_actor()}
    end)

    Application.put_env(:favn_view, :revoke_operator_managed_session_fun, fn _, _ ->
      send(test_pid, :revoke_called)
      :ok
    end)

    assert {:ok, view, _html} = conn |> authenticated_conn() |> live(~p"/admin")

    render_submit(view, "update_membership", %{
      "actor_id" => "actor-admin",
      "role" => "viewer",
      "status" => "revoked"
    })

    render_click(view, "revoke_session", %{"session_id" => "session-current"})

    refute_receive :membership_called
    refute_receive :revoke_called
  end

  test "admin mutations reject malformed roles and statuses without calling the orchestrator", %{
    conn: conn
  } do
    put_identity_boundary(:admin)
    put_admin_pages()
    test_pid = self()

    Application.put_env(:favn_view, :update_operator_actor_membership_fun, fn
      context, "actor-managed", roles, status ->
        send(test_pid, {:updated, context.workspace_id, roles, status})
        {:ok, managed_actor()}
    end)

    Application.put_env(:favn_view, :create_operator_actor_fun, fn _, _, _, _, _ ->
      send(test_pid, :created)
      {:ok, managed_actor()}
    end)

    Application.put_env(:favn_view, :attach_operator_actor_fun, fn _, _, _ ->
      send(test_pid, :attached)
      {:ok, managed_actor()}
    end)

    assert {:ok, view, _html} = conn |> authenticated_conn() |> live(~p"/admin")

    render_submit(view, "update_membership", %{
      "actor_id" => "actor-managed",
      "role" => "platform_admin",
      "status" => "disabled_globally"
    })

    render_submit(view, "create_actor", %{
      "username" => "new-user",
      "display_name" => "New User",
      "password" => "matching-secret",
      "password_confirmation" => "matching-secret",
      "role" => "platform_admin"
    })

    render_submit(view, "attach_actor", %{
      "username" => "existing-user",
      "role" => "platform_admin"
    })

    refute_receive {:updated, _, _, _}
    refute_receive :created
    refute_receive :attached

    render_submit(view, "update_membership", %{
      "actor_id" => "actor-managed",
      "role" => "operator",
      "status" => "suspended"
    })

    assert_receive {:updated, "workspace-one", [:operator], :suspended}
  end

  test "actor creation confirms passwords without retaining or rendering them", %{conn: conn} do
    put_identity_boundary(:admin)
    put_admin_pages()
    test_pid = self()

    Application.put_env(:favn_view, :create_operator_actor_fun, fn _, _, _, _, _ ->
      send(test_pid, :create_called)
      {:ok, managed_actor()}
    end)

    assert {:ok, view, html} = conn |> authenticated_conn() |> live(~p"/admin")
    refute html =~ "super-secret-password"

    render_submit(view, "create_actor", %{
      "username" => "new-user",
      "display_name" => "New User",
      "password" => "super-secret-password",
      "password_confirmation" => "different",
      "role" => "operator"
    })

    refute_receive :create_called
    refute render(view) =~ "super-secret-password"
  end

  test "password rotation is self-only and forces a new login", %{conn: conn} do
    put_identity_boundary(:viewer)
    test_pid = self()

    Application.put_env(:favn_view, :change_operator_password_fun, fn
      context, "current-secret-value", "replacement-secret-value" ->
        send(test_pid, {:password_changed, context.actor_id})
        :ok
    end)

    assert {:ok, view, html} = conn |> authenticated_conn() |> live(~p"/account/security")
    refute html =~ "current-secret-value"
    refute html =~ "replacement-secret-value"

    view
    |> element("form[phx-submit=change_password]")
    |> render_submit(%{
      "current_password" => "current-secret-value",
      "new_password" => "replacement-secret-value",
      "new_password_confirmation" => "replacement-secret-value"
    })

    assert_receive {:password_changed, "actor-admin"}
    assert_redirect(view, "/login")
  end

  defp put_identity_boundary(role) do
    {actor, session} = identity(role)

    Application.put_env(:favn_view, :introspect_operator_session_fun, fn
      "workspace-one", "opaque-token" -> {:ok, session, actor}
    end)

    Application.put_env(:favn_view, :list_operator_workspaces_fun, fn context ->
      assert context.workspace_id == "workspace-one"
      {:ok, [%{id: "workspace-one", name: "One", status: :active}]}
    end)

    Application.put_env(:favn_view, :subscribe_operator_identity_fun, fn context ->
      assert context.workspace_id == "workspace-one"
      :ok
    end)
  end

  defp put_admin_pages do
    Application.put_env(:favn_view, :page_operator_actors_fun, fn _, _ ->
      {:ok, page([managed_actor()])}
    end)

    Application.put_env(:favn_view, :page_operator_sessions_fun, fn _, _ ->
      {:ok, page([managed_session()])}
    end)

    Application.put_env(:favn_view, :page_operator_audit_fun, fn _, _ ->
      {:ok, page([])}
    end)
  end

  defp authenticated_conn(conn) do
    init_test_session(conn, %{
      operator_workspace_id: "workspace-one",
      operator_session_token: "opaque-token",
      live_socket_id: FavnView.Auth.live_socket_id("session-current")
    })
  end

  defp identity(role) do
    actor = %{
      id: "actor-admin",
      username: "admin",
      display_name: "Admin",
      roles: [role]
    }

    session = %{
      id: "session-current",
      actor_id: actor.id,
      workspace_id: "workspace-one",
      provider: "password_local",
      issued_at: DateTime.utc_now(),
      status: :active,
      expires_at: DateTime.add(DateTime.utc_now(), 3_600),
      revoked_at: nil
    }

    {actor, session}
  end

  defp managed_actor do
    %{
      id: "actor-managed",
      username: "managed",
      display_name: "Managed Actor",
      roles: [:operator],
      status: :active,
      global_status: :active,
      membership_status: :active,
      workspace_id: "workspace-one",
      access_version: 1,
      version: 1
    }
  end

  defp managed_session do
    %{
      id: "session-other",
      actor_id: "actor-managed",
      workspace_id: "workspace-one",
      provider: "password_local",
      issued_at: DateTime.utc_now(),
      expires_at: DateTime.add(DateTime.utc_now(), 3_600),
      revoked_at: nil
    }
  end

  defp page(items) do
    %{items: items, next_cursor: nil, has_more?: false}
  end
end
