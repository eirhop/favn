defmodule FavnView.WorkspaceSessionTest do
  use FavnView.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias FavnView.Auth
  alias FavnView.Auth.Scope
  alias FavnView.Components.WorkspaceMenu
  alias FavnOrchestrator.WorkspaceConfiguration

  defmodule IdentityProbeLive do
    use FavnView, :live_view

    alias FavnView.Auth

    @impl true
    def mount(params, session, socket) do
      case Auth.on_mount(:require_authenticated_operator, params, session, socket) do
        {:cont, socket} -> {:ok, socket}
        {:halt, socket} -> {:ok, socket}
      end
    end

    @impl true
    def render(assigns) do
      ~H"""
      <div id="identity-probe">connected</div>
      """
    end
  end

  # Every real LiveView pattern-matches only the messages it sends itself, so an
  # identity message that reaches `handle_info/2` raises instead of being ignored.
  defmodule StrictProbeLive do
    use FavnView, :live_view

    alias FavnView.Auth

    @impl true
    def mount(params, session, socket) do
      case Auth.on_mount(:require_authenticated_operator, params, session, socket) do
        {:cont, socket} -> {:ok, socket}
        {:halt, socket} -> {:ok, socket}
      end
    end

    @impl true
    def handle_info(:expected_message, socket), do: {:noreply, socket}

    @impl true
    def render(assigns) do
      ~H"""
      <div id="strict-probe">{@current_scope.actor.display_name}</div>
      """
    end
  end

  @env_keys [
    :introspect_operator_session_fun,
    :active_workspace_configuration_fun,
    :list_operator_workspaces_fun,
    :subscribe_operator_identity_fun,
    :switch_operator_workspace_fun
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

  test "fetching a browser scope exposes only active workspace choices", %{conn: conn} do
    {actor, session} = identity("workspace-one", "session-one")

    Application.put_env(:favn_view, :introspect_operator_session_fun, fn
      "workspace-one", "opaque-token" -> {:ok, session, actor}
    end)

    Application.put_env(:favn_view, :list_operator_workspaces_fun, fn _context ->
      {:ok,
       [
         %{id: "workspace-one", name: "One", status: :active},
         %{id: "workspace-two", name: "Two", status: :active},
         %{id: "workspace-old", name: "Old", status: :suspended}
       ]}
    end)

    Application.put_env(:favn_view, :active_workspace_configuration_fun, fn context ->
      {:ok, workspace_configuration(context.workspace_id, "deployment-one", "Europe/Oslo")}
    end)

    conn =
      conn
      |> init_test_session(%{
        operator_workspace_id: "workspace-one",
        operator_session_token: "opaque-token"
      })
      |> Auth.fetch_current_scope([])

    assert %Scope{
             workspace_id: "workspace-one",
             workspace_configuration: %WorkspaceConfiguration{
               workspace_id: "workspace-one",
               deployment_id: "deployment-one",
               default_timezone: "Europe/Oslo",
               default_timezone_source: :application_default
             }
           } = conn.assigns.current_scope

    assert Enum.map(conn.assigns.operator_workspaces, & &1.id) == [
             "workspace-one",
             "workspace-two"
           ]
  end

  test "configuration failure preserves the authenticated session and returns unavailable", %{
    conn: conn
  } do
    {actor, session} = identity("workspace-one", "session-one")

    Application.put_env(:favn_view, :introspect_operator_session_fun, fn
      "workspace-one", "opaque-token" -> {:ok, session, actor}
    end)

    Application.put_env(:favn_view, :active_workspace_configuration_fun, fn _context ->
      {:error, :storage_unavailable}
    end)

    conn =
      conn
      |> init_test_session(%{
        operator_workspace_id: "workspace-one",
        operator_session_token: "opaque-token"
      })
      |> Auth.fetch_current_scope([])

    assert conn.assigns.current_scope == nil
    assert conn.assigns.workspace_configuration_error == :storage_unavailable
    assert get_session(conn, :operator_workspace_id) == "workspace-one"
    assert get_session(conn, :operator_session_token) == "opaque-token"

    conn = Auth.require_operator_authenticated(conn, [])
    assert conn.status == 503
    assert conn.resp_body == "Workspace configuration unavailable"
  end

  test "scope construction rejects configuration from another workspace", %{conn: conn} do
    {actor, session} = identity("workspace-one", "session-one")

    Application.put_env(:favn_view, :introspect_operator_session_fun, fn
      "workspace-one", "opaque-token" -> {:ok, session, actor}
    end)

    Application.put_env(:favn_view, :active_workspace_configuration_fun, fn _context ->
      {:ok, workspace_configuration("workspace-two", "deployment-two", "America/New_York")}
    end)

    conn =
      conn
      |> init_test_session(%{
        operator_workspace_id: "workspace-one",
        operator_session_token: "opaque-token"
      })
      |> Auth.fetch_current_scope([])

    assert conn.assigns.current_scope == nil
    assert conn.assigns.workspace_configuration_error == :workspace_configuration_scope_mismatch
    assert get_session(conn, :operator_workspace_id) == "workspace-one"
  end

  test "workspace switching replaces the browser session and redirects home", %{conn: conn} do
    {actor, old_session} = identity("workspace-one", "session-one")
    scope = Scope.new("workspace-one", actor, old_session)

    Application.put_env(:favn_view, :switch_operator_workspace_fun, fn context, target ->
      assert context.actor_id == actor.id
      assert target == "workspace-two"

      {:ok,
       %{
         id: "session-two",
         actor_id: actor.id,
         workspace_id: "workspace-two",
         provider: "password_local",
         issued_at: DateTime.utc_now(),
         expires_at: DateTime.add(DateTime.utc_now(), 3_600),
         revoked_at: nil,
         token: "replacement-token"
       }}
    end)

    conn =
      conn
      |> init_test_session(%{
        operator_workspace_id: "workspace-one",
        operator_session_token: "old-token",
        live_socket_id: Auth.live_socket_id("session-one")
      })
      |> Phoenix.Controller.fetch_flash([])
      |> assign(:current_scope, scope)

    assert {:ok, conn} = Auth.switch_operator_workspace(conn, " workspace-two ")
    assert redirected_to(conn) == "/"
    assert get_session(conn, :operator_workspace_id) == "workspace-two"
    assert get_session(conn, :operator_session_token) == "replacement-token"
    assert get_session(conn, :live_socket_id) == Auth.live_socket_id("session-two")
  end

  test "workspace switch POST requires authentication and CSRF", %{conn: conn} do
    test_pid = self()

    Application.put_env(:favn_view, :switch_operator_workspace_fun, fn _context, _target ->
      send(test_pid, :switch_called)
      {:error, :forbidden}
    end)

    csrf_checked_conn = enable_csrf_check(conn)

    assert_raise Plug.CSRFProtection.InvalidCSRFTokenError, fn ->
      post(csrf_checked_conn, ~p"/workspaces/switch", %{"workspace_id" => "workspace-two"})
    end

    {conn, csrf_token} = authenticated_session(conn, %{})

    conn =
      post(conn, ~p"/workspaces/switch", %{
        "_csrf_token" => csrf_token,
        "workspace_id" => "workspace-two"
      })

    assert redirected_to(conn) =~ "/login"
    refute_received :switch_called
  end

  test "workspace switch POST preserves the session when the target is unauthorized", %{
    conn: conn
  } do
    {actor, session} = identity("workspace-one", "session-one")
    put_authenticated_boundary(actor, session)

    Application.put_env(:favn_view, :switch_operator_workspace_fun, fn _context, target ->
      assert target == "workspace-forged"
      {:error, :forbidden}
    end)

    {conn, csrf_token} =
      authenticated_session(conn, %{
        operator_workspace_id: "workspace-one",
        operator_session_token: "old-token",
        live_socket_id: Auth.live_socket_id("session-one")
      })

    conn =
      post(conn, ~p"/workspaces/switch", %{
        "_csrf_token" => csrf_token,
        "workspace_id" => "workspace-forged"
      })

    assert redirected_to(conn) == "/"
    assert get_session(conn, :operator_workspace_id) == "workspace-one"
    assert get_session(conn, :operator_session_token) == "old-token"
    assert get_session(conn, :live_socket_id) == Auth.live_socket_id("session-one")
  end

  test "workspace switch POST rotates the browser session and disconnects its old LiveView topic",
       %{conn: conn} do
    {actor, session} = identity("workspace-one", "session-one")
    put_authenticated_boundary(actor, session)
    old_live_socket_id = Auth.live_socket_id("session-one")
    :ok = FavnView.Endpoint.subscribe(old_live_socket_id)

    Application.put_env(:favn_view, :switch_operator_workspace_fun, fn _context, target ->
      assert target == "workspace-two"

      {:ok,
       %{
         id: "session-two",
         actor_id: actor.id,
         workspace_id: "workspace-two",
         provider: "password_local",
         issued_at: DateTime.utc_now(),
         expires_at: DateTime.add(DateTime.utc_now(), 3_600),
         revoked_at: nil,
         token: "replacement-token"
       }}
    end)

    {conn, csrf_token} =
      authenticated_session(conn, %{
        operator_workspace_id: "workspace-one",
        operator_session_token: "old-token",
        live_socket_id: old_live_socket_id
      })

    conn =
      post(conn, ~p"/workspaces/switch", %{
        "_csrf_token" => csrf_token,
        "workspace_id" => "workspace-two"
      })

    assert redirected_to(conn) == "/"
    assert get_session(conn, :operator_workspace_id) == "workspace-two"
    assert get_session(conn, :operator_session_token) == "replacement-token"
    assert get_session(conn, :live_socket_id) == Auth.live_socket_id("session-two")

    assert_receive %Phoenix.Socket.Broadcast{
      topic: ^old_live_socket_id,
      event: "disconnect"
    }
  end

  test "connected LiveViews subscribe to durable identity invalidation" do
    {actor, session} = identity("workspace-one", "session-one")

    Application.put_env(:favn_view, :introspect_operator_session_fun, fn
      "workspace-one", "opaque-token" -> {:ok, session, actor}
    end)

    Application.put_env(:favn_view, :list_operator_workspaces_fun, fn _context ->
      {:ok, [%{id: "workspace-one", name: "One", status: :active}]}
    end)

    test_pid = self()

    Application.put_env(:favn_view, :subscribe_operator_identity_fun, fn context ->
      send(test_pid, {:subscribed, context.actor_id, context.session_id})
      :ok
    end)

    socket = %Phoenix.LiveView.Socket{
      transport_pid: self(),
      private: %{lifecycle: %Phoenix.LiveView.Lifecycle{}}
    }

    assert {:cont, mounted} =
             Auth.on_mount(
               :require_authenticated_operator,
               %{},
               %{
                 "operator_workspace_id" => "workspace-one",
                 "operator_session_token" => "opaque-token"
               },
               socket
             )

    assert_receive {:subscribed, "actor-one", "session-one"}
    assert mounted.assigns.current_scope.workspace_id == "workspace-one"
  end

  test "connected mount revalidates after subscribing so revocation cannot be missed" do
    {actor, session} = identity("workspace-one", "session-one")
    Process.put(:introspection_count, 0)

    Application.put_env(:favn_view, :introspect_operator_session_fun, fn
      "workspace-one", "opaque-token" ->
        case Process.get(:introspection_count) do
          0 ->
            Process.put(:introspection_count, 1)
            {:ok, session, actor}

          1 ->
            {:error, :invalid_session}
        end
    end)

    Application.put_env(:favn_view, :subscribe_operator_identity_fun, fn _context -> :ok end)

    socket = %Phoenix.LiveView.Socket{
      transport_pid: self(),
      private: %{lifecycle: %Phoenix.LiveView.Lifecycle{}}
    }

    assert {:halt, redirected} =
             Auth.on_mount(
               :require_authenticated_operator,
               %{},
               %{
                 "operator_workspace_id" => "workspace-one",
                 "operator_session_token" => "opaque-token"
               },
               socket
             )

    assert {:redirect, %{to: "/login"}} = redirected.redirected
  end

  test "an actual connected LiveView leaves immediately on identity invalidation", %{conn: conn} do
    {actor, session} = identity("workspace-one", "session-one")
    put_live_identity_boundary(actor, session, :ok)

    assert {:ok, view, _html} =
             live_isolated(conn, IdentityProbeLive,
               session: %{
                 "operator_workspace_id" => "workspace-one",
                 "operator_session_token" => "opaque-token"
               }
             )

    send(view.pid, {:favn_identity_invalidated, :session})
    assert_redirect(view, "/login")
  end

  test "connected LiveViews revalidate durable sessions without relying on PubSub", %{conn: conn} do
    {actor, session} = identity("workspace-one", "session-one")
    {:ok, validity} = Agent.start_link(fn -> true end)

    Application.put_env(:favn_view, :introspect_operator_session_fun, fn
      "workspace-one", "opaque-token" ->
        if Agent.get(validity, & &1),
          do: {:ok, session, actor},
          else: {:error, :invalid_session}
    end)

    Application.put_env(:favn_view, :list_operator_workspaces_fun, fn _context ->
      {:ok, [%{id: "workspace-one", name: "One", status: :active}]}
    end)

    Application.put_env(:favn_view, :subscribe_operator_identity_fun, fn _context -> :ok end)

    assert {:ok, view, _html} =
             live_isolated(conn, IdentityProbeLive,
               session: %{
                 "operator_workspace_id" => "workspace-one",
                 "operator_session_token" => "opaque-token"
               }
             )

    Agent.update(validity, fn _ -> false end)
    send(view.pid, :favn_revalidate_operator_identity)
    assert_redirect(view, "/login")
  end

  test "workspace configuration failure does not redirect a valid LiveView session to login", %{
    conn: conn
  } do
    {actor, session} = identity("workspace-one", "session-one")
    put_live_identity_boundary(actor, session, :ok)
    {:ok, configuration_available?} = Agent.start_link(fn -> true end)

    Application.put_env(:favn_view, :active_workspace_configuration_fun, fn context ->
      if Agent.get(configuration_available?, & &1) do
        {:ok, workspace_configuration(context.workspace_id, "deployment-one", "Europe/Oslo")}
      else
        {:error, :storage_unavailable}
      end
    end)

    assert {:ok, view, _html} =
             live_isolated(conn, IdentityProbeLive,
               session: %{
                 "operator_workspace_id" => "workspace-one",
                 "operator_session_token" => "opaque-token"
               }
             )

    Agent.update(configuration_available?, fn _ -> false end)
    send(view.pid, :favn_revalidate_operator_identity)
    assert_redirect(view, "/")
  end

  test "a successful revalidation refreshes the scope without reaching the page", %{conn: conn} do
    {actor, session} = identity("workspace-one", "session-one")
    {:ok, current_actor} = Agent.start_link(fn -> actor end)

    Application.put_env(:favn_view, :introspect_operator_session_fun, fn
      "workspace-one", "opaque-token" -> {:ok, session, Agent.get(current_actor, & &1)}
    end)

    Application.put_env(:favn_view, :list_operator_workspaces_fun, fn _context ->
      {:ok, [%{id: "workspace-one", name: "One", status: :active}]}
    end)

    Application.put_env(:favn_view, :subscribe_operator_identity_fun, fn _context -> :ok end)

    assert {:ok, view, html} =
             live_isolated(conn, StrictProbeLive,
               session: %{
                 "operator_workspace_id" => "workspace-one",
                 "operator_session_token" => "opaque-token"
               }
             )

    assert html =~ "Operator"

    Agent.update(current_actor, &%{&1 | display_name: "Renamed Operator"})
    send(view.pid, :favn_revalidate_operator_identity)

    assert render(view) =~ "Renamed Operator"
  end

  test "connected LiveView mount fails closed when identity subscription fails", %{conn: conn} do
    {actor, session} = identity("workspace-one", "session-one")
    put_live_identity_boundary(actor, session, {:error, :pubsub_unavailable})

    assert {:error, {:redirect, %{to: "/login"}}} =
             live_isolated(conn, IdentityProbeLive,
               session: %{
                 "operator_workspace_id" => "workspace-one",
                 "operator_session_token" => "opaque-token"
               }
             )
  end

  test "identity invalidation redirects a connected LiveView to login" do
    socket = %Phoenix.LiveView.Socket{}

    assert {:halt, redirected} =
             Auth.handle_identity_invalidation(
               {:favn_identity_invalidated, :workspace_membership},
               socket
             )

    assert {:redirect, %{to: "/login"}} = redirected.redirected
  end

  test "workspace menu shows the current workspace and switches between active workspaces" do
    {actor, session} = identity("workspace-one", "session-one")
    scope = Scope.new("workspace-one", actor, session)

    hidden =
      render_component(&WorkspaceMenu.workspace_menu/1,
        current_scope: scope,
        workspaces: [%{id: "workspace-one", name: "One", status: :active}]
      )

    assert hidden =~ ~s(data-testid="workspace-menu")
    refute hidden =~ ~s(data-testid="workspace-menu-trigger")
    assert hidden =~ "One"

    visible =
      render_component(&WorkspaceMenu.workspace_menu/1,
        current_scope: scope,
        workspaces: [
          %{id: "workspace-one", name: "One", status: :active},
          %{id: "workspace-two", name: "Two", status: :active}
        ]
      )

    assert visible =~ ~s(data-testid="workspace-menu-trigger")
    assert visible =~ ~s(value="workspace-one")
    assert visible =~ ~s(value="workspace-two")
  end

  defp identity(workspace_id, session_id) do
    actor = %{
      id: "actor-one",
      username: "operator",
      display_name: "Operator",
      roles: [:admin]
    }

    session = %{
      id: session_id,
      actor_id: actor.id,
      workspace_id: workspace_id,
      provider: "password_local",
      issued_at: DateTime.utc_now(),
      expires_at: DateTime.add(DateTime.utc_now(), 3_600),
      revoked_at: nil
    }

    {actor, session}
  end

  defp workspace_configuration(workspace_id, deployment_id, timezone) do
    %WorkspaceConfiguration{
      workspace_id: workspace_id,
      deployment_id: deployment_id,
      default_timezone: timezone,
      default_timezone_source: :application_default
    }
  end

  defp put_authenticated_boundary(actor, session) do
    Application.put_env(:favn_view, :introspect_operator_session_fun, fn
      "workspace-one", "old-token" -> {:ok, session, actor}
    end)

    Application.put_env(:favn_view, :list_operator_workspaces_fun, fn _context ->
      {:ok,
       [
         %{id: "workspace-one", name: "One", status: :active},
         %{id: "workspace-two", name: "Two", status: :active}
       ]}
    end)
  end

  defp put_live_identity_boundary(actor, session, subscription_result) do
    Application.put_env(:favn_view, :introspect_operator_session_fun, fn
      "workspace-one", "opaque-token" -> {:ok, session, actor}
    end)

    Application.put_env(:favn_view, :list_operator_workspaces_fun, fn _context ->
      {:ok, [%{id: "workspace-one", name: "One", status: :active}]}
    end)

    Application.put_env(
      :favn_view,
      :subscribe_operator_identity_fun,
      fn _context -> subscription_result end
    )
  end

  defp authenticated_session(conn, session) do
    csrf_token = Plug.CSRFProtection.get_csrf_token()
    session = Map.put(session, "_csrf_token", Plug.CSRFProtection.dump_state())

    {conn |> enable_csrf_check() |> init_test_session(session), csrf_token}
  end

  defp enable_csrf_check(conn) do
    %{conn | private: Map.delete(conn.private, :plug_skip_csrf_protection)}
  end
end
