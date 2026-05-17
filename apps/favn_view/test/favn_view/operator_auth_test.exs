defmodule FavnView.OperatorAuthTest do
  use FavnView.ConnCase, async: false

  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnView.Auth.BrowserSessionStore

  setup do
    ensure_auth_store_started()
    :ok = AuthStore.reset()
    :ok = BrowserSessionStore.reset()

    :ok
  end

  test "login succeeds with valid credentials and safe return path", %{conn: conn} do
    assert {:ok, _actor} =
             Auth.create_actor("operator", "operator-password-long", "Operator", [:operator])

    conn =
      post(conn, ~p"/login", %{
        "operator" => %{
          "username" => "operator",
          "password" => "operator-password-long",
          "return_to" => "/pipelines"
        }
      })

    assert redirected_to(conn) == "/pipelines"
    assert is_binary(get_session(conn, :operator_browser_session_id))
    assert get_session(conn, :operator_session_token) == nil
    assert String.starts_with?(get_session(conn, :live_socket_id), "operator_browser_sessions:")
  end

  test "authenticated pages do not store or render raw browser auth session material", %{
    conn: conn
  } do
    assert {:ok, _actor} =
             Auth.create_actor("operator", "operator-password-long", "Operator", [:operator])

    conn =
      post(conn, ~p"/login", %{
        "operator" => %{"username" => "operator", "password" => "operator-password-long"}
      })

    browser_session_id = get_session(conn, :operator_browser_session_id)
    live_socket_id = get_session(conn, :live_socket_id)
    assert {:ok, browser_session} = BrowserSessionStore.fetch(browser_session_id)
    token = browser_session.operator_session_token
    refute get_session(conn, :operator_session_token) == token

    html = conn |> recycle() |> get(~p"/assets") |> html_response(200)

    refute html =~ token
    refute html =~ browser_session_id
    refute html =~ live_socket_id
  end

  test "login fails generically and rejects external return paths", %{conn: conn} do
    assert {:ok, _actor} =
             Auth.create_actor("operator", "operator-password-long", "Operator", [:operator])

    conn =
      post(conn, ~p"/login", %{
        "operator" => %{
          "username" => "operator",
          "password" => "wrong-password",
          "return_to" => "https://evil.example/assets"
        }
      })

    assert conn.status == 401
    assert html_response(conn, 401) =~ "Invalid username or password"
    refute html_response(conn, 401) =~ "https://evil.example/assets"
  end

  test "protected browser LiveView routes redirect anonymous operators", %{conn: conn} do
    conn = get(conn, ~p"/assets")

    assert redirected_to(conn) == "/login?return_to=%2Fassets"
  end

  test "anonymous auth cleanup deletes legacy raw-token cookie key", %{conn: conn} do
    conn =
      conn
      |> Plug.Test.init_test_session(operator_session_token: "legacy-token")
      |> get(~p"/assets")

    assert redirected_to(conn) == "/login?return_to=%2Fassets"
    assert get_session(conn, :operator_session_token) == nil
  end

  test "health and readiness stay public", %{conn: conn} do
    assert conn |> get(~p"/api/web/v1/health/live") |> json_response(200)

    ready_conn = get(conn, ~p"/api/web/v1/health/ready")
    assert ready_conn.status in [200, 503]
    assert json_response(ready_conn, ready_conn.status)
  end

  test "logout revokes orchestrator session broadcasts disconnect and clears session", %{
    conn: conn
  } do
    assert {:ok, _actor} =
             Auth.create_actor("operator", "operator-password-long", "Operator", [:operator])

    conn =
      post(conn, ~p"/login", %{
        "operator" => %{"username" => "operator", "password" => "operator-password-long"}
      })

    browser_session_id = get_session(conn, :operator_browser_session_id)
    live_socket_id = get_session(conn, :live_socket_id)
    assert {:ok, browser_session} = BrowserSessionStore.fetch(browser_session_id)
    token = browser_session.operator_session_token
    assert {:ok, _session, _actor} = FavnOrchestrator.introspect_operator_session(token)

    Phoenix.PubSub.subscribe(FavnView.PubSub, live_socket_id)

    conn = conn |> recycle() |> delete(~p"/logout")

    assert redirected_to(conn) == "/login"
    assert get_session(conn, :operator_browser_session_id) == nil
    assert get_session(conn, :operator_session_token) == nil
    assert get_session(conn, :live_socket_id) == nil
    assert {:error, :not_found} = BrowserSessionStore.fetch(browser_session_id)
    assert {:error, :invalid_session} = FavnOrchestrator.introspect_operator_session(token)
    assert_receive %Phoenix.Socket.Broadcast{topic: ^live_socket_id, event: "disconnect"}
  end

  test "favn_view auth code uses only public orchestrator facade functions" do
    files = [
      Path.expand("../../lib/favn_view/auth.ex", __DIR__),
      Path.expand("../../lib/favn_view/auth/scope.ex", __DIR__),
      Path.expand("../../lib/favn_view/auth/browser_session_store.ex", __DIR__),
      Path.expand("../../lib/favn_view/controllers/operator_session_controller.ex", __DIR__)
    ]

    forbidden = [
      "FavnOrchestrator.Auth",
      "Auth.Store",
      "FavnOrchestrator.Storage",
      "Storage.Adapter",
      "service_token",
      "service-token",
      "Scheduler.Runtime",
      "RunnerClient"
    ]

    for file <- files, term <- forbidden do
      refute File.read!(file) =~ term
    end
  end

  defp ensure_auth_store_started do
    case Process.whereis(AuthStore) do
      nil -> start_supervised!({AuthStore, []})
      _pid -> :ok
    end
  end
end
