defmodule FavnView.RuntimeSessionRotationTest do
  use ExUnit.Case, async: false

  alias Plug.Conn
  alias Plug.Test
  alias FavnView.Plugs.RuntimeSession
  alias FavnView.ProductionRuntimeConfig

  @cookie_key "_favn_rotation_test"
  @old_secret String.duplicate("o", 64)
  @new_secret String.duplicate("n", 64)
  @session_options [
    store: :cookie,
    key: @cookie_key,
    signing_salt: "favn-rotation-signing-v1",
    encryption_salt: "favn-rotation-encryption-v1",
    same_site: "Lax",
    http_only: true,
    secure: true
  ]

  setup do
    persistent_key = {ProductionRuntimeConfig, :config}
    previous_runtime_config = :persistent_term.get(persistent_key, :missing)
    previous_options = Application.fetch_env(:favn_view, :session_cookie_options)

    :persistent_term.erase(persistent_key)
    Application.put_env(:favn_view, :session_cookie_options, @session_options)

    on_exit(fn ->
      restore_application_env(previous_options)

      case previous_runtime_config do
        :missing -> :persistent_term.erase(persistent_key)
        config -> :persistent_term.put(persistent_key, config)
      end
    end)
  end

  test "changing the endpoint secret invalidates an existing browser session" do
    cookie = issue_cookie(@old_secret, actor_id: "actor-1")

    assert read_session(cookie, @old_secret, :actor_id) == "actor-1"
    assert read_session(cookie, @new_secret, :actor_id) == nil
  end

  defp issue_cookie(secret, values) do
    conn =
      secret
      |> session_conn()
      |> Conn.fetch_session()
      |> put_session_values(values)
      |> Conn.send_resp(200, "ok")

    conn.resp_cookies |> Map.fetch!(@cookie_key) |> Map.fetch!(:value)
  end

  defp read_session(cookie, secret, key) do
    secret
    |> session_conn()
    |> Conn.put_req_header("cookie", "#{@cookie_key}=#{cookie}")
    |> RuntimeSession.call([])
    |> Conn.fetch_session()
    |> Conn.get_session(key)
  end

  defp session_conn(secret) do
    conn = Test.conn(:get, "/")
    conn = %{conn | secret_key_base: secret}
    RuntimeSession.call(conn, [])
  end

  defp put_session_values(conn, values) do
    Enum.reduce(values, conn, fn {key, value}, acc -> Conn.put_session(acc, key, value) end)
  end

  defp restore_application_env({:ok, options}),
    do: Application.put_env(:favn_view, :session_cookie_options, options)

  defp restore_application_env(:error),
    do: Application.delete_env(:favn_view, :session_cookie_options)
end
