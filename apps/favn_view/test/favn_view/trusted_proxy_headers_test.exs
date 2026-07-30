defmodule FavnView.TrustedProxyHeadersTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias FavnView.Plugs.RequestParsers
  alias FavnView.Plugs.RuntimeTransportSecurity
  alias FavnView.Plugs.TrustedProxyHeaders
  alias FavnView.ProductionRuntimeConfig
  alias FavnView.Endpoint

  @secret_key_base String.duplicate("s", 64)
  @trusted_proxy {10, 42, 0, 5}
  @untrusted_peer {10, 42, 0, 6}

  setup do
    previous_endpoint = Application.get_env(:favn_view, FavnView.Endpoint)
    persistent_key = {ProductionRuntimeConfig, :config}
    previous_runtime_config = :persistent_term.get(persistent_key, :missing)

    :ok = apply_runtime_config()

    on_exit(fn ->
      Application.put_env(:favn_view, FavnView.Endpoint, previous_endpoint)

      case previous_runtime_config do
        :missing -> :persistent_term.erase(persistent_key)
        config -> :persistent_term.put(persistent_key, config)
      end
    end)

    :ok
  end

  test "replace policy rewrites only bounded identity from an allowlisted peer" do
    trusted =
      conn(:get, "http://view.internal/operator")
      |> with_peer(@trusted_proxy)
      |> put_req_header("x-forwarded-for", "192.0.2.10")
      |> put_req_header("x-forwarded-host", "attacker.example")
      |> put_req_header("x-forwarded-port", "8443")
      |> put_req_header("x-forwarded-proto", "https")
      |> put_req_header("forwarded", "for=198.51.100.7;proto=http")
      |> TrustedProxyHeaders.call([])

    assert trusted.remote_ip == {192, 0, 2, 10}
    assert trusted.host == "favn.example.com"
    assert trusted.port == 443
    assert trusted.scheme == :https

    for header <-
          ~w(forwarded x-forwarded-for x-forwarded-host x-forwarded-port x-forwarded-proto) do
      assert get_req_header(trusted, header) == []
    end
  end

  test "untrusted peer cannot forge HTTPS or the redirect authority" do
    response =
      conn(:get, "http://view.internal/operator")
      |> with_peer(@untrusted_peer)
      |> put_req_header("forwarded", "for=192.0.2.10;host=attacker.example;proto=https")
      |> put_req_header("x-forwarded-for", "192.0.2.10")
      |> put_req_header("x-forwarded-host", "attacker.example")
      |> put_req_header("x-forwarded-port", "443")
      |> put_req_header("x-forwarded-proto", "https")
      |> RuntimeTransportSecurity.call([])

    assert response.status == 301
    assert get_resp_header(response, "location") == ["https://favn.example.com/operator"]
    refute response.resp_body =~ "attacker.example"

    for header <-
          ~w(forwarded x-forwarded-for x-forwarded-host x-forwarded-port x-forwarded-proto) do
      assert get_req_header(response, header) == []
    end
  end

  test "localhost host spellings cannot bypass the fixed-origin redirect" do
    for request_host <- ["localhost", "127.0.0.1"] do
      response =
        conn(:get, "http://#{request_host}/operator")
        |> with_peer(@untrusted_peer)
        |> put_req_header("x-forwarded-proto", "https")
        |> RuntimeTransportSecurity.call([])

      assert response.status == 301

      assert get_resp_header(response, "location") == [
               "https://favn.example.com/operator"
             ]
    end
  end

  test "container-local readiness is the only plaintext loopback exception" do
    ready =
      conn(:get, "http://localhost/api/web/v1/health/ready")
      |> with_peer({127, 0, 0, 1})
      |> RuntimeTransportSecurity.call([])

    assert is_nil(ready.status)
    assert ready.scheme == :http

    redirected =
      conn(:get, "http://localhost/robots.txt")
      |> with_peer({127, 0, 0, 1})
      |> Endpoint.call([])

    assert redirected.status == 301

    assert get_resp_header(redirected, "location") == [
             "https://favn.example.com/robots.txt"
           ]
  end

  test "fixed public origin includes its explicit port in redirects" do
    :ok = apply_runtime_config("replace", "https://favn.example.com:8443")

    response =
      conn(:get, "http://attacker.example/operator")
      |> with_peer(@untrusted_peer)
      |> RuntimeTransportSecurity.call([])

    assert response.status == 301

    assert get_resp_header(response, "location") == [
             "https://favn.example.com:8443/operator"
           ]
  end

  test "endpoint transport boundary protects static files before Plug.Static" do
    response =
      conn(:get, "http://attacker.example/robots.txt")
      |> with_peer(@untrusted_peer)
      |> put_req_header("x-forwarded-proto", "https")
      |> Endpoint.call([])

    assert response.status == 301

    assert get_resp_header(response, "location") == [
             "https://favn.example.com/robots.txt"
           ]

    proxied =
      conn(:get, "http://view.internal/robots.txt")
      |> with_peer(@trusted_proxy)
      |> put_req_header("x-forwarded-for", "192.0.2.10")
      |> put_req_header("x-forwarded-proto", "https")
      |> Endpoint.call([])

    assert proxied.status == 200
  end

  test "endpoint transport boundary runs before LiveView socket dispatch" do
    response =
      conn(:get, "http://attacker.example/live/websocket?vsn=2.0.0")
      |> with_peer(@untrusted_peer)
      |> put_req_header("connection", "upgrade")
      |> put_req_header("upgrade", "websocket")
      |> put_req_header("sec-websocket-key", Base.encode64(:crypto.strong_rand_bytes(16)))
      |> put_req_header("sec-websocket-version", "13")
      |> put_req_header("x-forwarded-proto", "https")
      |> Endpoint.call([])

    assert response.status == 301

    assert get_resp_header(response, "location") == [
             "https://favn.example.com/live/websocket?vsn=2.0.0"
           ]

    proxied =
      conn(:get, "http://view.internal/live/websocket?vsn=2.0.0")
      |> with_peer(@trusted_proxy)
      |> put_req_header("connection", "upgrade")
      |> put_req_header("upgrade", "websocket")
      |> put_req_header("sec-websocket-key", Base.encode64(:crypto.strong_rand_bytes(16)))
      |> put_req_header("sec-websocket-version", "13")
      |> put_req_header("x-forwarded-for", "192.0.2.10")
      |> put_req_header("x-forwarded-proto", "https")
      |> Endpoint.call([])

    refute proxied.status == 301
  end

  test "append policy selects only the rightmost proxy-observed address" do
    :ok = apply_runtime_config("append")

    rewritten =
      conn(:get, "http://view.internal/operator")
      |> with_peer(@trusted_proxy)
      |> put_req_header("x-forwarded-for", "client-controlled, 192.0.2.44")
      |> put_req_header("x-forwarded-proto", "https")
      |> TrustedProxyHeaders.call([])

    assert rewritten.remote_ip == {192, 0, 2, 44}
  end

  test "replace policy fails closed for a chain or malformed address" do
    for forwarded_for <- ["198.51.100.99, 192.0.2.44", "not-an-ip"] do
      unchanged =
        conn(:get, "http://view.internal/operator")
        |> with_peer(@trusted_proxy)
        |> put_req_header("x-forwarded-for", forwarded_for)
        |> TrustedProxyHeaders.call([])

      assert unchanged.remote_ip == @trusted_proxy
    end

    missing =
      conn(:get, "http://view.internal/operator")
      |> with_peer(@trusted_proxy)
      |> TrustedProxyHeaders.call([])

    assert missing.remote_ip == @trusted_proxy
  end

  test "ambiguous forwarded scheme cannot bypass HTTPS enforcement" do
    for forwarded_proto <- ["https,http", String.duplicate("h", 9)] do
      response =
        conn(:get, "http://view.internal/operator")
        |> with_peer(@trusted_proxy)
        |> put_req_header("x-forwarded-proto", forwarded_proto)
        |> RuntimeTransportSecurity.call([])

      assert response.status == 301
    end

    duplicate_lines =
      conn(:get, "http://view.internal/operator")
      |> with_peer(@trusted_proxy)
      |> Map.put(:req_headers, [
        {"x-forwarded-proto", "https"},
        {"x-forwarded-proto", "http"}
      ])
      |> RuntimeTransportSecurity.call([])

    assert duplicate_lines.status == 301
  end

  test "append policy bounds the complete chain and requires a valid rightmost address" do
    :ok = apply_runtime_config("append")

    for forwarded_for <- [
          String.duplicate("1", 4_097),
          Enum.map_join(1..33, ",", fn _index -> "192.0.2.1" end),
          "192.0.2.1,not-an-ip"
        ] do
      unchanged =
        conn(:get, "http://view.internal/operator")
        |> with_peer(@trusted_proxy)
        |> put_req_header("x-forwarded-for", forwarded_for)
        |> TrustedProxyHeaders.call([])

      assert unchanged.remote_ip == @trusted_proxy
    end

    duplicate_lines =
      conn(:get, "http://view.internal/operator")
      |> with_peer(@trusted_proxy)
      |> Map.put(:req_headers, [
        {"x-forwarded-for", "192.0.2.44"},
        {"x-forwarded-for", "192.0.2.45"}
      ])
      |> TrustedProxyHeaders.call([])

    assert duplicate_lines.remote_ip == @trusted_proxy
  end

  test "ignore policy preserves the authorized socket peer" do
    :ok = apply_runtime_config("ignore")

    unchanged =
      conn(:get, "http://view.internal/operator")
      |> with_peer(@trusted_proxy)
      |> put_req_header("x-forwarded-for", "192.0.2.10")
      |> put_req_header("x-forwarded-proto", "https")
      |> TrustedProxyHeaders.call([])

    assert unchanged.remote_ip == @trusted_proxy
    assert unchanged.scheme == :https
  end

  test "ordinary request parsing uses the boot-frozen body limit" do
    oversized = String.duplicate("x", 65_537)

    assert_raise Plug.Parsers.RequestTooLargeError, fn ->
      :post
      |> conn("/operator", oversized)
      |> put_req_header("content-type", "application/json")
      |> RequestParsers.call([])
    end
  end

  defp apply_runtime_config(
         policy \\ "replace",
         origin \\ "https://favn.example.com"
       ) do
    {:ok, config} =
      ProductionRuntimeConfig.validate(%{
        "FAVN_VIEW_PUBLIC_ORIGIN" => origin,
        "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base,
        "FAVN_VIEW_TRUSTED_PROXY_CIDRS" => "10.42.0.5/32",
        "FAVN_VIEW_FORWARDED_FOR_POLICY" => policy,
        "FAVN_HTTP_BODY_LIMIT_BYTES" => "65536"
      })

    ProductionRuntimeConfig.apply(config)
  end

  defp with_peer(conn, address) do
    conn
    |> Map.put(:remote_ip, address)
    |> put_peer_data(%{address: address, port: 4_432, ssl_cert: nil})
  end
end
