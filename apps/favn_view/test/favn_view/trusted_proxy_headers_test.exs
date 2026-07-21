defmodule FavnView.TrustedProxyHeadersTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias FavnView.Plugs.RequestParsers
  alias FavnView.Plugs.TrustedProxyHeaders
  alias FavnView.ProductionRuntimeConfig

  @secret_key_base String.duplicate("s", 64)

  setup do
    previous_endpoint = Application.get_env(:favn_view, FavnView.Endpoint)
    persistent_key = {ProductionRuntimeConfig, :config}
    previous_runtime_config = :persistent_term.get(persistent_key, :missing)

    {:ok, config} =
      ProductionRuntimeConfig.validate(%{
        "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
        "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base,
        "FAVN_VIEW_TRUSTED_PROXY_CIDRS" => "10.0.0.0/8",
        "FAVN_HTTP_BODY_LIMIT_BYTES" => "65536"
      })

    :ok = ProductionRuntimeConfig.apply(config)

    on_exit(fn ->
      Application.put_env(:favn_view, FavnView.Endpoint, previous_endpoint)

      case previous_runtime_config do
        :missing -> :persistent_term.erase(persistent_key)
        config -> :persistent_term.put(persistent_key, config)
      end
    end)

    :ok
  end

  test "rewrites forwarded headers only for an allowlisted immediate peer" do
    trusted =
      conn(:get, "http://view.internal/operator")
      |> put_peer_data(%{address: {10, 20, 30, 40}, port: 4_432, ssl_cert: nil})
      |> put_req_header("x-forwarded-for", "192.0.2.10")
      |> put_req_header("x-forwarded-host", "favn.example.com")
      |> put_req_header("x-forwarded-port", "443")
      |> put_req_header("x-forwarded-proto", "https")
      |> TrustedProxyHeaders.call([])

    assert trusted.remote_ip == {192, 0, 2, 10}
    assert trusted.host == "favn.example.com"
    assert trusted.port == 443
    assert trusted.scheme == :https

    untrusted =
      conn(:get, "http://view.internal/operator")
      |> put_peer_data(%{address: {203, 0, 113, 5}, port: 4_432, ssl_cert: nil})
      |> put_req_header("x-forwarded-host", "attacker.example")
      |> put_req_header("x-forwarded-proto", "https")
      |> TrustedProxyHeaders.call([])

    assert untrusted.host == "view.internal"
    assert untrusted.scheme == :http
    assert get_req_header(untrusted, "x-forwarded-host") == []
    assert get_req_header(untrusted, "x-forwarded-proto") == []
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
end
