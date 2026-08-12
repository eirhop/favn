defmodule FavnView.ReleaseHealthTest do
  use ExUnit.Case, async: false

  alias FavnView.ReleaseHealth

  @persistent_key {ReleaseHealth, :probe}

  setup do
    previous = :persistent_term.get(@persistent_key, :missing)
    :persistent_term.erase(@persistent_key)

    on_exit(fn ->
      case previous do
        :missing -> :persistent_term.erase(@persistent_key)
        value -> :persistent_term.put(@persistent_key, value)
      end
    end)
  end

  test "accepts only a 200 response from the real View readiness route" do
    {port, server} = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")

    assert :ok = ReleaseHealth.run(%{bind_host: "0.0.0.0", port: port})
    assert_receive {:request, ^server, request}
    assert request =~ "GET /api/web/v1/health/ready HTTP/1.1"
    assert_receive {:served, ^server}
  end

  test "uses only the bind address frozen at View boot" do
    {port, server} = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")

    assert :ok = ReleaseHealth.configure(%{bind_host: "0.0.0.0", port: port})
    assert :ok = ReleaseHealth.run()
    assert_receive {:served, ^server}

    source = File.read!(Path.expand("../../lib/favn_view/release_health.ex", __DIR__))
    refute source =~ "System.get_env"
    refute source =~ "System.fetch_env"
  end

  test "normalizes non-ready, unreachable, and invalid probes" do
    {port, server} = serve_once("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")

    assert {:error, :not_ready} = ReleaseHealth.run(%{bind_host: "0.0.0.0", port: port})
    assert_receive {:served, ^server}

    assert {:error, :invalid_port} =
             ReleaseHealth.run(%{bind_host: "0.0.0.0", port: "nope"})

    assert {:error, :invalid_host} =
             ReleaseHealth.run(%{bind_host: "not-an-ip", port: port})

    assert {:error, :invalid_host} =
             ReleaseHealth.run(%{bind_host: "10.42.0.7", port: port})

    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, {_address, unused_port}} = :inet.sockname(listener)
    :ok = :gen_tcp.close(listener)

    assert {:error, :connect_failed} =
             ReleaseHealth.run(%{bind_host: "0.0.0.0", port: unused_port})

    assert {:error, :not_configured} = ReleaseHealth.run()
  end

  defp serve_once(response) do
    parent = self()
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_address, port}} = :inet.sockname(listener)

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        {:ok, request} = :gen_tcp.recv(socket, 0, 3_000)
        send(parent, {:request, self(), request})
        :ok = :gen_tcp.send(socket, response)
        :ok = :gen_tcp.close(socket)
        :ok = :gen_tcp.close(listener)
        send(parent, {:served, self()})
      end)

    {port, server}
  end
end
