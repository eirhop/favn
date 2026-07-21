defmodule FavnOrchestrator.ReleaseHealthTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ReleaseHealth

  test "accepts only a 200 readiness response" do
    {port, server} = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")

    assert :ok = ReleaseHealth.run(%{"FAVN_VIEW_PORT" => Integer.to_string(port)})
    assert_receive {:served, ^server}
  end

  test "normalizes non-ready, unreachable, and invalid ports" do
    {port, server} = serve_once("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")

    assert {:error, :not_ready} =
             ReleaseHealth.run(%{"FAVN_VIEW_PORT" => Integer.to_string(port)})

    assert_receive {:served, ^server}
    assert {:error, :invalid_port} = ReleaseHealth.run(%{"FAVN_VIEW_PORT" => "nope"})

    assert {:error, :invalid_host} =
             ReleaseHealth.run(%{
               "FAVN_VIEW_BIND_HOST" => "not-an-ip",
               "FAVN_VIEW_PORT" => Integer.to_string(port)
             })

    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, {_address, unused_port}} = :inet.sockname(listener)
    :ok = :gen_tcp.close(listener)

    assert {:error, :connect_failed} =
             ReleaseHealth.run(%{"FAVN_VIEW_PORT" => Integer.to_string(unused_port)})
  end

  defp serve_once(response) do
    parent = self()
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_address, port}} = :inet.sockname(listener)

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        {:ok, _request} = :gen_tcp.recv(socket, 0, 3_000)
        :ok = :gen_tcp.send(socket, response)
        :ok = :gen_tcp.close(socket)
        :ok = :gen_tcp.close(listener)
        send(parent, {:served, self()})
      end)

    {port, server}
  end
end
