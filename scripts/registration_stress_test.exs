Code.require_file("../deployment/docker-compose/registration-stress/support.exs", __DIR__)
ExUnit.start()

defmodule RegistrationStressSupportTest do
  use ExUnit.Case, async: false
  alias RegistrationStress.Support, as: S

  test "HTTP does not retry unavailable POSTs or follow redirects" do
    for response <- [
          "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 0\r\n",
          "HTTP/1.1 307 Temporary Redirect\r\nLocation: /redirected\r\n"
        ] do
      {url, server} = server(response, "{}")

      assert {:ok, status, %{}} =
               S.http(url, :post, %{test: true}, [{"Idempotency-Key", "original"}])

      assert status in [503, 307]
      assert_receive {:request, request}
      assert String.downcase(request) =~ "idempotency-key: original"
      assert :single_request == Task.await(server)
      refute_receive {:request, _}
    end
  end

  test "invalid success JSON is an error rather than an implicit retry" do
    {url, server} = server("HTTP/1.1 200 OK\r\n", "invalid")
    assert_raise JSON.DecodeError, fn -> S.http(url, :post, %{test: true}) end
    assert_receive {:request, _}
    assert :single_request == Task.await(server)
  end

  test "evidence is exclusive and synced, and pending interruption prevents next mutation" do
    path = Path.join(System.tmp_dir!(), "favn-stress-test-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm(path) end)
    io = S.evidence(path)
    S.emit(%{event: "submission_intent", idempotency_key: "original"}, io)
    File.close(io)
    assert JSON.decode!(File.read!(path))["idempotency_key"] == "original"
    assert_raise MatchError, fn -> S.evidence(path) end
    send(self(), :terminate)
    assert_raise RuntimeError, ~r/Interrupted/, fn -> S.sleep(0) end
  end

  test "SIGTERM lets the script finish its restoration block before VM shutdown" do
    path =
      Path.join(System.tmp_dir!(), "favn-stress-signal-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm(path) end)
    support = Path.expand("../deployment/docker-compose/registration-stress/support.exs", __DIR__)

    code = """
    Code.require_file(#{inspect(support)})
    RegistrationStress.Support.trap_termination()
    IO.puts("READY:" <> System.pid())
    try do
      RegistrationStress.Support.sleep(30_000)
    after
      File.write!(#{inspect(path)}, "restored")
    end
    """

    port =
      Port.open(
        {:spawn_executable, System.find_executable("elixir")},
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          {:line, 8192},
          {:args, ["--erl", "+S 2:2", "-e", code]}
        ]
      )

    assert_receive {^port, {:data, {:eol, "READY:" <> pid}}}, 5_000
    assert {_, 0} = System.cmd("kill", ["-TERM", pid])
    assert_receive {^port, {:exit_status, status}}, 5_000
    assert status != 0
    assert File.read!(path) == "restored"
  end

  defp server(header, body) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, {_, port}} = :inet.sockname(listen)
    owner = self()

    server =
      Task.async(fn ->
        try do
          {:ok, socket} = :gen_tcp.accept(listen, 5000)
          {:ok, request} = :gen_tcp.recv(socket, 0, 5000)
          send(owner, {:request, request})

          :ok =
            :gen_tcp.send(
              socket,
              header <> "Content-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n" <> body
            )

          :gen_tcp.close(socket)

          case :gen_tcp.accept(listen, 300) do
            {:error, :timeout} ->
              :single_request

            {:ok, duplicate} ->
              :gen_tcp.close(duplicate)
              :duplicate_request
          end
        after
          :gen_tcp.close(listen)
        end
      end)

    {"http://127.0.0.1:#{port}/runs", server}
  end
end
