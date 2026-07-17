defmodule FavnOrchestrator.API.ManifestPublicationTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias FavnOrchestrator.API.ManifestPublication
  alias FavnOrchestrator.API.ManifestPublication.Config
  alias FavnOrchestrator.Auth.ServiceTokens

  @opts ManifestPublication.init([])
  @path "/api/orchestrator/v1/manifests"
  @token "manifest-publication-test-token"

  defmodule ChunkedSuccessPlug do
    @moduledoc false

    import Plug.Conn, only: [send_resp: 3]

    def init(opts), do: opts

    def call(conn, opts) do
      conn = FavnOrchestrator.API.ManifestPublication.call(conn, opts)
      if conn.halted, do: conn, else: send_resp(conn, 204, "")
    end
  end

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)
    previous_config = Application.get_env(:favn_orchestrator, :manifest_publication)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "manifest_test",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true
      ]
    ])

    Application.put_env(:favn_orchestrator, :manifest_publication,
      compressed_limit_bytes: 1024 * 1024,
      decompressed_limit_bytes: 1024 * 1024
    )

    on_exit(fn ->
      restore_env(:api_service_tokens, previous_tokens)
      restore_env(:manifest_publication, previous_config)
    end)

    :ok
  end

  test "parses plain and gzip JSON into the same body params" do
    json = Jason.encode!(%{"manifest" => %{"metadata" => %{"owner" => "data"}}})

    plain = request(json)
    gzip = request(:zlib.gzip(json), [{"content-encoding", "gzip"}])

    refute plain.halted
    refute gzip.halted
    assert gzip.body_params == plain.body_params
    assert plain.body_params["manifest"]["metadata"]["owner"] == "data"
  end

  test "default budgets accept a representative multi-megabyte manifest payload" do
    put_limits(
      Config.default_compressed_limit_bytes(),
      Config.default_decompressed_limit_bytes()
    )

    json =
      Jason.encode!(%{
        "manifest" => %{"generated_sql" => String.duplicate("select 1;\n", 450_000)}
      })

    assert byte_size(json) > 4 * 1024 * 1024
    gzip = :zlib.gzip(json)
    assert byte_size(gzip) < byte_size(json)

    response = request(gzip, [{"content-encoding", "gzip"}])

    refute response.halted
    assert response.body_params["manifest"]["generated_sql"] =~ "select 1"
  end

  test "accepts and rejects the exact uncompressed boundary" do
    json = Jason.encode!(%{"manifest" => %{"padding" => String.duplicate("a", 128)}})
    size = byte_size(json)

    put_limits(1024, size)
    accepted = request(json)
    refute accepted.halted

    put_limits(1024, size - 1)
    rejected = request(json)

    assert rejected.status == 413

    assert %{
             "error" => %{
               "code" => "manifest_payload_too_large",
               "details" => %{
                 "encoding" => "identity",
                 "limit_kind" => "uncompressed",
                 "limit_bytes" => limit,
                 "observed_size_bytes" => observed,
                 "size_source" => "content_length"
               }
             }
           } = Jason.decode!(rejected.resp_body)

    assert limit == size - 1
    assert observed == size
  end

  test "accepts an exact-limit chunked HTTP/1 body and rejects one extra byte" do
    json = Jason.encode!(%{"manifest" => %{"padding" => String.duplicate("a", 128)}})
    put_limits(1024, byte_size(json))

    {:ok, server} =
      Bandit.start_link(
        plug: ChunkedSuccessPlug,
        ip: {127, 0, 0, 1},
        port: 0,
        startup_log: false
      )

    Process.unlink(server)

    on_exit(fn ->
      if Process.alive?(server), do: Supervisor.stop(server)
    end)

    assert {:ok, {{127, 0, 0, 1}, port}} = ThousandIsland.listener_info(server)
    assert chunked_status(port, json) == 204
    assert chunked_status(port, json <> " ") == 413
  end

  test "accepts and rejects the exact compressed boundary" do
    json = Jason.encode!(%{"manifest" => %{"padding" => String.duplicate("a", 4096)}})
    gzip = :zlib.gzip(json)
    size = byte_size(gzip)

    put_limits(size, byte_size(json))
    accepted = request(gzip, [{"content-encoding", "gzip"}])
    refute accepted.halted

    put_limits(size - 1, byte_size(json))
    rejected = request(gzip, [{"content-encoding", "gzip"}])

    assert rejected.status == 413

    assert %{
             "error" => %{
               "details" => %{
                 "encoding" => "gzip",
                 "limit_kind" => "compressed",
                 "limit_bytes" => limit,
                 "observed_size_bytes" => observed
               }
             }
           } = Jason.decode!(rejected.resp_body)

    assert limit == size - 1
    assert observed == size
  end

  test "rejects gzip content that expands beyond the decompressed limit" do
    json = Jason.encode!(%{"manifest" => %{"padding" => String.duplicate("a", 4096)}})
    gzip = :zlib.gzip(json)
    put_limits(byte_size(gzip), byte_size(json) - 1)

    response = request(gzip, [{"content-encoding", "gzip"}])

    assert response.status == 413

    assert %{
             "error" => %{
               "code" => "manifest_payload_too_large",
               "details" => %{
                 "limit_kind" => "decompressed",
                 "limit_bytes" => limit,
                 "observed_size_bytes" => observed,
                 "size_source" => "observed_at_least"
               }
             }
           } = Jason.decode!(response.resp_body)

    assert limit == byte_size(json) - 1
    assert observed >= byte_size(json)
  end

  test "rejects malformed gzip and unsupported content encoding" do
    malformed = request("not-gzip", [{"content-encoding", "gzip"}])
    unsupported = request("{}", [{"content-encoding", "br"}])

    assert malformed.status == 400

    assert %{"error" => %{"code" => "invalid_compression"}} =
             Jason.decode!(malformed.resp_body)

    assert unsupported.status == 415

    assert %{"error" => %{"code" => "unsupported_content_encoding"}} =
             Jason.decode!(unsupported.resp_body)

    assert get_resp_header(unsupported, "connection") == ["close"]
  end

  test "rejects an unsupported media type without retaining the unread body connection" do
    response = request("{}", [{"content-type", "text/plain"}])

    assert response.status == 415
    assert get_resp_header(response, "connection") == ["close"]

    assert %{"error" => %{"code" => "unsupported_media_type"}} =
             Jason.decode!(response.resp_body)
  end

  test "rejects unauthorized input before attempting decompression" do
    response = request("not-gzip", [{"content-encoding", "gzip"}], authorize?: false)

    assert response.status == 401

    assert %{"error" => %{"code" => "service_unauthorized"}} =
             Jason.decode!(response.resp_body)

    assert get_resp_header(response, "connection") == ["close"]
  end

  test "rejects invalid JSON with a stable error" do
    response = request(:zlib.gzip("not-json"), [{"content-encoding", "gzip"}])

    assert response.status == 400
    assert %{"error" => %{"code" => "invalid_json"}} = Jason.decode!(response.resp_body)
  end

  defp request(body, headers \\ [], opts \\ []) do
    conn =
      conn(:post, @path, body)
      |> put_req_header("content-type", "application/json")
      |> put_req_header("content-length", Integer.to_string(byte_size(body)))
      |> put_headers(headers)

    conn =
      if Keyword.get(opts, :authorize?, true) do
        put_req_header(conn, "authorization", "Bearer #{@token}")
      else
        conn
      end

    ManifestPublication.call(conn, @opts)
  end

  defp put_headers(conn, headers) do
    Enum.reduce(headers, conn, fn {key, value}, acc -> put_req_header(acc, key, value) end)
  end

  defp put_limits(compressed, decompressed) do
    Application.put_env(:favn_orchestrator, :manifest_publication,
      compressed_limit_bytes: compressed,
      decompressed_limit_bytes: decompressed
    )
  end

  defp chunked_status(port, body) do
    {:ok, socket} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw], 2_000)

    chunk_size = body |> byte_size() |> Integer.to_string(16)

    :ok =
      :gen_tcp.send(socket, [
        "POST #{@path} HTTP/1.1\r\n",
        "host: 127.0.0.1:#{port}\r\n",
        "authorization: Bearer #{@token}\r\n",
        "content-type: application/json\r\n",
        "transfer-encoding: chunked\r\n",
        "connection: close\r\n",
        "\r\n",
        chunk_size,
        "\r\n",
        body,
        "\r\n0\r\n\r\n"
      ])

    response = receive_until_closed(socket, [])
    :ok = :gen_tcp.close(socket)

    response
    |> String.split(" ", parts: 3)
    |> Enum.at(1)
    |> String.to_integer()
  end

  defp receive_until_closed(socket, chunks) do
    case :gen_tcp.recv(socket, 0, 2_000) do
      {:ok, chunk} -> receive_until_closed(socket, [chunk | chunks])
      {:error, :closed} -> chunks |> Enum.reverse() |> IO.iodata_to_binary()
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
