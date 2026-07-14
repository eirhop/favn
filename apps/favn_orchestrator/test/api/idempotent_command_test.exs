defmodule FavnOrchestrator.API.IdempotentCommandTest do
  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test, only: [conn: 2]

  alias FavnOrchestrator.API.IdempotentCommand
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Idempotency
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "favn_web",
        token_hash: ServiceTokens.hash_token("test-service-token"),
        enabled: true
      ]
    ])

    :ok = Memory.reset()

    on_exit(fn ->
      if is_nil(previous_tokens),
        do: Application.delete_env(:favn_orchestrator, :api_service_tokens),
        else: Application.put_env(:favn_orchestrator, :api_service_tokens, previous_tokens)
    end)

    :ok
  end

  test "persists an unknown outcome when command execution raises" do
    request = %{"target" => "asset:orders"}
    conn = authenticated_conn("raise-once")
    Process.put(:idempotent_command_calls, 0)

    response =
      IdempotentCommand.run(conn, "test.raise", "actor-1", "session-1", request, fn _context ->
        Process.put(:idempotent_command_calls, Process.get(:idempotent_command_calls) + 1)
        raise "unexpected callback failure"
      end)

    assert response.status == 500

    assert %{
             "error" => %{
               "code" => "internal_error",
               "details" => %{"outcome" => "unknown"}
             }
           } = Jason.decode!(response.resp_body)

    replay =
      "raise-once"
      |> authenticated_conn()
      |> IdempotentCommand.run(
        "test.raise",
        "actor-1",
        "session-1",
        request,
        fn _context -> flunk("terminal unknown outcome must be replayed") end
      )

    assert replay.status == 500
    assert Process.get(:idempotent_command_calls) == 1

    record_id =
      Idempotency.record_id(
        "test.raise",
        "actor-1",
        "session-1",
        "favn_web",
        Idempotency.key_hash("raise-once")
      )

    assert {:ok, %{status: :failed, response_status: 500}} = Idempotency.get(record_id)
  end

  defp authenticated_conn(idempotency_key) do
    conn(:post, "/commands")
    |> put_req_header("authorization", "Bearer test-service-token")
    |> put_req_header("idempotency-key", idempotency_key)
  end
end
