defmodule FavnOrchestrator.API.ManifestMemoryAdmissionTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias FavnOrchestrator.API.ManifestMemoryAdmission
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.Budget
  alias FavnOrchestrator.MemoryCapacity.Coordinator
  alias FavnOrchestrator.MemoryCapacity.Ledger

  @token "manifest-memory-admission-test-token"
  @gib 1_024 * 1_024 * 1_024

  defmodule Provider do
    def snapshot(agent: agent), do: Agent.get(agent, & &1)
  end

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "manifest-memory-test",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true
      ]
    ])

    {:ok, provider} =
      Agent.start_link(fn ->
        {:ok,
         %{
           source: :cgroup_v2,
           limit_bytes: 2 * @gib,
           usage_bytes: 128 * 1_024 * 1_024,
           headroom_bytes: 2 * @gib - 128 * 1_024 * 1_024
         }}
      end)

    ledger = unique_name(:ledger)
    coordinator = unique_name(:coordinator)
    table = unique_name(:table)

    children = [
      %{
        id: :ledger,
        start: {Ledger, :start_link, [[name: ledger, table: table]]},
        restart: :temporary
      },
      %{
        id: :coordinator,
        start:
          {Coordinator, :start_link,
           [
             [
               name: coordinator,
               ledger: ledger,
               provider: Provider,
               provider_opts: [agent: provider]
             ]
           ]},
        restart: :permanent
      }
    ]

    start_supervised!(%{
      id: unique_name(:supervisor),
      start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]}
    })

    on_exit(fn -> restore_env(:api_service_tokens, previous_tokens) end)

    %{coordinator: coordinator, provider: provider}
  end

  test "all low-level publication routes reserve before body parsing and release after response",
       context do
    for path <- [
          "/api/orchestrator/v1/execution-packages",
          "/api/orchestrator/v1/execution-packages/missing",
          "/api/orchestrator/v1/manifests"
        ] do
      admitted = call(path, context.coordinator)

      refute admitted.halted
      assert %Plug.Conn.Unfetched{} = admitted.body_params
      assert MemoryCapacity.diagnostics(server: context.coordinator).active_leases == 1

      _sent = send_resp(admitted, 204, "")
      assert MemoryCapacity.diagnostics(server: context.coordinator).active_leases == 0
    end
  end

  test "another manifest-heavy owner receives 429 without parsing the body", context do
    assert {:ok, token} =
             MemoryCapacity.acquire(Budget.manifest_base(),
               server: context.coordinator,
               exclusive: true,
               kind: :existing_upload
             )

    response = call("/api/orchestrator/v1/execution-packages", context.coordinator)

    assert response.status == 429
    assert response.halted
    assert %Plug.Conn.Unfetched{} = response.body_params
    assert %{"error" => %{"code" => "manifest_capacity_busy"}} = Jason.decode!(response.resp_body)

    MemoryCapacity.release(token, server: context.coordinator)
  end

  test "unknown node memory returns retryable 503 without parsing the body", context do
    Agent.update(context.provider, fn _ -> {:error, :unavailable} end)

    response = call("/api/orchestrator/v1/manifests", context.coordinator)

    assert response.status == 503
    assert response.halted
    assert %Plug.Conn.Unfetched{} = response.body_params
    assert get_resp_header(response, "retry-after") == ["5"]

    assert %{"error" => %{"code" => "memory_capacity_unknown"}} =
             Jason.decode!(response.resp_body)
  end

  defp call(path, coordinator) do
    conn(:post, path, :binary.copy("body-must-remain-unread", 1_024))
    |> put_req_header("authorization", "Bearer " <> @token)
    |> put_req_header("content-type", "application/json")
    |> ManifestMemoryAdmission.call(server: coordinator)
  end

  defp unique_name(kind),
    do: String.to_atom("#{__MODULE__}.#{kind}.#{System.unique_integer([:positive])}")

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
