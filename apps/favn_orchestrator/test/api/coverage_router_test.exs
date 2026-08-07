defmodule FavnOrchestrator.API.CoverageRouterTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias FavnOrchestrator.API.CoverageRouter
  alias FavnOrchestrator.Auth.ServiceTokens

  @token "coverage-router-test-token-with-32-bytes"

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "coverage_router_test",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true,
        platform_roles: []
      ]
    ])

    on_exit(fn -> restore_env(:api_service_tokens, previous_tokens) end)
    :ok
  end

  test "authenticates actor context before validating missing-window pagination" do
    response = request(:get, "/assets/asset-a/missing?limit=501")

    assert response.status == 401
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "unauthenticated"
  end

  test "authenticates actor context before validating a missing-window submission" do
    response = request(:post, "/assets/asset-a/backfill", %{})

    assert response.status == 401
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "unauthenticated"
  end

  test "maps bounded coverage overflow to a stable conflict" do
    assert {409, "coverage_window_limit_exceeded", _message, %{}} =
             CoverageRouter.error_response(:coverage_window_limit_exceeded)
  end

  test "maps target admission failures to stable conflicts" do
    details = %{
      target_id: "asset-a",
      selected_target_id: "asset-b",
      blocked_path: ["asset-a", "asset-b"],
      blocked_path_target_count: 2,
      blocked_path_truncated: false,
      compatibility_status: :rebuild_required,
      reason_code: "contract_changed"
    }

    assert {409, "rebuild_required", _message, ^details} =
             CoverageRouter.error_response({:rebuild_required, details})
  end

  defp request(method, path, params \\ nil) do
    method
    |> conn(path, "")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> fetch_query_params()
    |> maybe_put_params(params)
    |> CoverageRouter.call(CoverageRouter.init([]))
  end

  defp maybe_put_params(conn, nil), do: conn
  defp maybe_put_params(conn, params), do: Map.put(conn, :body_params, params)

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
