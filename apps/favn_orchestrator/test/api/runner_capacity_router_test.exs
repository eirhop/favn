defmodule FavnOrchestrator.API.RunnerCapacityRouterTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias FavnOrchestrator.API.Router
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Persistence.Results.RunnerCapacityDemand
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RunnerDemandLimiter
  alias FavnOrchestrator.RunnerRegistry

  @capacity_token "capacity-reader-credential-that-is-long-enough"
  @operator_token "platform-operator-token-that-is-long-enough"
  @release "rr_" <> String.duplicate("a", 64)

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)
    previous_pools = Application.get_env(:favn_orchestrator, :runner_pools)
    previous_demand = Application.get_env(:favn_orchestrator, :test_runner_capacity_demand)

    {:ok, capacity_config} =
      ServiceTokens.from_raw_token(
        "capacity-scaler",
        [:capacity_reader],
        @capacity_token,
        "FAVN_ORCHESTRATOR_CAPACITY_READER_TOKEN"
      )

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      capacity_config,
      [
        service_identity: "operator",
        token_hash: ServiceTokens.hash_token(@operator_token),
        enabled: true,
        platform_roles: [:platform_reader, :platform_operator]
      ]
    ])

    Application.put_env(:favn_orchestrator, :runner_pools, duckdb: [mode: :elastic])

    Application.put_env(
      :favn_orchestrator,
      :test_runner_capacity_demand,
      %RunnerCapacityDemand{
        runner_pool: "duckdb",
        required_runner_release_id: @release,
        outstanding_count: 7,
        queued_count: 5,
        active_count: 2,
        version: 1,
        updated_at: DateTime.utc_now(),
        healthy?: true
      }
    )

    start_supervised!({RunnerDemandLimiter, []})
    start_supervised!({RunnerRegistry, []})

    stores =
      Stores.__struct__()
      |> Map.from_struct()
      |> Map.keys()
      |> Map.new(&{&1, FavnOrchestrator.TestRunnerTaskStore})
      |> then(&struct!(Stores, &1))

    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})

    on_exit(fn ->
      restore(:api_service_tokens, previous_tokens)
      restore(:runner_pools, previous_pools)
      restore(:test_runner_capacity_demand, previous_demand)
    end)

    :ok
  end

  test "returns one exact no-store JSON demand value to capacity readers" do
    response = request("/internal/runner-demand/duckdb/#{@release}", @capacity_token)

    assert response.status == 200
    assert Jason.decode!(response.resp_body) == %{"outstanding" => 7}
    assert get_resp_header(response, "cache-control") == ["no-store"]
  end

  test "exports the same exact value as a bounded OpenMetrics gauge" do
    response =
      request("/internal/runner-demand/duckdb/#{@release}/metrics", @capacity_token)

    assert response.status == 200
    assert response.resp_body =~ "# TYPE favn_runner_outstanding gauge"
    assert response.resp_body =~ ~s(runner_pool="duckdb",runner_release_id="#{@release}")
    assert response.resp_body =~ "} 7\n"
  end

  test "does not grant capacity reads to general platform operators" do
    response = request("/internal/runner-demand/duckdb/#{@release}", @operator_token)
    assert response.status == 403
  end

  test "does not grant platform-reader API access to capacity credentials" do
    response = request("/api/orchestrator/v1/runner-capacity", @capacity_token)
    assert response.status == 403
  end

  test "does not grant platform-operator API access to capacity credentials" do
    response =
      :get
      |> conn("/api/orchestrator/v1/bootstrap/active-manifest")
      |> put_req_header("authorization", "Bearer #{@capacity_token}")
      |> put_req_header("x-favn-workspace-id", "workspace-a")
      |> Router.call(Router.init([]))

    assert response.status == 403
    assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) == "forbidden"
  end

  test "unknown release probes do not create unbounded telemetry metadata" do
    handler_id = "runner-demand-test-#{System.unique_integer([:positive])}"
    owner = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn, :orchestrator, :runner_demand_read],
        fn event, measurements, metadata, _config ->
          send(owner, {:runner_demand_telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    unknown_release = "rr_" <> String.duplicate("b", 64)
    response = request("/internal/runner-demand/duckdb/#{unknown_release}", @capacity_token)

    assert response.status == 503

    assert_receive {:runner_demand_telemetry, _event, _measurements, metadata}
    assert metadata.partition_status == :unavailable
    assert metadata.outcome == :error
    refute Map.has_key?(metadata, :runner_pool)
    refute Map.has_key?(metadata, :runner_release_id)
    refute inspect(metadata) =~ unknown_release
  end

  test "authenticated demand supports a zero to N to zero ScaledJob cycle" do
    put_outstanding(0)
    assert desired_new_jobs(read_outstanding(), 0, 10) == 0

    put_outstanding(3)
    assert desired_new_jobs(read_outstanding(), 0, 10) == 3

    # Favn demand includes assigned work. KEDA's default ScaledJob strategy
    # subtracts these already-running job executions.
    assert desired_new_jobs(read_outstanding(), 3, 10) == 0

    put_outstanding(0)
    assert desired_new_jobs(read_outstanding(), 0, 10) == 0
  end

  test "operator diagnostics expose bounded listing metadata and exact durable drain" do
    listing = request("/api/orchestrator/v1/runner-capacity", @operator_token)
    assert listing.status == 200

    assert %{
             "data" => %{
               "partition_limit" => 256,
               "truncated" => false,
               "partitions" => [%{"required_runner_release_id" => @release}]
             }
           } = Jason.decode!(listing.resp_body)

    exact =
      request(
        "/api/orchestrator/v1/runner-capacity/duckdb/#{@release}",
        @operator_token
      )

    assert exact.status == 200

    assert %{
             "data" => %{
               "required_runner_release_id" => @release,
               "durable_blockers" => 7,
               "drained" => false
             }
           } = Jason.decode!(exact.resp_body)
  end

  defp request(path, token) do
    :get
    |> conn(path)
    |> put_req_header("authorization", "Bearer #{token}")
    |> Router.call(Router.init([]))
  end

  defp put_outstanding(count) do
    demand =
      :favn_orchestrator
      |> Application.fetch_env!(:test_runner_capacity_demand)
      |> Map.put(:outstanding_count, count)
      |> Map.put(:queued_count, count)
      |> Map.put(:active_count, 0)

    Application.put_env(:favn_orchestrator, :test_runner_capacity_demand, demand)
  end

  defp read_outstanding do
    response = request("/internal/runner-demand/duckdb/#{@release}", @capacity_token)
    assert response.status == 200
    response.resp_body |> Jason.decode!() |> Map.fetch!("outstanding")
  end

  defp desired_new_jobs(outstanding, active_jobs, max_jobs),
    do: max(min(outstanding, max_jobs) - active_jobs, 0)

  defp restore(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
