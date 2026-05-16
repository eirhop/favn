defmodule FavnView.WebReadinessTest do
  use FavnView.ConnCase, async: false

  alias FavnView.ProductionRuntimeConfig
  alias FavnView.Readiness

  defmodule ReadyOrchestrator do
    def readiness do
      %{
        status: :ready,
        checks: [
          %{name: :api, status: :ok},
          %{name: :storage, status: :ok},
          %{name: :scheduler, status: :ok},
          %{name: :runner, status: :ok}
        ]
      }
    end
  end

  defmodule NotReadyOrchestrator do
    def readiness do
      %{
        status: :not_ready,
        checks: [
          %{name: :storage, status: :error, error: %{token: "secret-token-value"}}
        ]
      }
    end
  end

  defmodule SlowOrchestrator do
    def readiness do
      Process.sleep(100)
      %{status: :ready, checks: []}
    end
  end

  defmodule RaisingOrchestrator do
    def readiness, do: raise("boom token=secret")
  end

  defmodule ExitingOrchestrator do
    def readiness, do: exit({:boom, token: "secret"})
  end

  setup do
    keys = [
      :orchestrator_facade,
      :orchestrator_readiness_timeout_ms,
      :production_runtime_diagnostics,
      :public_origin,
      :production_runtime_config
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:favn_view, &1)})

    Application.delete_env(:favn_view, :production_runtime_diagnostics)
    Application.delete_env(:favn_view, :public_origin)
    Application.put_env(:favn_view, :orchestrator_facade, ReadyOrchestrator)
    Application.put_env(:favn_view, :orchestrator_readiness_timeout_ms, 50)
    Application.put_env(:favn_view, :production_runtime_config, false)

    on_exit(fn -> Enum.each(previous, fn {key, value} -> restore_env(key, value) end) end)

    :ok
  end

  test "web liveness is process-only", %{conn: conn} do
    response = get(conn, ~p"/api/web/v1/health/live")

    assert %{"data" => %{"status" => "ok", "checks" => [%{"name" => "process"}]}} =
             json_response(response, 200)
  end

  test "web readiness succeeds through the same-BEAM orchestrator facade", %{conn: conn} do
    response = get(conn, ~p"/api/web/v1/health/ready")

    assert %{"data" => %{"status" => "ready", "checks" => checks}} = json_response(response, 200)
    assert Enum.any?(checks, &(&1["name"] == "web_config" and &1["status"] == "ok"))
    assert Enum.any?(checks, &(&1["name"] == "orchestrator" and &1["status"] == "ok"))
  end

  test "web readiness reports invalid web config", %{conn: conn} do
    Application.put_env(:favn_view, :production_runtime_diagnostics, %{
      status: :invalid,
      error: {:missing_env, "FAVN_VIEW_PUBLIC_ORIGIN"}
    })

    response = get(conn, ~p"/api/web/v1/health/ready")

    assert %{"data" => %{"status" => "not_ready", "checks" => checks}} =
             json_response(response, 503)

    assert Enum.any?(checks, fn check ->
             check["name"] == "web_config" and check["status"] == "error" and
               inspect(check) =~ "FAVN_VIEW_PUBLIC_ORIGIN"
           end)
  end

  test "web readiness reports not-ready orchestrator without leaking upstream secrets", %{
    conn: conn
  } do
    Application.put_env(:favn_view, :orchestrator_facade, NotReadyOrchestrator)

    response = get(conn, ~p"/api/web/v1/health/ready")

    assert %{"data" => %{"status" => "not_ready", "checks" => checks}} =
             json_response(response, 503)

    assert Enum.any?(checks, &(&1["name"] == "orchestrator" and &1["status"] == "error"))
    refute response.resp_body =~ "secret-token-value"
  end

  test "web readiness reports same-BEAM orchestrator timeout", %{conn: conn} do
    Application.put_env(:favn_view, :orchestrator_facade, SlowOrchestrator)
    Application.put_env(:favn_view, :orchestrator_readiness_timeout_ms, 1)

    response = get(conn, ~p"/api/web/v1/health/ready")

    assert %{"data" => %{"status" => "not_ready", "checks" => checks}} =
             json_response(response, 503)

    assert Enum.any?(checks, fn check ->
             check["name"] == "orchestrator" and get_in(check, ["error", "kind"]) == "timeout"
           end)
  end

  test "web readiness reports raised orchestrator checks without crashing request", %{conn: conn} do
    Application.put_env(:favn_view, :orchestrator_facade, RaisingOrchestrator)

    response = get(conn, ~p"/api/web/v1/health/ready")

    assert %{"data" => %{"status" => "not_ready", "checks" => checks}} =
             json_response(response, 503)

    assert Enum.any?(checks, fn check ->
             check["name"] == "orchestrator" and check["status"] == "error"
           end)

    refute response.resp_body =~ "token=secret"
  end

  test "web readiness reports exited orchestrator checks without crashing request", %{conn: conn} do
    Application.put_env(:favn_view, :orchestrator_facade, ExitingOrchestrator)

    response = get(conn, ~p"/api/web/v1/health/ready")

    assert %{"data" => %{"status" => "not_ready", "checks" => checks}} =
             json_response(response, 503)

    assert Enum.any?(checks, fn check ->
             check["name"] == "orchestrator" and check["status"] == "error"
           end)

    refute response.resp_body =~ "secret"
  end

  test "production runtime config validates public origin and timeout" do
    assert {:ok, config} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
               "FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS" => "250"
             })

    assert config.public_origin == "https://favn.example.com"
    assert config.orchestrator_readiness_timeout_ms == 250

    assert {:error, %{error: {:invalid_env, "FAVN_VIEW_PUBLIC_ORIGIN", _expected}}} =
             ProductionRuntimeConfig.validate(%{"FAVN_VIEW_PUBLIC_ORIGIN" => "not a url"})

    assert {:error, %{error: {:invalid_env, "FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS", _}}} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
               "FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS" => "0"
             })
  end

  test "production runtime config wires public origin into endpoint config" do
    previous_endpoint = Application.get_env(:favn_view, FavnView.Endpoint)
    on_exit(fn -> Application.put_env(:favn_view, FavnView.Endpoint, previous_endpoint) end)

    assert :ok =
             ProductionRuntimeConfig.apply_from_env(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
               "FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS" => "250"
             })

    endpoint_config = Application.get_env(:favn_view, FavnView.Endpoint)
    assert Keyword.fetch!(endpoint_config, :url)[:scheme] == "https"
    assert Keyword.fetch!(endpoint_config, :url)[:host] == "favn.example.com"
    assert Keyword.fetch!(endpoint_config, :url)[:port] == 443
    assert Keyword.fetch!(endpoint_config, :check_origin) == ["https://favn.example.com"]
  end

  test "web readiness code uses only the public orchestrator facade" do
    files = [
      Path.expand("../../lib/favn_view/readiness.ex", __DIR__),
      Path.expand("../../lib/favn_view/controllers/health_controller.ex", __DIR__)
    ]

    for file <- files do
      source = File.read!(file)

      refute source =~ "FavnOrchestrator.Readiness"
      refute source =~ "FavnOrchestrator.Storage"
      refute source =~ "Scheduler.Runtime"
      refute source =~ "RunnerClient"
      refute source =~ "/api/orchestrator"
      refute source =~ "service_token"
    end
  end

  test "readiness can be called directly with an injected orchestrator facade" do
    assert %{status: :ready} = Readiness.readiness(orchestrator: ReadyOrchestrator)
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_view, key)
  defp restore_env(key, value), do: Application.put_env(:favn_view, key, value)
end
