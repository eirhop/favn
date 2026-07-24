defmodule FavnView.WebReadinessTest do
  use FavnView.ConnCase, async: false

  alias FavnView.ProductionRuntimeConfig
  alias FavnView.Readiness

  @secret_key_base String.duplicate("a", 64)

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
    persistent_key = {ProductionRuntimeConfig, :config}
    previous_runtime_config = :persistent_term.get(persistent_key, :missing)
    :persistent_term.erase(persistent_key)

    keys = [
      :orchestrator_facade,
      :orchestrator_readiness_timeout_ms,
      :production_runtime_diagnostics,
      :public_origin,
      :production_runtime_config,
      :require_secure_cookies,
      :session_cookie_options
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:favn_view, &1)})

    Application.delete_env(:favn_view, :production_runtime_diagnostics)
    Application.delete_env(:favn_view, :public_origin)
    Application.delete_env(:favn_view, :require_secure_cookies)
    Application.put_env(:favn_view, :orchestrator_facade, ReadyOrchestrator)
    Application.put_env(:favn_view, :orchestrator_readiness_timeout_ms, 50)
    Application.put_env(:favn_view, :production_runtime_config, false)

    on_exit(fn ->
      Enum.each(previous, fn {key, value} -> restore_env(key, value) end)

      case previous_runtime_config do
        :missing -> :persistent_term.erase(persistent_key)
        config -> :persistent_term.put(persistent_key, config)
      end
    end)

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
               "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base,
               "FAVN_VIEW_TRUSTED_PROXY_CIDRS" => "10.0.0.0/8,127.0.0.1/32",
               "FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS" => "250"
             })

    assert config.public_origin == "https://favn.example.com"
    assert config.orchestrator_readiness_timeout_ms == 250
    assert config.bind_host == "0.0.0.0"
    assert config.port == 4_000
    assert length(config.trusted_proxy_cidrs) == 2

    assert {:error, %{error: {:invalid_env, "FAVN_VIEW_PUBLIC_ORIGIN", _expected}}} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "http://favn.example.com",
               "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base
             })

    assert {:error, %{error: {:invalid_env, "FAVN_VIEW_PUBLIC_ORIGIN", _expected}}} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "http://localhost:4173",
               "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base,
               "FAVN_VIEW_TRUSTED_PROXY_CIDRS" => "127.0.0.1/32",
               "FAVN_UNSAFE_ALLOW_HTTP_LOCALHOST" => "true"
             })

    assert {:error, %{error: {:invalid_env, "FAVN_VIEW_PUBLIC_ORIGIN", _expected}}} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "not a url",
               "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base
             })

    for origin <- [
          "https://favn.example.com/path",
          "https://favn.example.com?debug=true",
          "https://favn.example.com#frag"
        ] do
      assert {:error, %{error: {:invalid_env, "FAVN_VIEW_PUBLIC_ORIGIN", _expected}}} =
               ProductionRuntimeConfig.validate(%{
                 "FAVN_VIEW_PUBLIC_ORIGIN" => origin,
                 "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base
               })
    end

    assert {:error, %{error: {:missing_env, "FAVN_VIEW_SECRET_KEY_BASE"}}} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com"
             })

    assert {:error, %{error: {:invalid_secret_env, "FAVN_VIEW_SECRET_KEY_BASE", _}}} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
               "FAVN_VIEW_SECRET_KEY_BASE" => "too-short"
             })

    assert {:error, %{error: {:invalid_env, "FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS", _}}} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
               "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base,
               "FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS" => "0"
             })
  end

  test "production runtime config wires public origin into endpoint config" do
    previous_endpoint = Application.get_env(:favn_view, FavnView.Endpoint)
    on_exit(fn -> Application.put_env(:favn_view, FavnView.Endpoint, previous_endpoint) end)

    assert :ok =
             ProductionRuntimeConfig.apply_from_env(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
               "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base,
               "FAVN_VIEW_TRUSTED_PROXY_CIDRS" => "10.0.0.0/8",
               "FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS" => "250"
             })

    endpoint_config = Application.get_env(:favn_view, FavnView.Endpoint)
    assert Keyword.fetch!(endpoint_config, :url)[:scheme] == "https"
    assert Keyword.fetch!(endpoint_config, :url)[:host] == "favn.example.com"
    assert Keyword.fetch!(endpoint_config, :url)[:port] == 443
    assert Keyword.fetch!(endpoint_config, :check_origin) == ["https://favn.example.com"]
    assert Keyword.fetch!(endpoint_config, :secret_key_base) == @secret_key_base
    assert Keyword.fetch!(endpoint_config, :server)
    assert Keyword.fetch!(endpoint_config, :http)[:ip] == {0, 0, 0, 0}
    assert Keyword.fetch!(endpoint_config, :http)[:port] == 4_000

    Application.put_env(:favn_view, :orchestrator_readiness_timeout_ms, 999)
    assert ProductionRuntimeConfig.configured_timeout_ms() == 250

    frozen = :persistent_term.get({ProductionRuntimeConfig, :config})
    refute Map.has_key?(frozen, :secret_key_base)
    refute inspect(frozen) =~ @secret_key_base
  end

  test "production runtime config requires hardened session cookie options when enabled" do
    secure_options =
      FavnView.Endpoint.session_options()
      |> Keyword.put(:secure, true)
      |> Keyword.put(:http_only, true)
      |> Keyword.put(:same_site, "Lax")
      |> Keyword.put(:encryption_salt, "test-encryption-salt")

    Application.put_env(:favn_view, :require_secure_cookies, true)
    Application.put_env(:favn_view, :session_cookie_options, secure_options)

    assert {:ok, _config} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
               "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base,
               "FAVN_VIEW_TRUSTED_PROXY_CIDRS" => "10.0.0.0/8"
             })

    Application.put_env(
      :favn_view,
      :session_cookie_options,
      Keyword.put(secure_options, :secure, false)
    )

    assert {:error, %{error: {:invalid_session_cookie, :secure_required}}} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
               "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base,
               "FAVN_VIEW_TRUSTED_PROXY_CIDRS" => "10.0.0.0/8"
             })

    Application.put_env(
      :favn_view,
      :session_cookie_options,
      Keyword.delete(secure_options, :encryption_salt)
    )

    assert {:error, %{error: {:invalid_session_cookie, :encryption_salt_required}}} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
               "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base,
               "FAVN_VIEW_TRUSTED_PROXY_CIDRS" => "10.0.0.0/8"
             })
  end

  test "production runtime config rejects public proxy networks and bounds HTTP settings" do
    base = %{
      "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
      "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base,
      "FAVN_VIEW_TRUSTED_PROXY_CIDRS" => "10.0.0.0/8"
    }

    assert {:error, %{error: {:invalid_env, "FAVN_VIEW_TRUSTED_PROXY_CIDRS", _expected}}} =
             base
             |> Map.put("FAVN_VIEW_TRUSTED_PROXY_CIDRS", "203.0.113.0/24")
             |> ProductionRuntimeConfig.validate()

    assert {:error, %{error: {:invalid_env, "FAVN_HTTP_MAX_CONNECTIONS", "1..100000"}}} =
             base
             |> Map.put("FAVN_HTTP_MAX_CONNECTIONS", "100001")
             |> ProductionRuntimeConfig.validate()

    assert {:ok, config} = ProductionRuntimeConfig.validate(base)
    :ok = ProductionRuntimeConfig.apply(config)
    assert ProductionRuntimeConfig.trusted_proxy?({10, 20, 30, 40})
    refute ProductionRuntimeConfig.trusted_proxy?({192, 168, 1, 1})
  end

  test "production config files do not commit a production secret key base" do
    prod_config = File.read!(Path.expand("../../../../config/prod.exs", __DIR__))

    refute prod_config =~ "secret_key_base"
    assert File.read!(Path.expand("../../../../config/dev.exs", __DIR__)) =~ "secret_key_base"
    assert File.read!(Path.expand("../../../../config/test.exs", __DIR__)) =~ "secret_key_base"
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
