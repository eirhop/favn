unless Code.ensure_loaded?(FavnRunner.ProductionRuntimeConfig) do
  defmodule FavnRunner.ProductionRuntimeConfig do
    @moduledoc false

    def validate(env) do
      case Map.get(env, "FAVN_RUNNER_MODE", "local") do
        "local" -> {:ok, %{mode: :local, topology: :single_node}}
        value -> {:error, %{error: {:invalid_env, "FAVN_RUNNER_MODE", value, "local"}}}
      end
    end
  end
end

defmodule FavnOrchestrator.ProductionRuntimeConfigTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.ProductionRuntimeConfig

  @token "orchestrator-service-token-32-char-min"
  @token_env "favn_web:#{@token}"

  test "validate/1 accepts the Phase 1 production defaults" do
    assert {:ok, config} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_STORAGE" => "sqlite",
               "FAVN_SQLITE_PATH" => "/var/lib/favn/orchestrator.sqlite3",
               "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => @token_env
             })

    assert config.storage == :sqlite
    assert config.sqlite[:database] == "/var/lib/favn/orchestrator.sqlite3"
    assert config.sqlite[:migration_mode] == :manual
    assert config.sqlite[:busy_timeout] == 5_000
    assert config.sqlite[:pool_size] == 1
    assert config.api_server == [enabled: true, host: "127.0.0.1", port: 4101]

    assert config.api_service_tokens == [
             %{
               service_identity: "favn_web",
               token_hash: ServiceTokens.hash_token(@token),
               enabled: true
             }
           ]

    assert config.scheduler == [enabled: true, tick_ms: 15_000, max_missed_all_occurrences: 1_000]
    assert config.runner == %{mode: :local, topology: :single_node}
  end

  test "validate/1 accepts explicit supported production values" do
    env = %{
      "FAVN_STORAGE" => "sqlite",
      "FAVN_SQLITE_PATH" => "/srv/favn/control.sqlite3",
      "FAVN_SQLITE_MIGRATION_MODE" => "auto",
      "FAVN_SQLITE_BUSY_TIMEOUT_MS" => "7500",
      "FAVN_SQLITE_POOL_SIZE" => "1",
      "FAVN_ORCHESTRATOR_API_BIND_HOST" => "0.0.0.0",
      "FAVN_ORCHESTRATOR_API_PORT" => "4444",
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" =>
        "favn_web:#{@token},bootstrap_cli:#{String.duplicate("bc", 17)}",
      "FAVN_RUNNER_MODE" => "local",
      "FAVN_SCHEDULER_ENABLED" => "false",
      "FAVN_SCHEDULER_TICK_MS" => "250",
      "FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES" => "2"
    }

    assert {:ok, config} = ProductionRuntimeConfig.validate(env)
    assert config.sqlite[:migration_mode] == :auto
    assert config.sqlite[:busy_timeout] == 7_500
    assert config.api_server == [enabled: true, host: "0.0.0.0", port: 4444]
    assert length(config.api_service_tokens) == 2
    assert config.scheduler == [enabled: false, tick_ms: 250, max_missed_all_occurrences: 2]
    assert config.runner == %{mode: :local, topology: :single_node}
  end

  test "validate/1 rejects missing required production config" do
    assert {:error, %{error: {:missing_env, "FAVN_STORAGE"}}} =
             ProductionRuntimeConfig.validate(%{})
  end

  test "validate/1 rejects unsupported storage and relative sqlite path" do
    assert {:error, %{error: {:invalid_env, "FAVN_STORAGE", "sqlite"}}} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_STORAGE" => "postgres",
               "FAVN_SQLITE_PATH" => "/var/lib/favn/orchestrator.sqlite3",
               "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => @token_env
             })

    assert {:error, %{error: {:invalid_env, "FAVN_SQLITE_PATH", "absolute path"}}} =
             ProductionRuntimeConfig.validate(%{
               "FAVN_STORAGE" => "sqlite",
               "FAVN_SQLITE_PATH" => "relative.sqlite3",
               "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => @token_env
             })
  end

  test "validate/1 rejects invalid Phase 1 bounds and redacts token values" do
    base = %{
      "FAVN_STORAGE" => "sqlite",
      "FAVN_SQLITE_PATH" => "/var/lib/favn/orchestrator.sqlite3",
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => @token_env
    }

    assert {:error, %{error: {:invalid_env, "FAVN_SQLITE_POOL_SIZE", 1}}} =
             base
             |> Map.put("FAVN_SQLITE_POOL_SIZE", "2")
             |> ProductionRuntimeConfig.validate()

    assert {:error, %{error: {:invalid_env, "FAVN_SCHEDULER_TICK_MS", ">= 100"}}} =
             base
             |> Map.put("FAVN_SCHEDULER_TICK_MS", "99")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{error: {:invalid_secret_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :too_short}}} =
             base
             |> Map.put("FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", "favn_web:short-secret")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{error: {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :duplicate_identity}}} =
             base
             |> Map.put(
               "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS",
               "favn_web:#{@token},favn_web:#{String.duplicate("bc", 17)}"
             )
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{error: {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :blank_identity}}} =
             base
             |> Map.put("FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", ":#{@token}")
             |> ProductionRuntimeConfig.validate()

    assert {:error, %{error: {:invalid_env, "FAVN_ORCHESTRATOR_AUTH_SESSION_TTL", ">= 1"}}} =
             base
             |> Map.put("FAVN_ORCHESTRATOR_AUTH_SESSION_TTL", "0")
             |> ProductionRuntimeConfig.validate()

    assert {:error, %{error: {:invalid_env, "FAVN_RUNNER_MODE", "local"}}} =
             base
             |> Map.put("FAVN_RUNNER_MODE", "distributed")
             |> ProductionRuntimeConfig.validate()
  end

  test "apply_from_env/1 writes orchestrator application env" do
    keys = [
      :storage_adapter,
      :storage_adapter_opts,
      :api_server,
      :api_service_tokens,
      :scheduler,
      :runner_client,
      :runner_client_opts,
      :production_runtime_diagnostics
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:favn_orchestrator, &1)})

    on_exit(fn -> Enum.each(previous, fn {key, value} -> restore_env(key, value) end) end)

    assert :ok =
             ProductionRuntimeConfig.apply_from_env(%{
               "FAVN_STORAGE" => "sqlite",
               "FAVN_SQLITE_PATH" => "/var/lib/favn/orchestrator.sqlite3",
               "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => @token_env,
               "FAVN_SCHEDULER_ENABLED" => "0"
             })

    assert Application.get_env(:favn_orchestrator, :storage_adapter) ==
             ProductionRuntimeConfig.sqlite_adapter()

    assert Application.get_env(:favn_orchestrator, :storage_adapter_opts)[:database] ==
             "/var/lib/favn/orchestrator.sqlite3"

    assert Application.get_env(:favn_orchestrator, :api_service_tokens) == [
             %{
               service_identity: "favn_web",
               token_hash: ServiceTokens.hash_token(@token),
               enabled: true
             }
           ]

    assert Application.get_env(:favn_orchestrator, :scheduler)[:enabled] == false

    assert Application.get_env(:favn_orchestrator, :runner_client) ==
             FavnOrchestrator.RunnerClient.LocalNode

    assert Application.get_env(:favn_orchestrator, :runner_client_opts) == []

    diagnostics = Application.get_env(:favn_orchestrator, :production_runtime_diagnostics)
    refute inspect(diagnostics) =~ "/var/lib/favn/orchestrator.sqlite3"
    assert diagnostics.runner == %{mode: :local, topology: :single_node}
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
