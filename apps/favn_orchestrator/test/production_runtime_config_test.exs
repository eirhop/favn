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

  @token "alpha-credential-value-1234567890abcd"
  @token_env "favn_web:#{@token}"
  @pin_key :binary.copy(<<7>>, 32) |> Base.encode64()
  @old_pin_key :binary.copy(<<6>>, 32) |> Base.encode64()

  setup do
    ca_file =
      Path.join(System.tmp_dir!(), "favn-runtime-config-ca-#{System.unique_integer()}.pem")

    File.write!(ca_file, "test-ca")
    on_exit(fn -> File.rm(ca_file) end)
    %{ca_file: ca_file}
  end

  test "validate/1 accepts the PostgreSQL production defaults", %{ca_file: ca_file} do
    assert {:ok, config} = ProductionRuntimeConfig.validate(base_env(ca_file))

    assert config.storage == :postgres
    assert config.postgres[:url] == "ecto://favn:secret@postgres.example/favn"
    assert config.postgres[:ssl_mode] == :verify_full
    assert config.postgres[:ssl_ca_file] == ca_file
    assert config.postgres[:pool_size] == 15

    assert config.runtime_input_pin == %{
             keys: %{1 => :binary.copy(<<7>>, 32)},
             current_version: 1
           }

    assert config.api_server == [enabled: true, host: "127.0.0.1", port: 4101]
    assert config.active_run_plan_max_bytes == 512 * 1_024 * 1_024

    assert config.api_service_tokens == [
             %{
               service_identity: "favn_web",
               token_hash: ServiceTokens.hash_token(@token),
               enabled: true,
               platform_roles: []
             }
           ]

    assert config.scheduler == [
             enabled: true,
             workspace_ids: ["salmon-one", "salmon-two"],
             tick_ms: 15_000,
             max_missed_all_occurrences: 1_000
           ]

    assert config.runner == %{mode: :local, topology: :single_node}
  end

  test "validate/1 accepts explicit supported production values", %{ca_file: ca_file} do
    env =
      ca_file
      |> base_env()
      |> Map.merge(%{
        "FAVN_STORAGE" => "postgresql",
        "FAVN_DATABASE_POOL_SIZE" => "32",
        "FAVN_DATABASE_QUEUE_TARGET_MS" => "75",
        "FAVN_DATABASE_QUEUE_INTERVAL_MS" => "1500",
        "FAVN_DATABASE_TIMEOUT_MS" => "30000",
        "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION" => "4",
        "FAVN_RUNTIME_INPUT_PIN_KEYS" => Jason.encode!(%{"1" => @old_pin_key, "4" => @pin_key}),
        "FAVN_ORCHESTRATOR_API_BIND_HOST" => "0.0.0.0",
        "FAVN_ORCHESTRATOR_API_PORT" => "4444",
        "FAVN_ORCHESTRATOR_ACTIVE_RUN_PLAN_MAX_BYTES" => "1073741824",
        "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" =>
          "favn_web:#{@token},bootstrap_cli:#{@token <> "-bravo"}",
        "FAVN_SCHEDULER_ENABLED" => "false",
        "FAVN_SCHEDULER_TICK_MS" => "250",
        "FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES" => "2"
      })

    assert {:ok, config} = ProductionRuntimeConfig.validate(env)
    assert config.postgres[:pool_size] == 32
    assert config.postgres[:queue_target] == 75
    assert config.postgres[:queue_interval] == 1_500
    assert config.postgres[:timeout] == 30_000
    assert config.runtime_input_pin.current_version == 4
    assert config.runtime_input_pin.keys |> Map.keys() |> Enum.sort() == [1, 4]
    assert config.api_server == [enabled: true, host: "0.0.0.0", port: 4444]
    assert config.active_run_plan_max_bytes == 1_073_741_824
    assert length(config.api_service_tokens) == 2

    assert config.scheduler == [
             enabled: false,
             workspace_ids: [],
             tick_ms: 250,
             max_missed_all_occurrences: 2
           ]
  end

  test "validate/1 rejects missing and unsupported storage", %{ca_file: ca_file} do
    assert {:error, %{error: {:missing_env, "FAVN_STORAGE"}}} =
             ProductionRuntimeConfig.validate(%{})

    assert {:error, %{error: {:invalid_env, "FAVN_STORAGE", "postgres"}}} =
             ca_file
             |> base_env()
             |> Map.put("FAVN_STORAGE", "sqlite")
             |> ProductionRuntimeConfig.validate()
  end

  test "validate/1 rejects unsafe PostgreSQL configuration", %{ca_file: ca_file} do
    base = base_env(ca_file)

    assert {:error, %{error: {:invalid_env, "FAVN_DATABASE_URL", "PostgreSQL connection URL"}}} =
             base
             |> Map.put("FAVN_DATABASE_URL", "sqlite:///tmp/favn.db")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{error: {:invalid_env, "FAVN_DATABASE_SSL_CA_FILE", "absolute readable file"}}} =
             base
             |> Map.put("FAVN_DATABASE_SSL_CA_FILE", "/missing/ca.pem")
             |> ProductionRuntimeConfig.validate()

    assert {:error, %{error: {:invalid_env, "FAVN_DATABASE_POOL_SIZE", "1..200"}}} =
             base
             |> Map.put("FAVN_DATABASE_POOL_SIZE", "201")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{
              error:
                {:invalid_env, "FAVN_ORCHESTRATOR_ACTIVE_RUN_PLAN_MAX_BYTES",
                 "67108864..8589934592"}
            }} =
             base
             |> Map.put("FAVN_ORCHESTRATOR_ACTIVE_RUN_PLAN_MAX_BYTES", "67108863")
             |> ProductionRuntimeConfig.validate()

    assert {:error, %{error: {:invalid_secret_env, "FAVN_RUNTIME_INPUT_PIN_KEYS", :invalid_key}}} =
             base
             |> Map.put("FAVN_RUNTIME_INPUT_PIN_KEYS", Jason.encode!(%{"1" => "not-a-key"}))
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{
              error:
                {:invalid_env, "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION",
                 "version present in FAVN_RUNTIME_INPUT_PIN_KEYS"}
            }} =
             base
             |> Map.put("FAVN_RUNTIME_INPUT_PIN_KEY_VERSION", "2")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{
              error:
                {:invalid_env, "FAVN_DATABASE_SSL_MODE",
                 "verify-full or explicit loopback plaintext interlock"}
            }} =
             base
             |> Map.put("FAVN_DATABASE_SSL_MODE", "disable")
             |> Map.delete("FAVN_DATABASE_SSL_CA_FILE")
             |> ProductionRuntimeConfig.validate()
  end

  test "validate/1 permits plaintext only for loopback with the explicit interlock", %{
    ca_file: ca_file
  } do
    assert {:error,
            %{
              error:
                {:invalid_env, "FAVN_DATABASE_SSL_MODE",
                 "verify-full or explicit loopback plaintext interlock"}
            }} =
             ca_file
             |> base_env()
             |> Map.delete("FAVN_DATABASE_SSL_CA_FILE")
             |> Map.put("FAVN_DATABASE_SSL_MODE", "disable")
             |> Map.put("FAVN_UNSAFE_ALLOW_PLAINTEXT_DATABASE", "true")
             |> ProductionRuntimeConfig.validate()

    env =
      ca_file
      |> base_env()
      |> Map.delete("FAVN_DATABASE_SSL_CA_FILE")
      |> Map.put("FAVN_DATABASE_URL", "ecto://favn:secret@127.0.0.1/favn")
      |> Map.put("FAVN_DATABASE_SSL_MODE", "disable")
      |> Map.put("FAVN_UNSAFE_ALLOW_PLAINTEXT_DATABASE", "true")

    assert {:ok, config} = ProductionRuntimeConfig.validate(env)
    assert config.postgres[:ssl_mode] == :disable
    assert config.postgres[:allow_insecure_database?]
    refute Keyword.has_key?(config.postgres, :ssl_ca_file)
  end

  test "validate/1 rejects invalid service, auth, scheduler, and runner values", %{
    ca_file: ca_file
  } do
    base = base_env(ca_file)

    assert {:error, %{error: {:invalid_env, "FAVN_SCHEDULER_TICK_MS", "100..86400000"}}} =
             base
             |> Map.put("FAVN_SCHEDULER_TICK_MS", "99")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{error: {:invalid_secret_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :too_short}}} =
             base
             |> Map.put("FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", "favn_web:short-secret")
             |> ProductionRuntimeConfig.validate()

    assert {:error, %{error: {:invalid_env, "FAVN_ORCHESTRATOR_AUTH_SESSION_TTL", "1..2592000"}}} =
             base
             |> Map.put("FAVN_ORCHESTRATOR_AUTH_SESSION_TTL", "0")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{
              error:
                {:invalid_env, "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD", "15..1024 byte password"}
            }} =
             base
             |> Map.put("FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD", "short")
             |> ProductionRuntimeConfig.validate()

    assert {:error, %{error: {:invalid_env, "FAVN_RUNNER_MODE", "local"}}} =
             base
             |> Map.put("FAVN_RUNNER_MODE", "distributed")
             |> ProductionRuntimeConfig.validate()
  end

  test "apply_from_env/1 freezes redacted PostgreSQL composition", %{ca_file: ca_file} do
    orchestrator_keys = [
      :persistence_backend,
      :persistence_options,
      :api_server,
      :api_service_tokens,
      :api_service_tokens_env,
      :workspace_ids,
      :auth_session_ttl_seconds,
      :active_run_plan_max_bytes,
      :scheduler,
      :runner_client,
      :runner_client_opts,
      :production_runtime_diagnostics,
      :auth_bootstrap_username,
      :auth_bootstrap_password,
      :auth_bootstrap_display_name,
      :auth_bootstrap_roles,
      :local_dev_mode
    ]

    postgres_keys = [:runtime_input_pin_keys, :runtime_input_pin_current_key_version]
    previous_orchestrator = snapshot_env(:favn_orchestrator, orchestrator_keys)
    previous_postgres = snapshot_env(:favn_storage_postgres, postgres_keys)

    on_exit(fn ->
      restore_env(:favn_orchestrator, previous_orchestrator)
      restore_env(:favn_storage_postgres, previous_postgres)
    end)

    assert :ok =
             ca_file
             |> base_env()
             |> Map.put(
               "FAVN_RUNTIME_INPUT_PIN_KEYS",
               Jason.encode!(%{"1" => @old_pin_key, "2" => @pin_key})
             )
             |> Map.put("FAVN_RUNTIME_INPUT_PIN_KEY_VERSION", "2")
             |> Map.put("FAVN_SCHEDULER_ENABLED", "0")
             |> ProductionRuntimeConfig.apply_from_env()

    assert Application.get_env(:favn_orchestrator, :persistence_backend) ==
             ProductionRuntimeConfig.postgres_backend()

    options = Application.get_env(:favn_orchestrator, :persistence_options)
    assert options[:url] == "ecto://favn:secret@postgres.example/favn"

    assert Application.get_env(:favn_storage_postgres, :runtime_input_pin_keys) == %{
             1 => :binary.copy(<<6>>, 32),
             2 => :binary.copy(<<7>>, 32)
           }

    assert Application.get_env(
             :favn_storage_postgres,
             :runtime_input_pin_current_key_version
           ) == 2

    assert Application.get_env(:favn_orchestrator, :scheduler)[:enabled] == false
    assert Application.get_env(:favn_orchestrator, :workspace_ids) == ["salmon-one", "salmon-two"]
    assert Application.get_env(:favn_orchestrator, :auth_bootstrap_username) == "admin"
    assert Application.get_env(:favn_orchestrator, :auth_bootstrap_roles) == [:admin]
    assert Application.get_env(:favn_orchestrator, :local_dev_mode) == false

    assert Application.get_env(:favn_orchestrator, :active_run_plan_max_bytes) ==
             512 * 1_024 * 1_024

    diagnostics = Application.get_env(:favn_orchestrator, :production_runtime_diagnostics)
    refute inspect(diagnostics) =~ "secret"
    refute inspect(diagnostics) =~ ca_file
    refute inspect(diagnostics) =~ @pin_key
    refute inspect(diagnostics) =~ @old_pin_key
    assert diagnostics.runtime_input_pin == %{current_version: 2, retained_versions: [1, 2]}
    assert diagnostics.active_run_plan == %{max_bytes: 512 * 1_024 * 1_024}
    assert diagnostics.runner == %{mode: :local, topology: :single_node}
  end

  defp base_env(ca_file) do
    %{
      "FAVN_STORAGE" => "postgres",
      "FAVN_DATABASE_URL" => "ecto://favn:secret@postgres.example/favn",
      "FAVN_DATABASE_SSL_CA_FILE" => ca_file,
      "FAVN_RUNTIME_INPUT_PIN_KEYS" => Jason.encode!(%{"1" => @pin_key}),
      "FAVN_WORKSPACE_IDS" => "salmon-one,salmon-two",
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => @token_env,
      "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME" => "admin",
      "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD" => "admin-password-long"
    }
  end

  defp snapshot_env(app, keys), do: Map.new(keys, &{&1, Application.get_env(app, &1)})

  defp restore_env(app, values) do
    Enum.each(values, fn
      {key, nil} -> Application.delete_env(app, key)
      {key, value} -> Application.put_env(app, key, value)
    end)
  end
end
