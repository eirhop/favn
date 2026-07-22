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

    assert config.instance_id == "control@control-plane.internal"
    assert config.postgres[:url] == "ecto://favn:secret@postgres.example/favn"
    assert config.postgres[:ssl_mode] == :verify_full
    assert config.postgres[:ssl_ca_file] == ca_file
    assert config.postgres[:pool_size] == 15

    assert config.runtime_input_pin == %{
             keys: %{1 => :binary.copy(<<7>>, 32)},
             current_version: 1
           }

    assert config.api_server == [enabled: true, host: "0.0.0.0", port: 4101]

    assert config.http_server == %{
             max_connections: 1_024,
             request_timeout_ms: 30_000,
             idle_timeout_ms: 60_000,
             body_limit_bytes: 1_048_576
           }

    assert config.shutdown_drain_timeout_ms == 120_000
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

    assert config.runner == %{
             topology: :beam_node,
             control_plane_node: "control@control-plane.internal",
             runner_node: "runner@runner.internal",
             distribution_port: 9_100,
             epmd_port: 4_369,
             cookie_configured?: true
           }

    assert config.runner_client == FavnOrchestrator.RunnerClient.BeamNode

    assert config.runner_client_opts == [
             runner_node: "runner@runner.internal",
             runner_module: Module.concat(["FavnRunner"]),
             runner_rpc_timeout_ms: 15_000,
             runner_diagnostics_timeout_ms: 5_000,
             runner_await_timeout_buffer_ms: 2_000
           ]
  end

  test "validate/1 accepts explicit supported production values", %{ca_file: ca_file} do
    env =
      ca_file
      |> base_env()
      |> Map.merge(%{
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
        "FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES" => "2",
        "FAVN_RUNNER_RPC_TIMEOUT_MS" => "30000",
        "FAVN_RUNNER_DIAGNOSTICS_TIMEOUT_MS" => "3000",
        "FAVN_RUNNER_AWAIT_TIMEOUT_BUFFER_MS" => "500",
        "ERL_EPMD_PORT" => "44369"
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

    assert config.runner.epmd_port == 44_369
    assert config.runner_client_opts[:runner_rpc_timeout_ms] == 30_000
    assert config.runner_client_opts[:runner_diagnostics_timeout_ms] == 3_000
    assert config.runner_client_opts[:runner_await_timeout_buffer_ms] == 500
  end

  test "local-development mode permits only the generated private PostgreSQL endpoint", %{
    ca_file: ca_file
  } do
    env =
      ca_file
      |> base_env()
      |> Map.drop(["FAVN_DATABASE_SSL_CA_FILE"])
      |> Map.merge(%{
        "FAVN_DEPLOYMENT_MODE" => "local-development",
        "FAVN_DATABASE_URL" =>
          "ecto://favn_runtime:local-password@postgres.favn.internal:5432/favn_dev",
        "FAVN_DATABASE_SSL_MODE" => "disable"
      })

    assert {:ok, config} = ProductionRuntimeConfig.validate(env)
    assert config.deployment_mode == :local_development
    assert config.postgres[:ssl_mode] == :disable
    assert config.postgres[:deployment_mode] == :local_development

    assert {:error,
            %{
              error:
                {:invalid_env, "FAVN_DATABASE_URL",
                 "postgres.favn.internal:5432 in local-development"}
            }} =
             env
             |> Map.put("FAVN_DATABASE_URL", "ecto://favn:secret@database.example/favn")
             |> ProductionRuntimeConfig.validate()
  end

  test "production composition is always PostgreSQL and requires no storage selector", %{
    ca_file: ca_file
  } do
    assert {:error, %{error: {:missing_env, "FAVN_CONTROL_PLANE_NODE"}}} =
             ProductionRuntimeConfig.validate(%{})

    assert {:ok, config} = ProductionRuntimeConfig.validate(base_env(ca_file))

    assert config.postgres[:url] == "ecto://favn:secret@postgres.example/favn"
  end

  test "shared and release config defer deployment parsing to the typed loader" do
    root = Path.expand("../../..", __DIR__)
    shared_config = File.read!(Path.join(root, "config/config.exs"))
    runtime_config = File.read!(Path.join(root, "config/runtime.exs"))

    refute shared_config =~ "System.get_env("
    refute shared_config =~ "System.fetch_env("
    refute runtime_config =~ "System.get_env("
    refute runtime_config =~ "System.fetch_env("
    assert runtime_config =~ "control_plane_runtime_config: true"
  end

  test "validate/1 rejects unsafe PostgreSQL configuration", %{ca_file: ca_file} do
    base = base_env(ca_file)

    assert {:error, %{error: {:invalid_env, "FAVN_DATABASE_URL", "PostgreSQL connection URL"}}} =
             base
             |> Map.put("FAVN_DATABASE_URL", "sqlite:///tmp/favn.db")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{
              error:
                {:invalid_env, "FAVN_DATABASE_URL",
                 "PostgreSQL connection URL without query parameters; use dedicated FAVN_DATABASE_* variables"}
            }} =
             base
             |> Map.put(
               "FAVN_DATABASE_URL",
               "ecto://favn:secret@postgres.example/favn?ssl=false&pool_size=1000000"
             )
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

    duplicate_keyring = ~s({"1":"#{@old_pin_key}","1":"#{@pin_key}"})

    assert {:error,
            %{
              error: {:invalid_secret_env, "FAVN_RUNTIME_INPUT_PIN_KEYS", :duplicate_version}
            }} =
             base
             |> Map.put("FAVN_RUNTIME_INPUT_PIN_KEYS", duplicate_keyring)
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
              error: {:invalid_env, "FAVN_DATABASE_SSL_MODE", "verify-full"}
            }} =
             base
             |> Map.put("FAVN_DATABASE_SSL_MODE", "disable")
             |> Map.delete("FAVN_DATABASE_SSL_CA_FILE")
             |> ProductionRuntimeConfig.validate()
  end

  test "validate/1 rejects plaintext even for loopback production deployments", %{
    ca_file: ca_file
  } do
    assert {:error,
            %{
              error: {:invalid_env, "FAVN_DATABASE_SSL_MODE", "verify-full"}
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

    assert {:error, %{error: {:invalid_env, "FAVN_DATABASE_SSL_MODE", "verify-full"}}} =
             ProductionRuntimeConfig.validate(env)
  end

  test "validate/1 uses the system trust store when no CA file override is supplied", %{
    ca_file: ca_file
  } do
    assert {:ok, config} =
             ca_file
             |> base_env()
             |> Map.delete("FAVN_DATABASE_SSL_CA_FILE")
             |> ProductionRuntimeConfig.validate()

    assert config.postgres[:ssl_mode] == :verify_full
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

    assert {:error,
            %{error: {:invalid_env, "FAVN_RUNNER_NODE", "different from control-plane node"}}} =
             base
             |> Map.put("FAVN_RUNNER_NODE", "control@control-plane.internal")
             |> ProductionRuntimeConfig.validate()

    assert {:error, %{error: {:invalid_env, "FAVN_RUNNER_NODE", "long name@private-dns-name"}}} =
             base
             |> Map.put("FAVN_RUNNER_NODE", "runner@localhost")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{
              error: {:invalid_secret_env, "FAVN_DISTRIBUTION_COOKIE", :insufficient_entropy}
            }} =
             base
             |> Map.put("FAVN_DISTRIBUTION_COOKIE", "short-cookie")
             |> ProductionRuntimeConfig.validate()
  end

  test "apply_from_env/1 freezes redacted PostgreSQL composition", %{ca_file: ca_file} do
    fresh_runner_node =
      "runner#{System.unique_integer([:positive, :monotonic])}@runner.internal"

    assert_raise ArgumentError, fn -> String.to_existing_atom(fresh_runner_node) end

    orchestrator_keys = [
      :persistence_backend,
      :persistence_options,
      :instance_id,
      :http_server,
      :shutdown_drain_timeout_ms,
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

    postgres_keys = [
      Module.concat(["FavnStoragePostgres.Repo"]),
      :runtime_input_pin_keys,
      :runtime_input_pin_current_key_version
    ]

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
             |> Map.put("FAVN_SCHEDULER_ENABLED", "false")
             |> Map.put("FAVN_RUNNER_NODE", fresh_runner_node)
             |> ProductionRuntimeConfig.apply_from_env()

    assert Application.get_env(:favn_orchestrator, :persistence_backend) ==
             ProductionRuntimeConfig.postgres_backend()

    options = Application.get_env(:favn_orchestrator, :persistence_options)
    assert options[:url] == "ecto://favn:secret@postgres.example/favn"
    assert options[:instance_id] == "control@control-plane.internal"

    assert Application.get_env(
             :favn_storage_postgres,
             Module.concat(["FavnStoragePostgres.Repo"])
           ) == options

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

    assert Application.get_env(:favn_orchestrator, :instance_id) ==
             "control@control-plane.internal"

    assert Application.get_env(:favn_orchestrator, :shutdown_drain_timeout_ms) == 120_000

    assert Application.get_env(:favn_orchestrator, :active_run_plan_max_bytes) ==
             512 * 1_024 * 1_024

    assert Application.get_env(:favn_orchestrator, :runner_client) ==
             FavnOrchestrator.RunnerClient.BeamNode

    assert Application.get_env(:favn_orchestrator, :runner_client_opts)[:runner_node] ==
             String.to_existing_atom(fresh_runner_node)

    diagnostics = Application.get_env(:favn_orchestrator, :production_runtime_diagnostics)
    refute inspect(diagnostics) =~ "secret"
    refute inspect(diagnostics) =~ ca_file
    refute inspect(diagnostics) =~ @pin_key
    refute inspect(diagnostics) =~ @old_pin_key
    assert diagnostics.runtime_input_pin == %{current_version: 2, retained_versions: [1, 2]}
    assert diagnostics.active_run_plan == %{max_bytes: 512 * 1_024 * 1_024}
    assert diagnostics.api_service_tokens.ids == ["favn_web"]

    assert diagnostics.runner == %{
             topology: :beam_node,
             control_plane_node: "control@control-plane.internal",
             runner_node: fresh_runner_node,
             distribution_port: 9_100,
             epmd_port: 4_369,
             cookie_configured?: true
           }

    refute inspect(diagnostics) =~ Map.fetch!(base_env(ca_file), "FAVN_DISTRIBUTION_COOKIE")

    frozen = FavnOrchestrator.RuntimeConfig.from_app_env()
    frozen_name = :"runtime_config_#{System.unique_integer([:positive])}"
    start_supervised!({FavnOrchestrator.RuntimeConfig, config: frozen, name: frozen_name})

    Application.put_env(:favn_orchestrator, :auth_session_ttl_seconds, 1)

    Application.put_env(:favn_orchestrator, :manifest_publication,
      compressed_limit_bytes: 1,
      decompressed_limit_bytes: 1
    )

    assert FavnOrchestrator.RuntimeConfig.current(frozen_name).auth_session_ttl_seconds == 43_200

    assert FavnOrchestrator.RuntimeConfig.current(frozen_name).manifest_publication ==
             frozen.manifest_publication
  end

  defp base_env(ca_file) do
    %{
      "FAVN_DATABASE_URL" => "ecto://favn:secret@postgres.example/favn",
      "FAVN_DATABASE_SSL_MODE" => "verify-full",
      "FAVN_DATABASE_SSL_CA_FILE" => ca_file,
      "FAVN_RUNTIME_INPUT_PIN_KEYS" => Jason.encode!(%{"1" => @pin_key}),
      "FAVN_WORKSPACE_IDS" => "salmon-one,salmon-two",
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => @token_env,
      "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME" => "admin",
      "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD" => "admin-password-long",
      "FAVN_CONTROL_PLANE_NODE" => "control@control-plane.internal",
      "FAVN_RUNNER_NODE" => "runner@runner.internal",
      "FAVN_DISTRIBUTION_COOKIE" => "bN7!tQ2#vL9@xR4$kM8%pC6&zH3*eW5?",
      "FAVN_BEAM_DISTRIBUTION_PORT" => "9100"
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
