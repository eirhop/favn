defmodule FavnStoragePostgres.StorageV2.ConfigTest do
  use ExUnit.Case, async: false

  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.RuntimeInputKeys

  defmodule TestAuthenticationProvider do
    def applications(_options), do: {:ok, [:favn_storage_postgres]}
    def child_specs(_options), do: {:ok, []}
    def connection_reference(_options), do: {:ok, [server: :safe_test_provider]}
    def connection_password(_reference), do: {:ok, "dynamic-token"}
    def status(_options), do: %{lifecycle_ready?: true}
  end

  setup do
    previous_environment = Application.get_env(:favn_storage_postgres, :environment)
    previous_keys = Application.get_env(:favn_storage_postgres, :runtime_input_pin_keys)

    previous_version =
      Application.get_env(:favn_storage_postgres, :runtime_input_pin_current_key_version)

    on_exit(fn ->
      restore_env(:environment, previous_environment)
      restore_env(:runtime_input_pin_keys, previous_keys)
      restore_env(:runtime_input_pin_current_key_version, previous_version)
    end)

    :ok
  end

  test "production requires verified TLS and diagnostics redact connection credentials" do
    Application.put_env(:favn_storage_postgres, :environment, :prod)
    url = "ecto://runtime:top-secret@postgres.internal.example/favn"

    assert {:error, :production_tls_required} =
             Config.repo_options(url: url, ssl_mode: :disable)

    assert {:error, :database_tls_trust_required} =
             Config.repo_options(
               url: url,
               ssl_mode: :verify_full,
               ssl_ca_file: "/file/that/does/not/exist"
             )

    Application.put_env(:favn_storage_postgres, :environment, :test)
    assert {:ok, options} = Config.repo_options(url: url, ssl_mode: :disable, pool_size: 7)

    assert Config.redacted(options) == %{
             configured?: true,
             authentication_mode: :password,
             pool_size: 7,
             queue_target_ms: 50,
             queue_interval_ms: 1_000,
             timeout_ms: 15_000,
             tls?: false
           }

    refute inspect(Config.redacted(options)) =~ "top-secret"
  end

  test "managed identity uses explicit components and suppresses ambient Postgrex defaults" do
    Application.put_env(:favn_storage_postgres, :environment, :test)
    previous_pgpassword = System.get_env("PGPASSWORD")
    previous_pguser = System.get_env("PGUSER")
    System.put_env("PGPASSWORD", "ambient-password-canary")
    System.put_env("PGUSER", "ambient-user-canary")

    on_exit(fn ->
      restore_system_env("PGPASSWORD", previous_pgpassword)
      restore_system_env("PGUSER", previous_pguser)
    end)

    assert {:ok, config} =
             Config.connection_config(
               authentication:
                 {:dynamic, TestAuthenticationProvider, [client_id: "identity-client-id-canary"]},
               hostname: "postgres.example",
               port: 5432,
               database: "favn",
               username: "favn_runtime",
               ssl_mode: :disable
             )

    assert config.repo_options[:hostname] == "postgres.example"
    assert config.repo_options[:database] == "favn"
    assert config.repo_options[:username] == "favn_runtime"
    assert config.repo_options[:password] == nil
    refute Keyword.has_key?(config.repo_options, :url)

    assert {FavnStoragePostgres.Authentication, :configure_connection,
            [TestAuthenticationProvider, [server: :safe_test_provider]]} =
             config.repo_options[:configure]

    repo_defaults = Postgrex.Utils.default_opts(config.repo_options)
    notification_defaults = Postgrex.Utils.default_opts(config.notification_options)

    refute Keyword.has_key?(repo_defaults, :password)
    refute Keyword.has_key?(notification_defaults, :password)
    assert repo_defaults[:username] == "favn_runtime"
    assert notification_defaults[:username] == "favn_runtime"

    refute inspect(config.repo_options) =~ "ambient-password-canary"
    refute inspect(config.notification_options) =~ "ambient-password-canary"
    refute inspect(config.repo_options[:configure]) =~ "identity-client-id-canary"

    assert Config.redacted(config.repo_options).authentication_mode ==
             :azure_managed_identity
  end

  test "authentication modes reject ambiguous and sensitive connection configuration" do
    Application.put_env(:favn_storage_postgres, :environment, :test)

    assert {:error, :database_url_not_allowed_with_dynamic_authentication} =
             Config.connection_config(
               authentication: {:dynamic, TestAuthenticationProvider, []},
               url: "ecto://runtime:secret@postgres.example/favn",
               hostname: "postgres.example",
               database: "favn",
               username: "favn_runtime"
             )

    assert {:error, :sensitive_database_connection_errors_not_allowed} =
             Config.repo_options(
               url: "ecto://runtime:secret@postgres.example/favn",
               ssl_mode: :disable,
               show_sensitive_data_on_connection_error: true
             )
  end

  test "release environment selects the fixed Azure provider and validates its identity" do
    Application.put_env(:favn_storage_postgres, :environment, :test)
    client_id = "11111111-2222-3333-4444-555555555555"

    env = %{
      "FAVN_DEPLOYMENT_MODE" => "production",
      "FAVN_DATABASE_AUTH_MODE" => "azure_managed_identity",
      "FAVN_DATABASE_HOST" => "favn.postgres.database.azure.com",
      "FAVN_DATABASE_PORT" => "5432",
      "FAVN_DATABASE_NAME" => "favn",
      "FAVN_DATABASE_USERNAME" => "favn_migrator",
      "FAVN_AZURE_MANAGED_IDENTITY_CLIENT_ID" => client_id,
      "FAVN_DATABASE_SSL_MODE" => "disable"
    }

    assert {:ok, config} = Config.connection_config_from_env(env)
    provider = Module.concat([Favn, Azure, ControlPlanePostgresAuth])
    assert config.authentication == {:dynamic, provider, [client_id: client_id]}
    assert config.repo_options[:password] == nil
    refute Keyword.has_key?(config.repo_options, :url)
    refute inspect(config.repo_options[:configure]) =~ client_id

    assert {:error, {:invalid_database_env, "FAVN_AZURE_MANAGED_IDENTITY_CLIENT_ID"}} =
             env
             |> Map.put("FAVN_AZURE_MANAGED_IDENTITY_CLIENT_ID", "not-a-client-uuid")
             |> Config.connection_config_from_env()

    assert {:error, :database_url_not_allowed_with_dynamic_authentication} =
             env
             |> Map.put("FAVN_DATABASE_URL", "ecto://runtime:secret@postgres.example/favn")
             |> Config.connection_config_from_env()
  end

  test "production rejects plaintext even with a development interlock" do
    Application.put_env(:favn_storage_postgres, :environment, :prod)
    url = "ecto://runtime:top-secret@127.0.0.1/favn"

    assert {:error, :production_tls_required} =
             Config.repo_options(
               url: url,
               ssl_mode: :disable,
               allow_insecure_database?: true
             )
  end

  test "release environment rejects the removed local deployment mode" do
    Application.put_env(:favn_storage_postgres, :environment, :prod)

    local_env = %{
      "FAVN_DEPLOYMENT_MODE" => "local-development",
      "FAVN_DATABASE_URL" =>
        "ecto://favn_runtime:top-secret@postgres.favn.internal:5432/favn_dev",
      "FAVN_DATABASE_SSL_MODE" => "disable"
    }

    assert {:error, {:invalid_env, "FAVN_DEPLOYMENT_MODE", "production"}} =
             Config.repo_options_from_env(local_env)
  end

  test "database URL query parameters cannot override validated connection options" do
    Application.put_env(:favn_storage_postgres, :environment, :test)

    assert {:error, :database_url_query_parameters_not_allowed} =
             Config.repo_options(
               url:
                 "ecto://runtime:top-secret@127.0.0.1/favn?ssl=false&pool_size=1000000&timeout=0",
               ssl_mode: :verify_full,
               pool_size: 15,
               timeout: 15_000
             )
  end

  test "release-task TLS parsing rejects a relative CA path" do
    Application.put_env(:favn_storage_postgres, :environment, :test)

    assert {:error, :database_tls_trust_required} =
             Config.repo_options_from_env(%{
               "FAVN_DATABASE_URL" => "ecto://runtime:top-secret@postgres.internal/favn",
               "FAVN_DATABASE_SSL_MODE" => "verify-full",
               "FAVN_DATABASE_SSL_CA_FILE" => "mix.exs"
             })
  end

  test "release-task environment parsing uses production connection bounds" do
    Application.put_env(:favn_storage_postgres, :environment, :test)

    env = %{
      "FAVN_DATABASE_URL" => "ecto://runtime:top-secret@127.0.0.1/favn",
      "FAVN_DATABASE_SSL_MODE" => "disable"
    }

    assert {:ok, options} = Config.repo_options_from_env(env)
    assert options[:pool_size] == 15
    assert options[:timeout] == 15_000

    assert {:error, {:invalid_database_env, "FAVN_DATABASE_POOL_SIZE"}} =
             env
             |> Map.put("FAVN_DATABASE_POOL_SIZE", "201")
             |> Config.repo_options_from_env()

    assert {:error, {:invalid_database_env, "FAVN_DATABASE_TIMEOUT_MS"}} =
             env
             |> Map.put("FAVN_DATABASE_TIMEOUT_MS", "120001")
             |> Config.repo_options_from_env()
  end

  test "runtime-input encryption requires an exact 256-bit current key and retains old versions" do
    Application.put_env(:favn_storage_postgres, :runtime_input_pin_keys, %{
      1 => :crypto.strong_rand_bytes(32),
      2 => Base.encode64(:crypto.strong_rand_bytes(32))
    })

    Application.put_env(:favn_storage_postgres, :runtime_input_pin_current_key_version, 2)

    assert {:ok, {2, current}} = RuntimeInputKeys.current()
    assert byte_size(current) == 32
    assert {:ok, old} = RuntimeInputKeys.fetch(1)
    assert byte_size(old) == 32

    Application.put_env(:favn_storage_postgres, :runtime_input_pin_keys, %{2 => "short"})
    assert {:error, :invalid_runtime_input_pin_key} = RuntimeInputKeys.current()
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_storage_postgres, key)
  defp restore_env(key, value), do: Application.put_env(:favn_storage_postgres, key, value)

  defp restore_system_env(name, nil), do: System.delete_env(name)
  defp restore_system_env(name, value), do: System.put_env(name, value)
end
