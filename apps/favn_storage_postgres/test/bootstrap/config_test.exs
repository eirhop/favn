defmodule FavnStoragePostgres.Bootstrap.ConfigTest do
  use ExUnit.Case, async: false

  alias FavnStoragePostgres.Bootstrap.Config
  alias FavnStoragePostgres.Bootstrap.Profile
  alias FavnStoragePostgres.Config, as: DatabaseConfig

  setup do
    previous_environment = Application.get_env(:favn_storage_postgres, :environment)
    Application.put_env(:favn_storage_postgres, :environment, :test)

    on_exit(fn ->
      if is_nil(previous_environment) do
        Application.delete_env(:favn_storage_postgres, :environment)
      else
        Application.put_env(:favn_storage_postgres, :environment, previous_environment)
      end
    end)
  end

  test "password bootstrap keeps three explicit roles and redacts every credential" do
    env = password_env()

    assert {:ok, config} = Config.from_env(:bootstrap, env)
    assert config.target_database == "favn"
    assert config.maintenance_database == "postgres"
    assert config.bootstrap.role == "favn_bootstrap"
    assert config.migrator.role == "favn_migrator"
    assert config.runtime.role == "favn_runtime"
    assert config.workspace.display_name == "Primary workspace"

    runtime_env =
      Config.connection_env(config, config.runtime, "favn", :runtime_operation)

    assert runtime_env["FAVN_DATABASE_AUTH_MODE"] == "password"
    assert runtime_env["FAVN_DATABASE_AUTH_PROFILE"] == "runtime_operation"
    assert URI.parse(runtime_env["FAVN_DATABASE_URL"]).path == "/favn"

    inspected = inspect(config)
    refute inspected =~ "bootstrap-secret"
    refute inspected =~ "migrator-secret"
    refute inspected =~ "runtime-secret"
  end

  test "managed identity profiles require explicit client and object identities" do
    env = azure_env()

    assert {:ok, config} = Config.from_env(:bootstrap, env)
    assert config.bootstrap.authentication_mode == :azure_managed_identity
    assert config.migrator.object_id == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    assert config.runtime.object_id == "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

    connection_env =
      Config.connection_env(config, config.migrator, "favn", :migrator_operation)

    assert connection_env["FAVN_DATABASE_USERNAME"] == "favn_migrator"
    assert connection_env["FAVN_DATABASE_AUTH_PROFILE"] == "migrator_operation"
    refute Map.has_key?(connection_env, "FAVN_DATABASE_MIGRATOR_AZURE_OBJECT_ID")

    runtime_env =
      Config.connection_env(config, config.runtime, "favn", :runtime_operation)

    assert {:ok, migrator_connection} =
             DatabaseConfig.connection_config_from_env(connection_env)

    assert {:ok, runtime_connection} =
             DatabaseConfig.connection_config_from_env(runtime_env)

    {:dynamic, _migrator_provider, migrator_options} = migrator_connection.authentication
    {:dynamic, _runtime_provider, runtime_options} = runtime_connection.authentication
    refute migrator_options[:server_name] == runtime_options[:server_name]
    refute migrator_options[:cache_name] == runtime_options[:cache_name]

    inspected = inspect(config)
    refute inspected =~ "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    refute inspected =~ "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

    assert {:error, :invalid_database_bootstrap_configuration} =
             env
             |> Map.delete("FAVN_DATABASE_RUNTIME_AZURE_OBJECT_ID")
             |> then(&Config.from_env(:bootstrap, &1))

    assert {:error, :invalid_database_bootstrap_configuration} =
             env
             |> Map.put("FAVN_DATABASE_MAINTENANCE_NAME", "maintenance")
             |> then(&Config.from_env(:bootstrap, &1))
  end

  test "upgrade rejects bootstrap authority and does not require workspace input" do
    env =
      password_env()
      |> Map.drop([
        "FAVN_DATABASE_BOOTSTRAP_AUTH_MODE",
        "FAVN_DATABASE_BOOTSTRAP_URL",
        "FAVN_WORKSPACE_ID",
        "FAVN_WORKSPACE_SLUG",
        "FAVN_WORKSPACE_NAME"
      ])

    assert {:ok, config} = Config.from_env(:upgrade, env)
    assert config.bootstrap == nil
    assert config.workspace == nil

    assert {:error, :invalid_database_bootstrap_configuration} =
             env
             |> Map.put(
               "FAVN_DATABASE_BOOTSTRAP_URL",
               "ecto://favn_bootstrap:bootstrap-secret@postgres.example/postgres"
             )
             |> then(&Config.from_env(:upgrade, &1))
  end

  test "status accepts an optional bootstrap profile but rejects ambiguous or shared roles" do
    without_bootstrap =
      password_env()
      |> Map.drop([
        "FAVN_DATABASE_BOOTSTRAP_AUTH_MODE",
        "FAVN_DATABASE_BOOTSTRAP_URL"
      ])

    assert {:ok, %{bootstrap: nil}} = Config.from_env(:status, without_bootstrap)

    assert {:error, :invalid_database_bootstrap_configuration} =
             without_bootstrap
             |> Map.put(
               "FAVN_DATABASE_MIGRATOR_URL",
               "ecto://favn_runtime:migrator-secret@postgres.example/favn"
             )
             |> then(&Config.from_env(:status, &1))

    assert {:error, :invalid_database_bootstrap_configuration} =
             without_bootstrap
             |> Map.put("FAVN_DATABASE_RUNTIME_AUTH_MODE", "password")
             |> Map.put(
               "FAVN_DATABASE_RUNTIME_AZURE_OBJECT_ID",
               "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
             )
             |> then(&Config.from_env(:status, &1))
  end

  test "all profiles must name one PostgreSQL endpoint and their exact lifecycle database" do
    assert {:error, :invalid_database_bootstrap_configuration} =
             password_env()
             |> Map.put(
               "FAVN_DATABASE_RUNTIME_URL",
               "ecto://favn_runtime:runtime-secret@other-postgres.example/favn"
             )
             |> then(&Config.from_env(:bootstrap, &1))

    assert {:error, :invalid_database_bootstrap_configuration} =
             password_env()
             |> Map.put(
               "FAVN_DATABASE_MIGRATOR_URL",
               "ecto://favn_migrator:migrator-secret@postgres.example:5433/favn"
             )
             |> then(&Config.from_env(:bootstrap, &1))

    assert {:error, :invalid_database_bootstrap_configuration} =
             password_env()
             |> Map.put(
               "FAVN_DATABASE_BOOTSTRAP_URL",
               "ecto://favn_bootstrap:bootstrap-secret@postgres.example/favn"
             )
             |> then(&Config.from_env(:bootstrap, &1))

    assert {:error, :invalid_database_bootstrap_configuration} =
             password_env()
             |> Map.put(
               "FAVN_DATABASE_RUNTIME_URL",
               "ecto://favn_runtime:runtime-secret@postgres.example/postgres"
             )
             |> then(&Config.from_env(:bootstrap, &1))
  end

  test "migrator and runtime cannot reuse reserved or administrator roles" do
    for role <- ["postgres", "pg_read_all_data", "azure_pg_admin"] do
      assert {:error, :invalid_database_bootstrap_configuration} =
               password_env()
               |> Map.put(
                 "FAVN_DATABASE_RUNTIME_URL",
                 "ecto://#{role}:runtime-secret@postgres.example/favn"
               )
               |> then(&Config.from_env(:bootstrap, &1))
    end
  end

  test "password profiles reject values outside the SASLprep-stable ASCII boundary" do
    for encoded_password <- ["contains%20space", "unicode-p%C3%A5ssword", "line%0Abreak"] do
      assert {:error, :invalid_database_bootstrap_configuration} =
               password_env()
               |> Map.put(
                 "FAVN_DATABASE_RUNTIME_URL",
                 "ecto://favn_runtime:#{encoded_password}@postgres.example/favn"
               )
               |> then(&Config.from_env(:bootstrap, &1))
    end
  end

  test "profile inspection never exposes password URLs or provider object ids" do
    password_profile = %Profile{
      purpose: :runtime,
      authentication_mode: :password,
      role: "favn_runtime",
      source: {:url, "ecto://favn_runtime:canary-secret@postgres.example/favn"},
      base_env: %{}
    }

    azure_profile = %Profile{
      purpose: :runtime,
      authentication_mode: :azure_managed_identity,
      role: "favn_runtime",
      source: {:azure, "postgres.example", 5432, "cccccccc-cccc-cccc-cccc-cccccccccccc"},
      object_id: "dddddddd-dddd-dddd-dddd-dddddddddddd",
      base_env: %{}
    }

    refute inspect(password_profile) =~ "canary-secret"
    refute inspect(azure_profile) =~ "cccccccc-cccc-cccc-cccc-cccccccccccc"
    refute inspect(azure_profile) =~ "dddddddd-dddd-dddd-dddd-dddddddddddd"
  end

  defp password_env do
    %{
      "FAVN_DEPLOYMENT_MODE" => "production",
      "FAVN_DATABASE_SSL_MODE" => "disable",
      "FAVN_DATABASE_BOOTSTRAP_AUTH_MODE" => "password",
      "FAVN_DATABASE_BOOTSTRAP_URL" =>
        "ecto://favn_bootstrap:bootstrap-secret@postgres.example/postgres",
      "FAVN_DATABASE_MIGRATOR_AUTH_MODE" => "password",
      "FAVN_DATABASE_MIGRATOR_URL" =>
        "ecto://favn_migrator:migrator-secret@postgres.example/favn",
      "FAVN_DATABASE_RUNTIME_AUTH_MODE" => "password",
      "FAVN_DATABASE_RUNTIME_URL" => "ecto://favn_runtime:runtime-secret@postgres.example/favn",
      "FAVN_WORKSPACE_ID" => "primary",
      "FAVN_WORKSPACE_SLUG" => "primary",
      "FAVN_WORKSPACE_NAME" => "Primary workspace"
    }
  end

  defp azure_env do
    %{
      "FAVN_DEPLOYMENT_MODE" => "production",
      "FAVN_DATABASE_SSL_MODE" => "disable",
      "FAVN_DATABASE_HOST" => "favn.postgres.database.azure.com",
      "FAVN_DATABASE_NAME" => "favn",
      "FAVN_DATABASE_BOOTSTRAP_AUTH_MODE" => "azure_managed_identity",
      "FAVN_DATABASE_BOOTSTRAP_USERNAME" => "favn_bootstrap",
      "FAVN_DATABASE_BOOTSTRAP_AZURE_MANAGED_IDENTITY_CLIENT_ID" =>
        "11111111-1111-1111-1111-111111111111",
      "FAVN_DATABASE_MIGRATOR_AUTH_MODE" => "azure_managed_identity",
      "FAVN_DATABASE_MIGRATOR_USERNAME" => "favn_migrator",
      "FAVN_DATABASE_MIGRATOR_AZURE_MANAGED_IDENTITY_CLIENT_ID" =>
        "22222222-2222-2222-2222-222222222222",
      "FAVN_DATABASE_MIGRATOR_AZURE_OBJECT_ID" => "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
      "FAVN_DATABASE_RUNTIME_AUTH_MODE" => "azure_managed_identity",
      "FAVN_DATABASE_RUNTIME_USERNAME" => "favn_runtime",
      "FAVN_DATABASE_RUNTIME_AZURE_MANAGED_IDENTITY_CLIENT_ID" =>
        "33333333-3333-3333-3333-333333333333",
      "FAVN_DATABASE_RUNTIME_AZURE_OBJECT_ID" => "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
      "FAVN_WORKSPACE_ID" => "primary"
    }
  end
end
