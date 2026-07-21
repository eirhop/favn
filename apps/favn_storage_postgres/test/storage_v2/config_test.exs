defmodule FavnStoragePostgres.StorageV2.ConfigTest do
  use ExUnit.Case, async: false

  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.RuntimeInputKeys

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
             pool_size: 7,
             queue_target_ms: 50,
             queue_interval_ms: 1_000,
             timeout_ms: 15_000,
             tls?: false
           }

    refute inspect(Config.redacted(options)) =~ "top-secret"
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
end
