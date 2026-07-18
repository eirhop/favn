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

  test "production plaintext requires an explicit development/test interlock" do
    Application.put_env(:favn_storage_postgres, :environment, :prod)
    url = "ecto://runtime:top-secret@127.0.0.1/favn"

    assert {:ok, options} =
             Config.repo_options(
               url: url,
               ssl_mode: :disable,
               allow_insecure_database?: true
             )

    assert options[:ssl] == false
    refute Keyword.has_key?(options, :ssl_mode)
    refute Keyword.has_key?(options, :allow_insecure_database?)
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
