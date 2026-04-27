defmodule Favn.Dev.ConsumerConfigTransportTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.ConsumerConfigTransport

  setup do
    keys = ConsumerConfigTransport.supported_keys()
    previous = Map.new(keys, fn key -> {key, Application.get_env(:favn, key, :__missing__)} end)
    previous_encoded = System.get_env("FAVN_DEV_CONSUMER_FAVN_CONFIG")

    on_exit(fn ->
      Enum.each(previous, fn
        {key, :__missing__} -> Application.delete_env(:favn, key)
        {key, value} -> Application.put_env(:favn, key, value)
      end)

      if previous_encoded do
        System.put_env("FAVN_DEV_CONSUMER_FAVN_CONFIG", previous_encoded)
      else
        System.delete_env("FAVN_DEV_CONSUMER_FAVN_CONFIG")
      end

      purge_bootstrap_module()
    end)

    :ok
  end

  test "roundtrips supported consumer config and module atoms" do
    config = [
      connection_modules: [MyApp.Connections.Warehouse],
      connections: [warehouse: [adapter: Favn.SQL.Adapter.DuckDB, database: "/tmp/warehouse.duckdb"]],
      runner_plugins: [{FavnDuckdb, [execution_mode: :in_process]}],
      duckdb_in_process_client: [pool: MyApp.DuckPool]
    ]

    encoded = ConsumerConfigTransport.encode(config)

    assert {:ok, ^config} = ConsumerConfigTransport.decode(encoded)
  end

  test "collect normalizes relative DuckDB database paths to the consumer project root" do
    Application.put_env(:favn, :connections,
      warehouse: [adapter: Favn.SQL.Adapter.DuckDB, database: "data/warehouse.duckdb"]
    )

    assert [connections: [warehouse: connection]] =
             ConsumerConfigTransport.collect(root_dir: "/tmp/consumer")

    assert connection[:database] == "/tmp/consumer/data/warehouse.duckdb"
  end

  test "apply_encoded applies decoded config to favn application env" do
    encoded = ConsumerConfigTransport.encode(connection_modules: [MyApp.Connections.Warehouse])

    assert :ok = ConsumerConfigTransport.apply_encoded(encoded)
    assert Application.get_env(:favn, :connection_modules) == [MyApp.Connections.Warehouse]
  end

  test "malformed base64 and payload return structured errors" do
    assert {:error, :invalid_base64} = ConsumerConfigTransport.decode("not base64")

    encoded = Base.encode64(:erlang.term_to_binary(%{"schema_version" => 1, "entries" => :bad}))

    assert {:error, :invalid_payload} = ConsumerConfigTransport.decode(encoded)
  end

  test "unsupported payload keys are rejected explicitly" do
    payload = %{
      "schema_version" => 1,
      "entries" => [%{"key" => "asset_modules", "value" => "ignored"}]
    }

    encoded = Base.encode64(:erlang.term_to_binary(payload))

    assert {:error, {:unsupported_key, "asset_modules"}} = ConsumerConfigTransport.decode(encoded)
  end

  test "redaction hides local secrets and plugin config" do
    config = [
      connections: [warehouse: [database_url: "duckdb://secret", password: "p@ssw0rd"]],
      runner_plugins: [{FavnDuckdb, token: "plugin-token"}],
      duckdb_in_process_client: [secret: "client-secret"],
      connection_modules: [MyApp.Connections.Warehouse]
    ]

    redacted = inspect(ConsumerConfigTransport.redact(config))

    refute redacted =~ "duckdb://secret"
    refute redacted =~ "p@ssw0rd"
    refute redacted =~ "plugin-token"
    refute redacted =~ "client-secret"
    assert redacted =~ "[REDACTED]"
    assert redacted =~ "MyApp.Connections.Warehouse"
  end

  test "bootstrap snippet reports malformed config without raw payload values" do
    code = ConsumerConfigTransport.bootstrap_eval_snippet()

    assert code =~ "Base.decode64(encoded)"
    assert code =~ ":erlang.binary_to_term(binary, [:safe])"
    assert code =~ "invalid FAVN_DEV_CONSUMER_FAVN_CONFIG"
    refute code =~ "decode64!"
    refute code =~ "binary_to_term()"
  end

  test "bootstrap snippet executes and applies supported config" do
    encoded =
      ConsumerConfigTransport.encode(
        connection_modules: [MyApp.Connections.Warehouse],
        runner_plugins: [{FavnDuckdb, [execution_mode: :in_process]}]
      )

    System.put_env("FAVN_DEV_CONSUMER_FAVN_CONFIG", encoded)

    purge_bootstrap_module()
    assert {:ok, _bindings} = Code.eval_string(ConsumerConfigTransport.bootstrap_eval_snippet())
    assert Application.get_env(:favn, :connection_modules) == [MyApp.Connections.Warehouse]
    assert Application.get_env(:favn, :runner_plugins) == [{FavnDuckdb, [execution_mode: :in_process]}]
  end

  test "bootstrap snippet raises a redacted structured error for bad payloads" do
    secret = "super-secret-password"

    payload = %{
      "schema_version" => 1,
      "entries" => [%{"key" => "connections", "value" => %{"password" => secret}}]
    }

    System.put_env("FAVN_DEV_CONSUMER_FAVN_CONFIG", Base.encode64(:erlang.term_to_binary(payload)))

    purge_bootstrap_module()

    error =
      assert_raise RuntimeError, ~r/invalid FAVN_DEV_CONSUMER_FAVN_CONFIG: :invalid_payload/, fn ->
        Code.eval_string(ConsumerConfigTransport.bootstrap_eval_snippet())
      end

    refute Exception.message(error) =~ secret
  end

  defp purge_bootstrap_module do
    :code.purge(Favn.Dev.ConsumerConfigBootstrap)
    :code.delete(Favn.Dev.ConsumerConfigBootstrap)
    :ok
  end
end
