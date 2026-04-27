defmodule Favn.Dev.ConsumerConfigTransportTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.ConsumerConfigTransport

  setup do
    keys = ConsumerConfigTransport.supported_keys()
    previous = Map.new(keys, fn key -> {key, Application.get_env(:favn, key, :__missing__)} end)

    on_exit(fn ->
      Enum.each(previous, fn
        {key, :__missing__} -> Application.delete_env(:favn, key)
        {key, value} -> Application.put_env(:favn, key, value)
      end)
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
end
