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
      connections: [
        warehouse: [adapter: Favn.SQL.Adapter.DuckDB, database: "/tmp/warehouse.duckdb"]
      ],
      runner_plugins: [{FavnDuckdb, [execution_mode: :in_process]}],
      duckdb_in_process_client: [pool: MyApp.DuckPool, enabled: true, note: nil],
      duckdb_adbc: [driver: "/opt/duckdb/1.5.2/libduckdb.so", entrypoint: "duckdb_adbc_init"]
    ]

    encoded = ConsumerConfigTransport.encode(config)

    assert {:ok, ^config} = ConsumerConfigTransport.decode(encoded)
  end

  test "encoded atoms carry bounded transport kinds" do
    payload =
      ConsumerConfigTransport.encode(
        connection_modules: [MyApp.Connections.Warehouse],
        connections: [warehouse: [adapter: Favn.SQL.Adapter.DuckDB]]
      )
      |> decode_payload!()

    assert payload == %{
             "schema_version" => 1,
             "entries" => [
               %{
                 "key" => "connection_modules",
                 "value" => %{
                   "$type" => "list",
                   "items" => [
                     %{
                       "$type" => "atom",
                       "kind" => "module",
                       "value" => "Elixir.MyApp.Connections.Warehouse"
                     }
                   ]
                 }
               },
               %{
                 "key" => "connections",
                 "value" => %{
                   "$type" => "list",
                   "items" => [
                     %{
                       "$type" => "tuple",
                       "items" => [
                         %{"$type" => "atom", "kind" => "local", "value" => "warehouse"},
                         %{
                           "$type" => "list",
                           "items" => [
                             %{
                               "$type" => "tuple",
                               "items" => [
                                 %{"$type" => "atom", "kind" => "local", "value" => "adapter"},
                                 %{
                                   "$type" => "atom",
                                   "kind" => "module",
                                   "value" => "Elixir.Favn.SQL.Adapter.DuckDB"
                                 }
                               ]
                             }
                           ]
                         }
                       ]
                     }
                   ]
                 }
               }
             ]
           }
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

  test "top-level entry and atom shape limits reject unsafe config transport" do
    assert {:error, :invalid_payload} =
             ConsumerConfigTransport.decode(
               encode_payload(%{
                 "schema_version" => 1,
                 "entries" => duplicate_entries(6)
               })
             )

    assert {:error, :invalid_payload} =
             ConsumerConfigTransport.decode(
               encode_payload(%{
                 "schema_version" => 1,
                 "entries" => [
                   %{
                     "key" => "connection_modules",
                     "value" => %{
                       "$type" => "atom",
                       "kind" => "module",
                       "value" => "Elixir.Bad-name"
                     }
                   }
                 ]
               })
             )

    assert {:error, :invalid_payload} =
             ConsumerConfigTransport.decode(
               encode_payload(%{
                 "schema_version" => 1,
                 "entries" => [
                   %{
                     "key" => "connections",
                     "value" => %{"$type" => "atom", "kind" => "local", "value" => "bad-name"}
                   }
                 ]
               })
             )

    missing_atom = "definitely_missing_atom_#{System.unique_integer([:positive])}"

    assert {:error, :invalid_payload} =
             ConsumerConfigTransport.decode(
               encode_payload(%{
                 "schema_version" => 1,
                 "entries" => [
                   %{
                     "key" => "connections",
                     "value" => %{
                       "$type" => "atom",
                       "kind" => "existing",
                       "value" => missing_atom
                     }
                   }
                 ]
               })
             )
  end

  test "payload byte limit rejects oversized config transport" do
    encoded = String.duplicate("a", 1_048_577) |> :erlang.term_to_binary() |> Base.encode64()

    assert {:error, :invalid_payload} = ConsumerConfigTransport.decode(encoded)
    assert_bootstrap_invalid_payload(encoded)
  end

  test "collection item limits reject oversized list map and tuple transport values" do
    encoded_list =
      encode_payload_with_value(%{"$type" => "list", "items" => duplicate_values(2_001)})

    encoded_map =
      encode_payload_with_value(%{
        "$type" => "map",
        "entries" => duplicate_map_entries(2_001)
      })

    encoded_tuple =
      encode_payload_with_value(%{"$type" => "tuple", "items" => duplicate_values(2_001)})

    for encoded <- [encoded_list, encoded_map, encoded_tuple] do
      assert {:error, :invalid_payload} = ConsumerConfigTransport.decode(encoded)
      assert_bootstrap_invalid_payload(encoded)
    end
  end

  test "decode depth limit rejects deeply nested transport values" do
    encoded = encode_payload_with_value(nested_list_value(34))

    assert {:error, :invalid_payload} = ConsumerConfigTransport.decode(encoded)
    assert_bootstrap_invalid_payload(encoded)
  end

  test "redaction hides local secrets and plugin config" do
    config = [
      connections: [warehouse: [database_url: "duckdb://secret", password: "p@ssw0rd"]],
      runner_plugins: [{FavnDuckdb, token: "plugin-token"}],
      duckdb_in_process_client: [secret: "client-secret"],
      duckdb_adbc: [driver: "/opt/duckdb/1.5.2/libduckdb.so", token: "driver-token"],
      connection_modules: [MyApp.Connections.Warehouse]
    ]

    redacted = inspect(ConsumerConfigTransport.redact(config))

    refute redacted =~ "duckdb://secret"
    refute redacted =~ "p@ssw0rd"
    refute redacted =~ "plugin-token"
    refute redacted =~ "client-secret"
    refute redacted =~ "/opt/duckdb/1.5.2/libduckdb.so"
    refute redacted =~ "driver-token"
    assert redacted =~ "[REDACTED]"
    assert redacted =~ "MyApp.Connections.Warehouse"
  end

  test "bootstrap snippet reports malformed config without raw payload values" do
    code = ConsumerConfigTransport.bootstrap_eval_snippet()

    assert code =~ "Base.decode64(encoded)"
    assert code =~ ":erlang.binary_to_term(binary, [:safe])"
    assert code =~ "String.to_existing_atom(value)"
    assert code =~ "invalid FAVN_DEV_CONSUMER_FAVN_CONFIG"
    refute code =~ "decode64!"
    refute code =~ "binary_to_term()"
  end

  test "bootstrap snippet executes and applies supported config" do
    encoded =
      ConsumerConfigTransport.encode(
        connection_modules: [MyApp.Connections.Warehouse],
        runner_plugins: [{FavnDuckdb, [execution_mode: :in_process]}],
        duckdb_adbc: [driver: "/opt/duckdb/1.5.2/libduckdb.so", entrypoint: "duckdb_adbc_init"]
      )

    System.put_env("FAVN_DEV_CONSUMER_FAVN_CONFIG", encoded)

    purge_bootstrap_module()
    assert {:ok, _bindings} = Code.eval_string(ConsumerConfigTransport.bootstrap_eval_snippet())
    assert Application.get_env(:favn, :connection_modules) == [MyApp.Connections.Warehouse]

    assert Application.get_env(:favn, :runner_plugins) == [
             {FavnDuckdb, [execution_mode: :in_process]}
           ]

    assert Application.get_env(:favn, :duckdb_adbc) == [
             driver: "/opt/duckdb/1.5.2/libduckdb.so",
             entrypoint: "duckdb_adbc_init"
           ]
  end

  test "bootstrap snippet raises a redacted structured error for bad payloads" do
    secret = "super-secret-password"

    payload = %{
      "schema_version" => 1,
      "entries" => [%{"key" => "connections", "value" => %{"password" => secret}}]
    }

    System.put_env(
      "FAVN_DEV_CONSUMER_FAVN_CONFIG",
      Base.encode64(:erlang.term_to_binary(payload))
    )

    purge_bootstrap_module()

    error =
      assert_raise RuntimeError,
                   ~r/invalid FAVN_DEV_CONSUMER_FAVN_CONFIG: :invalid_payload/,
                   fn ->
                     Code.eval_string(ConsumerConfigTransport.bootstrap_eval_snippet())
                   end

    refute Exception.message(error) =~ secret
  end

  test "bootstrap snippet rejects unsafe atom transport payloads" do
    payload = %{
      "schema_version" => 1,
      "entries" => [
        %{
          "key" => "connection_modules",
          "value" => %{"$type" => "atom", "kind" => "module", "value" => "Elixir.Bad-name"}
        }
      ]
    }

    System.put_env("FAVN_DEV_CONSUMER_FAVN_CONFIG", encode_payload(payload))

    purge_bootstrap_module()

    assert_raise RuntimeError, ~r/invalid FAVN_DEV_CONSUMER_FAVN_CONFIG: :invalid_payload/, fn ->
      Code.eval_string(ConsumerConfigTransport.bootstrap_eval_snippet())
    end
  end

  defp decode_payload!(encoded) do
    encoded
    |> Base.decode64!()
    |> :erlang.binary_to_term([:safe])
  end

  defp encode_payload(payload), do: payload |> :erlang.term_to_binary() |> Base.encode64()

  defp encode_payload_with_value(value) do
    encode_payload(%{
      "schema_version" => 1,
      "entries" => [%{"key" => "connections", "value" => value}]
    })
  end

  defp assert_bootstrap_invalid_payload(encoded) do
    System.put_env("FAVN_DEV_CONSUMER_FAVN_CONFIG", encoded)
    purge_bootstrap_module()

    assert_raise RuntimeError, ~r/invalid FAVN_DEV_CONSUMER_FAVN_CONFIG: :invalid_payload/, fn ->
      Code.eval_string(ConsumerConfigTransport.bootstrap_eval_snippet())
    end
  end

  defp duplicate_entries(count) do
    Enum.map(1..count, fn _index -> %{"key" => "connection_modules", "value" => []} end)
  end

  defp duplicate_values(count), do: Enum.map(1..count, &Integer.to_string/1)

  defp duplicate_map_entries(count) do
    Enum.map(1..count, fn index ->
      %{"key" => Integer.to_string(index), "value" => Integer.to_string(index)}
    end)
  end

  defp nested_list_value(depth) do
    Enum.reduce(1..depth, "leaf", fn _index, value ->
      %{"$type" => "list", "items" => [value]}
    end)
  end

  defp purge_bootstrap_module do
    :code.purge(Favn.Dev.ConsumerConfigBootstrap)
    :code.delete(Favn.Dev.ConsumerConfigBootstrap)
    :ok
  end
end
