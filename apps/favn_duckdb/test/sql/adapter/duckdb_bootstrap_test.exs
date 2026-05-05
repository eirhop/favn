defmodule FavnDuckdb.SQLAdapterDuckDBBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.Error
  alias FavnDuckdb.TestSupport

  defmodule FakeClient do
    use FavnDuckdb.TestSupport.FakeClient

    alias FavnDuckdb.TestSupport

    @impl true
    def query(_conn_ref, sql, params) do
      TestSupport.record({:query, sql, params})

      if TestSupport.mode(:bootstrap_fail_sql, nil) == sql do
        {:error, "failed while running #{sql}"}
      else
        {:ok, make_ref()}
      end
    end

    @impl true
    def release(resource) do
      TestSupport.record({:release, resource})
      :ok
    end
  end

  setup do
    TestSupport.start_events()

    on_exit(fn ->
      TestSupport.reset()
    end)

    :ok
  end

  test "schema field accepts keyword or map bootstrap config" do
    assert %{key: :duckdb_bootstrap, type: {:custom, validator}} = DuckDB.bootstrap_schema_field()
    assert :ok = validator.([])
    assert :ok = validator.(%{})
    assert {:error, :expected_duckdb_bootstrap_keyword_or_map} = validator.(:invalid)
  end

  test "schema field accepts arbitrary extension identifiers as atoms or binaries" do
    assert %{type: {:custom, validator}} = DuckDB.bootstrap_schema_field()

    assert :ok =
             validator.(extensions: [install: ["ducklake", :json], load: ["postgres", "azure"]])

    assert {:error, {:invalid_identifier, "invalid extension"}} =
             validator.(extensions: [load: ["invalid extension"]])
  end

  test "runs DuckLake bootstrap statements in configured order" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert :ok = DuckDB.bootstrap(conn, resolved(), [])

    assert statements() == [
             "INSTALL ducklake",
             "INSTALL postgres",
             "INSTALL azure",
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure",
             "CREATE SECRET \"azure_adls\" (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'storageaccount')",
             "ATTACH 'postgres://user:password@localhost:5432/ducklake' AS \"lake\" (TYPE ducklake, DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/raw')",
             ~s(USE "lake")
           ]
  end

  test "bootstrap failure reports failing step without exposing secret values" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    secret_metadata = "postgres://user:password@localhost:5432/ducklake"

    failing_sql =
      "ATTACH '#{secret_metadata}' AS \"lake\" (TYPE ducklake, DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/raw')"

    TestSupport.put_mode(:bootstrap_fail_sql, failing_sql)

    assert {:error,
            %Error{
              operation: :bootstrap,
              connection: :warehouse,
              message: "DuckDB connection bootstrap failed at attach_lake",
              details: %{
                step: "attach_lake",
                bootstrap_kind: :ducklake_attach,
                statement: safe_statement,
                reason: reason,
                adapter_details: adapter_details
              }
            }} = DuckDB.bootstrap(conn, resolved(), [])

    refute safe_statement =~ secret_metadata
    refute safe_statement =~ "abfss://lake@storageaccount.dfs.core.windows.net/raw"
    refute reason =~ secret_metadata
    refute inspect(adapter_details) =~ secret_metadata
    assert safe_statement =~ "redacted"
    assert reason =~ "redacted"
  end

  test "invalid bootstrap config returns structured invalid config error" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    resolved = %Resolved{
      resolved()
      | config: %{
          database: ":memory:",
          duckdb_bootstrap: [extensions: [load: ["invalid extension"]]]
        }
    }

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :bootstrap,
              connection: :warehouse,
              details: %{reason: reason}
            }} = DuckDB.bootstrap(conn, resolved, [])

    assert reason =~ "invalid_identifier"
  end

  test "malformed bootstrap config returns structured invalid config errors" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    invalid_configs = [
      [extensions: :bad],
      [secrets: [azure_adls: :bad]],
      [:not_a_keyword_tuple],
      [secrets: [:not_a_secret_tuple]],
      [attach: :bad]
    ]

    for duckdb_bootstrap <- invalid_configs do
      resolved = %Resolved{
        resolved()
        | config: %{database: ":memory:", duckdb_bootstrap: duckdb_bootstrap}
      }

      assert {:error,
              %Error{
                type: :invalid_config,
                operation: :bootstrap,
                connection: :warehouse,
                details: %{reason: reason}
              }} = DuckDB.bootstrap(conn, resolved, [])

      assert is_binary(reason)
    end
  end

  defp resolved do
    %Resolved{
      name: :warehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{
        database: ":memory:",
        duckdb_bootstrap: [
          extensions: [
            install: [:ducklake, :postgres, :azure],
            load: [:ducklake, :postgres, :azure]
          ],
          secrets: [
            azure_adls: [
              type: :azure,
              provider: :credential_chain,
              account_name: "storageaccount"
            ]
          ],
          attach: [
            name: :lake,
            type: :ducklake,
            metadata: "postgres://user:password@localhost:5432/ducklake",
            data_path: "abfss://lake@storageaccount.dfs.core.windows.net/raw"
          ],
          use: :lake
        ]
      },
      secret_fields: [:duckdb_bootstrap]
    }
  end

  defp statements do
    TestSupport.events()
    |> Enum.flat_map(fn
      {:query, sql, []} -> [sql]
      _event -> []
    end)
  end
end
