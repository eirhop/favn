defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Error
  alias FavnDuckdbADBC.TestSupport

  defmodule FakeClient do
    use FavnDuckdbADBC.TestSupport.FakeClient

    alias FavnDuckdbADBC.TestSupport

    @impl true
    def execute(_conn_ref, sql, params) do
      TestSupport.record({:execute, sql, params})

      if TestSupport.mode(:bootstrap_fail_sql, nil) == sql do
        {:error, "failed while running #{sql}"}
      else
        {:ok, nil}
      end
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
    assert %{key: :duckdb_bootstrap, type: {:custom, validator}} = ADBC.bootstrap_schema_field()
    assert :ok = validator.([])
    assert :ok = validator.(%{})
    assert {:error, :expected_duckdb_bootstrap_keyword_or_map} = validator.(:invalid)
  end

  test "runs DuckLake bootstrap statements in configured order" do
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient)

    assert :ok = ADBC.bootstrap(conn, resolved(), [])

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
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient)

    secret_metadata = "postgres://user:password@localhost:5432/ducklake"

    failing_sql =
      "ATTACH '#{secret_metadata}' AS \"lake\" (TYPE ducklake, DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/raw')"

    TestSupport.put_mode(:bootstrap_fail_sql, failing_sql)

    assert {:error,
            %Error{
              operation: :bootstrap,
              connection: :warehouse,
              message: "DuckDB ADBC connection bootstrap failed at attach_lake",
              details: %{
                statement: safe_statement,
                reason: reason,
                adapter_details: adapter_details
              }
            }} = ADBC.bootstrap(conn, resolved(), [])

    refute safe_statement =~ secret_metadata
    refute safe_statement =~ "abfss://lake@storageaccount.dfs.core.windows.net/raw"
    refute reason =~ secret_metadata
    refute inspect(adapter_details) =~ secret_metadata
    assert safe_statement =~ "redacted"
    assert reason =~ "redacted"
  end

  defp resolved do
    %Resolved{
      name: :warehouse,
      adapter: ADBC,
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
      {:execute, sql, []} -> [sql]
      _event -> []
    end)
  end
end
