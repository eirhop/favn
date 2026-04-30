defmodule FavnDuckdb.SQLAdapterDuckDBBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.Error

  @events_table :favn_duckdb_bootstrap_events

  defmodule FakeClient do
    @behaviour Favn.SQL.Adapter.DuckDB.Client

    @events_table :favn_duckdb_bootstrap_events

    @impl true
    def open(_database), do: {:ok, make_ref()}

    @impl true
    def connection(_db_ref), do: {:ok, make_ref()}

    @impl true
    def query(_conn_ref, sql, params) do
      record({:query, sql, params})

      if Application.get_env(:favn, :duckdb_bootstrap_fail_sql) == sql do
        {:error, "failed while running #{sql}"}
      else
        {:ok, make_ref()}
      end
    end

    @impl true
    def fetch_all(_result_ref), do: []

    @impl true
    def columns(_result_ref), do: []

    @impl true
    def begin_transaction(_conn_ref), do: :ok

    @impl true
    def commit(_conn_ref), do: :ok

    @impl true
    def rollback(_conn_ref), do: :ok

    @impl true
    def appender(_conn_ref, _table_name, _schema), do: {:ok, make_ref()}

    @impl true
    def appender_add_rows(_appender_ref, _rows), do: :ok

    @impl true
    def appender_flush(_appender_ref), do: :ok

    @impl true
    def appender_close(_appender_ref), do: :ok

    @impl true
    def release(resource) do
      record({:release, resource})
      :ok
    end

    defp record(event) do
      if :ets.whereis(@events_table) != :undefined do
        :ets.insert(@events_table, {System.unique_integer([:positive, :monotonic]), event})
      end
    end
  end

  setup do
    if :ets.whereis(@events_table) != :undefined do
      :ets.delete(@events_table)
    end

    :ets.new(@events_table, [:named_table, :ordered_set, :public])
    Application.delete_env(:favn, :duckdb_bootstrap_fail_sql)

    on_exit(fn ->
      Application.delete_env(:favn, :duckdb_bootstrap_fail_sql)

      if :ets.whereis(@events_table) != :undefined do
        :ets.delete(@events_table)
      end
    end)

    :ok
  end

  test "schema field accepts keyword or map bootstrap config" do
    assert %{key: :duckdb_bootstrap, type: {:custom, validator}} = DuckDB.bootstrap_schema_field()
    assert :ok = validator.([])
    assert :ok = validator.(%{})
    assert {:error, :expected_duckdb_bootstrap_keyword_or_map} = validator.(:invalid)
  end

  test "schema field accepts supported extension names as binaries" do
    assert %{type: {:custom, validator}} = DuckDB.bootstrap_schema_field()

    assert :ok = validator.(extensions: [install: ["ducklake"], load: ["postgres", "azure"]])

    assert {:error, {:unsupported_extension, "unknown"}} =
             validator.(extensions: [load: ["unknown"]])
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

    Application.put_env(:favn, :duckdb_bootstrap_fail_sql, failing_sql)

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
      | config: %{database: ":memory:", duckdb_bootstrap: [extensions: [load: [:unknown]]]}
    }

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :bootstrap,
              connection: :warehouse,
              details: %{reason: reason}
            }} = DuckDB.bootstrap(conn, resolved, [])

    assert reason =~ "unsupported_extension"
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
    @events_table
    |> :ets.tab2list()
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
    |> Enum.flat_map(fn
      {:query, sql, []} -> [sql]
      _event -> []
    end)
  end
end
