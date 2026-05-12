defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Adapter.DuckDB.ADBC.Bootstrap
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

  defmodule FakeTokenProvider do
    @behaviour Favn.Azure.PostgresEntraTokenProvider

    alias Favn.Azure.Token

    @impl true
    def fetch_token(_auth, _opts) do
      {:ok, %Token{access_token: "entra-token", expires_on: "1770000000"}}
    end
  end

  defmodule FailingTokenProvider do
    @behaviour Favn.Azure.PostgresEntraTokenProvider

    alias Favn.Azure.TokenError

    @impl true
    def fetch_token(_auth, _opts) do
      {:error,
       %TokenError{
         type: :connection_error,
         message: "managed identity token request failed",
         retryable?: true,
         details: %{status: 429, access_token: "entra-token"}
       }}
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

  test "runs DuckLake bootstrap with ADLS and PostgreSQL secrets" do
    resolved = ducklake_postgres_resolved()
    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    assert :ok = ADBC.bootstrap(conn, resolved, [])

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure",
             "CREATE SECRET \"azure_adls\" (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'storageaccount', CHAIN 'cli;env', SCOPE 'abfss://lake@storageaccount.dfs.core.windows.net/')",
             "CREATE SECRET \"oceanos_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'super-secret')",
             "ATTACH 'ducklake:postgres:sslmode=require' AS \"oceanos_lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/data/', META_SECRET \"oceanos_meta\")",
             ~s(USE "oceanos_lake")
           ]
  end

  test "injects Azure PostgreSQL Entra token into temporary PostgreSQL secret" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    assert :ok = ADBC.bootstrap(conn, resolved, azure_token_provider_module: FakeTokenProvider)

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "CREATE SECRET \"oceanos_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'entra-token')",
             "ATTACH 'ducklake:postgres:sslmode=require' AS \"oceanos_lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/data/', META_SECRET \"oceanos_meta\")",
             ~s(USE "oceanos_lake")
            ]
  end

  test "builds Azure PostgreSQL Entra bootstrap steps without fetching token" do
    resolved = ducklake_postgres_entra_resolved()

    assert {:ok, steps} = Bootstrap.build_steps(resolved, azure_token_provider_module: FakeTokenProvider)

    create_secret = Enum.find(steps, &(&1.id == "create_secret_oceanos_meta"))

    refute IO.iodata_to_binary(create_secret.statement) =~ "entra-token"
    assert IO.iodata_to_binary(create_secret.safe_statement) =~ "PASSWORD 'redacted'"
    assert statements() == []
  end

  test "token acquisition failure returns redacted bootstrap error" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    assert {:error,
            %Error{
              type: :connection_error,
              operation: :bootstrap,
              message: "DuckDB ADBC connection bootstrap failed at create_secret_oceanos_meta",
              details: %{statement: safe_statement, adapter_details: adapter_details}
            }} = ADBC.bootstrap(conn, resolved, azure_token_provider_module: FailingTokenProvider)

    assert safe_statement =~ "PASSWORD 'redacted'"
    refute safe_statement =~ "entra-token"
    refute inspect(adapter_details) =~ "entra-token"
    assert adapter_details.access_token == :redacted
    assert statements() == ["LOAD ducklake", "LOAD postgres"]
  end

  test "PostgreSQL Entra secret execution failure redacts fetched token" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    failing_sql =
      "CREATE SECRET \"oceanos_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'entra-token')"

    TestSupport.put_mode(:bootstrap_fail_sql, failing_sql)

    assert {:error,
            %Error{
              operation: :bootstrap,
              details: %{
                statement: safe_statement,
                reason: reason,
                adapter_details: adapter_details
              }
            }} = ADBC.bootstrap(conn, resolved, azure_token_provider_module: FakeTokenProvider)

    refute safe_statement =~ "entra-token"
    refute reason =~ "entra-token"
    refute inspect(adapter_details) =~ "entra-token"
    assert safe_statement =~ "PASSWORD 'redacted'"
    assert reason =~ "PASSWORD 'redacted'"
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

  defp ducklake_postgres_resolved do
    %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{
        database: ":memory:",
        duckdb_bootstrap: [
          extensions: [load: [:ducklake, :postgres, :azure]],
          secrets: [
            azure_adls: [
              type: :azure,
              provider: :credential_chain,
              account_name: "storageaccount",
              chain: [:cli, :env],
              scope: "abfss://lake@storageaccount.dfs.core.windows.net/"
            ],
            oceanos_meta: [
              type: :postgres,
              host: "pg.example.com",
              port: 5432,
              database: "ducklake",
              user: "ducklake_user",
              password: "super-secret",
              sslmode: :require
            ]
          ],
          attach: [
            name: :oceanos_lake,
            type: :ducklake,
            metadata: [type: :postgres, secret: :oceanos_meta],
            data_path: "abfss://lake@storageaccount.dfs.core.windows.net/data/"
          ],
          use: :oceanos_lake
        ]
      },
      secret_fields: [:duckdb_bootstrap]
    }
  end

  defp ducklake_postgres_entra_resolved do
    %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{
        database: ":memory:",
        duckdb_bootstrap: [
          extensions: [load: [:ducklake, :postgres]],
          secrets: [
            oceanos_meta: [
              type: :postgres,
              host: "pg.example.com",
              port: 5432,
              database: "ducklake",
              user: "ducklake_user",
              auth: [type: :azure_postgres_entra, provider: :azure_cli],
              sslmode: :require
            ]
          ],
          attach: [
            name: :oceanos_lake,
            type: :ducklake,
            metadata: [type: :postgres, secret: :oceanos_meta],
            data_path: "abfss://lake@storageaccount.dfs.core.windows.net/data/"
          ],
          use: :oceanos_lake
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
