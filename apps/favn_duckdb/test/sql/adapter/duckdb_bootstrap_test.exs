defmodule FavnDuckdb.SQLAdapterDuckDBBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.Adapter.DuckDB.Bootstrap
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

  defmodule FakeTokenProvider do
    @behaviour Favn.Azure.PostgresEntraTokenProvider

    alias Favn.Azure.Token
    alias FavnDuckdb.TestSupport

    @impl true
    def fetch_token(auth, _opts) do
      TestSupport.record({:token_auth, auth})
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

  test "config schema fields accept open and duckdb config" do
    assert [
             %{key: :open, type: {:custom, open_validator}},
             %{key: :duckdb, type: {:custom, validator}} | _
           ] =
             DuckDB.config_schema_fields()

    assert :ok = open_validator.(database: ":memory:")
    assert :ok = open_validator.(database: ".favn/data/session.duckdb")
    assert {:error, {:missing_open_field, :database}} = open_validator.([])
    assert :ok = validator.([])
    assert :ok = validator.(%{})
    assert {:error, :expected_duckdb_keyword_or_map} = validator.(:invalid)
  end

  test "config schema fields reject old DuckDB connection keys" do
    fields = DuckDB.config_schema_fields()
    message = "DuckDB connection config now uses open: [database: ...] and duckdb: [...]"

    for key <- [:database, :duckdb_bootstrap, :write_concurrency] do
      assert %{type: {:custom, validator}} = Enum.find(fields, &(&1.key == key))
      assert {:error, error} = validator.(":memory:")
      assert error =~ message
    end
  end

  test "schema field accepts arbitrary extension identifiers as atoms or binaries" do
    assert %{type: {:custom, validator}} = duckdb_schema_field()

    assert :ok =
             validator.(load: ["ducklake", :json, "postgres", "azure"])

    assert {:error, {:invalid_identifier, "invalid extension"}} =
             validator.(load: ["invalid extension"])
  end

  test "schema field accepts typed settings and rejects unknown settings" do
    assert %{type: {:custom, validator}} = duckdb_schema_field()

    assert :ok = validator.(settings: [azure_transport_option_type: :curl])
    assert :ok = validator.(settings: [azure_transport_option_type: "default"])

    assert {:error, {:unsupported_setting, :some_unknown_setting}} =
             validator.(settings: [some_unknown_setting: "value"])

    assert {:error, {:invalid_setting_value, :azure_transport_option_type, "bad"}} =
             validator.(settings: [azure_transport_option_type: :bad])
  end

  test "normalizes setting atom and string values into SET statements" do
    assert {:ok, atom_steps} = Bootstrap.build_steps(settings_resolved(:curl))
    assert {:ok, string_steps} = Bootstrap.build_steps(settings_resolved("default"))

    assert Enum.find(atom_steps, &(&1.id == "set_azure_transport_option_type"))
           |> Map.fetch!(:statement)
           |> IO.iodata_to_binary() == "SET azure_transport_option_type = 'curl'"

    assert Enum.find(string_steps, &(&1.id == "set_azure_transport_option_type"))
           |> Map.fetch!(:statement)
           |> IO.iodata_to_binary() == "SET azure_transport_option_type = 'default'"
  end

  test "schema field validates Azure credential chain and scope" do
    assert %{type: {:custom, validator}} = duckdb_schema_field()

    assert :ok =
             validator.(
               secrets: [
                 azure_adls: [
                   type: :azure,
                   provider: :credential_chain,
                   account_name: "storageaccount",
                   chain: [:cli, "env"],
                   scope: "abfss://lake@storageaccount.dfs.core.windows.net/"
                 ]
               ]
             )

    assert {:error, {:invalid_azure_credential_chain, "bad"}} =
             validator.(
               secrets: [
                 azure_adls: [
                   type: :azure,
                   provider: :credential_chain,
                   account_name: "storageaccount",
                   chain: [:cli, "bad"]
                 ]
               ]
             )

    assert {:error, {:invalid_azure_scope, :missing_trailing_slash}} =
             validator.(
               secrets: [
                 azure_adls: [
                   type: :azure,
                   provider: :credential_chain,
                   account_name: "storageaccount",
                   scope: "abfss://lake@storageaccount.dfs.core.windows.net"
                 ]
               ]
             )
  end

  test "schema field redacts invalid PostgreSQL password values" do
    assert %{type: {:custom, validator}} = duckdb_schema_field()

    assert {:error, {:invalid_secret_field, "lakehouse_meta", :password}} =
             validator.(
               secrets: [
                 lakehouse_meta: [
                   type: :postgres,
                   host: "pg.example.com",
                   port: 5432,
                   database: "ducklake",
                   user: "ducklake_user",
                   password: {:raw_secret, "super-secret"}
                 ]
               ]
             )
  end

  test "schema field accepts Azure PostgreSQL Entra auth and rejects password conflicts" do
    assert %{type: {:custom, validator}} = duckdb_schema_field()

    assert :ok =
             validator.(
               secrets: [
                 lakehouse_meta: [
                   type: :postgres,
                   host: "pg.example.com",
                   port: 5432,
                   database: "ducklake",
                   user: "ducklake_user",
                   auth: [
                     type: :azure_postgres_entra,
                     provider: :managed_identity,
                     client_id: "client-1",
                     endpoint: :auto
                   ]
                 ]
               ]
             )

    assert {:error, {:conflicting_secret_fields, "lakehouse_meta", [:password, :auth]}} =
             validator.(
               secrets: [
                 lakehouse_meta: [
                   type: :postgres,
                   host: "pg.example.com",
                   port: 5432,
                   database: "ducklake",
                   user: "ducklake_user",
                   password: "super-secret",
                   auth: [type: :azure_postgres_entra, provider: :azure_cli]
                 ]
               ]
             )
  end

  test "runs DuckLake bootstrap statements in configured order" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert :ok = DuckDB.bootstrap(conn, resolved(), [])

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure",
             "CREATE SECRET \"azure_adls\" (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'storageaccount')",
             "CREATE SECRET \"lakehouse_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'super-secret')",
             "ATTACH 'ducklake:postgres:' AS \"lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/raw', META_SECRET \"lakehouse_meta\")",
             ~s(USE "lake")
           ]
  end

  test "runs multiple DuckDB catalog attach statements" do
    resolved = duckdb_catalogs_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    assert :ok = DuckDB.bootstrap(conn, resolved, [])

    assert statements() == [
             ~s(ATTACH '.favn/data/raw.duckdb' AS "raw"),
             ~s(ATTACH '.favn/data/mart.duckdb' AS "mart")
           ]
  end

  test "required_catalogs filters planned DuckDB catalog attach steps" do
    assert {:ok, steps} =
             Bootstrap.build_steps(duckdb_catalogs_resolved(), required_catalogs: ["raw"])

    assert planned_statements(steps) == [~s(ATTACH '.favn/data/raw.duckdb' AS "raw")]
  end

  test "validates SQLite and PostgreSQL DuckLake attachment requirements" do
    assert %{type: {:custom, validator}} = duckdb_schema_field()

    assert :ok =
             validator.(
               attach: [
                 source: [
                   type: :ducklake,
                   metadata: "ducklake:sqlite:/absolute/path/source.sqlite",
                   data_path: "/absolute/path/files/source",
                   write_concurrency: 1
                 ]
               ]
             )

    assert {:error, :empty_ducklake_sqlite_metadata_path} =
             validator.(
               attach: [
                 source: [
                   type: :ducklake,
                   metadata: "ducklake:sqlite:",
                   data_path: "/absolute/path/files/source"
                 ]
               ]
             )

    for data_path <- [nil, ""] do
      assert {:error, {:missing_attach_field, :data_path}} =
               validator.(
                 attach: [
                   source: [
                     type: :ducklake,
                     metadata: "ducklake:sqlite:/absolute/path/source.sqlite",
                     data_path: data_path
                   ]
                 ]
               )
    end

    assert {:error, {:invalid_write_concurrency, 0}} =
             validator.(
               attach: [
                 source: [
                   type: :ducklake,
                   metadata: "ducklake:sqlite:/absolute/path/source.sqlite",
                   data_path: "/absolute/path/files/source",
                   write_concurrency: 0
                 ]
               ]
             )

    assert {:error, {:missing_attach_field, :meta_secret}} =
             validator.(
               attach: [
                 source: [
                   type: :ducklake,
                   metadata: "ducklake:postgres:",
                   data_path: "/absolute/path/files/source"
                 ]
               ]
             )

    assert {:error, {:unknown_ducklake_meta_secret, "source", "missing_meta"}} =
             validator.(
               attach: [
                 source: [
                   type: :ducklake,
                   metadata: "ducklake:postgres:",
                   meta_secret: :missing_meta,
                   data_path: "/absolute/path/files/source"
                 ]
               ]
             )
  end

  test "builds and filters SQLite DuckLake attach statements without META_SECRET" do
    assert {:ok, all_steps} = Bootstrap.build_steps(ducklake_sqlite_resolved())

    assert planned_statements(all_steps) == [
             "ATTACH 'ducklake:sqlite:/absolute/path/source.sqlite' AS \"source\" (DATA_PATH '/absolute/path/files/source')",
             "ATTACH 'ducklake:sqlite:/absolute/path/mart.sqlite' AS \"mart\" (DATA_PATH '/absolute/path/files/mart')"
           ]

    assert {:ok, source_steps} =
             Bootstrap.build_steps(ducklake_sqlite_resolved(), required_catalogs: [:source])

    assert planned_statements(source_steps) == [
             "ATTACH 'ducklake:sqlite:/absolute/path/source.sqlite' AS \"source\" (DATA_PATH '/absolute/path/files/source')"
           ]
  end

  test "nil required_catalogs preserves all planned DuckDB catalog attach steps" do
    assert {:ok, steps} =
             Bootstrap.build_steps(duckdb_catalogs_resolved(), required_catalogs: nil)

    assert planned_statements(steps) == [
             ~s(ATTACH '.favn/data/raw.duckdb' AS "raw"),
             ~s(ATTACH '.favn/data/mart.duckdb' AS "mart")
           ]
  end

  test "required_catalogs skips planned USE when configured catalog is filtered out" do
    resolved = duckdb_catalogs_resolved(use: :mart)

    assert {:ok, steps} = Bootstrap.build_steps(resolved, required_catalogs: [:raw])

    assert planned_statements(steps) == [~s(ATTACH '.favn/data/raw.duckdb' AS "raw")]
  end

  test "required_catalogs filters DuckLake secrets when catalog is filtered out" do
    assert {:ok, steps} =
             Bootstrap.build_steps(ducklake_postgres_resolved(), required_catalogs: [:raw])

    assert planned_statements(steps) == [
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure"
           ]
  end

  test "runs DuckLake bootstrap with ADLS and PostgreSQL secrets" do
    resolved = ducklake_postgres_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    assert :ok = DuckDB.bootstrap(conn, resolved, [])

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure",
             "CREATE SECRET \"azure_adls\" (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'storageaccount', CHAIN 'cli;env', SCOPE 'abfss://lake@storageaccount.dfs.core.windows.net/')",
             "CREATE SECRET \"lakehouse_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'super-secret')",
             "ATTACH 'ducklake:postgres:sslmode=require' AS \"lakehouse_lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/data/', META_SECRET \"lakehouse_meta\")",
             ~s(USE "lakehouse_lake")
           ]
  end

  test "runs settings after extension load and before secrets, attach, and use" do
    resolved = ducklake_postgres_resolved(settings: [azure_transport_option_type: :curl])
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    assert :ok = DuckDB.bootstrap(conn, resolved, [])

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure",
             "SET azure_transport_option_type = 'curl'",
             "CREATE SECRET \"azure_adls\" (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'storageaccount', CHAIN 'cli;env', SCOPE 'abfss://lake@storageaccount.dfs.core.windows.net/')",
             "CREATE SECRET \"lakehouse_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'super-secret')",
             "ATTACH 'ducklake:postgres:sslmode=require' AS \"lakehouse_lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/data/', META_SECRET \"lakehouse_meta\")",
             ~s(USE "lakehouse_lake")
           ]
  end

  test "injects Azure PostgreSQL Entra token into temporary PostgreSQL secret" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    assert :ok = DuckDB.bootstrap(conn, resolved, azure_token_provider_module: FakeTokenProvider)

    assert {:token_auth,
            [
              type: :azure_postgres_entra,
              provider: :managed_identity,
              client_id: "client-1",
              endpoint: :auto
            ]} in TestSupport.events()

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "CREATE SECRET \"lakehouse_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'entra-token')",
             "ATTACH 'ducklake:postgres:sslmode=require' AS \"lakehouse_lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/data/', META_SECRET \"lakehouse_meta\")",
             ~s(USE "lakehouse_lake")
           ]
  end

  test "builds Azure PostgreSQL Entra bootstrap steps without fetching token" do
    resolved = ducklake_postgres_entra_resolved()

    assert {:ok, steps} =
             Bootstrap.build_steps(resolved, azure_token_provider_module: FakeTokenProvider)

    refute {:token_auth,
            [
              type: :azure_postgres_entra,
              provider: :managed_identity,
              client_id: "client-1",
              endpoint: :auto
            ]} in TestSupport.events()

    create_secret = Enum.find(steps, &(&1.id == "create_secret_lakehouse_meta"))

    refute IO.iodata_to_binary(create_secret.statement) =~ "entra-token"
    assert IO.iodata_to_binary(create_secret.safe_statement) =~ "PASSWORD 'redacted'"
  end

  test "token acquisition failure returns redacted bootstrap error" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    assert {:error,
            %Error{
              type: :connection_error,
              operation: :bootstrap,
              message: "DuckDB connection bootstrap failed at create_secret_lakehouse_meta",
              details: %{statement: safe_statement, adapter_details: adapter_details}
            }} =
             DuckDB.bootstrap(conn, resolved, azure_token_provider_module: FailingTokenProvider)

    assert safe_statement =~ "PASSWORD 'redacted'"
    refute safe_statement =~ "entra-token"
    refute inspect(adapter_details) =~ "entra-token"
    assert adapter_details.access_token == :redacted
    assert statements() == ["LOAD ducklake", "LOAD postgres"]
  end

  test "PostgreSQL Entra secret execution failure redacts fetched token" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    failing_sql =
      "CREATE SECRET \"lakehouse_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'entra-token')"

    TestSupport.put_mode(:bootstrap_fail_sql, failing_sql)

    assert {:error,
            %Error{
              operation: :bootstrap,
              details: %{
                statement: safe_statement,
                reason: reason,
                adapter_details: adapter_details
              }
            }} = DuckDB.bootstrap(conn, resolved, azure_token_provider_module: FakeTokenProvider)

    refute safe_statement =~ "entra-token"
    refute reason =~ "entra-token"
    refute inspect(adapter_details) =~ "entra-token"
    assert safe_statement =~ "PASSWORD 'redacted'"
    assert reason =~ "PASSWORD 'redacted'"
  end

  test "bootstrap failure reports failing step without exposing secret values" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    secret_metadata = "ducklake:postgres:"

    failing_sql =
      "ATTACH '#{secret_metadata}' AS \"lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/raw', META_SECRET \"lakehouse_meta\")"

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

  test "setting execution failure reports bootstrap setting step" do
    resolved = settings_resolved(:curl)
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    TestSupport.put_mode(:bootstrap_fail_sql, "SET azure_transport_option_type = 'curl'")

    assert {:error,
            %Error{
              operation: :bootstrap,
              details: %{
                step: "set_azure_transport_option_type",
                bootstrap_kind: :set_setting,
                statement: "SET azure_transport_option_type = 'curl'"
              }
            }} = DuckDB.bootstrap(conn, resolved, [])
  end

  test "PostgreSQL secret bootstrap failure redacts password only" do
    resolved = ducklake_postgres_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    failing_sql =
      "CREATE SECRET \"lakehouse_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'super-secret')"

    TestSupport.put_mode(:bootstrap_fail_sql, failing_sql)

    assert {:error,
            %Error{
              operation: :bootstrap,
              details: %{
                statement: safe_statement,
                reason: reason,
                adapter_details: adapter_details
              }
            }} = DuckDB.bootstrap(conn, resolved, [])

    refute safe_statement =~ "super-secret"
    refute reason =~ "super-secret"
    refute inspect(adapter_details) =~ "super-secret"
    assert safe_statement =~ "HOST 'pg.example.com'"
    assert safe_statement =~ "USER 'ducklake_user'"
    assert safe_statement =~ "PASSWORD 'redacted'"
  end

  test "invalid bootstrap config returns structured invalid config error" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    resolved = %Resolved{
      resolved()
      | config: %{
          open: [database: ":memory:"],
          duckdb: [load: ["invalid extension"]]
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
      [load: :bad],
      [secrets: [azure_adls: :bad]],
      [:not_a_keyword_tuple],
      [secrets: [:not_a_secret_tuple]],
      [attach: :bad]
    ]

    for duckdb <- invalid_configs do
      resolved = %Resolved{
        resolved()
        | config: %{open: [database: ":memory:"], duckdb: duckdb}
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
        open: [database: ":memory:"],
        duckdb: [
          load: [:ducklake, :postgres, :azure],
          secrets: [
            azure_adls: [
              type: :azure,
              provider: :credential_chain,
              account_name: "storageaccount"
            ],
            lakehouse_meta: [
              type: :postgres,
              host: "pg.example.com",
              port: 5432,
              database: "ducklake",
              user: "ducklake_user",
              password: "super-secret"
            ]
          ],
          attach: [
            lake: [
              type: :ducklake,
              metadata: "ducklake:postgres:",
              meta_secret: :lakehouse_meta,
              data_path: "abfss://lake@storageaccount.dfs.core.windows.net/raw"
            ]
          ],
          use: :lake
        ]
      },
      secret_fields: [:duckdb]
    }
  end

  defp duckdb_schema_field do
    Enum.find(DuckDB.config_schema_fields(), &(&1.key == :duckdb))
  end

  defp ducklake_postgres_resolved(opts \\ []) do
    settings = Keyword.get(opts, :settings, [])

    %Resolved{
      name: :warehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{
        open: [database: ":memory:"],
        duckdb: [
          load: [:ducklake, :postgres, :azure],
          settings: settings,
          secrets: [
            azure_adls: [
              type: :azure,
              provider: :credential_chain,
              account_name: "storageaccount",
              chain: [:cli, :env],
              scope: "abfss://lake@storageaccount.dfs.core.windows.net/"
            ],
            lakehouse_meta: [
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
            lakehouse_lake: [
              type: :ducklake,
              metadata: "ducklake:postgres:",
              meta_secret: :lakehouse_meta,
              data_path: "abfss://lake@storageaccount.dfs.core.windows.net/data/"
            ]
          ],
          use: :lakehouse_lake
        ]
      },
      secret_fields: [:duckdb]
    }
  end

  defp duckdb_catalogs_resolved(opts \\ []) do
    use_catalog = Keyword.get(opts, :use)

    duckdb = [
      attach: [
        raw: [type: :duckdb, path: ".favn/data/raw.duckdb"],
        mart: [type: :duckdb, path: ".favn/data/mart.duckdb"]
      ]
    ]

    duckdb = duckdb ++ if(use_catalog, do: [use: use_catalog], else: [])

    %Resolved{
      name: :important_lakehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{
        open: [database: ":memory:"],
        duckdb: duckdb
      },
      secret_fields: []
    }
  end

  defp ducklake_sqlite_resolved do
    %Resolved{
      name: :local_lakehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{
        open: [database: ":memory:"],
        duckdb: [
          attach: [
            source: [
              type: :ducklake,
              metadata: "ducklake:sqlite:/absolute/path/source.sqlite",
              data_path: "/absolute/path/files/source",
              write_concurrency: 1
            ],
            mart: [
              type: :ducklake,
              metadata: "ducklake:sqlite:/absolute/path/mart.sqlite",
              data_path: "/absolute/path/files/mart",
              write_concurrency: 1
            ]
          ]
        ]
      },
      secret_fields: []
    }
  end

  defp settings_resolved(value) do
    %Resolved{
      name: :warehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{
        open: [database: ":memory:"],
        duckdb: [
          load: [:azure],
          settings: [azure_transport_option_type: value]
        ]
      },
      secret_fields: [:duckdb]
    }
  end

  defp ducklake_postgres_entra_resolved do
    %Resolved{
      name: :warehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{
        open: [database: ":memory:"],
        duckdb: [
          load: [:ducklake, :postgres],
          secrets: [
            lakehouse_meta: [
              type: :postgres,
              host: "pg.example.com",
              port: 5432,
              database: "ducklake",
              user: "ducklake_user",
              auth: [
                type: :azure_postgres_entra,
                provider: :managed_identity,
                client_id: "client-1",
                endpoint: :auto
              ],
              sslmode: :require
            ]
          ],
          attach: [
            lakehouse_lake: [
              type: :ducklake,
              metadata: "ducklake:postgres:",
              meta_secret: :lakehouse_meta,
              data_path: "abfss://lake@storageaccount.dfs.core.windows.net/data/"
            ]
          ],
          use: :lakehouse_lake
        ]
      },
      secret_fields: [:duckdb]
    }
  end

  defp statements do
    TestSupport.events()
    |> Enum.flat_map(fn
      {:query, sql, []} -> [sql]
      _event -> []
    end)
  end

  defp planned_statements(steps) do
    Enum.map(steps, &IO.iodata_to_binary(&1.statement))
  end
end
