defmodule FavnDuckdb.SQLAdapterDuckDBBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.Adapter.DuckDB.Bootstrap
  alias Favn.SQL.Error
  alias FavnDuckdb.TestSupport

  @moduletag :tmp_dir

  defmodule FakeClient do
    use FavnDuckdb.TestSupport.FakeClient

    alias FavnDuckdb.TestSupport

    @impl true
    def query(_conn_ref, sql, params) do
      TestSupport.record({:query, sql, params})

      if TestSupport.mode(:bootstrap_fail_sql, nil) == sql,
        do: {:error, "failed while running #{sql}"},
        else: {:ok, make_ref()}
    end
  end

  defmodule ChangingProvider do
    @behaviour Favn.RuntimeValue.Provider

    @impl true
    def fetch_runtime_value(counter) do
      value =
        Agent.get_and_update(counter, fn count -> {{:ok, "token-#{count + 1}"}, count + 1} end)

      value
    end
  end

  setup do
    TestSupport.start_events()
    on_exit(&TestSupport.reset/0)
    :ok
  end

  test "config schema accepts native script configuration and rejects structured bootstrap", %{
    tmp_dir: dir
  } do
    script = write_sql!(dir, "startup.sql", "SET timezone = 'UTC';")

    assert [
             %{key: :open, type: {:custom, open_validator}},
             %{key: :duckdb, type: {:custom, duckdb_validator}} | _
           ] = DuckDB.config_schema_fields()

    assert :ok = open_validator.(database: ":memory:")
    assert :ok = duckdb_validator.(startup: [file: script])

    assert {:error, {:unknown_config_keys, :duckdb, [:load]}} =
             duckdb_validator.(load: [:azure])

    assert {:error, {:invalid_script_file, :absolute_path_required}} =
             duckdb_validator.(startup: [file: "relative.sql"])
  end

  test "bootstrap executes startup then selected resources as whole SQL files", %{tmp_dir: dir} do
    startup = write_sql!(dir, "startup.sql", "SET timezone = @timezone;")
    extension = write_sql!(dir, "extension.sql", "INSTALL azure;\nLOAD azure;")
    storage = write_sql!(dir, "storage.sql", "CREATE SECRET landing (TOKEN @token);")

    resolved =
      resolved(
        startup: [file: startup, params: [timezone: "UTC"]],
        resources: [
          landing_storage: [file: storage, params: [token: "token"]],
          azure_extension: [file: extension]
        ],
        catalogs: [lake: [resource: :landing_storage]]
      )

    assert :ok =
             Bootstrap.run(conn(), resolved,
               required_catalogs: [:lake],
               required_resources: [:azure_extension]
             )

    assert statements() == [
             "SET timezone = 'UTC';",
             "INSTALL azure;\nLOAD azure;",
             "CREATE SECRET landing (TOKEN 'token');"
           ]
  end

  test "bootstrap errors identify the resource and redact secret parameters", %{tmp_dir: dir} do
    script = write_sql!(dir, "storage.sql", "CREATE SECRET landing (TOKEN @token);")
    statement = "CREATE SECRET landing (TOKEN 'super-secret');"
    TestSupport.put_mode(:bootstrap_fail_sql, statement)

    resolved =
      resolved(
        [resources: [landing_storage: [file: script, params: [token: "super-secret"]]]],
        [[:duckdb, :resources, :landing_storage, :params, :token]]
      )

    assert {:error, %Error{} = error} =
             Bootstrap.run(conn(), resolved, required_resources: [:landing_storage])

    assert error.operation == :bootstrap
    assert error.details.step == "resource:landing_storage"
    assert error.details.statement =~ "[REDACTED]"
    refute inspect(error) =~ "super-secret"
  end

  test "bootstrap rejects a changed pool fingerprint", %{tmp_dir: dir} do
    script = write_sql!(dir, "startup.sql", "select 1;")
    resolved = resolved(startup: [file: script])

    assert {:error, %Error{details: %{reason: :session_script_fingerprint_changed}}} =
             Bootstrap.run(conn(), resolved,
               favn_pool_fingerprint: %{session_scripts: %{old: true}}
             )
  end

  test "pool preparation resolves a deferred token only once for bootstrap", %{tmp_dir: dir} do
    script = write_sql!(dir, "storage.sql", "CREATE SECRET landing (TOKEN @token);")
    {:ok, counter} = Agent.start_link(fn -> 0 end)
    token = Favn.RuntimeValue.new(ChangingProvider, counter, secret?: true)

    resolved =
      resolved(resources: [landing_storage: [file: script, params: [token: token]]])

    opts = [required_resources: [:landing_storage], duckdb_client: FakeClient]

    assert {:ok, fingerprint, preparation} = DuckDB.prepare_pool(resolved, opts)
    assert Agent.get(counter, & &1) == 1

    bootstrap_opts =
      opts
      |> Keyword.put(:favn_pool_preparation, preparation)
      |> Keyword.put(:favn_pool_fingerprint, fingerprint)

    assert :ok = Bootstrap.run(conn(), resolved, bootstrap_opts)
    assert Agent.get(counter, & &1) == 1
    assert statements() == ["CREATE SECRET landing (TOKEN 'token-1');"]
  end

  test "the native client executes a multi-statement SQL file as one script", %{tmp_dir: dir} do
    script =
      write_sql!(
        dir,
        "native.sql",
        "CREATE TEMP TABLE session_ready(value INTEGER);\nINSERT INTO session_ready VALUES (42);"
      )

    resolved = resolved(startup: [file: script])

    assert {:ok, conn} = DuckDB.connect(resolved, [])

    try do
      assert :ok = Bootstrap.run(conn, resolved, [])
      assert {:ok, result} = DuckDB.query(conn, "SELECT value FROM session_ready", [])
      assert result.rows == [%{"value" => 42}]
    after
      DuckDB.disconnect(conn, [])
    end
  end

  test "database is read only from open config" do
    assert {:ok, ":memory:"} = Bootstrap.database(resolved([]))

    assert {:error, %Error{type: :invalid_config}} =
             Bootstrap.database(%Resolved{resolved([]) | config: %{database: "old.duckdb"}})
  end

  defp resolved(duckdb, secret_paths \\ []) do
    %Resolved{
      name: :warehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{open: [database: ":memory:"], duckdb: duckdb},
      secret_paths: secret_paths
    }
  end

  defp conn do
    %DuckDB.Conn{
      db_ref: make_ref(),
      conn_ref: make_ref(),
      connection: :warehouse,
      client: FakeClient
    }
  end

  defp statements do
    TestSupport.events()
    |> Enum.flat_map(fn
      {:query, sql, []} -> [sql]
      _event -> []
    end)
  end

  defp write_sql!(dir, name, sql) do
    path = Path.join(dir, name)
    File.write!(path, sql)
    path
  end
end
