defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Adapter.DuckDB.ADBC.Bootstrap
  alias Favn.SQL.Error
  alias FavnDuckdbADBC.TestSupport

  @moduletag :tmp_dir

  defmodule FakeClient do
    use FavnDuckdbADBC.TestSupport.FakeClient

    alias FavnDuckdbADBC.TestSupport

    @impl true
    def execute(_conn_ref, sql, params) do
      TestSupport.record({:execute, sql, params})

      if TestSupport.mode(:bootstrap_fail_sql, nil) == sql,
        do: {:error, "failed while running #{sql}"},
        else: {:ok, nil}
    end
  end

  setup do
    TestSupport.start_events()
    on_exit(&TestSupport.reset/0)
    :ok
  end

  test "config schema accepts native script configuration and rejects structured bootstrap", %{tmp_dir: dir} do
    script = write_sql!(dir, "startup.sql", "SET timezone = 'UTC';")

    assert [
             %{key: :open, type: {:custom, open_validator}},
             %{key: :duckdb, type: {:custom, duckdb_validator}} | _
           ] = ADBC.config_schema_fields()

    assert :ok = open_validator.(database: ":memory:")
    assert :ok = duckdb_validator.(startup: [file: script])

    assert {:error, {:unknown_config_keys, :duckdb, [:attach]}} =
             duckdb_validator.(attach: [lake: []])
  end

  test "bootstrap executes startup then selected resources as whole SQL files", %{tmp_dir: dir} do
    startup = write_sql!(dir, "startup.sql", "SET timezone = @timezone;")
    extension = write_sql!(dir, "extension.sql", "INSTALL azure;\nLOAD azure;")
    storage = write_sql!(dir, "storage.sql", "CREATE SECRET landing (TOKEN @token);")

    resolved =
      resolved([
        startup: [file: startup, params: [timezone: "UTC"]],
        resources: [
          landing_storage: [file: storage, params: [token: "token"]],
          azure_extension: [file: extension]
        ],
        catalogs: [lake: [resource: :landing_storage]]
      ])

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

    assert error.details.step == "resource:landing_storage"
    assert error.details.statement =~ "[REDACTED]"
    refute inspect(error) =~ "super-secret"
  end

  test "bootstrap rejects a changed pool fingerprint", %{tmp_dir: dir} do
    script = write_sql!(dir, "startup.sql", "select 1;")
    resolved = resolved(startup: [file: script])

    assert {:error, %Error{details: %{reason: :session_script_fingerprint_changed}}} =
             Bootstrap.run(conn(), resolved, favn_pool_fingerprint: %{session_scripts: %{old: true}})
  end

  defp resolved(duckdb, secret_paths \\ []) do
    %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"], duckdb: duckdb},
      secret_paths: secret_paths
    }
  end

  defp conn do
    %ADBC.Conn{
      db_ref: make_ref(),
      conn_ref: make_ref(),
      connection: :warehouse,
      client: FakeClient,
      max_rows: 100,
      max_result_bytes: 1_000_000
    }
  end

  defp statements do
    TestSupport.events()
    |> Enum.flat_map(fn
      {:execute, sql, []} -> [sql]
      _event -> []
    end)
  end

  defp write_sql!(dir, name, sql) do
    path = Path.join(dir, name)
    File.write!(path, sql)
    path
  end
end
