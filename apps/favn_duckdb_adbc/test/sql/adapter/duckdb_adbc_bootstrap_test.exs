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

      cond do
        TestSupport.mode(:ducklake_metadata_race_sql, nil) == sql and
            Enum.count(TestSupport.events(), &match?({:execute, ^sql, []}, &1)) == 1 ->
          {:error, "duplicate key value violates unique constraint \"pg_type_typname_nsp_index\""}

        TestSupport.mode(:bootstrap_fail_sql, nil) == sql ->
          {:error, "failed while running #{sql}"}

        true ->
          {:ok, nil}
      end
    end
  end

  defmodule ChangingProvider do
    @behaviour Favn.RuntimeValue.Provider

    @impl true
    def fetch_runtime_value(counter) do
      Agent.get_and_update(counter, fn count -> {{:ok, "token-#{count + 1}"}, count + 1} end)
    end
  end

  defmodule ConcurrentMetadataClient do
    use FavnDuckdbADBC.TestSupport.FakeClient

    @impl true
    def execute(agent, _sql, _params) when is_pid(agent) do
      {role, test_pid} =
        Agent.get_and_update(agent, fn %{attempts: attempts} = state ->
          role =
            case attempts do
              0 -> :contender
              1 -> :winner
              _later -> :retry
            end

          {{role, state.test_pid}, %{state | attempts: attempts + 1}}
        end)

      case role do
        :contender ->
          send(test_pid, {:ducklake_metadata_contender, self()})

          receive do
            :ducklake_metadata_committed ->
              {:error,
               "duplicate key value violates unique constraint \"pg_type_typname_nsp_index\""}
          end

        :winner ->
          send(test_pid, :ducklake_metadata_winner)
          {:ok, nil}

        :retry ->
          send(test_pid, :ducklake_metadata_retry)
          {:ok, nil}
      end
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

    assert error.details.step == "resource:landing_storage"
    assert error.details.statement =~ "[REDACTED]"
    refute inspect(error) =~ "super-secret"
  end

  test "bounded bootstrap retry recovers after a DuckLake metadata initialization race", %{
    tmp_dir: dir
  } do
    script = write_sql!(dir, "ducklake.sql", "ATTACH 'ducklake:postgres:' AS lake;")
    statement = "ATTACH 'ducklake:postgres:' AS lake;"
    TestSupport.put_mode(:ducklake_metadata_race_sql, statement)
    resolved = resolved(resources: [lake_metadata: [file: script]])

    assert {:ok, :bootstrapped} =
             Favn.SQL.Retry.run(
               fn ->
                 case Bootstrap.run(conn(), resolved, required_resources: [:lake_metadata]) do
                   :ok -> {:ok, :bootstrapped}
                   {:error, _reason} = error -> error
                 end
               end,
               phase: :session_bootstrap,
               policy: [max_attempts: 2, base_delay_ms: 0, max_delay_ms: 0],
               sleep_fun: fn _delay -> :ok end
             )

    assert statements() == [statement, statement]
  end

  test "ATTACH classifies the known DuckLake metadata race as bootstrap-retryable", %{
    tmp_dir: dir
  } do
    script = write_sql!(dir, "ducklake_error.sql", "ATTACH 'ducklake:postgres:' AS lake;")
    statement = "ATTACH 'ducklake:postgres:' AS lake;"
    TestSupport.put_mode(:ducklake_metadata_race_sql, statement)
    resolved = resolved(resources: [lake_metadata: [file: script]])

    assert {:error,
            %Error{
              retryable?: true,
              operation: :bootstrap,
              details: %{
                classification: :conflict,
                reason: :ducklake_metadata_initialization_race
              }
            }} = Bootstrap.run(conn(), resolved, required_resources: [:lake_metadata])
  end

  test "the DuckLake constraint is non-retryable outside an ATTACH resource", %{tmp_dir: dir} do
    script = write_sql!(dir, "not_attach.sql", "SELECT 1;")
    statement = "SELECT 1;"
    TestSupport.put_mode(:ducklake_metadata_race_sql, statement)
    resolved = resolved(resources: [not_attach: [file: script]])

    assert {:error, %Error{retryable?: false} = error} =
             Bootstrap.run(conn(), resolved, required_resources: [:not_attach])

    refute Map.has_key?(error.details, :reason)
  end

  test "concurrent DuckLake bootstrap callers converge after one metadata creator wins", %{
    tmp_dir: dir
  } do
    script = write_sql!(dir, "concurrent_ducklake.sql", "ATTACH 'ducklake:postgres:' AS lake;")
    resolved = resolved(resources: [lake_metadata: [file: script]])
    test_pid = self()
    {:ok, race} = Agent.start_link(fn -> %{attempts: 0, test_pid: test_pid} end)

    first = Task.async(fn -> retry_bootstrap(race_conn(race), resolved) end)
    assert_receive {:ducklake_metadata_contender, contender}, 500

    second = Task.async(fn -> retry_bootstrap(race_conn(race), resolved) end)
    assert_receive :ducklake_metadata_winner, 500
    send(contender, :ducklake_metadata_committed)

    assert [{:ok, :bootstrapped}, {:ok, :bootstrapped}] = Task.await_many([first, second])
    assert_receive :ducklake_metadata_retry, 500
    assert Agent.get(race, & &1.attempts) == 3
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

    opts = [required_resources: [:landing_storage], duckdb_adbc_client: FakeClient]

    assert {:ok, fingerprint, preparation} = ADBC.prepare_pool(resolved, opts)
    assert Agent.get(counter, & &1) == 1

    bootstrap_opts =
      opts
      |> Keyword.put(:favn_pool_preparation, preparation)
      |> Keyword.put(:favn_pool_fingerprint, fingerprint)

    assert :ok = Bootstrap.run(conn(), resolved, bootstrap_opts)
    assert Agent.get(counter, & &1) == 1
    assert statements() == ["CREATE SECRET landing (TOKEN 'token-1');"]
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

  defp race_conn(agent) do
    %ADBC.Conn{
      db_ref: make_ref(),
      conn_ref: agent,
      connection: :warehouse,
      client: ConcurrentMetadataClient,
      max_rows: 100,
      max_result_bytes: 1_000_000
    }
  end

  defp retry_bootstrap(conn, resolved) do
    Favn.SQL.Retry.run(
      fn ->
        case Bootstrap.run(conn, resolved, required_resources: [:lake_metadata]) do
          :ok -> {:ok, :bootstrapped}
          {:error, _reason} = error -> error
        end
      end,
      phase: :session_bootstrap,
      policy: [max_attempts: 2, base_delay_ms: 0, max_delay_ms: 0],
      sleep_fun: fn _delay -> :ok end
    )
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
