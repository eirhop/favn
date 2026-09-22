defmodule FavnDuckdbADBC.RejectedCommitRetryTest do
  use ExUnit.Case, async: false
  @moduletag :adbc_integration
  alias Favn.Connection.{Registry, Resolved}
  alias Favn.RuntimeCatalog.Publication
  alias Favn.SQL.{Client, PoolConfig}
  alias Favn.SQL.Adapter.DuckDB.ADBC

  defmodule Asset do
    use Favn.SQLAsset
    relation(connection: :conflict_test, catalog: "mart", schema: "main", name: "data")
    materialized(:table)

    query do
      ~SQL"SELECT 42 AS value"
    end
  end

  defmodule AppendAsset do
    use Favn.SQLAsset
    relation(connection: :conflict_test, catalog: "mart", schema: "main", name: "data")
    window(Favn.Window.daily(timezone: "Etc/UTC"))
    materialized({:incremental, strategy: :append})

    query do
      ~SQL"SELECT 42 AS value"
    end
  end

  defmodule GroupAsset do
    use Favn.SQLAsset
    relation(connection: :conflict_test, catalog: "mart", schema: "main", name: "data")
    materialized({:incremental, strategy: :replace_groups, replacement_key: [:id]})

    replacement_scope :full do
      ~SQL"SELECT @initial_id AS id"
    end

    replacement_scope :incremental do
      ~SQL"SELECT @changed_id AS id"
    end

    query do
      ~SQL"SELECT id, 42 AS value FROM replacement_scope()"
    end
  end

  defmodule BarrierAdapter do
    for {name, arity} <- ADBC.__info__(:functions),
        name not in [:transaction, :runtime_catalog_backend, :query] do
      args = Macro.generate_arguments(arity, __MODULE__)

      def unquote(name)(unquote_splicing(args)),
        do: apply(ADBC, unquote(name), [unquote_splicing(args)])
    end

    def runtime_catalog_backend, do: __MODULE__
    defdelegate resolve(session, relation, opts), to: ADBC.RuntimeCatalog
    defdelegate prepare(session, publication, relation, opts), to: ADBC.RuntimeCatalog
    defdelegate record(session, prepared, output, opts), to: ADBC.RuntimeCatalog

    def qualify_materialization_retry(session, publication, relation, opts) do
      case :ets.lookup(:favn_751_barrier, :qualification_timeout) do
        [{:qualification_timeout, true}] ->
          Client.query(session, "qualification timeout probe", timeout_ms: 50)

        [] ->
          ADBC.RuntimeCatalog.qualify_materialization_retry(session, publication, relation, opts)
      end
    end

    def query(_, "qualification timeout probe", _), do: Process.sleep(:infinity)
    def query(conn, sql, opts), do: ADBC.query(conn, sql, opts)

    def transaction(conn, fun, opts) do
      case :ets.lookup(:favn_751_barrier, :fault) do
        [{:fault, fault}] -> fault_transaction(conn, fun, opts, fault)
        [] -> concurrent_transaction(conn, fun, opts)
      end
    end

    defp fault_transaction(conn, fun, opts, fault) do
      attempt = :ets.update_counter(:favn_751_barrier, :fault_attempts, 1)
      reject? = fault in [:reject_once, :contradictory] and attempt == 1

      result =
        ADBC.transaction(
          conn,
          fn tx ->
            with {:ok, _} = result <- fun.(tx) do
              if reject?,
                do:
                  {:error, %Favn.SQL.Error{type: :execution_error, message: "injected rejection"}},
                else: result
            end
          end,
          opts
        )

      cond do
        reject? and match?({:error, %{details: %{transaction_outcome: :rolled_back}}}, result) ->
          # Native rollback is confirmed; inject the qualified commit-rejection shape.
          {:error, conflict()}

        fault == :contradictory and match?({:ok, _}, result) ->
          # The native transaction really committed. Contradictory prior proof must not win.
          {:error, %{conflict() | cause: %{unknown_outcome?: true}}}

        fault == :lost_ack and match?({:ok, _}, result) ->
          {:error,
           %Favn.SQL.Error{
             type: :execution_error,
             operation: :transaction,
             message: "commit acknowledgement lost",
             details: %{transaction_stage: :commit, unknown_outcome?: true}
           }}

        true ->
          result
      end
    end

    defp conflict,
      do: %Favn.SQL.Error{
        type: :transaction_conflict,
        operation: :transaction,
        message: "injected rejected commit",
        details: %{transaction_stage: :commit, transaction_outcome: :rolled_back}
      }

    defp concurrent_transaction(conn, fun, opts) do
      result =
        ADBC.transaction(
          conn,
          fn tx ->
            result = fun.(tx)

            if match?({:ok, _}, result) do
              [{:observer, observer}] = :ets.lookup(:favn_751_barrier, :observer)
              count = :ets.update_counter(:favn_751_barrier, :commits, 1)
              send(observer, {:transaction_body, self(), count})

              if count <= 3 do
                receive do
                  :commit -> :ok
                after
                  10_000 -> raise "commit barrier expired"
                end
              end
            end

            result
          end,
          opts
        )

      case result do
        {:error, error} ->
          [{:observer, observer}] = :ets.lookup(:favn_751_barrier, :observer)
          send(observer, {:native_rejection, error})

        _ ->
          :ok
      end

      result
    end
  end

  defmodule PartialNativeAdapter do
    def connect(resolved, _) do
      {:ok, database} =
        Adbc.Database.start_link(
          driver: System.fetch_env!("DUCKDB_ADBC_DRIVER"),
          entrypoint: "duckdb_adbc_init"
        )

      send(resolved.config.observer, {:native_database, database})
      Process.sleep(:infinity)
    end
  end

  test "deadline reaps a real ADBC database allocated before connection checkout" do
    resolved = %Resolved{
      name: :partial,
      module: __MODULE__,
      adapter: PartialNativeAdapter,
      config: %{observer: self()}
    }

    registry =
      start_supervised!(
        {Registry, name: __MODULE__.PartialRegistry, connections: %{partial: resolved}}
      )

    parent = self()

    assert {:error, %{details: %{session_phase: :acquiring, unknown_outcome?: false}}} =
             Client.with_session(:partial, [registry_name: registry, timeout_ms: 300], fn _ ->
               send(parent, :unexpected_callback)
             end)

    assert_receive {:native_database, database}
    refute Process.alive?(database)
    refute_receive :unexpected_callback
  end

  test "three independent managed writers converge after native PostgreSQL DuckLake rejection, cold and warm" do
    root = Path.join(System.tmp_dir!(), "favn_751_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    schema = Path.basename(root)
    uri = URI.parse(System.fetch_env!("FAVN_DATABASE_URL"))
    [user, password] = String.split(uri.userinfo, ":", parts: 2)

    pg =
      "host=#{uri.host} port=#{uri.port} dbname=#{String.trim_leading(uri.path, "/")} user=#{user} password=#{password}"

    startup = Path.join(root, "startup.sql")

    File.write!(
      startup,
      "INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:postgres:#{pg}' AS mart (METADATA_SCHEMA '#{schema}', DATA_PATH '#{root}/data');"
    )

    previous = Application.get_env(:favn, :duckdb_adbc)

    Application.put_env(:favn, :duckdb_adbc,
      driver: System.fetch_env!("DUCKDB_ADBC_DRIVER"),
      entrypoint: "duckdb_adbc_init"
    )

    registry = FavnRunner.ConnectionRegistry

    previous_connections =
      if Process.whereis(registry),
        do: Registry.list(registry_name: registry) |> Map.new(&{&1.name, &1}),
        else: %{}

    resolved = %Resolved{
      name: :conflict_test,
      module: __MODULE__,
      adapter: BarrierAdapter,
      config: %{
        open: [database: ":memory:"],
        pool: %PoolConfig{enabled: false},
        duckdb: [startup: [file: startup], catalogs: [mart: [write_concurrency: 3]]]
      }
    }

    if Process.whereis(registry),
      do:
        Registry.reload(Map.put(previous_connections, :conflict_test, resolved),
          registry_name: registry
        ),
      else: start_supervised!({Registry, name: registry, connections: %{conflict_test: resolved}})

    :ets.new(:favn_751_barrier, [:named_table, :public])
    :ets.insert(:favn_751_barrier, [{:observer, self()}, {:commits, 100}])

    on_exit(fn ->
      if previous,
        do: Application.put_env(:favn, :duckdb_adbc, previous),
        else: Application.delete_env(:favn, :duckdb_adbc)

      if Process.whereis(registry),
        do: Registry.reload(previous_connections, registry_name: registry)

      {:ok, conn} =
        ADBC.connect(%{resolved | adapter: ADBC},
          duckdb_adbc: [
            driver: System.fetch_env!("DUCKDB_ADBC_DRIVER"),
            entrypoint: "duckdb_adbc_init"
          ]
        )

      try do
        assert {:ok, _} =
                 ADBC.execute(
                   conn,
                   "LOAD postgres; ATTACH 'postgres:#{pg}' AS cleanup; DROP SCHEMA cleanup.#{schema} CASCADE",
                   []
                 )
      after
        ADBC.disconnect(conn, [])
      end

      File.rm_rf!(root)
    end)

    assert {:ok, _} = materialize(0, 0)
    assert_receive {:transaction_body, _, _}

    for round <- 1..2 do
      :ets.insert(:favn_751_barrier, {:commits, 0})
      tasks = for n <- 1..3, do: Task.async(fn -> materialize(n, round) end)

      owners =
        for _ <- 1..3 do
          assert_receive {:transaction_body, owner, count}, 15_000
          assert count <= 3
          owner
        end

      assert length(Enum.uniq(owners)) == 3
      Enum.each(owners, &send(&1, :commit))
      results = Enum.map(tasks, &Task.await(&1, 30_000))
      assert Enum.all?(results, &match?({:ok, _}, &1)), inspect(results)
      [{:commits, attempts}] = :ets.lookup(:favn_751_barrier, :commits)
      assert attempts in 4..12

      for _ <- 1..(attempts - 3) do
        assert_receive {:native_rejection,
                        %Favn.SQL.Error{
                          type: :transaction_conflict,
                          message: message,
                          details: %{
                            transaction_stage: :commit,
                            transaction_outcome: :rolled_back,
                            classification: Adbc.Error
                          }
                        }}

        assert message =~ "Transaction conflict - attempting to insert into table with index"
        assert message =~ "but another transaction has deleted inlined data from it"
      end

      drain_bodies()

      assert {:ok, session} =
               Client.connect(:conflict_test,
                 registry_name: registry,
                 required_catalogs: ["mart"]
               )

      try do
        for n <- 1..3 do
          assert {:ok, %{rows: [%{"value" => 42}]}} =
                   Client.query(session, "SELECT * FROM mart.main.data_#{n}", [])
        end

        assert {:ok, %{rows: [%{"n" => receipts}]}} =
                 Client.query(
                   session,
                   "SELECT count(*) AS n FROM mart.favn_runtime.publication",
                   []
                 )

        assert receipts == 1 + round * 3

        assert {:ok, %{rows: [%{"n" => 4}]}} =
                 Client.query(
                   session,
                   "SELECT count(*) AS n FROM mart.favn_runtime.contract_snapshot",
                   []
                 )

        assert {:ok, %{rows: versions}} =
                 Client.query(
                   session,
                   "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded AND extension_name IN ('ducklake', 'postgres_scanner')",
                   []
                 )

        IO.puts("issue751 round=#{round} attempts=#{attempts} native=#{inspect(versions)}")
      after
        Client.disconnect(session)
      end
    end

    :ets.insert(:favn_751_barrier, {:qualification_timeout, true})
    [{:commits, commits_before}] = :ets.lookup(:favn_751_barrier, :commits)
    assert {:error, error, metadata} = materialize(9, 3)
    assert error.details.asset_write_outcome == :not_started
    assert metadata.transaction_outcome == :not_started
    assert metadata.write_outcome == :not_started
    assert [{:commits, ^commits_before}] = :ets.lookup(:favn_751_barrier, :commits)
    :ets.delete(:favn_751_barrier, :qualification_timeout)

    for {fault, n, expected_attempts} <- [{:lost_ack, 4, 1}, {:contradictory, 5, 2}] do
      :ets.insert(:favn_751_barrier, [{:fault, fault}, {:fault_attempts, 0}])
      assert {:error, error, metadata} = materialize(n, 3)
      assert error.details.asset_write_outcome == :unknown
      refute error.details.asset_retryable?
      assert metadata.write_outcome == :unknown

      assert [{:fault_attempts, ^expected_attempts}] =
               :ets.lookup(:favn_751_barrier, :fault_attempts)

      assert {:ok, session} =
               Client.connect(:conflict_test,
                 registry_name: registry,
                 required_catalogs: ["mart"]
               )

      try do
        assert {:ok, %{rows: [%{"value" => 42}]}} =
                 Client.query(session, "SELECT * FROM mart.main.data_#{n}", [])

        assert {:ok, %{rows: [%{"n" => 1}]}} =
                 Client.query(
                   session,
                   "SELECT count(*) AS n FROM mart.favn_runtime.publication WHERE target_id='target_#{n}'",
                   []
                 )
      after
        Client.disconnect(session)
      end
    end

    for {module, n} <- [{AppendAsset, 6}, {GroupAsset, 7}], round <- 4..5 do
      :ets.insert(:favn_751_barrier, [{:fault, :reject_once}, {:fault_attempts, 0}])
      params = if round == 4, do: %{initial_id: 1}, else: %{changed_id: 1}
      assert {:ok, _} = materialize(n, round, module, params)
      assert [{:fault_attempts, 2}] = :ets.lookup(:favn_751_barrier, :fault_attempts)

      assert {:ok, session} =
               Client.connect(:conflict_test,
                 registry_name: registry,
                 required_catalogs: ["mart"]
               )

      try do
        assert {:ok, %{rows: [%{"n" => count}]}} =
                 Client.query(session, "SELECT count(*) AS n FROM mart.main.data_#{n}", [])

        assert count == if(module == AppendAsset, do: round - 3, else: 1)

        assert {:ok, %{rows: [%{"n" => receipts}]}} =
                 Client.query(
                   session,
                   "SELECT count(*) AS n FROM mart.favn_runtime.publication WHERE target_id='target_#{n}'",
                   []
                 )

        assert receipts == round - 3
      after
        Client.disconnect(session)
      end
    end
  end

  defp drain_bodies do
    receive do
      {:transaction_body, _, _} -> drain_bodies()
    after
      0 -> :ok
    end
  end

  defp materialize(n, round, module \\ Asset, params \\ %{}) do
    definition = module.__favn_sql_asset_definition__()

    {:ok, package} =
      Favn.Manifest.ExecutionPackage.new(
        {module, :asset},
        Favn.Manifest.SQLExecution.from_definition(definition)
      )

    asset = %Favn.Manifest.Asset{
      ref: {module, :asset},
      module: module,
      name: :asset,
      type: :sql,
      relation: %{definition.asset.relation | name: "data_#{n}"},
      materialization: definition.materialization,
      window: definition.asset.window_spec,
      target_descriptor:
        struct(Favn.Manifest.TargetDescriptor, adapter: "Elixir.Favn.SQL.Adapter.DuckDB.ADBC")
    }

    window =
      if module == AppendAsset do
        key = Favn.Window.Key.new!(:day, ~U[2026-01-01 00:00:00Z], "Etc/UTC")

        Favn.Window.Runtime.new_range!(
          :day,
          ~U[2026-01-01 00:00:00Z],
          ~U[2026-01-03 00:00:00Z],
          key,
          logical_window_count: 2
        )
      end

    work = %Favn.Contracts.RunnerWork{
      run_id: "run_#{round}",
      metadata: %{window: window},
      asset_step_id: "step_#{n}",
      attempt: 1,
      manifest_version_id: "manifest",
      manifest_content_hash: String.duplicate("a", 64),
      required_runner_release_id: "release",
      logical_target_id: "target_#{n}",
      target_generation_id: "generation"
    }

    {:ok, publication} = Publication.new(asset, work, "workspace", "latest")

    version = %Favn.Manifest.Version{
      manifest_version_id: work.manifest_version_id,
      content_hash: work.manifest_content_hash
    }

    Favn.SQLAsset.Runtime.run_manifest(
      asset,
      package,
      version,
      %{},
      %{work | runtime_publication: publication},
      %Favn.Run.Context{run_id: work.run_id, window: window, params: params}
    )
  end
end
