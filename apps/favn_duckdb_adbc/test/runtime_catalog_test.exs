defmodule FavnDuckdbADBC.RuntimeCatalogTest do
  use ExUnit.Case, async: false
  @moduletag :adbc_integration
  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.RuntimeCatalog.Publication
  alias Favn.SQL.{Client, Session}
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Adapter.DuckDB.ADBC.RuntimeCatalog

  defmodule BarrierAdapter do
    alias Favn.SQL.Adapter.DuckDB.ADBC
    def runtime_catalog_backend, do: ADBC.runtime_catalog_backend()
    def execute(conn, sql, opts), do: ADBC.execute(conn, sql, opts)
    def transaction(conn, fun, opts), do: ADBC.transaction(conn, fun, opts)

    def query(conn, sql, opts) do
      result = ADBC.query(conn, sql, opts)

      if barrier = :persistent_term.get({__MODULE__, :barrier}, nil) do
        {parent, fragment, expected} =
          case barrier do
            {parent, fragment} -> {parent, fragment, :absent}
            other -> other
          end

        matches =
          case {expected, result} do
            {:absent, {:ok, %{rows: []}}} -> true
            {:present, {:ok, %{rows: [_ | _]}}} -> true
            _ -> false
          end

        if String.contains?(IO.iodata_to_binary(sql), fragment) and
             matches do
          send(parent, {:absence_observed, self()})

          receive do
            :continue -> :ok
          after
            5000 -> raise "absence barrier expired"
          end
        end
      end

      result
    end
  end

  defmodule LostAckClient do
    alias Favn.SQL.Adapter.DuckDB.ADBC.Client.ADBC, as: Native

    for {name, arity} <- [
          query: 3,
          execute: 3,
          fetch_all: 4,
          columns: 1,
          begin_transaction: 1,
          rollback: 1,
          release: 1
        ] do
      args = Macro.generate_arguments(arity, __MODULE__)

      def unquote(name)(unquote_splicing(args)),
        do: apply(Native, unquote(name), [unquote_splicing(args)])
    end

    def commit(conn) do
      :ok = Native.commit(conn)
      {:error, :lost_commit_acknowledgement}
    end
  end

  defmodule PlainAsset do
    use Favn.SQLAsset
    relation(connection: :runtime_test, catalog: "mart", schema: "main", name: "plain")
    materialized(:table)

    query do
      ~SQL"SELECT 42 AS value"
    end
  end

  defmodule UnqualifiedAsset do
    use Favn.SQLAsset
    relation(connection: :runtime_test, name: "unqualified")
    materialized(:table)

    query do
      ~SQL"SELECT 1 AS value"
    end
  end

  defmodule AppendAsset do
    use Favn.SQLAsset
    relation(connection: :runtime_test, catalog: "mart", schema: "main", name: "append_rows")
    window(Favn.Window.daily(timezone: "Etc/UTC"))
    materialized({:incremental, strategy: :append})

    query do
      ~SQL"SELECT 42 AS value"
    end
  end

  defmodule WindowAsset do
    use Favn.SQLAsset
    relation(connection: :runtime_test, catalog: "mart", schema: "main", name: "window_rows")
    window(Favn.Window.daily(timezone: "Etc/UTC"))
    materialized({:incremental, strategy: :delete_insert, window_column: :day})

    query do
      ~SQL"SELECT 42 AS value, TIMESTAMP '2026-01-01 12:00:00' AS day WHERE @include_rows"
    end
  end

  defmodule GroupAsset do
    use Favn.SQLAsset
    relation(connection: :runtime_test, catalog: "mart", schema: "main", name: "group_rows")
    materialized({:incremental, strategy: :replace_groups, replacement_key: [:id]})

    replacement_scope :incremental do
      ~SQL"SELECT 1 AS id"
    end

    replacement_scope :full do
      ~SQL"SELECT 1 AS id"
    end

    query do
      ~SQL"SELECT id, 42 AS value FROM replacement_scope() WHERE @include_rows"
    end
  end

  defmodule EmptyGroupAsset do
    use Favn.SQLAsset
    relation(connection: :runtime_test, catalog: "mart", schema: "main", name: "empty_group_rows")
    materialized({:incremental, strategy: :replace_groups, replacement_key: [:id]})

    replacement_scope :incremental do
      ~SQL"SELECT 1 AS id"
    end

    replacement_scope :full do
      ~SQL"SELECT 1 AS id"
    end

    query do
      ~SQL"SELECT id, 42 AS value FROM replacement_scope() WHERE false"
    end
  end

  for backend <- [:duckdb, :ducklake] do
    test "#{backend}: retry qualification uses native destination type and excludes candidates" do
      with_session(unquote(backend), fn session ->
        expected = unquote(if backend == :ducklake, do: :supported, else: :unsupported)
        session = %{session | required_catalogs: ["mart", "read_only_input"]}

        assert {:ok, ^expected} =
                 Favn.SQL.RuntimeCatalog.qualify_materialization_retry(
                   session,
                   publication(),
                   relation(),
                   []
                 )

        assert {:ok, :unsupported} =
                 Favn.SQL.RuntimeCatalog.qualify_materialization_retry(
                   session,
                   %{publication() | candidate: true},
                   relation(),
                   []
                 )
      end)
    end

    test "#{backend}: lost commit acknowledgement stays unknown despite a committed receipt" do
      with_session(unquote(backend), fn s ->
        fault = %{s | conn: %{s.conn | client: LostAckClient}}
        assert {:error, error} = write(fault, publication(), 7)
        assert error.details.transaction_stage == :rollback
        assert error.details.original_error.details.transaction_stage == :commit
        assert [%{"value" => 7}] = query(s, "SELECT * FROM mart.main.data")
        assert [%{"n" => 1}] = query(s, "SELECT count(*) n FROM mart.favn_runtime.publication")
        assert {:error, _} = write(s, publication(), 8)
        assert [%{"value" => 7}] = query(s, "SELECT * FROM mart.main.data")
      end)
    end

    test "#{backend}: foreign schema objects and damaged managed views fail before data changes" do
      with_session(unquote(backend), fn s ->
        assert {:ok, _} = Client.execute(s, "CREATE SCHEMA mart.favn_runtime", [])

        assert {:ok, _} =
                 Client.execute(
                   s,
                   "CREATE TABLE mart.favn_runtime.foreign_object(value INTEGER)",
                   []
                 )

        assert {:error, _} = write(s, publication(), 1)
        assert {:ok, _} = Client.execute(s, "DROP TABLE mart.favn_runtime.foreign_object", [])
        assert {:ok, _} = write(s, publication(), 1)
        assert {:ok, _} = Client.execute(s, "DROP VIEW mart.favn_runtime.freshness", [])
        assert {:error, _} = write(s, %{publication() | publication_id: "rp_damage"}, 2)
        assert [%{"value" => 1}] = query(s, "SELECT * FROM mart.main.data")
      end)
    end

    test "#{backend}: runner atomically publishes managed SQL mutations and preserves skipped-write identity" do
      root = Path.join(System.tmp_dir!(), "runtime-runner-#{System.unique_integer([:positive])}")
      File.mkdir_p!(root)
      backend = unquote(backend)
      startup = Path.join(root, "startup.sql")

      sql =
        startup_sql(backend, root) <>
          "CREATE SCHEMA IF NOT EXISTS mart.sales; CREATE TABLE IF NOT EXISTS mart.sales.unqualified(value INTEGER); SET search_path='memory.main,mart.sales';"

      File.write!(startup, sql)
      old = Application.get_env(:favn, :duckdb_adbc)

      Application.put_env(:favn, :duckdb_adbc,
        driver: System.fetch_env!("DUCKDB_ADBC_DRIVER"),
        entrypoint: "duckdb_adbc_init"
      )

      on_exit(fn ->
        if old,
          do: Application.put_env(:favn, :duckdb_adbc, old),
          else: Application.delete_env(:favn, :duckdb_adbc)

        File.rm_rf!(root)
      end)

      resolved = %Resolved{
        name: :runtime_test,
        adapter: ADBC,
        module: __MODULE__,
        config: %{
          open: [database: ":memory:"],
          duckdb: [startup: [file: startup], catalogs: [mart: [write_concurrency: 1]]],
          pool: %Favn.SQL.PoolConfig{enabled: false}
        }
      }

      registry = FavnRunner.ConnectionRegistry

      if Process.whereis(registry) do
        previous =
          Favn.Connection.Registry.list(registry_name: registry) |> Map.new(&{&1.name, &1})

        on_exit(fn -> Favn.Connection.Registry.reload(previous, registry_name: registry) end)

        Favn.Connection.Registry.reload(Map.put(previous, :runtime_test, resolved),
          registry_name: registry
        )
      else
        start_supervised!(
          {Favn.Connection.Registry, name: registry, connections: %{runtime_test: resolved}}
        )
      end

      for module <- [
            PlainAsset,
            UnqualifiedAsset,
            AppendAsset,
            WindowAsset,
            GroupAsset,
            EmptyGroupAsset
          ] do
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
          relation: definition.asset.relation,
          materialization: definition.materialization,
          window: definition.asset.window_spec,
          target_descriptor:
            struct(Favn.Manifest.TargetDescriptor, adapter: "Elixir.Favn.SQL.Adapter.DuckDB.ADBC")
        }

        window =
          if module in [AppendAsset, WindowAsset] do
            key = Favn.Window.Key.new!(:day, ~U[2026-01-01 00:00:00Z], "Etc/UTC")

            Favn.Window.Runtime.new!(
              :day,
              ~U[2026-01-01 00:00:00Z],
              ~U[2026-01-02 00:00:00Z],
              key
            )
          end

        work = %Favn.Contracts.RunnerWork{
          run_id: "run",
          asset_step_id: inspect(module),
          attempt: 1,
          metadata: %{window: window},
          manifest_version_id: "manifest",
          manifest_content_hash: String.duplicate("a", 64),
          required_runner_release_id: "release",
          logical_target_id: inspect(module),
          target_generation_id: "11111111-1111-4111-8111-111111111111",
          target_operation:
            if(module == UnqualifiedAsset, do: nil, else: :normal_materialization),
          write_relation: asset.relation
        }

        version = %Favn.Manifest.Version{
          manifest_version_id: work.manifest_version_id,
          content_hash: work.manifest_content_hash
        }

        expected =
          if module != UnqualifiedAsset do
            %Favn.Contracts.GenerationPrecondition{
              mode: :initial,
              marker: %Favn.Contracts.GenerationMarker{
                target_id: work.logical_target_id,
                active_relation: asset.relation,
                active_generation_id: work.target_generation_id,
                activation_operation_id: "initial-runtime",
                activation_token: "initial-runtime",
                activated_at: ~U[2026-09-24 12:00:00Z]
              }
            }
          end

        attempts = if module in [PlainAsset, GroupAsset, WindowAsset], do: [1, 2, 3], else: [1, 2]

        Enum.reduce(attempts, expected, fn attempt, expected ->
          work = %{work | attempt: attempt + 1}
          {:ok, p} = Publication.new(asset, work, "workspace", "latest")
          work = %{work | runtime_publication: p}

          context = %Favn.Run.Context{
            run_id: "run",
            window: window,
            params: %{include_rows: attempt in [1, 3]},
            run_started_at: ~U[2026-01-01 12:00:00Z]
          }

          execution =
            if module == PlainAsset and attempt in [1, 3] do
              sql = "SELECT false AS passed"

              check =
                Favn.SQL.Check.new!(
                  name: :skip_write,
                  when: :target_exists,
                  uses_query?: false,
                  uses_target?: false,
                  at: :before_materialize,
                  on_violation: :skip_materialization,
                  sql: sql,
                  template: Favn.SQL.Template.compile!(sql, file: "native-skip.sql", line: 1)
                )

              {:ok, skipped} =
                Favn.Manifest.ExecutionPackage.new(asset.ref, %{
                  package.sql_execution
                  | checks: [check]
                })

              skipped
            else
              package
            end

          result =
            Favn.SQLAsset.Runtime.run_manifest(
              asset,
              execution,
              version,
              %{},
              work,
              context,
              expected
            )

          assert {:ok, output} = result

          if module == PlainAsset and attempt == 3 do
            assert output.write_outcome == :no_op
            assert is_nil(output.runtime_publication)
          else
            assert output.runtime_publication["publication_id"] == p.publication_id
          end

          if attempt == 1, do: assert(output.write_outcome == :written)

          if module == GroupAsset and attempt == 3,
            do: assert(output.group_replacement.operation == :replaced)

          if module == EmptyGroupAsset and attempt == 1,
            do: assert(output.group_replacement.operation == :bootstrap_empty)

          if expected do
            assert :ok =
                     Favn.Contracts.GenerationCommit.validate(output.generation_commit, expected)

            %{
              expected
              | mode: :existing,
                physical_fingerprint: output.generation_commit.physical_fingerprint
            }
          end
        end)

        {:ok, conn} =
          ADBC.connect(resolved, duckdb_adbc: Application.fetch_env!(:favn, :duckdb_adbc))

        try do
          assert {:ok, _} = ADBC.execute(conn, sql, [])

          assert {:ok, %{rows: [%{"n" => count}]}} =
                   ADBC.query(
                     conn,
                     "SELECT count(*) n FROM mart.#{if module == UnqualifiedAsset, do: "sales", else: "main"}.#{asset.relation.name}",
                     []
                   )

          expected_count =
            case module do
              AppendAsset -> 2
              PlainAsset -> 1
              UnqualifiedAsset -> 1
              GroupAsset -> 1
              WindowAsset -> 1
              _ -> 0
            end

          assert count == expected_count

          if module == GroupAsset do
            assert {:ok, %{rows: [%{"id" => 1, "value" => 42}]}} =
                     ADBC.query(conn, "SELECT * FROM mart.main.group_rows", [])
          end

          if module == UnqualifiedAsset do
            assert {:ok, %{rows: [%{"relation_catalog" => "mart", "relation_schema" => "sales"}]}} =
                     ADBC.query(
                       conn,
                       "SELECT DISTINCT relation_catalog,relation_schema FROM mart.favn_runtime.publication WHERE target_id=?",
                       params: [inspect(module)]
                     )
          end

          assert {:ok,
                  %{rows: [%{"quality_status" => "not_checked", "time_freshness" => "unknown"}]}} =
                   ADBC.query(
                     conn,
                     "SELECT quality_status,time_freshness FROM mart.favn_runtime.freshness WHERE target_id=?",
                     params: [inspect(module)]
                   )
        after
          ADBC.disconnect(conn, [])
        end
      end
    end

    test "#{backend}: concurrent first target insertion has one atomic winner" do
      with_session(unquote(backend), fn s ->
        assert {:ok, _} = write(s, %{publication() | target_id: "bootstrap"}, 0)

        :persistent_term.put(
          {BarrierAdapter, :barrier},
          {self(), "SELECT generation_id, revision"}
        )

        on_exit(fn -> :persistent_term.erase({BarrierAdapter, :barrier}) end)

        tasks =
          for n <- 1..2 do
            Task.async(fn ->
              {:ok, ref} = s.conn.client.connection(s.conn.db_ref)
              conn = %{s.conn | conn_ref: ref}

              try do
                write(
                  %{s | adapter: BarrierAdapter, conn: conn},
                  %{publication() | publication_id: "rp_race_#{n}"},
                  n
                )
              after
                s.conn.client.release(ref)
              end
            end)
          end

        workers =
          for _ <- 1..2 do
            assert_receive {:absence_observed, worker}, 5000
            worker
          end

        Enum.each(workers, &send(&1, :continue))
        results = Enum.map(tasks, &Task.await(&1, 10000))
        assert Enum.count(results, &match?({:ok, _}, &1)) == 1, inspect(results)
        assert Enum.count(results, &match?({:error, _}, &1)) == 1, inspect(results)

        assert [%{"n" => 1}] =
                 query(
                   s,
                   "SELECT count(*) n FROM mart.favn_runtime.publication WHERE target_id='target'"
                 )

        assert [%{"n" => 1}] =
                 query(
                   s,
                   "SELECT count(*) n FROM mart.favn_runtime.asset_state WHERE target_id='target' AND scope_key='__target__'"
                 )
      end)
    end

    test "#{backend}: different targets introducing the same contract cannot duplicate it" do
      with_session(unquote(backend), fn s ->
        for n <- 1..2 do
          assert {:ok, _} =
                   write(
                     s,
                     %{publication() | target_id: "target_#{n}", publication_id: "rp_init_#{n}"},
                     0,
                     "replace",
                     "old",
                     "data_#{n}"
                   )
        end

        :persistent_term.put({BarrierAdapter, :barrier}, {self(), "SELECT document"})
        on_exit(fn -> :persistent_term.erase({BarrierAdapter, :barrier}) end)

        tasks =
          for n <- 1..2 do
            Task.async(fn ->
              {:ok, ref} = s.conn.client.connection(s.conn.db_ref)

              try do
                write(
                  %{s | adapter: BarrierAdapter, conn: %{s.conn | conn_ref: ref}},
                  %{publication() | target_id: "target_#{n}", publication_id: "rp_shared_#{n}"},
                  n,
                  "replace",
                  "new",
                  "data_#{n}"
                )
              after
                s.conn.client.release(ref)
              end
            end)
          end

        workers =
          for _ <- 1..2 do
            assert_receive {:absence_observed, worker}, 5000
            worker
          end

        Enum.each(workers, &send(&1, :continue))
        results = Enum.map(tasks, &Task.await(&1, 10000))
        assert Enum.count(results, &match?({:ok, _}, &1)) == 1, inspect(results)

        assert [%{"n" => 2}] =
                 query(s, "SELECT count(*) n FROM mart.favn_runtime.contract_snapshot")

        assert [%{"n" => 3}] = query(s, "SELECT count(*) n FROM mart.favn_runtime.publication")
      end)
    end

    test "#{backend}: established independent targets avoid the shared revision guard" do
      with_session(unquote(backend), fn s ->
        for n <- 1..2 do
          assert {:ok, _} =
                   write(
                     s,
                     %{
                       publication()
                       | target_id: "independent_#{n}",
                         publication_id: "rp_init_#{n}"
                     },
                     0,
                     "replace",
                     "shared",
                     "data_#{n}"
                   )
        end

        revision = query(s, "SELECT revision FROM mart.favn_runtime.runtime_schema")

        :persistent_term.put(
          {BarrierAdapter, :barrier},
          {self(), "SELECT generation_id, revision", :present}
        )

        on_exit(fn -> :persistent_term.erase({BarrierAdapter, :barrier}) end)

        tasks =
          for n <- 1..2 do
            Task.async(fn ->
              {:ok, ref} = s.conn.client.connection(s.conn.db_ref)

              try do
                write(
                  %{s | adapter: BarrierAdapter, conn: %{s.conn | conn_ref: ref}},
                  %{
                    publication()
                    | target_id: "independent_#{n}",
                      publication_id: "rp_next_#{n}"
                  },
                  n,
                  "replace",
                  "shared",
                  "data_#{n}"
                )
              after
                s.conn.client.release(ref)
              end
            end)
          end

        workers =
          for _ <- 1..2 do
            assert_receive {:absence_observed, worker}, 5000
            worker
          end

        Enum.each(workers, &send(&1, :continue))
        results = Enum.map(tasks, &Task.await(&1, 10000))
        committed = Enum.count(results, &match?({:ok, _}, &1))
        assert committed in 1..2, inspect(results)

        IO.puts(
          "#{unquote(backend)} established independent writes committed=#{committed}/2; no retries"
        )

        assert revision == query(s, "SELECT revision FROM mart.favn_runtime.runtime_schema")

        for {result, n} <- Enum.with_index(results, 1) do
          expected = if match?({:ok, _}, result), do: n, else: 0
          assert [%{"value" => ^expected}] = query(s, "SELECT value FROM mart.main.data_#{n}")
          count = if expected == 0, do: 1, else: 2

          assert [%{"n" => ^count}] =
                   query(
                     s,
                     "SELECT count(*) n FROM mart.favn_runtime.publication WHERE target_id='independent_#{n}'"
                   )
        end
      end)
    end

    test "#{backend}: data and metadata commit together; duplicates and metadata failures roll back" do
      with_session(unquote(backend), fn s ->
        p = publication()
        assert {:ok, receipt} = write(s, p, 10)
        assert receipt["publication_id"] == p.publication_id
        assert [%{"value" => 10}] = query(s, "SELECT * FROM mart.main.data")

        assert [%{"time_freshness" => "unknown", "quality_status" => "not_checked"}] =
                 query(s, "SELECT time_freshness,quality_status FROM mart.favn_runtime.freshness")

        assert {:error, _} = write(s, p, 20)
        assert [%{"value" => 10}] = query(s, "SELECT * FROM mart.main.data")

        bad = %{
          p
          | publication_id: "rp_bad",
            policy: %{"mode" => "calendar_period", "kind" => "day", "timezone" => "Etc/UTC"}
        }

        assert {:error, _} = write(s, bad, 30)
        assert [%{"value" => 10}] = query(s, "SELECT * FROM mart.main.data")
        assert [%{"n" => 1}] = query(s, "SELECT count(*) n FROM mart.favn_runtime.publication")
      end)
    end

    test "#{backend}: exact window evidence, full replacement invalidation and group coverage" do
      with_session(unquote(backend), fn s ->
        jan = window("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")
        mar = window("2026-03-01T00:00:00Z", "2026-04-01T00:00:00Z")
        p = %{publication() | windows: [jan]}
        assert {:ok, _} = write(s, p, 1)
        p2 = %{p | publication_id: "rp_two", windows: [mar]}
        assert {:ok, _} = write(s, p2, 2, "append")
        assert [%{"n" => 2}] = query(s, "SELECT count(*) n FROM mart.favn_runtime.coverage")
        assert {:ok, _} = write(s, %{p2 | publication_id: "rp_three"}, 3)
        assert [%{"n" => 1}] = query(s, "SELECT count(*) n FROM mart.favn_runtime.coverage")
        assert {:ok, _} = write(s, %{p2 | publication_id: "rp_four"}, 4, "replace_groups")
        assert [] = query(s, "SELECT * FROM mart.favn_runtime.coverage")

        assert [%{"coverage_support" => "unsupported"}] =
                 query(s, "SELECT coverage_support FROM mart.favn_runtime.freshness")
      end)
    end

    test "#{backend}: an abandoned first candidate does not block the existing active table" do
      with_session(unquote(backend), fn s ->
        assert {:ok, _} =
                 Client.execute(s, "CREATE TABLE mart.main.data AS SELECT 1 AS value", [])

        candidate = %{
          publication()
          | candidate: true,
            generation_id: "candidate",
            publication_id: "rp_aborted"
        }

        assert {:ok, _} = write(s, candidate, 2, "replace", nil, "candidate")
        assert [] = query(s, "SELECT * FROM mart.favn_runtime.freshness")
        assert {:ok, _} = Client.execute(s, "DROP TABLE mart.main.candidate", [])
        assert {:ok, _} = write(s, publication(), 3)

        assert [%{"publication_id" => "rp_one"}] =
                 query(s, "SELECT publication_id FROM mart.favn_runtime.freshness")

        assert [%{"value" => 3}] = query(s, "SELECT * FROM mart.main.data")
      end)
    end

    test "#{backend}: reserved schema is rejected case-insensitively and through the default schema" do
      with_session(unquote(backend), fn s ->
        assert {:ok, _} = Client.execute(s, "CREATE SCHEMA mart.favn_runtime", [])

        assert {:error, _} =
                 RuntimeCatalog.prepare(
                   s,
                   publication(),
                   %{relation() | schema: "FAVN_RUNTIME"},
                   []
                 )

        assert {:ok, _} = Client.execute(s, "SET search_path='mart.favn_runtime'", [])

        assert {:error, _} =
                 RuntimeCatalog.prepare(
                   s,
                   publication(),
                   %{relation() | catalog: nil, schema: nil},
                   []
                 )

        assert [] =
                 query(
                   s,
                   "SELECT table_name FROM information_schema.tables WHERE table_catalog='mart' AND table_schema='favn_runtime'"
                 )
      end)
    end

    test "#{backend}: existing unqualified targets follow the session search path" do
      with_session(unquote(backend), fn s ->
        assert {:ok, _} = Client.execute(s, "CREATE SCHEMA mart.sales", [])

        assert {:ok, _} =
                 Client.execute(s, "CREATE TABLE mart.sales.data AS SELECT 1 AS value", [])

        assert {:ok, _} = Client.execute(s, "SET search_path='memory.main,mart.sales'", [])
        unresolved = %{relation() | catalog: nil, schema: nil}
        assert {:ok, resolved} = RuntimeCatalog.resolve(s, unresolved, [])
        assert resolved.catalog == "mart" and resolved.schema == "sales"

        assert {:ok, _} =
                 Client.transaction(s, fn tx ->
                   with {:ok, prepared} <- RuntimeCatalog.prepare(tx, publication(), resolved, []),
                        {:ok, _} <-
                          Client.execute(tx, "INSERT INTO mart.sales.data VALUES (2)", []),
                        do:
                          RuntimeCatalog.record(
                            tx,
                            prepared,
                            %{
                              contract: %{},
                              mutation: "append",
                              check_results: [],
                              write_outcome: :written
                            },
                            []
                          )
                 end)

        assert [%{"n" => 2}] = query(s, "SELECT count(*) n FROM mart.sales.data")

        assert [%{"relation_catalog" => "mart", "relation_schema" => "sales"}] =
                 query(
                   s,
                   "SELECT relation_catalog,relation_schema FROM mart.favn_runtime.publication"
                 )
      end)
    end

    test "#{backend}: ambiguous and catalog-only references fail closed" do
      with_session(unquote(backend), fn s ->
        for name <- ["existing", "missing"] do
          assert {:ok, _} =
                   Client.execute(
                     s,
                     "CREATE TABLE IF NOT EXISTS mart.main.existing(v INTEGER)",
                     []
                   )

          assert {:error, %Favn.SQL.Error{type: :runtime_catalog_ambiguous_target}} =
                   RuntimeCatalog.resolve(s, %{relation() | name: name, schema: nil}, [])
        end

        assert {:ok, _} = Client.execute(s, "ATTACH ':memory:' AS other", [])

        for sql <- [
              "CREATE SCHEMA memory.a",
              "CREATE SCHEMA mart.b",
              "CREATE SCHEMA other.a",
              "CREATE TABLE mart.b.data(v INTEGER)",
              "CREATE TABLE other.a.data(v INTEGER)",
              "SET search_path='memory.a,mart.b,other.a'"
            ] do
          assert {:ok, _} = Client.execute(s, sql, [])
        end

        assert {:error, %Favn.SQL.Error{type: :runtime_catalog_ambiguous_target}} =
                 RuntimeCatalog.resolve(s, %{relation() | catalog: nil, schema: nil}, [])

        assert [] =
                 query(
                   s,
                   "SELECT table_name FROM information_schema.tables WHERE table_schema='favn_runtime'"
                 )
      end)
    end

    test "#{backend}: full qualification preserves case-insensitive catalog identity" do
      with_session(unquote(backend), fn s ->
        assert {:ok, resolved} = RuntimeCatalog.resolve(s, %{relation() | catalog: "MART"}, [])
        assert resolved.catalog == "mart"
        assert {:ok, _} = write(s, publication(), 1, "replace", nil, "data", "main", "MART")
        assert [%{"value" => 1}] = query(s, "SELECT value FROM mart.main.data")

        assert [%{"relation_catalog" => "mart"}] =
                 query(s, "SELECT relation_catalog FROM mart.favn_runtime.publication")
      end)
    end

    test "#{backend}: Unicode case differences never redirect a native target" do
      with_session(unquote(backend), fn s ->
        assert {:ok, _} = Client.execute(s, ~s|ATTACH ':memory:' AS "märt"|, [])
        assert {:ok, _} = Client.execute(s, ~s|CREATE SCHEMA mart."säles"|, [])
        assert {:error, _} = RuntimeCatalog.resolve(s, %{relation() | catalog: "MÄRT"}, [])

        assert {:error, _} =
                 RuntimeCatalog.resolve(s, %{relation() | catalog: nil, schema: "SÄLES"}, [])

        assert [] =
                 query(
                   s,
                   "SELECT table_name FROM information_schema.tables WHERE table_schema='favn_runtime'"
                 )
      end)
    end

    test "#{backend}: quoted mixed-case identifiers resolve to the actual catalog" do
      with_session(unquote(backend), fn s ->
        assert {:ok, _} = Client.execute(s, ~s(CREATE SCHEMA mart."Sa'les"), [])

        assert {:ok, _} =
                 Client.execute(s, ~s|CREATE TABLE mart."Sa'les"."Da""ta"(v INTEGER)|, [])

        assert {:ok, _} = Client.execute(s, ~s|SET search_path='memory.main,mart."Sa''les"'|, [])
        ref = %{relation() | catalog: nil, schema: "sa'les", name: ~s(da"ta)}
        assert {:ok, resolved} = RuntimeCatalog.resolve(s, ref, [])
        assert resolved.catalog == "mart"
        assert resolved.schema == "Sa'les"
        assert resolved.name == ~s(Da"ta)
      end)
    end

    test "#{backend}: candidate table and runtime metadata activate and reconcile together" do
      with_session(unquote(backend), fn s ->
        assert {:ok, _} = Client.execute(s, "CREATE SCHEMA mart.sales", [])
        assert {:ok, _} = Client.execute(s, "SET search_path='memory.main,mart.sales'", [])
        gen = "11111111-1111-4111-8111-111111111111"
        p = %{publication() | generation_id: gen, candidate: true}
        assert {:ok, _} = write(s, p, 2, "replace", nil, "candidate", "sales")
        assert [] = query(s, "SELECT publication_id FROM mart.favn_runtime.freshness")
        candidate = %{relation() | schema: "sales", name: "candidate"}
        assert {:ok, inspection} = ADBC.inspect_generation(s.conn, candidate, [])

        request = %Favn.SQL.GenerationActivation{
          workspace_id: p.workspace_id,
          logical_target_id: p.target_id,
          stable_relation: %{relation() | catalog: nil, schema: "sales"},
          candidate_relation: %{candidate | catalog: nil},
          retired_relation: %{relation() | catalog: nil, schema: "sales", name: "retired"},
          candidate_generation_id: gen,
          expected_active_generation_id: nil,
          expected_candidate_fingerprint: inspection.physical_fingerprint.fingerprint,
          activation_operation_id: "activation",
          activation_token: "activation-token",
          activated_at: DateTime.utc_now()
        }

        assert {:ok, _} = ADBC.activate_generation(s.conn, request, [])
        assert [%{"value" => 2}] = query(s, "SELECT * FROM mart.sales.data")

        assert [%{"publication_id" => "rp_one"}] =
                 query(s, "SELECT publication_id FROM mart.favn_runtime.freshness")

        assert {:ok, _} =
                 ADBC.reconcile_generation(
                   s.conn,
                   %Favn.SQL.GenerationReconciliation{
                     logical_target_id: p.target_id,
                     stable_relation: %{relation() | schema: "sales"}
                   },
                   []
                 )

        assert [%{"n" => 1}] = query(s, "SELECT count(*) n FROM mart.favn_runtime.publication")
      end)
    end

    test "#{backend}: expired calendar deadlines are queried without metadata writes; no-op adds no receipt" do
      with_session(unquote(backend), fn s ->
        p = %{
          publication()
          | freshness_key: "calendar:day:Etc/UTC:2020-01-01",
            policy: %{"mode" => "calendar_period", "kind" => "day", "timezone" => "Etc/UTC"}
        }

        assert {:ok, _} = write(s, p, 1)

        assert [%{"time_freshness" => "expired"}] =
                 query(s, "SELECT time_freshness FROM mart.favn_runtime.freshness")

        assert {:ok, nil} =
                 Client.transaction(s, fn tx ->
                   with {:ok, prepared} <-
                          RuntimeCatalog.prepare(
                            tx,
                            %{p | publication_id: "rp_skip"},
                            relation(),
                            []
                          ),
                        do:
                          Favn.SQL.RuntimeCatalog.record(
                            tx,
                            prepared,
                            %{write_outcome: :no_op},
                            []
                          )
                 end)

        assert [%{"n" => 1}] = query(s, "SELECT count(*) n FROM mart.favn_runtime.publication")
      end)
    end
  end

  defp startup_sql(:ducklake, root),
    do:
      "LOAD ducklake; ATTACH 'ducklake:#{root}/catalog.ducklake' AS mart (DATA_PATH '#{root}/data');"

  defp startup_sql(:duckdb, root), do: "ATTACH '#{root}/mart.duckdb' AS mart;"

  defp publication do
    struct!(Publication,
      workspace_id: "workspace",
      publication_id: "rp_one",
      target_id: "target",
      generation_id: "generation",
      asset_ref: "Example.daily",
      run_id: "run",
      step_id: "step",
      attempt: 1,
      manifest_id: "mv_one",
      manifest_hash: String.duplicate("a", 64),
      runner_release: "release",
      freshness_key: "latest",
      policy: nil,
      windows: [],
      coverage: nil,
      candidate: false
    )
  end

  defp relation,
    do: %RelationRef{connection: :warehouse, catalog: "mart", schema: "main", name: "data"}

  defp window(s, e),
    do: %{"kind" => "month", "timezone" => "Etc/UTC", "start_at" => s, "end_at" => e}

  defp write(
         session,
         p,
         value,
         mutation \\ "replace",
         contract \\ nil,
         name \\ "data",
         schema \\ "main",
         catalog \\ "mart"
       ) do
    Client.transaction(session, fn tx ->
      with {:ok, prepared} <-
             RuntimeCatalog.prepare(
               tx,
               p,
               %{relation() | catalog: catalog, schema: schema, name: name},
               []
             ),
           {:ok, _} <-
             Client.execute(
               tx,
               "CREATE OR REPLACE TABLE #{catalog}.#{schema}.#{name} AS SELECT #{value} AS value",
               []
             ),
           do:
             RuntimeCatalog.record(
               tx,
               prepared,
               %{
                 contract: %{"ref" => "Example.daily", "contract" => contract},
                 mutation: mutation,
                 check_results: [],
                 write_outcome: :written
               },
               []
             )
    end)
  end

  defp query(s, sql) do
    assert {:ok, result} = Client.query(s, sql, [])
    result.rows
  end

  defp with_session(backend, fun) do
    resolved = %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"]}
    }

    assert {:ok, conn} =
             ADBC.connect(resolved,
               duckdb_adbc: [
                 driver: System.fetch_env!("DUCKDB_ADBC_DRIVER"),
                 entrypoint: "duckdb_adbc_init"
               ]
             )

    root = Path.join(System.tmp_dir!(), "runtime-catalog-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    try do
      if backend == :ducklake do
        assert {:ok, _} = ADBC.execute(conn, "LOAD ducklake", [])

        assert {:ok, _} =
                 ADBC.execute(
                   conn,
                   "ATTACH 'ducklake:#{root}/catalog.ducklake' AS mart (DATA_PATH '#{root}/data')",
                   []
                 )
      else
        assert {:ok, _} = ADBC.execute(conn, "ATTACH '#{root}/mart.duckdb' AS mart", [])
      end

      {:ok, capabilities} = ADBC.capabilities(resolved, [])

      fun.(%Session{
        adapter: ADBC,
        resolved: resolved,
        conn: conn,
        capabilities: capabilities,
        required_catalogs: ["mart"]
      })
    after
      ADBC.disconnect(conn, [])
      File.rm_rf!(root)
    end
  end
end
