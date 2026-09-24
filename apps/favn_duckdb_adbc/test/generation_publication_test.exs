defmodule FavnDuckdbADBC.GenerationPublicationTest do
  use ExUnit.Case, async: false
  @moduletag :adbc_integration
  alias Favn.Connection.Resolved
  alias Favn.Contracts.{GenerationCommit, GenerationMarker, GenerationPrecondition}
  alias Favn.RelationRef
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.{Error, GenerationReconciliation}

  for backend <- [:duckdb, :ducklake] do
    describe "#{backend} atomic generation publication" do
      setup do
        resolved = %Resolved{
          name: :warehouse,
          adapter: ADBC,
          module: __MODULE__,
          config: %{open: [database: ":memory:"]}
        }

        opts =
          case System.get_env("DUCKDB_ADBC_DRIVER") do
            nil -> []
            driver -> [duckdb_adbc: [driver: driver, entrypoint: "duckdb_adbc_init"]]
          end

        assert {:ok, conn} = ADBC.connect(resolved, opts)
        root = Path.join(System.tmp_dir!(), "favn_atomic_#{System.unique_integer([:positive])}")

        on_exit(fn ->
          ADBC.disconnect(conn, [])
          File.rm_rf!(root)
        end)

        catalog =
          if unquote(backend) == :ducklake do
            File.mkdir_p!(root)
            assert {:ok, _} = ADBC.execute(conn, "INSTALL ducklake; LOAD ducklake", [])

            assert {:ok, _} =
                     ADBC.execute(
                       conn,
                       "ATTACH 'ducklake:#{root}/metadata.ducklake' AS lake (DATA_PATH '#{root}/data')",
                       []
                     )

            "lake"
          else
            "memory"
          end

        relation = %RelationRef{
          connection: :warehouse,
          catalog: catalog,
          schema: "main",
          name: "asset"
        }

        expected = %GenerationPrecondition{
          mode: :initial,
          marker: %GenerationMarker{
            target_id: "Example.asset",
            active_relation: relation,
            active_generation_id: "11111111-1111-4111-8111-111111111111",
            activation_operation_id: "initial-generation",
            activation_token: "initial-token",
            activated_at: ~U[2026-09-24 12:00:00.000000Z]
          }
        }

        %{root: root, conn: conn, expected: expected, table: "#{catalog}.main.asset"}
      end

      test "worker preserves committed identity through colliding secret redaction", c do
        alias Favn.Contracts.{RunnerWork, RunnerResult}
        alias Favn.Manifest.{Asset, ExecutionPackage, Graph, SQLExecution, Version}
        alias Favn.RuntimeConfig.Ref

        :ok =
          FavnRunner.ReleaseVerifier.verify_test_startup(%{
            "FAVN_RUNNER_RELEASE_ID" => FavnTestSupport.runner_release_id()
          })

        assert {:ok, _} = Application.ensure_all_started(:favn_runner)
        File.mkdir_p!(c.root)
        startup = Path.join(c.root, "worker-startup.sql")
        catalog = if unquote(backend) == :ducklake, do: "lake", else: "worker"

        sql =
          if unquote(backend) == :ducklake do
            "LOAD ducklake; ATTACH 'ducklake:#{c.root}/worker.ducklake' AS lake (DATA_PATH '#{c.root}/worker-data')"
          else
            "ATTACH '#{c.root}/worker.duckdb' AS worker"
          end

        File.write!(startup, sql)

        resolved = %Resolved{
          name: :warehouse,
          adapter: ADBC,
          module: __MODULE__,
          config: %{
            open: [database: ":memory:"],
            duckdb: [
              startup: [file: startup],
              catalogs: [
                {if(unquote(backend) == :ducklake, do: :lake, else: :worker),
                 [write_concurrency: 1]}
              ]
            ],
            pool: %Favn.SQL.PoolConfig{enabled: false}
          }
        }

        registry = FavnRunner.ConnectionRegistry

        previous =
          Favn.Connection.Registry.list(registry_name: registry) |> Map.new(&{&1.name, &1})

        old_driver = Application.get_env(:favn, :duckdb_adbc)
        old_secret = System.get_env("FAVN_PUBLICATION_COLLISION")
        System.put_env("FAVN_PUBLICATION_COLLISION", "initial-token")

        Application.put_env(:favn, :duckdb_adbc,
          driver: System.fetch_env!("DUCKDB_ADBC_DRIVER"),
          entrypoint: "duckdb_adbc_init"
        )

        on_exit(fn ->
          Favn.Connection.Registry.reload(previous, registry_name: registry)

          if old_driver,
            do: Application.put_env(:favn, :duckdb_adbc, old_driver),
            else: Application.delete_env(:favn, :duckdb_adbc)

          if old_secret,
            do: System.put_env("FAVN_PUBLICATION_COLLISION", old_secret),
            else: System.delete_env("FAVN_PUBLICATION_COLLISION")
        end)

        :ok = Favn.Connection.Registry.reload(%{warehouse: resolved}, registry_name: registry)
        ref = {__MODULE__, :asset}
        query = "SELECT 42::INTEGER AS id"

        template =
          Favn.SQL.Template.compile!(query,
            module: __MODULE__,
            file: "publication.sql",
            line: 1,
            scope: :query,
            enforce_query_root: true
          )

        assert {:ok, package} =
                 ExecutionPackage.new(ref, %SQLExecution{sql: query, template: template})

        relation = %{c.expected.marker.active_relation | catalog: catalog}

        asset =
          %Asset{
            ref: ref,
            module: __MODULE__,
            name: :asset,
            type: :sql,
            runner_pool: :default,
            execution: %{entrypoint: :asset, arity: 1},
            relation: relation,
            materialization: :table,
            execution_package_hash: package.content_hash,
            runtime_config: %{
              api: %{
                name: Ref.secret_env!("FAVN_PUBLICATION_COLLISION"),
                activation_token: Ref.secret_env!("FAVN_PUBLICATION_COLLISION"),
                physical_fingerprint: Ref.secret_env!("FAVN_PUBLICATION_COLLISION")
              }
            }
          }
          |> FavnTestSupport.with_target_descriptor()

        assert {:ok, version} =
                 Version.new(%Favn.Manifest{
                   runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
                   assets: [asset],
                   graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]}
                 })

        expected = %{
          c.expected
          | marker: %{
              c.expected.marker
              | active_relation: relation,
                target_id: asset.target_descriptor.target_id
            }
        }

        work = %RunnerWork{
          run_id: "generation-redaction",
          asset_step_id: "asset-step",
          asset_ref: ref,
          manifest_version_id: version.manifest_version_id,
          manifest_content_hash: version.content_hash,
          required_runner_release_id: FavnTestSupport.runner_release_id(),
          execution_package: package,
          target_operation: :normal_materialization,
          logical_target_id: expected.marker.target_id,
          target_generation_id: expected.marker.active_generation_id,
          target_descriptor_hash: asset.target_descriptor.descriptor_hash,
          active_relation: relation,
          write_relation: relation
        }

        assert {:ok, _} =
                 FavnRunner.Worker.start_link(%{
                   server: self(),
                   execution_id: "generation-redaction",
                   work: work,
                   asset: asset,
                   version: version,
                   generation_precondition: expected
                 })

        assert_receive {:runner_result, "generation-redaction", %RunnerResult{} = result}, 10_000
        assert result.status == :ok, inspect(result.error)
        assert [asset_result] = result.asset_results
        assert :ok = GenerationCommit.validate(asset_result.evidence.generation_commit, expected)

        assert {:ok, encoded} =
                 Favn.Contracts.RunnerTask.PersistenceCodec.encode_result(
                   :asset_attempt,
                   :succeeded,
                   result
                 )

        assert {:ok, decoded} =
                 Favn.Contracts.RunnerTask.PersistenceCodec.decode_result(
                   :asset_attempt,
                   :succeeded,
                   encoded,
                   version,
                   [package]
                 )

        assert decoded.asset_results
               |> hd()
               |> Map.fetch!(:evidence)
               |> Map.fetch!(:generation_commit) == asset_result.evidence.generation_commit
      end

      test "first write and replacement retain exact identity", c do
        assert {:ok, receipt} = write(c, "SELECT 1::INTEGER AS id")
        assert :ok = GenerationCommit.validate(receipt, c.expected)

        existing = %{
          c.expected
          | mode: :existing,
            physical_fingerprint: receipt.physical_fingerprint
        }

        assert {:ok, ^receipt} = write(%{c | expected: existing}, "SELECT 2::INTEGER AS id")
        assert {:ok, %{rows: [%{"id" => 2}]}} = ADBC.query(c.conn, "SELECT * FROM #{c.table}", [])
      end

      test "empty bootstrap, incremental writes, group replacement and no-op preserve identity",
           c do
        assert {:ok, receipt} = write(c, "SELECT 1::INTEGER AS id WHERE false")

        existing = %{
          c.expected
          | mode: :existing,
            physical_fingerprint: receipt.physical_fingerprint
        }

        mutations = [
          ["INSERT INTO #{c.table} VALUES (1), (2)"],
          ["UPDATE #{c.table} SET id=3 WHERE id=2"],
          ["DELETE FROM #{c.table} WHERE id=1", "INSERT INTO #{c.table} VALUES (4), (5)"],
          []
        ]

        for statements <- mutations do
          assert {:ok, ^receipt} =
                   ADBC.transaction(
                     c.conn,
                     fn tx ->
                       with {:ok, _} <- ADBC.prepare_generation_write(tx, existing, []) do
                         Enum.each(statements, fn sql ->
                           assert {:ok, _} = ADBC.execute(tx, sql, [])
                         end)

                         ADBC.publish_generation_write(tx, existing, [])
                       end
                     end,
                     []
                   )
        end

        assert {:ok, %{rows: [%{"id" => 3}, %{"id" => 4}, %{"id" => 5}]}} =
                 ADBC.query(c.conn, "SELECT id FROM #{c.table} ORDER BY id", [])

        assert :ok = GenerationCommit.validate(receipt, existing)
      end

      test "failure after publication rolls back table and marker", c do
        assert {:error, %Error{}} =
                 ADBC.transaction(
                   c.conn,
                   fn tx ->
                     with {:ok, _} <- ADBC.prepare_generation_write(tx, c.expected, []),
                          {:ok, _} <-
                            ADBC.execute(tx, "CREATE TABLE #{c.table} AS SELECT 1 AS id", []),
                          {:ok, _} <- ADBC.publish_generation_write(tx, c.expected, []) do
                       {:error,
                        %Error{
                          type: :execution_error,
                          message: "injected after publication",
                          retryable?: false
                        }}
                     end
                   end,
                   []
                 )

        assert {:ok, nil} = ADBC.relation(c.conn, c.expected.marker.active_relation, [])
        assert {:ok, nil} = read_marker(c)
        assert {:ok, _} = write(c, "SELECT 1 AS id")
      end

      test "initial check skip cannot publish an absent target", c do
        assert {:error, %Error{}} =
                 ADBC.transaction(
                   c.conn,
                   fn tx ->
                     with {:ok, _} <- ADBC.prepare_generation_write(tx, c.expected, []),
                          do: ADBC.publish_generation_write(tx, c.expected, [])
                   end,
                   []
                 )

        assert {:ok, nil} = read_marker(c)
      end

      test "initial work cannot adopt an existing unbound table", c do
        assert {:ok, _} = ADBC.execute(c.conn, "CREATE TABLE #{c.table} AS SELECT 8 AS id", [])
        assert {:error, %Error{}} = write(c, "SELECT 1 AS id")
        assert {:ok, %{rows: [%{"id" => 8}]}} = ADBC.query(c.conn, "SELECT * FROM #{c.table}", [])
      end

      test "identical external replacement fails before the next write", c do
        assert {:ok, receipt} = write(c, "SELECT 1 AS id")

        c = %{
          c
          | expected: %{
              c.expected
              | mode: :existing,
                physical_fingerprint: receipt.physical_fingerprint
            }
        }

        assert {:ok, _} =
                 ADBC.execute(c.conn, "CREATE OR REPLACE TABLE #{c.table} AS SELECT 8 AS id", [])

        assert {:error, %Error{}} = write(c, "SELECT 2 AS id")
        assert {:ok, %{rows: [%{"id" => 8}]}} = ADBC.query(c.conn, "SELECT * FROM #{c.table}", [])
      end

      test "unapproved shape change rolls back the entire replacement", c do
        assert {:ok, receipt} = write(c, "SELECT 1::INTEGER AS id")

        c = %{
          c
          | expected: %{
              c.expected
              | mode: :existing,
                physical_fingerprint: receipt.physical_fingerprint
            }
        }

        assert {:error, %Error{}} = write(c, "SELECT 'changed' AS id")
        assert {:ok, %{rows: [%{"id" => 1}]}} = ADBC.query(c.conn, "SELECT * FROM #{c.table}", [])
        assert {:ok, ^receipt} = write(c, "SELECT 2::INTEGER AS id")
      end
    end
  end

  defp write(c, query) do
    ADBC.transaction(
      c.conn,
      fn tx ->
        with {:ok, _} <- ADBC.prepare_generation_write(tx, c.expected, []),
             {:ok, _} <- ADBC.execute(tx, "CREATE OR REPLACE TABLE #{c.table} AS #{query}", []),
             do: ADBC.publish_generation_write(tx, c.expected, [])
      end,
      preserve_body_result_on_commit_error?: true
    )
  end

  defp read_marker(c),
    do:
      ADBC.reconcile_generation(
        c.conn,
        %GenerationReconciliation{
          logical_target_id: c.expected.marker.target_id,
          stable_relation: c.expected.marker.active_relation,
          require_relation_instance?: false
        },
        []
      )
end
