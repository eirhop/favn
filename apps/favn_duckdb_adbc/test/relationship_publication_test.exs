defmodule FavnDuckdbADBC.RelationshipPublicationTest do
  use ExUnit.Case, async: false
  @moduletag :adbc_integration

  alias Favn.Connection.{Registry, Resolved}
  alias Favn.Contracts.{RunnerWork, TargetGenerationPin}
  alias Favn.Manifest.{Asset, ExecutionPackage, SQLExecution, Version}
  alias Favn.Run.Context
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.PoolConfig
  alias Favn.SQLAsset.Runtime

  defmodule Store do
    use Favn.SQLAsset
    relation(connection: :relationship_publication, schema: "main", name: "stores_active")
    materialized(:table)

    contract do
      grain(by: [:store_id])
      column(:store_id, :integer, null: false)
    end

    query do
      ~SQL"SELECT 1 AS store_id"
    end
  end

  defmodule Sales do
    use Favn.SQLAsset
    depends(Store)
    relation(connection: :relationship_publication, schema: "main", name: "sales")
    window(Favn.Window.daily(timezone: "Etc/UTC"))
    materialized({:incremental, strategy: :append})

    contract do
      grain(by: [:id])
      column(:id, :integer, null: false)
      column(:store_id, :integer, null: false)

      relationship(:store, Store,
        on: [store_id: :store_id],
        cardinality: :one_to_one,
        on_violation: :fail
      )
    end

    query do
      ~SQL"SELECT CAST(@id AS INTEGER) AS id, CAST(@store_id AS INTEGER) AS store_id"
    end
  end

  test "checked publication uses pinned keys and enforces fail/warn before and after incremental mutation" do
    path =
      Path.join(
        System.tmp_dir!(),
        "favn-relationship-publication-#{System.unique_integer([:positive])}.duckdb"
      )

    previous_driver = Application.get_env(:favn, :duckdb_adbc)
    driver = System.get_env("DUCKDB_ADBC_DRIVER")

    if driver && driver != "",
      do: Application.put_env(:favn, :duckdb_adbc, driver: driver, entrypoint: "duckdb_adbc_init")

    on_exit(fn ->
      if previous_driver,
        do: Application.put_env(:favn, :duckdb_adbc, previous_driver),
        else: Application.delete_env(:favn, :duckdb_adbc)

      File.rm(path)
      File.rm(path <> ".wal")
    end)

    resolved = %Resolved{
      name: :relationship_publication,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: path], pool: %PoolConfig{enabled: false}}
    }

    if Process.whereis(FavnRunner.ConnectionRegistry) do
      previous =
        Registry.list(registry_name: FavnRunner.ConnectionRegistry) |> Map.new(&{&1.name, &1})

      on_exit(fn -> Registry.reload(previous, registry_name: FavnRunner.ConnectionRegistry) end)

      Registry.reload(Map.put(previous, resolved.name, resolved),
        registry_name: FavnRunner.ConnectionRegistry
      )
    else
      start_supervised!(
        {Registry, name: FavnRunner.ConnectionRegistry, connections: %{resolved.name => resolved}}
      )
    end

    scenarios = [
      {:pinned_pass, :fail, 2, 1, :passed, nil},
      {:reference_fail, :fail, 1, 2, :failed, "relationship.store.reference"},
      {:reference_warn, :warn, 1, 2, :warned, "relationship.store.reference"},
      {:retained_collision_fail, :fail, 2, 2, :failed, "relationship.store.unique"},
      {:retained_collision_warn, :warn, 2, 2, :warned, "relationship.store.unique"}
    ]

    for {scenario, policy, candidate_key, retained_key, outcome, claim_id} <- scenarios do
      # One runner publication owns each physical database. Fixture writers never
      # reopen and reset a database previously handed to the runner.
      scenario_path = path <> ".#{scenario}"

      scenario_resolved = %{
        resolved
        | config: %{resolved.config | open: [database: scenario_path]}
      }

      on_exit(fn ->
        File.rm(scenario_path)
        File.rm(scenario_path <> ".wal")
      end)

      Registry.reload(%{resolved.name => scenario_resolved},
        registry_name: FavnRunner.ConnectionRegistry
      )

      with_connection(scenario_resolved, fn conn ->
        execute(conn, "CREATE TABLE stores_active(store_id INTEGER)")
        execute(conn, "INSERT INTO stores_active VALUES (1)")
        execute(conn, "CREATE TABLE stores_pinned(store_id INTEGER)")
        execute(conn, "INSERT INTO stores_pinned VALUES (2)")
        execute(conn, "CREATE TABLE sales(id INTEGER, store_id INTEGER)")
        execute(conn, "INSERT INTO sales VALUES (100, #{retained_key})")
      end)

      assert rows(scenario_resolved) == [%{"id" => 100, "store_id" => retained_key}]
      result = publish(policy, candidate_key)

      if outcome == :failed do
        assert {:error, _, meta} = result
        assert Enum.any?(meta.check_results, &(&1.claim_id == claim_id and &1.outcome == :failed))

        if claim_id == "relationship.store.unique" do
          assert Enum.any?(
                   meta.check_results,
                   &(&1.claim_id == "relationship.store.reference" and &1.outcome == :passed)
                 )
        end

        assert rows(scenario_resolved) == [%{"id" => 100, "store_id" => retained_key}]
      else
        assert {:ok, output} = result
        assert output.write_outcome == :written
        assert output.quality_status == if(outcome == :warned, do: :warning, else: :passed)

        if claim_id do
          assert Enum.any?(
                   output.check_results,
                   &(&1.claim_id == claim_id and &1.outcome == :warned)
                 )
        end

        assert rows(scenario_resolved) == [
                 %{"id" => 100, "store_id" => retained_key},
                 %{"id" => 101, "store_id" => candidate_key}
               ]
      end
    end
  end

  defp publish(policy, store_id) do
    [raw] = Sales.__favn_assets_raw__()
    [contract] = raw.contracts
    [relationship] = contract.definition.relationships

    contract = %{
      contract
      | definition: %{
          contract.definition
          | relationships: [%{relationship | on_violation: policy}]
        }
    }

    definition = Favn.SQLAsset.finalize_raw_definition(%{raw | contracts: [contract]})

    {:ok, package} =
      ExecutionPackage.new({Sales, :asset}, SQLExecution.from_definition(definition))

    asset = %Asset{
      ref: {Sales, :asset},
      module: Sales,
      name: :asset,
      type: :sql,
      depends_on: [{Store, :asset}],
      relation: definition.asset.relation,
      materialization: definition.materialization,
      window: definition.asset.window_spec,
      relation_inputs: definition.relation_inputs
    }

    active = Store.__favn_sql_asset_definition__().asset.relation

    pin = %TargetGenerationPin{
      asset_ref: {Store, :asset},
      relation: %{active | name: "stores_pinned"},
      target_id: "store",
      target_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987654",
      descriptor_hash: String.duplicate("a", 64)
    }

    work = %RunnerWork{upstream_generation_pins: [pin]}

    version = %Version{
      manifest_version_id: "relationship-test",
      content_hash: String.duplicate("b", 64)
    }

    window = %Favn.Window.Runtime{
      kind: :day,
      timezone: "Etc/UTC",
      start_at: ~U[2026-01-01 00:00:00Z],
      end_at: ~U[2026-01-02 00:00:00Z]
    }

    context = %Context{
      run_id: "relationship-test",
      run_started_at: ~U[2026-01-01 00:00:00Z],
      window: window,
      params: %{id: 101, store_id: store_id}
    }

    Runtime.run_manifest(asset, package, version, %{Store => active}, work, context)
  end

  defp execute(conn, sql), do: assert({:ok, _} = ADBC.execute(conn, sql, []))

  defp rows(resolved) do
    with_connection(resolved, fn conn ->
      assert {:ok, result} = ADBC.query(conn, "SELECT id, store_id FROM sales ORDER BY id", [])
      result.rows
    end)
  end

  defp with_connection(resolved, fun) do
    assert {:ok, conn} = ADBC.connect(resolved, [])

    try do
      fun.(conn)
    after
      ADBC.disconnect(conn, [])
    end
  end
end
