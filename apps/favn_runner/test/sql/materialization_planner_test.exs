defmodule FavnRunner.SQLMaterializationPlannerTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.Resolved
  alias Favn.Manifest
  alias Favn.Manifest.{Asset, Graph, Version}
  alias Favn.RelationRef

  alias Favn.SQL.{
    Capabilities,
    Column,
    Params,
    Relation,
    Render,
    Result,
    Session,
    Template,
    WritePlan
  }

  alias Favn.SQLAsset.Definition
  alias Favn.Window.{Key, Runtime, Spec}
  alias FavnRunner.SQL.MaterializationPlanner

  test "materialization planner is runner-owned" do
    assert Code.ensure_loaded?(MaterializationPlanner)
    refute Code.ensure_loaded?(Favn.SQL.MaterializationPlanner)
  end

  test "builds a shared SQL write plan from runner SQL render output" do
    assert {:ok, %WritePlan{} = write_plan} =
             MaterializationPlanner.build(session(), definition(:table), render(:table))

    assert write_plan.asset_ref == {__MODULE__, :asset}
    assert write_plan.connection == :warehouse
    assert write_plan.materialization == :table
    assert write_plan.target.name == "orders"
    assert write_plan.target.type == :table
    assert write_plan.select_sql == "SELECT 1 AS id"
    assert write_plan.replace_existing? == true
    assert write_plan.metadata == %{rebuild?: true}
  end

  test "uses rehydrated options and the exact planned incremental window" do
    start_at = ~U[2026-07-14 00:00:00Z]
    end_at = ~U[2026-07-15 00:00:00Z]
    runtime = Runtime.new!(:day, start_at, end_at, Key.new!(:day, start_at, "Etc/UTC"))

    materialization =
      canonical_materialization(
        {:incremental, strategy: :delete_insert, window_column: :partition_day}
      )

    assert {:ok, %WritePlan{} = write_plan} =
             MaterializationPlanner.build(
               session(transactions: :supported),
               definition(materialization, Spec.new!(:day, lookback: 1)),
               %Render{render(materialization) | runtime: runtime}
             )

    assert write_plan.window == runtime
    assert write_plan.effective_window == runtime
    assert write_plan.strategy == :delete_insert
    assert write_plan.window_column == "partition_day"

    assert write_plan.metadata.delete_scope == %{
             window_column: "partition_day",
             predicate: :half_open
           }
  end

  test "uses staged relation columns instead of probing a zero-row query" do
    materialization =
      canonical_materialization(
        {:incremental, strategy: :delete_insert, window_column: :partition_day}
      )

    candidate = RelationRef.new!(name: "favn_check_candidate_123")

    assert {:ok, %WritePlan{strategy: :delete_insert}} =
             MaterializationPlanner.build(
               session([transactions: :supported], tracker: self()),
               definition(materialization, Spec.new!(:day)),
               incremental_render(materialization),
               {:relation, candidate}
             )

    assert_received {:columns, ^candidate}
    refute_received :query
  end

  test "reports unavailable query metadata instead of a missing delete-scope column" do
    materialization =
      canonical_materialization(
        {:incremental, strategy: :delete_insert, window_column: :partition_day}
      )

    assert {:error, error} =
             MaterializationPlanner.build(
               session([transactions: :supported], query_columns: []),
               definition(materialization, Spec.new!(:day)),
               incremental_render(materialization)
             )

    assert error.type == :materialization_planning_failed
    assert error.message == "incremental source column metadata is unavailable"
    assert error.details == %{source: :query, columns: []}
  end

  test "rejects a staged relation that genuinely lacks the delete-scope column" do
    materialization =
      canonical_materialization(
        {:incremental, strategy: :delete_insert, window_column: :partition_day}
      )

    candidate = RelationRef.new!(name: "favn_check_candidate_456")

    assert {:error, error} =
             MaterializationPlanner.build(
               session([transactions: :supported],
                 columns_by_relation: %{
                   candidate.name => [
                     %Column{name: "id", position: 1, data_type: "INTEGER", nullable?: false}
                   ]
                 }
               ),
               definition(materialization, Spec.new!(:day)),
               incremental_render(materialization),
               {:relation, candidate}
             )

    assert error.type == :materialization_planning_failed
    assert error.message == "incremental delete scope column is missing"

    assert error.details == %{
             window_column: "partition_day",
             source: :relation,
             columns: ["id"]
           }
  end

  defp session(capability_opts \\ [], adapter_opts \\ []) do
    default_columns = [
      %Column{name: "id", position: 1, data_type: "INTEGER", nullable?: false},
      %Column{name: "partition_day", position: 2, data_type: "DATE", nullable?: false}
    ]

    conn =
      Map.merge(
        %{
          columns_by_relation: %{},
          query_columns: ["id", "partition_day"],
          relation_columns: default_columns,
          tracker: nil
        },
        Map.new(adapter_opts)
      )

    %Session{
      adapter: __MODULE__.Adapter,
      resolved: %Resolved{
        name: :warehouse,
        adapter: __MODULE__.Adapter,
        module: __MODULE__,
        config: %{}
      },
      conn: conn,
      capabilities: struct!(Capabilities, capability_opts)
    }
  end

  defp incremental_render(materialization) do
    start_at = ~U[2026-07-14 00:00:00Z]
    end_at = ~U[2026-07-15 00:00:00Z]
    runtime = Runtime.new!(:day, start_at, end_at, Key.new!(:day, start_at, "Etc/UTC"))
    %Render{render(materialization) | runtime: runtime}
  end

  defp definition(materialization, window_spec \\ nil) do
    template =
      Template.compile!("SELECT 1 AS id",
        file: "test/fixtures/materialization_planner_test.sql",
        line: 1,
        enforce_query_root: true
      )

    %Definition{
      module: __MODULE__,
      asset: %{
        ref: {__MODULE__, :asset},
        relation: relation(),
        file: "test/fixtures/materialization_planner_test.sql",
        window_spec: window_spec
      },
      sql: template.source,
      template: template,
      materialization: materialization
    }
  end

  defp render(materialization) do
    %Render{
      asset_ref: {__MODULE__, :asset},
      connection: :warehouse,
      relation: relation(),
      materialization: materialization,
      sql: "SELECT 1 AS id",
      params: %Params{format: :positional, bindings: []}
    }
  end

  defp relation do
    RelationRef.new!(%{connection: :warehouse, schema: "analytics", name: "orders"})
  end

  defp canonical_materialization(materialization) do
    ref = {__MODULE__, :asset}

    manifest = %Manifest{
      assets: [
        %Asset{
          ref: ref,
          module: elem(ref, 0),
          name: elem(ref, 1),
          type: :sql,
          materialization: materialization,
          execution_package_hash: String.duplicate("a", 64)
        }
      ],
      graph: %Graph{nodes: [ref], topo_order: [ref]}
    }

    assert {:ok, version} =
             Version.new(manifest, manifest_version_id: "mv_runner_incremental_materialization")

    version.manifest.assets |> hd() |> Map.fetch!(:materialization)
  end

  defmodule Adapter do
    def relation(_conn, ref, _opts) do
      {:ok,
       %Relation{
         catalog: ref.catalog,
         schema: ref.schema,
         name: ref.name,
         type: :table
       }}
    end

    def query(conn, _statement, _opts) do
      notify(conn, :query)
      {:ok, %Result{kind: :query, command: "SELECT", columns: conn.query_columns}}
    end

    def columns(conn, ref, _opts) do
      notify(conn, {:columns, ref})
      {:ok, Map.get(conn.columns_by_relation, ref.name, conn.relation_columns)}
    end

    defp notify(%{tracker: tracker}, message) when is_pid(tracker), do: send(tracker, message)
    defp notify(_conn, _message), do: :ok
  end
end
