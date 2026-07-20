defmodule FavnRunner.SQLMaterializationPlannerTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.Resolved
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

  test "uses the exact planned runtime window as the incremental write scope" do
    start_at = ~U[2026-07-14 00:00:00Z]
    end_at = ~U[2026-07-15 00:00:00Z]
    runtime = Runtime.new!(:day, start_at, end_at, Key.new!(:day, start_at, "Etc/UTC"))

    materialization =
      {:incremental, strategy: :delete_insert, window_column: :partition_day}

    assert {:ok, %WritePlan{} = write_plan} =
             MaterializationPlanner.build(
               session(transactions: :supported),
               definition(materialization, Spec.new!(:day, lookback: 1)),
               %Render{render(materialization) | runtime: runtime}
             )

    assert write_plan.window == runtime
    assert write_plan.effective_window == runtime

    assert write_plan.metadata.delete_scope == %{
             window_column: "partition_day",
             predicate: :half_open
           }
  end

  defp session(capability_opts \\ []) do
    %Session{
      adapter: __MODULE__.Adapter,
      resolved: %Resolved{
        name: :warehouse,
        adapter: __MODULE__.Adapter,
        module: __MODULE__,
        config: %{}
      },
      conn: :conn,
      capabilities: struct!(Capabilities, capability_opts)
    }
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

  defmodule Adapter do
    def relation(:conn, ref, _opts) do
      {:ok,
       %Relation{
         catalog: ref.catalog,
         schema: ref.schema,
         name: ref.name,
         type: :table
       }}
    end

    def query(:conn, _statement, _opts),
      do: {:ok, %Result{kind: :query, command: "SELECT", columns: ["id", "partition_day"]}}

    def columns(:conn, _ref, _opts),
      do:
        {:ok,
         [
           %Column{name: "id", position: 1, data_type: "INTEGER", nullable?: false},
           %Column{name: "partition_day", position: 2, data_type: "DATE", nullable?: false}
         ]}
  end
end
