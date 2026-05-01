defmodule FavnRunner.SQLMaterializationPlannerTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.{Capabilities, Params, Render, Session, Template, WritePlan}
  alias Favn.SQLAsset.Definition
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

  defp session do
    %Session{
      adapter: __MODULE__.Adapter,
      resolved: %Resolved{
        name: :warehouse,
        adapter: __MODULE__.Adapter,
        module: __MODULE__,
        config: %{}
      },
      conn: :conn,
      capabilities: %Capabilities{}
    }
  end

  defp definition(materialization) do
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
        window_spec: nil
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
  end
end
