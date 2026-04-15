defmodule Favn.DSLCompilerTest do
  use ExUnit.Case, async: true

  defmodule RawCustomers do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "sales"]
    use Favn.Asset

    @meta owner: "data", category: :sales, tags: [:raw]
    @relation true
    def asset(_ctx), do: :ok
  end

  defmodule SalesSchedules do
    use Favn.Triggers.Schedules

    schedule(:daily, cron: "0 3 * * *", timezone: "Etc/UTC")
  end

  defmodule SalesPipeline do
    use Favn.Pipeline

    pipeline :daily_sales do
      asset(RawCustomers)
      deps(:all)
      schedule({SalesSchedules, :daily})
    end
  end

  defmodule SQLHelpers do
    use Favn.SQL

    defsql add_tax(amount) do
      ~SQL[@amount * 1.25]
    end
  end

  defmodule SalesSnapshot do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "gold", schema: "sales"]
    use Favn.SQLAsset

    @materialized :view
    @depends RawCustomers

    query do
      ~SQL[
      select *
      from RawCustomers
      ]
    end
  end

  test "asset and pipeline DSL compile through public facade" do
    assert Favn.asset_module?(RawCustomers)

    assert {:ok, [asset]} = Favn.list_assets([RawCustomers])
    assert asset.ref == {RawCustomers, :asset}

    assert {:ok, fetched} = Favn.get_asset(RawCustomers)
    assert fetched.ref == {RawCustomers, :asset}

    assert {:ok, pipeline} = Favn.get_pipeline(SalesPipeline)
    assert pipeline.name == :daily_sales
  end

  test "sql dsl compiles reusable definitions" do
    assert [%Favn.SQL.Definition{name: :add_tax, arity: 1}] =
             SQLHelpers.__favn_sql_definitions__()
  end

  test "sql asset dsl compiles into canonical sql asset" do
    assert {:ok, [asset]} = Favn.list_assets([SalesSnapshot])

    assert asset.ref == {SalesSnapshot, :asset}
    assert asset.type == :sql
  end

  test "runtime sql bridge now fails with normalized sql errors" do
    assert {:error, %Favn.SQL.Error{type: :invalid_config, operation: :connect}} =
             Favn.SQL.connect(:warehouse)

    assert {:error, %Favn.SQL.Error{type: :invalid_config, operation: :session}} =
             Favn.SQL.query(:session, "select 1")

    assert {:error, %Favn.SQL.Error{type: :invalid_config, operation: :session}} =
             Favn.SQL.materialize(:session, :plan)

    assert {:error, %Favn.SQL.Error{type: :invalid_config, operation: :session}} =
             Favn.SQL.get_relation(:session, :relation)

    assert {:error, %Favn.SQL.Error{type: :invalid_config, operation: :session}} =
             Favn.SQL.columns(:session, :relation)
  end
end
