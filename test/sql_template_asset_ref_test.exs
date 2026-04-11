defmodule Favn.SQLTemplateAssetRefTest do
  use ExUnit.Case

  alias Favn.SQL.Template

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true)
    end)

    :ok
  end

  test "detects direct asset refs in from and join positions" do
    sql = """
    select *
    from MyApp.Gold.Sales.FctOrders o
    left outer join /* customer lookup */ MyApp.Silver.Sales.StgCustomers c on c.customer_id = o.customer_id
    """

    template = Template.compile!(sql, file: "test/sql_template_asset_ref_test.exs", line: 1)

    assert Enum.map(template.asset_refs, & &1.module) == [
             MyApp.Gold.Sales.FctOrders,
             MyApp.Silver.Sales.StgCustomers
           ]
  end

  test "ignores module-looking values outside relation position" do
    sql = """
    -- from MyApp.Gold.Sales.FctOrders
    select 'MyApp.Gold.Sales.FctOrders' as label,
           "MyApp.Gold.Sales.FctOrders" as quoted,
           MyApp.Gold.Sales.FctOrders as expression_like
    """

    template = Template.compile!(sql, file: "test/sql_template_asset_ref_test.exs", line: 1)
    assert template.asset_refs == []
  end

  test "does not treat table function syntax as direct asset ref" do
    sql = """
    select *
    from MyApp.Gold.Sales.FctOrders(@window_start)
    """

    template = Template.compile!(sql, file: "test/sql_template_asset_ref_test.exs", line: 1)
    assert template.asset_refs == []
  end

  test "does not support update from and merge into asset refs" do
    update_sql = """
    update silver.sales.orders as o
    set customer_id = s.customer_id
    from MyApp.Silver.Sales.StgCustomers s
    """

    merge_sql = """
    merge into silver.sales.orders as tgt
    using MyApp.Silver.Sales.StgCustomers as src
    on tgt.customer_id = src.customer_id
    """

    update_template =
      Template.compile!(update_sql, file: "test/sql_template_asset_ref_test.exs", line: 1)

    merge_template =
      Template.compile!(merge_sql, file: "test/sql_template_asset_ref_test.exs", line: 1)

    assert update_template.asset_refs == []
    assert merge_template.asset_refs == []
  end

  test "raises for compiled module that is not a single asset module" do
    bad_module = Module.concat(__MODULE__, "NotAsset#{System.unique_integer([:positive])}")

    Code.compile_string(
      "defmodule #{inspect(bad_module)} do\nend",
      "test/dynamic_sql_template_asset_ref.exs"
    )

    assert_raise CompileError,
                 ~r/invalid SQL asset reference .* expected a compiled single-asset module/,
                 fn ->
                   Template.compile!(
                     "select * from #{inspect(bad_module)}",
                     file: "test/sql_template_asset_ref_test.exs",
                     line: 1
                   )
                 end
  end

  test "raises for compiled single-asset module without produced relation" do
    no_produces = Module.concat(__MODULE__, "NoProduces#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(no_produces)} do
        use Favn.Asset

        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_template_asset_ref.exs"
    )

    assert_raise CompileError,
                 ~r/does not resolve to a produced relation/,
                 fn ->
                   Template.compile!(
                     "select * from #{inspect(no_produces)}",
                     file: "test/sql_template_asset_ref_test.exs",
                     line: 1
                   )
                 end
  end

  test "raises on self-reference" do
    current_module = Module.concat(__MODULE__, "SelfRef#{System.unique_integer([:positive])}")

    assert_raise CompileError, ~r/cannot reference itself as a relation/, fn ->
      Template.compile!(
        "select * from #{inspect(current_module)}",
        file: "test/sql_template_asset_ref_test.exs",
        line: 1,
        module: current_module
      )
    end
  end
end
