defmodule Favn.SQLDSLTest do
  use ExUnit.Case

  alias Favn.Assets.Compiler
  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.Template
  alias Favn.SQLAsset.Compiler, as: SQLAssetCompiler

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true)
    end)

    :ok
  end

  test "supports defsql reusable SQL and query validation" do
    root = Module.concat(__MODULE__, "Root#{System.unique_integer([:positive])}")
    sql_module = Module.concat(root, SQL)
    raw_orders = Module.concat(root, RawOrders)
    asset_module = Module.concat(root, Revenue)

    Code.compile_string(
      """
      defmodule #{inspect(raw_orders)} do
        use Favn.Asset

        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_dsl_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(sql_module)} do
        use Favn.SQL

        defsql cents_to_dollars(amount_cents) do
          ~SQL[
          cast((@amount_cents / 100.0) as numeric(16, 2))
          ]
        end

        defsql orders_in_window(start_at, end_at) do
          ~SQL[
          select order_id, customer_id, total_amount
          from #{inspect(raw_orders)}
          where inserted_at >= @start_at
            and inserted_at < @end_at
          ]
        end
      end
      """,
      "test/dynamic_sql_dsl_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Namespace, connection: :warehouse, catalog: :gold, schema: :sales
        use Favn.SQLAsset
        use #{inspect(sql_module)}

        @materialized :table

        query do
          ~SQL[
          select
            customer_id,
            sum(cents_to_dollars(total_amount_cents)) as gross_revenue
          from orders_in_window(@window_start, @window_end)
          where (@country is null or country = @country)
          group by 1
          ]
        end
      end
      """,
      "test/dynamic_sql_dsl_test.exs"
    )

    assert {:ok, %Favn.SQLAsset.Definition{} = definition} =
             SQLAssetCompiler.fetch_definition(asset_module)

    assert [
             %SQLDefinition{name: :cents_to_dollars, arity: 1, shape: :expression},
             %SQLDefinition{name: :orders_in_window, arity: 2, shape: :relation}
           ] =
             Enum.sort_by(definition.sql_definitions, &{&1.name, &1.arity})

    assert definition.template.runtime_inputs == MapSet.new([:window_start, :window_end])
    assert definition.template.param_inputs == MapSet.new([:country])

    assert Enum.any?(definition.template.calls, fn %Template.Call{} = call ->
             call.name == :cents_to_dollars and call.context == :expression
           end)

    assert Enum.any?(definition.template.calls, fn %Template.Call{} = call ->
             call.name == :orders_in_window and call.context == :relation
           end)

    assert {:ok, [_asset]} = Compiler.compile_module_assets(asset_module)
  end

  test "rejects reserved runtime names as defsql arguments" do
    assert_raise CompileError, ~r/defsql argument @window_start is reserved/, fn ->
      Code.compile_string(
        """
        defmodule #{inspect(Module.concat(__MODULE__, "Reserved#{System.unique_integer([:positive])}"))} do
          use Favn.SQL

          defsql bad(window_start) do
            ~SQL[
            @window_start
            ]
          end
        end
        """,
        "test/dynamic_sql_dsl_test.exs"
      )
    end
  end

  test "rejects expression macro in relation position" do
    root = Module.concat(__MODULE__, "ExprInFrom#{System.unique_integer([:positive])}")
    sql_module = Module.concat(root, SQL)
    asset_module = Module.concat(root, Asset)

    Code.compile_string(
      """
      defmodule #{inspect(sql_module)} do
        use Favn.SQL

        defsql cents_to_dollars(amount_cents) do
          ~SQL[
          cast((@amount_cents / 100.0) as numeric(16, 2))
          ]
        end
      end
      """,
      "test/dynamic_sql_dsl_test.exs"
    )

    assert_raise CompileError,
                 ~r/invalid SQL call cents_to_dollars\/1 in relation position/,
                 fn ->
                   Code.compile_string(
                     """
                     defmodule #{inspect(asset_module)} do
                       use Favn.Namespace, connection: :warehouse, catalog: :gold, schema: :sales
                       use Favn.SQLAsset
                       use #{inspect(sql_module)}

                       @materialized :view

                       query do
                         ~SQL[
                         select *
                         from cents_to_dollars(total_amount_cents)
                         ]
                       end
                     end
                     """,
                     "test/dynamic_sql_dsl_test.exs"
                   )
                 end
  end

  test "rejects relation macro in expression position" do
    root = Module.concat(__MODULE__, "RelInExpr#{System.unique_integer([:positive])}")
    sql_module = Module.concat(root, SQL)
    raw_orders = Module.concat(root, RawOrders)
    asset_module = Module.concat(root, Asset)

    Code.compile_string(
      """
      defmodule #{inspect(raw_orders)} do
        use Favn.Asset

        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_dsl_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(sql_module)} do
        use Favn.SQL

        defsql orders_in_window(start_at, end_at) do
          ~SQL[
          select *
          from #{inspect(raw_orders)}
          where inserted_at >= @start_at and inserted_at < @end_at
          ]
        end
      end
      """,
      "test/dynamic_sql_dsl_test.exs"
    )

    assert_raise CompileError,
                 ~r/invalid SQL call orders_in_window\/2 in expression position/,
                 fn ->
                   Code.compile_string(
                     """
                     defmodule #{inspect(asset_module)} do
                       use Favn.Namespace, connection: :warehouse, catalog: :gold, schema: :sales
                       use Favn.SQLAsset
                       use #{inspect(sql_module)}

                       @materialized :view

                       query do
                         ~SQL[
                         select orders_in_window(@window_start, @window_end) as bad
                         ]
                       end
                     end
                     """,
                     "test/dynamic_sql_dsl_test.exs"
                   )
                 end
  end

  test "rejects invalid compiled module in relation position" do
    bad_module = Module.concat(__MODULE__, "BadRelation#{System.unique_integer([:positive])}")

    asset_module =
      Module.concat(__MODULE__, "BadRelationAsset#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(bad_module)} do
      end
      """,
      "test/dynamic_sql_dsl_test.exs"
    )

    assert_raise CompileError,
                 ~r/invalid SQL asset reference .*expected a compiled single-asset module/,
                 fn ->
                   Code.compile_string(
                     """
                     defmodule #{inspect(asset_module)} do
                       use Favn.Namespace, connection: :warehouse, catalog: :gold, schema: :sales
                       use Favn.SQLAsset

                        @materialized :view

                        query do
                          ~SQL[
                          select * from #{inspect(bad_module)}
                          ]
                        end
                      end
                     """,
                     "test/dynamic_sql_dsl_test.exs"
                   )
                 end
  end

  test "rejects defsql arity mismatch" do
    root = Module.concat(__MODULE__, "Arity#{System.unique_integer([:positive])}")
    sql_module = Module.concat(root, SQL)
    asset_module = Module.concat(root, Asset)

    Code.compile_string(
      """
      defmodule #{inspect(sql_module)} do
        use Favn.SQL

        defsql cents_to_dollars(amount_cents) do
          ~SQL[
          cast((@amount_cents / 100.0) as numeric(16, 2))
          ]
        end
      end
      """,
      "test/dynamic_sql_dsl_test.exs"
    )

    assert_raise CompileError,
                 ~r/invalid SQL call cents_to_dollars\/2; expected one of arities \[1\]/,
                 fn ->
                   Code.compile_string(
                     """
                     defmodule #{inspect(asset_module)} do
                       use Favn.Namespace, connection: :warehouse, catalog: :gold, schema: :sales
                       use Favn.SQLAsset
                       use #{inspect(sql_module)}

                       @materialized :view

                       query do
                         ~SQL[
                         select cents_to_dollars(total_amount_cents, fee_cents)
                         ]
                       end
                     end
                     """,
                     "test/dynamic_sql_dsl_test.exs"
                   )
                 end
  end
end
