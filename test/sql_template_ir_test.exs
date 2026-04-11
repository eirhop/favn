defmodule Favn.SQLTemplateIRTest do
  use ExUnit.Case

  alias Favn.SQL.Template

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true)
    end)

    :ok
  end

  test "ignores placeholders inside strings and comments" do
    template =
      Template.compile!(
        """
        -- @ignored_line
        select '@ignored_string', col /* @ignored_block */
        from sales.orders
        where country = @country
        """,
        file: "test/sql_template_ir_test.exs",
        line: 1
      )

    assert Template.query_params(template) == MapSet.new([:country])
    assert Template.runtime_inputs(template) == MapSet.new()
  end

  test "rejects malformed placeholders" do
    assert_raise CompileError, ~r/invalid SQL placeholder/, fn ->
      Template.compile!("select @1bad", file: "test/sql_template_ir_test.exs", line: 1)
    end
  end

  test "infers query root kind after leading comments" do
    template =
      Template.compile!(
        """
        -- leading comment
        /* another comment */
        with src as (select 1)
        select * from src
        """,
        file: "test/sql_template_ir_test.exs",
        line: 1
      )

    assert template.root_kind == :query
  end

  test "preserves repeated calls and nested call args" do
    root = Module.concat(__MODULE__, "Nested#{System.unique_integer([:positive])}")
    sql_module = Module.concat(root, SQL)

    Code.compile_string(
      """
      defmodule #{inspect(sql_module)} do
        use Favn.SQL

        defsql cents_to_dollars(amount_cents) do
          ~SQL[cast((@amount_cents / 100.0) as numeric(16, 2))]
        end

        defsql add_money(left_amount, right_amount) do
          ~SQL[@left_amount + @right_amount]
        end
      end
      """,
      "test/dynamic_sql_template_ir_test.exs"
    )

    definitions = sql_module.__favn_sql_definitions__()
    catalog = Map.new(definitions, &{{&1.name, &1.arity}, &1})

    template =
      Template.compile!(
        "select add_money(cents_to_dollars(fee_cents), cents_to_dollars(tax_cents)), cents_to_dollars(total_cents)",
        file: "test/sql_template_ir_test.exs",
        line: 1,
        known_definitions: catalog,
        scope: :query
      )

    calls = Template.calls(template)

    assert Enum.count(calls, &(&1.definition.name == :cents_to_dollars)) == 3
    assert Enum.count(calls, &(&1.definition.name == :add_money)) == 1
  end

  test "parses relation macro calls across newlines" do
    root = Module.concat(__MODULE__, "Relation#{System.unique_integer([:positive])}")
    sql_module = Module.concat(root, SQL)
    raw_orders = Module.concat(root, RawOrders)

    Code.compile_string(
      """
      defmodule #{inspect(raw_orders)} do
        use Favn.Namespace, connection: :warehouse, catalog: :raw, schema: :sales
        use Favn.Asset

        @produces true

        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_template_ir_test.exs"
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
      "test/dynamic_sql_template_ir_test.exs"
    )

    [definition] = sql_module.__favn_sql_definitions__()
    catalog = %{{definition.name, definition.arity} => definition}

    template =
      Template.compile!(
        "select * from orders_in_window(\n  @window_start,\n  @window_end\n)",
        file: "test/sql_template_ir_test.exs",
        line: 1,
        known_definitions: catalog,
        scope: :query
      )

    assert [
             %Template.Call{
               context: :relation,
               definition: %{name: :orders_in_window},
               args: [_, _]
             }
           ] =
             Template.calls(template)
  end

  test "rejects undefined defsql placeholders" do
    assert_raise CompileError, ~r/undefined defsql placeholder @country/, fn ->
      Code.compile_string(
        """
        defmodule #{inspect(Module.concat(__MODULE__, "BadPlaceholder#{System.unique_integer([:positive])}"))} do
          use Favn.SQL

          defsql bad_filter(start_at) do
            ~SQL[
            @country >= @start_at
            ]
          end
        end
        """,
        "test/dynamic_sql_template_ir_test.exs"
      )
    end
  end

  test "rejects duplicate imported visible definitions" do
    root = Module.concat(__MODULE__, "Conflicts#{System.unique_integer([:positive])}")
    sql_a = Module.concat(root, SQLA)
    sql_b = Module.concat(root, SQLB)
    asset_module = Module.concat(root, Asset)

    Code.compile_string(
      """
      defmodule #{inspect(sql_a)} do
        use Favn.SQL

        defsql cents_to_dollars(amount_cents) do
          ~SQL[@amount_cents]
        end
      end
      """,
      "test/dynamic_sql_template_ir_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(sql_b)} do
        use Favn.SQL

        defsql cents_to_dollars(amount_cents) do
          ~SQL[@amount_cents]
        end
      end
      """,
      "test/dynamic_sql_template_ir_test.exs"
    )

    assert_raise CompileError, ~r/duplicate visible defsql cents_to_dollars\/1/, fn ->
      Code.compile_string(
        """
        defmodule #{inspect(asset_module)} do
          use Favn.Namespace, connection: :warehouse, catalog: :gold, schema: :sales
          use Favn.SQLAsset
          use #{inspect(sql_a)}
          use #{inspect(sql_b)}

          @materialized :view

          query do
            ~SQL[select 1]
          end
        end
        """,
        "test/dynamic_sql_template_ir_test.exs"
      )
    end
  end

  test "rejects cyclic defsql definitions" do
    assert_raise CompileError, ~r/cyclic defsql definitions detected/, fn ->
      Code.compile_string(
        """
        defmodule #{inspect(Module.concat(__MODULE__, "Cycles#{System.unique_integer([:positive])}"))} do
          use Favn.SQL

          defsql first_filter(amount_cents) do
            ~SQL[second_filter(@amount_cents)]
          end

          defsql second_filter(amount_cents) do
            ~SQL[first_filter(@amount_cents)]
          end
        end
        """,
        "test/dynamic_sql_template_ir_test.exs"
      )
    end
  end
end
