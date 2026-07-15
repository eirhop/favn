defmodule Favn.SQLCheckDSLTest do
  use ExUnit.Case, async: false

  alias Favn.SQL.Check

  defmodule CheckSQL do
    use Favn.SQL

    defsql candidate_rows(relation) do
      ~SQL"select * from @relation"
    end

    defsql existing_target(value) do
      ~SQL"case when @value = 1 then (select count(*) from target()) else 0 end"
    end

    defsql(file_candidate_rows(relation), file: "fixtures/sql/file_candidate_rows.sql")
  end

  defmodule CheckedOrders do
    use Favn.Namespace,
      relation: [connection: :warehouse, catalog: "gold", schema: "sales"]

    use Favn.SQLAsset

    @materialized :table

    check :candidate_has_rows,
      at: :before_materialize,
      on_false: :fail,
      message: "candidate must contain rows" do
      ~SQL"select count(*) > 0 as passed, count(*) as row_count from query()"
    end

    check :target_has_rows,
      at: :after_materialize,
      on_false: :warn do
      ~SQL"select count(*) > 0 as passed, count(*) as row_count from target()"
    end

    query do
      ~SQL"select 1 as order_id"
    end
  end

  defmodule NestedCheckedOrders do
    use Favn.Namespace,
      relation: [connection: :warehouse, catalog: "gold", schema: "sales"]

    use CheckSQL
    use Favn.SQLAsset

    @materialized :table

    check :nested_candidate_has_rows,
      at: :before_materialize,
      on_false: :fail do
      ~SQL"select count(*) > 0 as passed from file_candidate_rows(query())"
    end

    query do
      ~SQL"select 1 as order_id"
    end
  end

  test "compiles ordered checks with runtime relation usage" do
    definition = CheckedOrders.__favn_sql_asset_definition__()

    assert [
             %Check{
               name: :candidate_has_rows,
               at: :before_materialize,
               on_false: :fail,
               uses_query?: true,
               uses_target?: false
             },
             %Check{
               name: :target_has_rows,
               at: :after_materialize,
               on_false: :warn,
               uses_query?: false,
               uses_target?: true
             }
           ] = definition.checks
  end

  test "propagates runtime relation usage through nested file-backed defsql calls" do
    assert [%Check{uses_query?: true, uses_target?: false}] =
             NestedCheckedOrders.__favn_sql_asset_definition__().checks
  end

  test "rejects duplicate check names" do
    assert_raise CompileError, ~r/duplicate SQL check :duplicate/, fn ->
      compile_definition!("""
      check :duplicate, at: :before_materialize, on_false: :fail do
        ~SQL"select true as passed"
      end

      check :duplicate, at: :after_materialize, on_false: :warn do
        ~SQL"select true as passed"
      end
      """)
    end
  end

  test "bounds checks per asset to the durable metadata limit" do
    checks =
      Enum.map_join(1..51, "\n", fn index ->
        """
        check :check_#{index}, at: :before_materialize, on_false: :fail do
          ~SQL"select true as passed"
        end
        """
      end)

    assert_raise CompileError, ~r/SQL assets support at most 50 checks/, fn ->
      compile_definition!(checks)
    end
  end

  test "rejects skip checks without a target existence condition" do
    assert_raise CompileError, ~r/:skip_materialization requires when: :target_exists/, fn ->
      compile_definition!("""
      check :skip, at: :before_materialize, on_false: :skip_materialization do
        ~SQL"select false as passed"
      end
      """)
    end
  end

  test "rejects checked views" do
    assert_raise CompileError, ~r/SQL checks do not support :view materialization/, fn ->
      compile_definition!(
        """
        check :valid, at: :after_materialize, on_false: :fail do
          ~SQL"select true as passed"
        end
        """,
        :view
      )
    end
  end

  test "rejects check-only runtime relations in the asset query" do
    assert_raise CompileError, ~r/query\(\) may only be used inside SQL check bodies/, fn ->
      compile_query!("", "select * from query()")
    end
  end

  test "rejects check-only runtime relations reached through nested defsql" do
    assert_raise CompileError, ~r/target\(\) may only be used inside SQL check bodies/, fn ->
      compile_query!(
        "use #{inspect(CheckSQL)}",
        "select existing_target(1) as target_count"
      )
    end
  end

  test "rejects duplicate check options" do
    assert_raise CompileError, ~r/duplicate SQL check options: :at/, fn ->
      compile_definition!("""
      check :duplicate_option,
        at: :before_materialize,
        at: :after_materialize,
        on_false: :fail do
        ~SQL"select true as passed"
      end
      """)
    end
  end

  defp compile_definition!(checks, materialization \\ :table) do
    module = Module.concat([__MODULE__, "Dynamic#{System.unique_integer([:positive])}"])

    Code.compile_string(
      """
      defmodule #{inspect(module)} do
        use Favn.Namespace, relation: [connection: :warehouse, schema: "test"]
        use Favn.SQLAsset

        @materialized #{inspect(materialization)}

        #{checks}

        query do
          ~SQL"select 1 as id"
        end
      end
      """,
      "test/sql_check_dsl_test.exs"
    )

    module.__favn_sql_asset_definition__()
  end

  defp compile_query!(preamble, query_sql) do
    module = Module.concat([__MODULE__, "DynamicQuery#{System.unique_integer([:positive])}"])

    Code.compile_string(
      """
      defmodule #{inspect(module)} do
        use Favn.Namespace, relation: [connection: :warehouse, schema: "test"]
        #{preamble}
        use Favn.SQLAsset

        @materialized :table

        query do
          ~SQL"#{query_sql}"
        end
      end
      """,
      "test/sql_check_dsl_test.exs"
    )

    module.__favn_sql_asset_definition__()
  end
end
