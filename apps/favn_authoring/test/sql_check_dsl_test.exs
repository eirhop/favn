defmodule Favn.SQLCheckDSLTest do
  use ExUnit.Case, async: false

  alias Favn.SQL.{Check, Contract}
  alias Favn.SQL.Contract.{Column, Grain, Lineage, RowCount, UniqueKey}

  defmodule SourceAsset do
  end

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
      on_violation: :fail,
      message: "candidate must contain rows" do
      ~SQL"select count(*) > 0 as passed, count(*) as row_count from query()"
    end

    check :target_has_rows,
      at: :after_materialize,
      on_violation: :warn do
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
      on_violation: :fail do
      ~SQL"select count(*) > 0 as passed from file_candidate_rows(query())"
    end

    query do
      ~SQL"select 1 as order_id"
    end
  end

  defmodule ContractedRecords do
    use Favn.Namespace,
      relation: [connection: :warehouse, catalog: "generic", schema: "records"]

    use Favn.SQLAsset

    @materialized :table

    contract do
      grain(by: [:record_id], description: "one normalized record")

      column(:record_id, :integer,
        null: false,
        description: "stable record identity",
        tags: [:identifier],
        from: [{SourceAsset, :source_id}],
        via: :transformation
      )

      column(:payload, :string,
        from: [{{SourceAsset, :alternate}, :raw_payload}, {"external.dataset", "payload"}],
        renamed_from: :body
      )

      unique([:record_id])

      row_count(
        min: 1,
        when: :target_exists,
        on_violation: :skip_materialization
      )
    end

    check :payload_is_bounded, at: :before_materialize, on_violation: :warn do
      ~SQL"select true as passed from query()"
    end

    query do
      ~SQL"select 1 as record_id, 'value' as payload"
    end
  end

  test "compiles ordered checks with runtime relation usage" do
    definition = CheckedOrders.__favn_sql_asset_definition__()

    assert [
             %Check{
               name: :candidate_has_rows,
               at: :before_materialize,
               on_violation: :fail,
               uses_query?: true,
               uses_target?: false
             },
             %Check{
               name: :target_has_rows,
               at: :after_materialize,
               on_violation: :warn,
               uses_query?: false,
               uses_target?: true
             }
           ] = definition.checks
  end

  test "propagates runtime relation usage through nested file-backed defsql calls" do
    assert [%Check{uses_query?: true, uses_target?: false}] =
             NestedCheckedOrders.__favn_sql_asset_definition__().checks
  end

  test "compiles a typed contract and deterministic generated checks" do
    definition = ContractedRecords.__favn_sql_asset_definition__()

    assert %Contract{
             grain: %Grain{by: [:record_id], description: "one normalized record"},
             columns: [
               %Column{
                 name: :record_id,
                 type: :integer,
                 nullable?: false,
                 tags: ["identifier"],
                 via: :transformation,
                 sources: [
                   %Lineage{
                     kind: :asset,
                     asset_ref: {SourceAsset, :asset},
                     column: :source_id
                   }
                 ]
               },
               %Column{
                 name: :payload,
                 type: :string,
                 nullable?: true,
                 renamed_from: :body,
                 sources: [
                   %Lineage{kind: :asset, asset_ref: {SourceAsset, :alternate}},
                   %Lineage{kind: :external, dataset: "external.dataset", column: "payload"}
                 ]
               }
             ],
             unique_keys: [%UniqueKey{columns: [:record_id]}],
             row_count: %RowCount{
               min: 1,
               when: :target_exists,
               on_violation: :skip_materialization
             }
           } = definition.contract

    assert [row_count, not_null, unique, custom] = definition.checks
    assert row_count.origin == :contract
    assert row_count.claim_id == "row_count.min.1"
    assert row_count.on_violation == :skip_materialization
    assert row_count.when == :target_exists
    assert not_null.claim_id == "columns.not_null"
    assert unique.claim_id == "keys.unique"
    assert custom.origin == :authored
    assert custom.claim_id == nil
    assert Enum.all?([row_count, not_null, unique], & &1.uses_query?)
  end

  test "supports descriptive-only grain without generating a uniqueness claim" do
    definition =
      compile_definition!("""
      contract do
        grain description: "one emitted value"
        column :value, :string
      end
      """)

    assert %Contract{grain: %Grain{by: [], description: "one emitted value"}} =
             definition.contract

    assert definition.checks == []
  end

  test "rejects nullable structured grain columns" do
    assert_raise CompileError, ~r/contract grain column :id cannot be nullable/, fn ->
      compile_definition!("""
      contract do
        grain by: [:id]
        column :id, :integer
      end
      """)
    end
  end

  test "rejects missing contract key columns" do
    assert_raise CompileError, ~r/contract key references missing column :missing/, fn ->
      compile_definition!("""
      contract do
        column :id, :integer
        unique [:missing]
      end
      """)
    end
  end

  test "rejects the removed on_false option" do
    assert_raise CompileError, ~r/unknown SQL check option :on_false/, fn ->
      compile_definition!("""
      check :old_vocabulary, at: :before_materialize, on_false: :fail do
        ~SQL"select true as passed"
      end
      """)
    end
  end

  test "rejects duplicate check names" do
    assert_raise CompileError, ~r/duplicate SQL check :duplicate/, fn ->
      compile_definition!("""
      check :duplicate, at: :before_materialize, on_violation: :fail do
        ~SQL"select true as passed"
      end

      check :duplicate, at: :after_materialize, on_violation: :warn do
        ~SQL"select true as passed"
      end
      """)
    end
  end

  test "bounds checks per asset to the durable metadata limit" do
    checks =
      Enum.map_join(1..51, "\n", fn index ->
        """
        check :check_#{index}, at: :before_materialize, on_violation: :fail do
          ~SQL"select true as passed"
        end
        """
      end)

    assert_raise CompileError, ~r/SQL assets support at most 50 authored checks/, fn ->
      compile_definition!(checks)
    end
  end

  test "wide required contracts use one grouped non-null check" do
    columns =
      Enum.map_join(1..60, "\n", fn index ->
        "column :column_#{index}, :integer, null: false"
      end)

    definition =
      compile_definition!("""
      contract do
        #{columns}
      end
      """)

    assert [%Check{origin: :contract, claim_id: "columns.not_null"}] = definition.checks
    assert length(definition.contract.columns) == 60
  end

  test "rejects skip checks without a target existence condition" do
    assert_raise CompileError, ~r/:skip_materialization requires when: :target_exists/, fn ->
      compile_definition!("""
      check :skip, at: :before_materialize, on_violation: :skip_materialization do
        ~SQL"select false as passed"
      end
      """)
    end
  end

  test "rejects checked views" do
    assert_raise CompileError, ~r/SQL checks do not support :view materialization/, fn ->
      compile_definition!(
        """
        check :valid, at: :after_materialize, on_violation: :fail do
          ~SQL"select true as passed"
        end
        """,
        :view
      )
    end
  end

  test "rejects contracted views even when the contract generates no checks" do
    assert_raise CompileError,
                 ~r/SQL output contracts do not support :view materialization/,
                 fn ->
                   compile_definition!(
                     """
                     contract do
                       grain description: "one generated result"
                       column :id, :integer
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
        on_violation: :fail do
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
