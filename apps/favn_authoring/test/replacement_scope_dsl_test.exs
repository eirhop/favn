defmodule Favn.ReplacementScopeDSLTest do
  use ExUnit.Case, async: true

  alias Favn.SQLAsset.Definition
  alias Favn.SQLAsset.RelationUsage

  test "compiles inline incremental and full scopes with a composite key" do
    definition =
      compile_definition!("""
      materialized {:incremental,
                    strategy: :replace_groups,
                    replacement_key: [:tenant_id, :customer_id]}

      replacement_scope :incremental do
        ~SQL"select tenant_id, customer_id from changed_customers"
      end

      replacement_scope :full do
        ~SQL"select tenant_id, customer_id from all_customers"
      end

      query do
        ~SQL"select rows.* from prepared_rows rows join replacement_scope() scope using (tenant_id, customer_id)"
      end
      """)

    assert %Definition{
             materialization:
               {:incremental,
                strategy: :replace_groups, replacement_key: [:tenant_id, :customer_id]},
             incremental_scope_sql: "select tenant_id, customer_id from changed_customers",
             full_scope_sql: "select tenant_id, customer_id from all_customers"
           } = definition

    assert RelationUsage.runtime_relations(definition.template) ==
             MapSet.new([:replacement_scope])
  end

  test "compiles file-backed scopes" do
    definition =
      compile_definition!("""
      materialized {:incremental, strategy: :replace_groups, replacement_key: [:customer_id]}
      replacement_scope :incremental, file: "fixtures/sql/changed_customer_scope.sql"
      replacement_scope :full, file: "fixtures/sql/all_customer_scope.sql"

      query do
        ~SQL"select rows.* from prepared_rows rows join replacement_scope() using (customer_id)"
      end
      """)

    assert definition.incremental_scope_sql =~ "changed_customers"
    assert definition.full_scope_sql =~ "all_customers"
  end

  test "requires one scope of each mode" do
    assert_raise CompileError, ~r/requires exactly one replacement_scope :full/, fn ->
      compile_definition!("""
      materialized {:incremental, strategy: :replace_groups, replacement_key: [:customer_id]}
      replacement_scope :incremental do
        ~SQL"select customer_id from changed_customers"
      end
      query do
        ~SQL"select * from replacement_scope()"
      end
      """)
    end
  end

  test "rejects duplicate scope modes" do
    assert_raise CompileError, ~r/duplicate replacement_scope :incremental/, fn ->
      compile_definition!("""
      materialized {:incremental, strategy: :replace_groups, replacement_key: [:customer_id]}
      replacement_scope :incremental do
        ~SQL"select customer_id from changed_customers"
      end
      replacement_scope :incremental do
        ~SQL"select customer_id from other_changes"
      end
      replacement_scope :full do
        ~SQL"select customer_id from all_customers"
      end
      query do
        ~SQL"select * from replacement_scope()"
      end
      """)
    end
  end

  test "rejects runtime relations inside scope SQL" do
    assert_raise CompileError, ~r/may not use runtime SQL relations: target\(\)/, fn ->
      compile_definition!("""
      materialized {:incremental, strategy: :replace_groups, replacement_key: [:customer_id]}
      replacement_scope :incremental do
        ~SQL"select customer_id from target()"
      end
      replacement_scope :full do
        ~SQL"select customer_id from all_customers"
      end
      query do
        ~SQL"select * from replacement_scope()"
      end
      """)
    end
  end

  test "rejects replacement_scope outside replace_groups" do
    assert_raise CompileError, ~r/replacement_scope declarations require/, fn ->
      compile_definition!("""
      materialized :table
      replacement_scope :incremental do
        ~SQL"select customer_id from changed_customers"
      end
      query do
        ~SQL"select 1 as customer_id"
      end
      """)
    end
  end

  test "rejects missing, duplicate, or non-atom replacement keys" do
    for materialization <- [
          "{:incremental, strategy: :replace_groups}",
          "{:incremental, strategy: :replace_groups, replacement_key: []}",
          "{:incremental, strategy: :replace_groups, replacement_key: [:id, :id]}",
          "{:incremental, strategy: :replace_groups, replacement_key: [\"id\"]}"
        ] do
      assert_raise CompileError, fn ->
        compile_definition!("""
        materialized #{materialization}
        replacement_scope :incremental do
          ~SQL"select id from changes"
        end
        replacement_scope :full do
          ~SQL"select id from all_ids"
        end
        query do
          ~SQL"select * from replacement_scope()"
        end
        """)
      end
    end
  end

  test "replace_groups is non-windowed" do
    assert_raise CompileError, ~r/non-windowed and does not accept window/, fn ->
      compile_definition!("""
      window Favn.Window.daily()
      materialized {:incremental, strategy: :replace_groups, replacement_key: [:id]}
      replacement_scope :incremental do
        ~SQL"select id from changes"
      end
      replacement_scope :full do
        ~SQL"select id from all_ids"
      end
      query do
        ~SQL"select * from replacement_scope()"
      end
      """)
    end
  end

  defp compile_definition!(body) do
    module = Module.concat(__MODULE__, "Dynamic#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module)} do
        use Favn.SQLAsset
        relation connection: :warehouse, schema: "test"
        #{body}
      end
      """,
      "test/replacement_scope_dsl_test.exs"
    )

    module.__favn_sql_asset_definition__()
  end
end
