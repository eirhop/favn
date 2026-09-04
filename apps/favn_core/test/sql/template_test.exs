defmodule Favn.SQL.TemplateTest do
  use ExUnit.Case, async: false

  alias Favn.SQL.Definition
  alias Favn.SQL.Template
  alias Favn.SQL.Template.{Call, Placeholder, Relation, Text}

  test "one literal run preserves quotes, comments, formatting, and Unicode coordinates" do
    sql = "SELECT 'å; @ignored' AS label,\n    12.5 + (2 * 3) AS value /* @nope */;\n-- tail"

    template = Template.compile!(sql, file: "literal.sql", line: 7, column: 9, offset: 40)

    assert [%Text{sql: ^sql, span: span}] = template.nodes
    assert span == template.span
    assert {span.start_offset, span.start_line, span.start_column} == {40, 7, 9}

    assert {span.end_offset, span.end_line, span.end_column} ==
             {40 + length(String.to_charlist(sql)), 9, 8}

    assert Template.query_params(template) == MapSet.new()
  end

  test "special nodes keep exact source locations between maximal literal runs" do
    sql = "SELECT 'å' AS label, @country\nFROM warehouse.orders\nWHERE id = @id"
    template = Template.compile!(sql, file: "locations.sql", line: 3, offset: 100)

    assert [
             %Text{sql: "SELECT 'å' AS label, "},
             %Placeholder{name: "country", span: country_span},
             %Text{sql: "\nFROM "},
             %Relation{raw: "warehouse.orders", span: relation_span},
             %Text{sql: "\nWHERE id = "},
             %Placeholder{name: "id", span: id_span}
           ] = template.nodes

    assert {country_span.start_offset, country_span.start_line, country_span.start_column} ==
             {121, 3, 22}

    assert {relation_span.start_offset, relation_span.start_line, relation_span.start_column} ==
             {135, 4, 6}

    assert {id_span.start_line, id_span.start_column} == {5, 12}
    assert Template.query_params(template) == MapSet.new(["country", "id"])
  end

  test "helper argument fragments compact recursively without swallowing nested calls" do
    sql = "SELECT safe_macro( 1 + 2, safe_macro(3 * 4, @country)) AS value"

    template =
      Template.compile!(sql,
        known_definitions: %{{:safe_macro, 2} => definition(:safe_macro, 2)},
        file: "nested.sql",
        line: 1
      )

    assert [%Text{sql: "SELECT "}, %Call{args: [literal, nested]}, %Text{sql: " AS value"}] =
             template.nodes

    assert [%Text{sql: " 1 + 2"}] = literal.nodes
    assert [%Text{sql: " "}, %Call{args: [inner_literal, parameter]}] = nested.nodes
    assert [%Text{sql: "3 * 4"}] = inner_literal.nodes
    assert [%Text{sql: " "}, %Placeholder{name: "country", span: span}] = parameter.nodes
    assert span.start_offset == 44
    assert span.start_column == 45
    assert length(Template.calls(template)) == 2
  end

  test "reserves only window and Favn-owned execution runtime inputs" do
    assert Template.reserved_runtime_inputs() == [
             :window_start,
             :window_end,
             :favn_run_id,
             :favn_run_started_at
           ]

    template =
      Template.compile!("SELECT @favn_run_id, @favn_run_started_at",
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )

    assert Template.runtime_inputs(template) ==
             MapSet.new([:favn_run_id, :favn_run_started_at])

    assert Template.query_params(template) == MapSet.new()
  end

  test "keeps query parameter names as binaries without requiring existing atoms" do
    template =
      Template.compile!("SELECT @country AS value",
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )

    assert Template.query_params(template) == MapSet.new(["country"])
  end

  test "keeps arbitrary query parameter names as binaries" do
    name = "unsafe_param_#{System.unique_integer([:positive])}"

    template =
      Template.compile!("SELECT @#{name} AS value",
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )

    assert Template.query_params(template) == MapSet.new([name])
  end

  test "resolves known SQL calls by existing definition names" do
    template =
      Template.compile!("SELECT safe_macro(@safe_param) AS value",
        known_definitions: %{{:safe_macro, 1} => definition(:safe_macro, 1)},
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )

    assert [%Call{definition: %{name: :safe_macro, arity: 1}}] = Template.calls(template)
  end

  test "accepts normal literals as defsql arguments" do
    template =
      Template.compile!("SELECT safe_macro(1, 'value', false) AS value",
        known_definitions: %{{:safe_macro, 3} => definition(:safe_macro, 3)},
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )

    assert [%Call{definition: %{name: :safe_macro, arity: 3}, args: args}] =
             Template.calls(template)

    assert length(args) == 3
  end

  test "does not create atoms for unknown SQL call candidates" do
    name = "unsafe_call_#{System.unique_integer([:positive])}"

    template =
      Template.compile!("SELECT #{name}(1) AS value",
        known_definitions: %{{:safe_macro, 1} => definition(:safe_macro, 1)},
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )

    assert Template.calls(template) == []
  end

  test "fails loudly for unknown relation-style asset references" do
    module = "UnsafeAsset#{System.unique_integer([:positive])}"

    assert_raise CompileError, ~r/unknown SQL asset reference #{module}\.MissingAsset/, fn ->
      Template.compile!("SELECT * FROM #{module}.MissingAsset",
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )
    end
  end

  test "recognizes FROM only in relation clauses" do
    template =
      Template.compile!(
        """
        SELECT
          extract(day FROM orders),
          unit.id IS DISTINCT FROM orders
        FROM orders
        """,
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )

    assert [%Relation{raw: "orders"}] = Template.relation_refs(template)
  end

  test "recognizes comma-separated relations in a FROM list" do
    template =
      Template.compile!(
        "SELECT * FROM local_table, raw.sales.orders WHERE local_table.id = orders.id",
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )

    assert Enum.map(Template.relation_refs(template), & &1.raw) == [
             "local_table",
             "raw.sales.orders"
           ]
  end

  test "recognizes relations in nested query scopes" do
    template =
      Template.compile!("SELECT * FROM (SELECT * FROM orders) AS nested_orders",
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )

    assert [%Relation{raw: "orders"}] = Template.relation_refs(template)
  end

  test "rejects undefined defsql placeholders without returning non-name values" do
    assert_raise CompileError, ~r/undefined defsql placeholder @missing/, fn ->
      Template.compile!("coalesce(@known, @missing)",
        file: "test/fixtures/template_test.sql",
        line: 1,
        scope: :definition,
        local_args: [:known]
      )
    end
  end

  defp definition(name, arity) do
    %Definition{
      module: __MODULE__,
      name: name,
      arity: arity,
      params: [],
      shape: :expression,
      sql: "1",
      template: nil,
      file: "test/fixtures/template_test.sql",
      line: 1,
      declared_file: "test/fixtures/template_test.sql",
      declared_line: 1
    }
  end
end
