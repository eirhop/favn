defmodule Favn.SQL.TemplateTest do
  use ExUnit.Case, async: false

  alias Favn.SQL.Definition
  alias Favn.SQL.Template
  alias Favn.SQL.Template.Call

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
