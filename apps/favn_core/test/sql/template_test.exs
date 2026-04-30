defmodule Favn.SQL.TemplateTest do
  use ExUnit.Case, async: false

  alias Favn.SQL.Definition
  alias Favn.SQL.Template
  alias Favn.SQL.Template.Call

  test "parses existing query parameter atoms without creating new atoms" do
    template =
      Template.compile!("SELECT @safe_param AS value",
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )

    assert Template.query_params(template) == MapSet.new([:safe_param])
  end

  test "rejects unknown query parameter atoms" do
    name = "unsafe_param_#{System.unique_integer([:positive])}"

    assert_raise CompileError, ~r/unknown SQL placeholder @#{name}/, fn ->
      Template.compile!("SELECT @#{name} AS value",
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )
    end
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

  test "does not create module atoms for unknown relation-style asset references" do
    module = "UnsafeAsset#{System.unique_integer([:positive])}"

    template =
      Template.compile!("SELECT * FROM #{module}.MissingAsset",
        file: "test/fixtures/template_test.sql",
        line: 1,
        enforce_query_root: true
      )

    assert Template.asset_refs(template) == []
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
