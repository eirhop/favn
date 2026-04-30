defmodule FavnRunner.SQLRendererTest do
  use ExUnit.Case, async: true

  alias Favn.RelationRef
  alias Favn.SQL.Template
  alias Favn.SQLAsset.Definition
  alias Favn.SQLAsset.Renderer

  test "renders binary query params from string keys" do
    assert {:ok, rendered} = Renderer.render(definition(), params: %{"country" => "NO"})

    assert rendered.sql == "SELECT ? AS country"
    assert [%{name: "country", value: "NO"}] = rendered.params.bindings
    assert rendered.metadata.query_param_names == ["country"]
  end

  test "renders binary query params from atom keys" do
    assert {:ok, rendered} = Renderer.render(definition(), params: %{country: "NO"})

    assert rendered.sql == "SELECT ? AS country"
    assert [%{name: "country", value: "NO"}] = rendered.params.bindings
  end

  defp definition do
    template =
      Template.compile!("SELECT @country AS country",
        file: "test/fixtures/renderer_test.sql",
        line: 1,
        enforce_query_root: true
      )

    %Definition{
      module: __MODULE__,
      asset: %{
        ref: {__MODULE__, :asset},
        relation: RelationRef.new!(%{connection: :warehouse, schema: "gold", name: "orders"}),
        file: "test/fixtures/renderer_test.sql",
        window_spec: nil
      },
      sql: template.source,
      template: template,
      materialization: :view
    }
  end
end
