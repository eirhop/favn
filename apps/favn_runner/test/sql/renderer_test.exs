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

  test "renders schema-only and catalog-schema asset refs distinctly" do
    schema_source = Module.concat(__MODULE__, SchemaSource)
    catalog_schema_source = Module.concat(__MODULE__, CatalogSchemaSource)

    definition =
      asset_ref_definition(
        "SELECT * FROM #{inspect(schema_source)} JOIN #{inspect(catalog_schema_source)} USING (id)",
        %{
          schema_source =>
            RelationRef.new!(%{connection: :warehouse, schema: "gold", name: "orders"}),
          catalog_schema_source =>
            RelationRef.new!(%{
              connection: :warehouse,
              catalog: "lakehouse",
              schema: "gold",
              name: "customers"
            })
        }
      )

    assert {:ok, rendered} = Renderer.render(definition)

    assert rendered.sql == "SELECT * FROM gold.orders JOIN lakehouse.gold.customers USING (id)"
  end

  test "rejects catalog-qualified asset refs without schema" do
    source = Module.concat(__MODULE__, CatalogOnlySource)

    definition =
      asset_ref_definition(
        "SELECT * FROM #{inspect(source)}",
        %{
          source =>
            RelationRef.new!(%{connection: :warehouse, catalog: "lakehouse", name: "orders"})
        }
      )

    assert {:error, error} = Renderer.render(definition)

    assert error.type == :invalid_relation
    assert error.phase == :render
    assert error.message =~ "catalog-qualified SQL references require schema"
    assert error.message =~ "catalog \"lakehouse\""
    assert error.message =~ "name \"orders\""
  end

  test "rejects catalog-qualified target relations without schema" do
    definition =
      definition(%{
        connection: :warehouse,
        catalog: "mart",
        name: "order_summary"
      })

    assert {:error, error} = Renderer.render(definition, params: %{country: "NO"})

    assert error.type == :invalid_relation
    assert error.phase == :render
    assert error.message =~ "SQL asset target relations with catalog require schema"
    assert error.message =~ "catalog \"mart\""
    assert error.message =~ "name \"order_summary\""
  end

  test "renders plain relation refs with inherited catalog and schema" do
    definition =
      definition(
        %{
          connection: :warehouse,
          catalog: "mart",
          schema: "sales",
          name: "order_summary"
        },
        "SELECT * FROM orders JOIN finance.invoices USING (order_id) JOIN raw.finance.payments USING (invoice_id)"
      )

    assert {:ok, rendered} = Renderer.render(definition)

    assert rendered.sql ==
             "SELECT * FROM mart.sales.orders JOIN mart.finance.invoices USING (order_id) JOIN raw.finance.payments USING (invoice_id)"
  end

  defp definition(
         relation_attrs \\ %{connection: :warehouse, schema: "gold", name: "orders"},
         sql \\ "SELECT @country AS country"
       ) do
    template =
      Template.compile!(sql,
        file: "test/fixtures/renderer_test.sql",
        line: 1,
        enforce_query_root: true
      )

    %Definition{
      module: __MODULE__,
      asset: %{
        ref: {__MODULE__, :asset},
        relation: RelationRef.new!(relation_attrs),
        file: "test/fixtures/renderer_test.sql",
        window_spec: nil
      },
      sql: template.source,
      template: template,
      materialization: :view
    }
  end

  defp asset_ref_definition(sql, relation_by_module)
       when is_binary(sql) and is_map(relation_by_module) do
    template =
      Template.compile!(sql,
        file: "test/fixtures/renderer_asset_ref_test.sql",
        line: 1,
        enforce_query_root: true
      )

    %Definition{
      module: __MODULE__,
      asset: %{
        ref: {__MODULE__, :asset},
        relation: RelationRef.new!(%{connection: :warehouse, schema: "gold", name: "target"}),
        file: "test/fixtures/renderer_asset_ref_test.sql",
        window_spec: nil
      },
      sql: template.source,
      template: template,
      materialization: :view,
      raw_asset: %{
        manifest_relation_by_module: relation_by_module,
        deferred_resolution: :manifest_only
      }
    }
  end
end
