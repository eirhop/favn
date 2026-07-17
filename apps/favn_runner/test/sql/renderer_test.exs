defmodule FavnRunner.SQLRendererTest do
  use ExUnit.Case, async: true

  alias Favn.RelationRef
  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.Contract
  alias Favn.SQL.Contract.Param
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

  test "binds referenced asset settings without copying unused settings" do
    definition = put_in(definition().asset.settings, %{country: "NO", unused: %{nested: true}})

    assert {:ok, rendered} = Renderer.render(definition)
    assert [%{name: "country", source: :setting, value: "NO"}] = rendered.params.bindings
  end

  test "rejects collisions between settings and runtime params" do
    definition = put_in(definition().asset.settings, %{country: "NO"})

    assert {:error, error} = Renderer.render(definition, params: %{country: "SE"})
    assert error.type == :binding_failure
    assert error.message =~ "declared in both asset settings and runtime params"
  end

  test "rejects non-scalar settings only when SQL references them" do
    definition = put_in(definition().asset.settings, %{country: ["NO"]})

    assert {:error, error} = Renderer.render(definition)
    assert error.message =~ "must be a scalar bind value"
  end

  test "binds Favn-owned run identity and start time as runtime inputs" do
    started_at = ~U[2026-07-17 10:15:30Z]

    definition =
      definition(
        %{connection: :warehouse, schema: "gold", name: "orders"},
        "SELECT @favn_run_id AS run_id, @favn_run_started_at AS started_at"
      )

    assert {:ok, rendered} =
             Renderer.render(definition,
               runtime: %{favn_run_id: "run-123", favn_run_started_at: started_at}
             )

    assert rendered.sql == "SELECT ? AS run_id, ? AS started_at"

    assert [
             %{name: :favn_run_id, source: :runtime, value: "run-123"},
             %{name: :favn_run_started_at, source: :runtime, value: ^started_at}
           ] = rendered.params.bindings

    assert rendered.metadata.runtime_input_names == [:favn_run_id, :favn_run_started_at]
  end

  test "rejects missing or invalid Favn-owned runtime inputs without exposing values" do
    definition =
      definition(
        %{connection: :warehouse, schema: "gold", name: "orders"},
        "SELECT @favn_run_id AS run_id"
      )

    assert {:error, missing} = Renderer.render(definition)
    assert missing.type == :missing_runtime_input
    assert missing.message =~ ":favn_run_id"

    assert {:error, invalid} = Renderer.render(definition, runtime: %{favn_run_id: 42})
    assert invalid.type == :binding_failure
    assert invalid.details == %{name: :favn_run_id, expected: "a non-empty string"}
    refute inspect(invalid.details) =~ "42"
  end

  test "rejects atom and string reserved names in params and settings without exposing values" do
    collisions = [
      {definition(), [params: %{favn_run_id: "spoofed-run"}], :favn_run_id, :params},
      {definition(), [params: %{"favn_run_started_at" => "spoofed-start"}], :favn_run_started_at,
       :params},
      {put_in(definition().asset.settings, %{window_start: "spoofed-window"}), [], :window_start,
       :settings},
      {put_in(definition().asset.settings, %{"window_end" => "spoofed-window"}), [], :window_end,
       :settings}
    ]

    Enum.each(collisions, fn {definition, opts, name, source} ->
      assert {:error, error} = Renderer.render(definition, opts)
      assert error.type == :binding_failure
      assert error.details == %{name: name, source: source}
      refute inspect(error) =~ "spoofed"

      assert {:error, contract_error} = Renderer.validate_contract_params(definition, opts)
      assert contract_error.details == %{name: name, source: source}
      refute inspect(contract_error) =~ "spoofed"
    end)
  end

  test "validates parameterized row-count inputs before SQL execution" do
    contract =
      Contract.new!(%{
        columns: [%{name: :id, type: :integer}],
        row_count: [equals: Param.new!(:expected_rows)]
      })

    definition = %{definition() | contract: contract}

    assert :ok = Renderer.validate_contract_params(definition, params: %{expected_rows: 10})

    assert {:error, missing} = Renderer.validate_contract_params(definition, params: %{})
    assert missing.type == :missing_query_param
    assert missing.message == "missing SQL contract parameter @expected_rows"

    assert {:error, invalid} =
             Renderer.validate_contract_params(definition, params: %{expected_rows: -1})

    assert invalid.type == :binding_failure
    assert invalid.details == %{name: :expected_rows, expected: :non_neg_integer}
    refute Map.has_key?(invalid.details, :value)
  end

  test "contract parameters follow the normal setting and collision rules" do
    contract =
      Contract.new!(%{
        columns: [%{name: :id, type: :integer}],
        row_count: [equals: Param.new!(:expected_rows)]
      })

    definition =
      definition()
      |> Map.put(:contract, contract)
      |> put_in([Access.key!(:asset), Access.key!(:settings)], %{expected_rows: 10})

    assert :ok = Renderer.validate_contract_params(definition, params: %{})

    assert {:error, collision} =
             Renderer.validate_contract_params(definition, params: %{expected_rows: 10})

    assert collision.type == :binding_failure
    assert collision.details.sources == [:settings, :params]
  end

  test "rejects value placeholders in relation position" do
    assert_raise CompileError,
                 ~r/placeholders are values, not relation or identifier names/,
                 fn ->
                   Template.compile!("SELECT * FROM @source",
                     file: "test/fixtures/renderer_test.sql",
                     line: 1,
                     enforce_query_root: true
                   )
                 end
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

  test "renders defsql plain relation refs with definition namespace defaults" do
    sql_definition =
      reusable_sql_definition(
        :orders,
        [:ignored],
        "(SELECT * FROM orders) AS scoped_orders",
        %{catalog: "raw", schema: "sales"}
      )

    definition =
      definition(
        %{
          connection: :warehouse,
          catalog: "mart",
          schema: "sales",
          name: "order_summary"
        },
        "SELECT * FROM orders(@country)",
        [sql_definition]
      )

    assert {:ok, rendered} = Renderer.render(definition, params: %{country: "NO"})
    assert rendered.sql == "SELECT * FROM (SELECT * FROM raw.sales.orders) AS scoped_orders"
  end

  defp definition(
         relation_attrs \\ %{connection: :warehouse, schema: "gold", name: "orders"},
         sql \\ "SELECT @country AS country",
         sql_definitions \\ []
       ) do
    definition_catalog = Map.new(sql_definitions, &{SQLDefinition.key(&1), &1})

    template =
      Template.compile!(sql,
        known_definitions: definition_catalog,
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
        window_spec: nil,
        settings: %{}
      },
      sql: template.source,
      template: template,
      sql_definitions: sql_definitions,
      materialization: :view
    }
  end

  defp reusable_sql_definition(name, params, sql, relation_defaults) do
    sql_params =
      params
      |> Enum.with_index(fn param, index -> %SQLDefinition.Param{name: param, index: index} end)

    template =
      Template.compile!(sql,
        file: "test/fixtures/reusable_renderer_test.sql",
        line: 1,
        module: Module.concat(__MODULE__, ReusableSQL),
        scope: :definition,
        local_arg_index: Map.new(sql_params, &{&1.name, &1.index}),
        enforce_query_root: false
      )

    %SQLDefinition{
      module: Module.concat(__MODULE__, ReusableSQL),
      name: name,
      arity: length(params),
      params: sql_params,
      shape: :relation,
      sql: sql,
      template: template,
      file: "test/fixtures/reusable_renderer_test.sql",
      line: 1,
      declared_file: "test/fixtures/reusable_renderer_test.ex",
      declared_line: 1,
      relation_defaults: relation_defaults
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
        window_spec: nil,
        settings: %{}
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
