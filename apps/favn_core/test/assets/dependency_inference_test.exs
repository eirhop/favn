defmodule Favn.Assets.DependencyInferenceTest do
  use ExUnit.Case, async: true

  alias Favn.Asset.RelationInput
  alias Favn.Assets.DependencyInference
  alias Favn.RelationRef

  @upstream_ref {__MODULE__.Upstream, :asset}
  @downstream_ref {__MODULE__.Downstream, :asset}

  test "binds plain SQL relation candidates only to explicit dependencies" do
    upstream_relation = relation("raw", "sales", "orders")
    downstream_relation = relation("core", "sales", "order_summary")

    declared_input = relation_input("raw.sales.orders", upstream_relation)

    expression_false_positive =
      relation_input(
        "cluster.production_site_id",
        relation("core", "cluster", "production_site_id")
      )

    assets = [
      asset(@upstream_ref, upstream_relation),
      asset(
        @downstream_ref,
        downstream_relation,
        [@upstream_ref],
        [declared_input, expression_false_positive]
      )
    ]

    assert {:ok, catalog} = DependencyInference.infer_assets(assets)
    downstream = Enum.find(catalog.assets, &(&1.ref == @downstream_ref))

    assert downstream.depends_on == [@upstream_ref]
    assert downstream.diagnostics == []

    assert [
             %RelationInput{
               kind: :plain_relation,
               asset_ref: @upstream_ref,
               relation_ref: ^upstream_relation,
               resolution: :resolved
             }
           ] = downstream.relation_inputs

    assert [%{asset_ref: @upstream_ref, provenance: [:explicit]}] = downstream.dependencies
  end

  test "does not infer a dependency from undeclared plain SQL text" do
    upstream_relation = relation("raw", "sales", "orders")
    downstream_relation = relation("core", "sales", "order_summary")

    assets = [
      asset(@upstream_ref, upstream_relation),
      asset(
        @downstream_ref,
        downstream_relation,
        [],
        [relation_input("raw.sales.orders", upstream_relation)]
      )
    ]

    assert {:ok, catalog} = DependencyInference.infer_assets(assets)
    downstream = Enum.find(catalog.assets, &(&1.ref == @downstream_ref))

    assert downstream.depends_on == []
    assert downstream.dependencies == []
    assert downstream.relation_inputs == []
    assert downstream.diagnostics == []
  end

  defp asset(ref, relation, depends_on \\ [], relation_inputs \\ []) do
    %{
      ref: ref,
      relation: relation,
      depends_on: depends_on,
      relation_inputs: relation_inputs
    }
  end

  defp relation(catalog, schema, name) do
    RelationRef.new!(%{
      connection: :warehouse,
      catalog: catalog,
      schema: schema,
      name: name
    })
  end

  defp relation_input(raw, relation) do
    %RelationInput{
      kind: :plain_relation,
      raw: raw,
      relation_ref: relation
    }
  end
end
