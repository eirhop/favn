defmodule FavnRunner.RelationshipGenerationTest do
  use ExUnit.Case, async: true
  alias Favn.Contracts.{RunnerWork, TargetGenerationPin}
  alias Favn.Manifest.Asset
  alias Favn.RelationRef
  alias Favn.SQL.{Check, Template}
  alias Favn.SQL.Contract.Relationship
  alias Favn.SQLAsset.{Definition, Renderer}
  alias FavnRunner.GenerationWork

  test "relationship checks read the pinned generation even when compiled references contain the active relation" do
    active = RelationRef.new!(connection: :warehouse, schema: "mart", name: "store_active")
    pinned = %{active | name: "store_generation_42"}
    output = %{active | name: "sales"}

    asset = %Asset{
      ref: {Example.Sales, :asset},
      module: Example.Sales,
      name: :asset,
      type: :sql,
      relation: output
    }

    pin = %TargetGenerationPin{
      asset_ref: {Example.Store, :asset},
      relation: pinned,
      target_id: "store",
      target_generation_id: "generation42",
      descriptor_hash: String.duplicate("a", 64)
    }

    work = %RunnerWork{upstream_generation_pins: [pin]}
    {asset, relations} = GenerationWork.apply_overrides(asset, %{Example.Store => active}, work)

    relationship =
      Relationship.new!(
        name: :store,
        target: {Example.Store, :asset},
        on: [store_id: :id],
        cardinality: :many_to_one,
        on_violation: :fail
      )

    [spec] = Relationship.check_specs(relationship)

    template =
      Template.compile!(spec.sql, file: "relationship.sql", line: 1, resolve_asset_refs: false)

    template = %{
      template
      | nodes:
          Enum.map(template.nodes, fn
            %Template.AssetRef{} = ref -> %{ref | resolution: :resolved, relation: active}
            node -> node
          end)
    }

    check =
      Check.new!(
        Map.merge(spec, %{
          template: template,
          uses_query?: true,
          uses_target?: false,
          origin: :contract,
          file: "relationship.sql",
          line: 1
        })
      )

    definition = %Definition{
      module: Example.Sales,
      asset: Map.merge(Map.from_struct(asset), %{file: "relationship.sql", window_spec: nil}),
      sql: spec.sql,
      template: template,
      materialization: :table,
      raw_asset: %{manifest_relation_by_module: relations, deferred_resolution: :manifest_only}
    }

    assert {:ok, rendered} =
             Renderer.render_check(definition, check, runtime_relations: %{query: "candidate"})

    assert rendered.sql =~ "mart.store_generation_42"
    refute rendered.sql =~ "store_active"
    assert length(rendered.resolved_asset_refs) == 2

    missing = %{
      definition
      | raw_asset: %{manifest_relation_by_module: %{}, deferred_resolution: :manifest_only}
    }

    assert {:error, %{type: :unresolved_asset_ref}} =
             Renderer.render_check(missing, check, runtime_relations: %{query: "candidate"})
  end
end
