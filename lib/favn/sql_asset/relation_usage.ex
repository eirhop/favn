defmodule Favn.SQLAsset.RelationUsage do
  @moduledoc false

  alias Favn.Asset.RelationInput
  alias Favn.Namespace
  alias Favn.RelationRef
  alias Favn.SQL.Template

  @spec collect(module(), Template.t()) :: [RelationInput.t()]
  def collect(module, %Template{} = template) when is_atom(module) do
    defaults = Namespace.resolve(module)
    connection = Map.get(defaults, :connection)
    catalog = Map.get(defaults, :catalog)
    schema = Map.get(defaults, :schema)

    plain_relation_inputs =
      template
      |> Template.relation_refs()
      |> Enum.map(fn relation_ref ->
        to_plain_relation_input(relation_ref, connection, catalog, schema)
      end)

    direct_asset_inputs =
      template
      |> Template.asset_refs()
      |> Enum.map(&to_direct_asset_input/1)

    plain_relation_inputs ++ direct_asset_inputs
  end

  defp to_plain_relation_input(
         %Template.Relation{raw: raw, segments: segments, span: span},
         connection,
         catalog,
         schema
       ) do
    relation_ref =
      case segments do
        [name] ->
          RelationRef.new!(%{
            connection: connection,
            catalog: catalog,
            schema: schema,
            name: name
          })

        [schema_name, name] ->
          RelationRef.new!(%{
            connection: connection,
            catalog: catalog,
            schema: schema_name,
            name: name
          })

        [catalog_name, schema_name, name] ->
          RelationRef.new!(%{
            connection: connection,
            catalog: catalog_name,
            schema: schema_name,
            name: name
          })
      end

    %RelationInput{
      kind: :plain_relation,
      raw: raw,
      relation_ref: relation_ref,
      span: span
    }
  end

  defp to_direct_asset_input(%Template.AssetRef{} = asset_ref) do
    %RelationInput{
      kind: :direct_asset_ref,
      raw: inspect(asset_ref.module),
      relation_ref: asset_ref.produced_relation,
      asset_ref: asset_ref.asset_ref,
      resolution: asset_ref.resolution,
      span: asset_ref.span
    }
  end
end
