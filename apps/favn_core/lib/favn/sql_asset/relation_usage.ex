defmodule Favn.SQLAsset.RelationUsage do
  @moduledoc false

  alias Favn.Asset.RelationInput
  alias Favn.RelationRef
  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.Template
  alias Favn.SQL.Template.Call

  @spec collect(module(), Template.t(), [SQLDefinition.t()]) :: [RelationInput.t()]
  def collect(module, %Template{} = template, sql_definitions \\ []) when is_atom(module) do
    definition_catalog =
      sql_definitions
      |> Enum.map(fn %SQLDefinition{} = definition ->
        {SQLDefinition.key(definition), definition}
      end)
      |> Map.new()

    root_defaults = resolve_namespace_defaults(module)

    {inputs, _visited} =
      collect_template(module, template, definition_catalog, %{}, root_defaults)

    inputs
  end

  defp collect_template(
         module,
         %Template{} = template,
         definition_catalog,
         visited,
         root_defaults
       ) do
    defaults = resolve_namespace_defaults(module)
    connection = Map.get(defaults, :connection) || Map.get(root_defaults, :connection)
    catalog = Map.get(defaults, :catalog) || Map.get(root_defaults, :catalog)
    schema = Map.get(defaults, :schema) || Map.get(root_defaults, :schema)

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

    {nested_inputs, visited_after_calls} =
      template
      |> Template.calls()
      |> Enum.reduce({[], visited}, fn %Call{definition: definition_ref}, {acc, visited_acc} ->
        key = {definition_ref.name, definition_ref.arity}

        case Map.fetch(definition_catalog, key) do
          {:ok, %SQLDefinition{} = definition} ->
            id = {definition.module, definition.name, definition.arity}

            if Map.has_key?(visited_acc, id) do
              {acc, visited_acc}
            else
              {definition_inputs, visited_after_definition} =
                collect_template(
                  definition.module,
                  definition.template,
                  definition_catalog,
                  Map.put(visited_acc, id, true),
                  root_defaults
                )

              {acc ++ definition_inputs, visited_after_definition}
            end

          :error ->
            {acc, visited_acc}
        end
      end)

    {plain_relation_inputs ++ direct_asset_inputs ++ nested_inputs, visited_after_calls}
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
      relation_ref: asset_ref.relation,
      asset_ref: asset_ref.asset_ref,
      resolution: asset_ref.resolution,
      span: asset_ref.span
    }
  end

  defp resolve_namespace_defaults(module) when is_atom(module) do
    namespace_module = Module.concat([Favn, Namespace])

    with {:module, ^namespace_module} <- Code.ensure_loaded(namespace_module),
         true <- function_exported?(namespace_module, :resolve_relation, 1) do
      namespace_module.resolve_relation(module)
    else
      _ -> %{}
    end
  end
end
