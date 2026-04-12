defmodule Favn.Assets.DependencyInference do
  @moduledoc false

  alias Favn.Asset
  alias Favn.Asset.Dependency
  alias Favn.Asset.RelationInput
  alias Favn.Assets.Compiler
  alias Favn.Diagnostic
  alias Favn.RelationRef

  @type error :: {:dependency_inference_error, Favn.Ref.t(), Diagnostic.t()}

  @spec infer(%{
          assets: [Asset.t()],
          assets_by_ref: %{Favn.Ref.t() => Asset.t()},
          relation_owners: %{RelationRef.t() => Favn.Ref.t()}
        }) ::
          {:ok,
           %{
             assets: [Asset.t()],
             assets_by_ref: %{Favn.Ref.t() => Asset.t()},
             relation_owners: %{RelationRef.t() => Favn.Ref.t()},
             diagnostics: [Diagnostic.t()]
           }}
          | {:error, error()}
  def infer(catalog) do
    relation_owner_entries = Map.to_list(catalog.relation_owners)

    catalog.assets
    |> Enum.reduce_while({:ok, [], %{}, []}, fn %Asset{} = asset,
                                                {:ok, assets_acc, by_ref_acc, diagnostics_acc} ->
      case infer_asset(asset, catalog, relation_owner_entries) do
        {:ok, inferred_asset} ->
          merged_diagnostics = diagnostics_acc ++ inferred_asset.diagnostics

          {:cont,
           {:ok, [inferred_asset | assets_acc],
            Map.put(by_ref_acc, inferred_asset.ref, inferred_asset), merged_diagnostics}}

        {:error, %Diagnostic{} = diagnostic} ->
          {:halt, {:error, {:dependency_inference_error, asset.ref, diagnostic}}}
      end
    end)
    |> case do
      {:ok, assets, assets_by_ref, diagnostics} ->
        {:ok,
         catalog
         |> Map.put(:assets, Enum.reverse(assets))
         |> Map.put(:assets_by_ref, assets_by_ref)
         |> Map.put(:diagnostics, diagnostics)}

      {:error, _reason} = error ->
        error
    end
  end

  defp infer_asset(%Asset{} = asset, catalog, relation_owner_entries) do
    initial_dependencies = explicit_dependency_map(asset)

    asset.relation_inputs
    |> Enum.reduce_while({:ok, initial_dependencies, []}, fn input,
                                                             {:ok, dependency_map, diagnostics} ->
      case infer_input_dependency(asset, input, catalog, relation_owner_entries) do
        {:ok, :no_dependency, new_diagnostics} ->
          {:cont, {:ok, dependency_map, diagnostics ++ new_diagnostics}}

        {:ok, dependency_ref, dependency_provenance, new_diagnostics} ->
          updated_map =
            add_dependency(dependency_map, dependency_ref, dependency_provenance, input)

          {:cont, {:ok, updated_map, diagnostics ++ new_diagnostics}}

        {:error, %Diagnostic{} = diagnostic} ->
          {:halt, {:error, diagnostic}}
      end
    end)
    |> case do
      {:ok, dependency_map, diagnostics} ->
        dependencies =
          dependency_map
          |> Map.values()
          |> Enum.sort_by(& &1.asset_ref)

        depends_on = Enum.map(dependencies, & &1.asset_ref)

        {:ok,
         %{asset | dependencies: dependencies, depends_on: depends_on, diagnostics: diagnostics}}

      {:error, %Diagnostic{} = diagnostic} ->
        {:error, diagnostic}
    end
  end

  defp explicit_dependency_map(%Asset{} = asset) do
    Enum.reduce(asset.depends_on, %{}, fn ref, acc ->
      Map.put(acc, ref, %Dependency{asset_ref: ref, provenance: [:explicit], relation_inputs: []})
    end)
  end

  defp infer_input_dependency(
         asset,
         %RelationInput{kind: :plain_relation, relation_ref: relation_ref} = input,
         _catalog,
         relation_owner_entries
       ) do
    case matching_owners(relation_ref, relation_owner_entries) do
      [] ->
        {:ok, :no_dependency, [unmanaged_relation_warning(asset, input)]}

      [{_owner_relation, dependency_ref}] ->
        if dependency_ref == asset.ref do
          {:ok, :no_dependency, []}
        else
          {:ok, dependency_ref, :inferred_sql_relation, []}
        end

      owners ->
        {:error, ambiguous_relation_error(asset, input, owners)}
    end
  end

  defp infer_input_dependency(
         asset,
         %RelationInput{kind: :direct_asset_ref} = input,
         catalog,
         _relation_owner_entries
       ) do
    with {:ok, dependency_ref, relation_ref} <- resolve_direct_asset_ref(asset, input),
         :ok <- ensure_same_connection(asset, input, relation_ref) do
      if dependency_ref == asset.ref do
        {:ok, :no_dependency, []}
      else
        if Map.has_key?(catalog.assets_by_ref, dependency_ref) do
          {:ok, dependency_ref, :inferred_sql_asset_ref, []}
        else
          {:ok, :no_dependency, []}
        end
      end
    else
      {:error, %Diagnostic{} = diagnostic} ->
        {:error, diagnostic}
    end
  end

  defp add_dependency(map, dependency_ref, provenance, %RelationInput{} = relation_input) do
    Map.update(
      map,
      dependency_ref,
      %Dependency{
        asset_ref: dependency_ref,
        provenance: [provenance],
        relation_inputs: [relation_input]
      },
      fn existing ->
        %Dependency{} = existing

        %{
          existing
          | provenance: (existing.provenance ++ [provenance]) |> Enum.uniq() |> Enum.sort(),
            relation_inputs: existing.relation_inputs ++ [relation_input]
        }
      end
    )
  end

  defp matching_owners(%RelationRef{} = relation_ref, relation_owner_entries) do
    Enum.filter(relation_owner_entries, fn {owner_relation, _owner_ref} ->
      relation_matches?(relation_ref, owner_relation)
    end)
  end

  defp relation_matches?(%RelationRef{} = input, %RelationRef{} = owner) do
    owner.connection == input.connection and
      owner.name == input.name and
      field_matches?(input.catalog, owner.catalog) and
      field_matches?(input.schema, owner.schema)
  end

  defp field_matches?(nil, _value), do: true
  defp field_matches?(left, right), do: left == right

  defp unmanaged_relation_warning(asset, %RelationInput{} = input) do
    %Diagnostic{
      severity: :warning,
      stage: :registry,
      code: :unmanaged_relation_reference,
      message:
        "SQL relation #{inspect(input.raw || input.relation_ref)} used by #{inspect(asset.ref)} is not owned by a registered asset",
      asset_ref: asset.ref,
      span: input.span,
      details: %{relation_ref: input.relation_ref}
    }
  end

  defp ambiguous_relation_error(asset, %RelationInput{} = input, owners) do
    %Diagnostic{
      severity: :error,
      stage: :registry,
      code: :ambiguous_relation_owner,
      message:
        "SQL relation #{inspect(input.raw || input.relation_ref)} used by #{inspect(asset.ref)} resolves to multiple owned relations",
      asset_ref: asset.ref,
      span: input.span,
      details: %{relation_ref: input.relation_ref, owners: owners}
    }
  end

  defp resolve_direct_asset_ref(_asset, %RelationInput{
         asset_ref: {module, :asset} = asset_ref,
         relation_ref: relation_ref,
         resolution: :resolved
       })
       when is_atom(module) do
    {:ok, asset_ref, relation_ref}
  end

  defp resolve_direct_asset_ref(asset, %RelationInput{
         asset_ref: {module, :asset},
         resolution: :deferred,
         span: span
       })
       when is_atom(module) do
    case Compiler.compile_module_assets(module) do
      {:ok, [%Asset{relation: %RelationRef{} = relation_ref}]} ->
        {:ok, {module, :asset}, relation_ref}

      _other ->
        {:error,
         %Diagnostic{
           severity: :error,
           stage: :registry,
           code: :unresolved_direct_asset_ref,
           message:
             "direct SQL asset reference #{inspect(module)} used by #{inspect(asset.ref)} could not be resolved",
           asset_ref: asset.ref,
           span: span,
           details: %{module: module}
         }}
    end
  end

  defp resolve_direct_asset_ref(asset, %RelationInput{} = input) do
    {:error,
     %Diagnostic{
       severity: :error,
       stage: :registry,
       code: :unresolved_direct_asset_ref,
       message:
         "direct SQL asset reference #{inspect(input.raw)} used by #{inspect(asset.ref)} could not be resolved",
       asset_ref: asset.ref,
       span: input.span,
       details: %{input: input}
     }}
  end

  defp ensure_same_connection(asset, input, %RelationRef{connection: connection}) do
    if connection == asset.relation.connection do
      :ok
    else
      {:error,
       %Diagnostic{
         severity: :error,
         stage: :registry,
         code: :cross_connection_direct_asset_ref,
         message:
           "direct SQL asset reference #{inspect(input.raw)} for #{inspect(asset.ref)} resolves to connection #{inspect(connection)}, expected #{inspect(asset.relation.connection)}",
         asset_ref: asset.ref,
         span: input.span,
         details: %{expected_connection: asset.relation.connection, actual_connection: connection}
       }}
    end
  end

  defp ensure_same_connection(asset, input, nil) do
    {:error,
     %Diagnostic{
       severity: :error,
       stage: :registry,
       code: :unresolved_direct_asset_ref,
       message:
         "direct SQL asset reference #{inspect(input.raw)} for #{inspect(asset.ref)} did not resolve a produced relation",
       asset_ref: asset.ref,
       span: input.span,
       details: %{}
     }}
  end
end
