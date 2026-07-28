defmodule Favn.Assets.DependencyInference do
  @moduledoc false

  alias Favn.Asset.Dependency
  alias Favn.Asset.RelationInput
  alias Favn.Assets.Compiler
  alias Favn.Diagnostic
  alias Favn.RelationRef

  @type catalog :: %{
          required(:assets) => [map()],
          required(:assets_by_ref) => %{Favn.Ref.t() => map()},
          optional(:diagnostics) => [Diagnostic.t()]
        }

  @spec infer_assets([map()]) :: {:ok, catalog()} | {:error, error()}
  def infer_assets(assets) when is_list(assets) do
    infer(%{
      assets: assets,
      assets_by_ref: Map.new(assets, &{Map.get(&1, :ref), &1}),
      diagnostics: []
    })
  end

  @type error :: {:dependency_inference_error, Favn.Ref.t(), Diagnostic.t()}

  @spec infer(catalog()) :: {:ok, catalog()} | {:error, error()}
  def infer(catalog) do
    catalog.assets
    |> Enum.reduce_while({:ok, [], %{}, []}, fn asset,
                                                {:ok, assets_acc, by_ref_acc, diagnostics_acc} ->
      case infer_asset(asset, catalog) do
        {:ok, inferred_asset} ->
          merged_diagnostics = diagnostics_acc ++ Map.get(inferred_asset, :diagnostics, [])
          inferred_ref = Map.get(inferred_asset, :ref)

          {:cont,
           {:ok, [inferred_asset | assets_acc], Map.put(by_ref_acc, inferred_ref, inferred_asset),
            merged_diagnostics}}

        {:error, %Diagnostic{} = diagnostic} ->
          {:halt, {:error, {:dependency_inference_error, Map.get(asset, :ref), diagnostic}}}
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

  defp infer_asset(asset, catalog) when is_map(asset) do
    initial_dependencies = explicit_dependency_map(asset)

    asset
    |> Map.get(:relation_inputs, [])
    |> Enum.reduce_while({:ok, initial_dependencies, [], []}, fn input,
                                                                 {:ok, dependency_map,
                                                                  relation_inputs, diagnostics} ->
      case infer_input_dependency(asset, input, catalog) do
        {:ok, :no_dependency, new_diagnostics} ->
          {:cont, {:ok, dependency_map, relation_inputs, diagnostics ++ new_diagnostics}}

        {:ok, dependency_ref, dependency_provenance, bound_input, new_diagnostics} ->
          updated_map =
            add_dependency(dependency_map, dependency_ref, dependency_provenance, bound_input)

          {:cont,
           {:ok, updated_map, [bound_input | relation_inputs], diagnostics ++ new_diagnostics}}

        {:error, %Diagnostic{} = diagnostic} ->
          {:halt, {:error, diagnostic}}
      end
    end)
    |> case do
      {:ok, dependency_map, relation_inputs, diagnostics} ->
        dependencies =
          dependency_map
          |> Map.values()
          |> Enum.sort_by(& &1.asset_ref)

        depends_on = Enum.map(dependencies, & &1.asset_ref)

        {:ok,
         asset
         |> Map.put(:dependencies, dependencies)
         |> Map.put(:depends_on, depends_on)
         |> Map.put(:relation_inputs, Enum.reverse(relation_inputs))
         |> Map.put(:diagnostics, diagnostics)}

      {:error, %Diagnostic{} = diagnostic} ->
        {:error, diagnostic}
    end
  end

  defp explicit_dependency_map(asset) when is_map(asset) do
    Enum.reduce(Map.get(asset, :depends_on, []), %{}, fn ref, acc ->
      Map.put(acc, ref, %Dependency{asset_ref: ref, provenance: [:explicit], relation_inputs: []})
    end)
  end

  defp infer_input_dependency(
         asset,
         %RelationInput{kind: :plain_relation, relation_ref: relation_ref} = input,
         catalog
       ) do
    case matching_explicit_dependencies(asset, relation_ref, catalog) do
      [] ->
        {:ok, :no_dependency, []}

      [{dependency_ref, %RelationRef{} = dependency_relation}] ->
        bound_input = %{
          input
          | asset_ref: dependency_ref,
            relation_ref: dependency_relation,
            resolution: :resolved
        }

        {:ok, dependency_ref, :explicit, bound_input, []}

      dependencies ->
        {:error, ambiguous_explicit_relation_error(asset, input, dependencies)}
    end
  end

  defp infer_input_dependency(
         asset,
         %RelationInput{kind: :direct_asset_ref} = input,
         catalog
       ) do
    with {:ok, dependency_ref, relation_ref} <- resolve_direct_asset_ref(asset, input),
         :ok <- ensure_same_connection(asset, input, relation_ref) do
      if dependency_ref == Map.get(asset, :ref) do
        {:ok, :no_dependency, []}
      else
        if Map.has_key?(catalog.assets_by_ref, dependency_ref) do
          bound_input = %{input | relation_ref: relation_ref, resolution: :resolved}
          {:ok, dependency_ref, :inferred_sql_asset_ref, bound_input, []}
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

  defp matching_explicit_dependencies(asset, %RelationRef{} = relation_ref, catalog) do
    asset
    |> Map.get(:depends_on, [])
    |> Enum.flat_map(fn dependency_ref ->
      case Map.get(catalog.assets_by_ref, dependency_ref) do
        %{relation: %RelationRef{} = dependency_relation} ->
          [{dependency_ref, dependency_relation}]

        _other ->
          []
      end
    end)
    |> Enum.filter(fn {_dependency_ref, dependency_relation} ->
      relation_ref == dependency_relation
    end)
  end

  defp ambiguous_explicit_relation_error(asset, %RelationInput{} = input, dependencies) do
    %Diagnostic{
      severity: :error,
      stage: :registry,
      code: :ambiguous_explicit_relation,
      message:
        "SQL relation #{inspect(input.raw || input.relation_ref)} used by #{inspect(Map.get(asset, :ref))} matches multiple declared dependencies",
      asset_ref: Map.get(asset, :ref),
      span: input.span,
      details: %{relation_ref: input.relation_ref, dependencies: dependencies}
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
      {:ok, [%{relation: %RelationRef{} = relation_ref}]} ->
        {:ok, {module, :asset}, relation_ref}

      _other ->
        {:error,
         %Diagnostic{
           severity: :error,
           stage: :registry,
           code: :unresolved_direct_asset_ref,
           message:
             "direct SQL asset reference #{inspect(module)} used by #{inspect(Map.get(asset, :ref))} could not be resolved",
           asset_ref: Map.get(asset, :ref),
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
         "direct SQL asset reference #{inspect(input.raw)} used by #{inspect(Map.get(asset, :ref))} could not be resolved",
       asset_ref: Map.get(asset, :ref),
       span: input.span,
       details: %{input: input}
     }}
  end

  defp ensure_same_connection(asset, input, %RelationRef{connection: connection}) do
    expected_connection = asset |> Map.get(:relation, %{}) |> Map.get(:connection)

    if connection == expected_connection do
      :ok
    else
      {:error,
       %Diagnostic{
         severity: :error,
         stage: :registry,
         code: :cross_connection_direct_asset_ref,
         message:
           "direct SQL asset reference #{inspect(input.raw)} for #{inspect(Map.get(asset, :ref))} resolves to connection #{inspect(connection)}, expected #{inspect(expected_connection)}",
         asset_ref: Map.get(asset, :ref),
         span: input.span,
         details: %{expected_connection: expected_connection, actual_connection: connection}
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
         "direct SQL asset reference #{inspect(input.raw)} for #{inspect(Map.get(asset, :ref))} did not resolve a relation",
       asset_ref: Map.get(asset, :ref),
       span: input.span,
       details: %{}
     }}
  end
end
