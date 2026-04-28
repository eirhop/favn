defmodule FavnRunner.Inspection do
  @moduledoc false

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest.Asset
  alias Favn.RelationRef
  alias Favn.SQL.Client
  alias Favn.SQL.Result

  @include_items [:relation, :columns, :row_count, :sample, :table_metadata]
  @runner_registry FavnRunner.ConnectionRegistry

  @spec inspect_relation(RelationInspectionRequest.t(), Favn.Manifest.Version.t()) ::
          {:ok, RelationInspectionResult.t()} | {:error, term()}
  def inspect_relation(%RelationInspectionRequest{} = request, version) do
    with {:ok, asset, relation_ref} <- resolve_relation(request, version),
         {:ok, session} <-
           Client.connect(relation_ref.connection, registry_name: @runner_registry) do
      try do
        {:ok, inspect_with_session(request, asset, relation_ref, session)}
      after
        Client.disconnect(session)
      end
    end
  end

  defp resolve_relation(%RelationInspectionRequest{asset_ref: asset_ref}, version)
       when is_tuple(asset_ref) do
    case Enum.find(List.wrap(version.manifest.assets), &match?(%Asset{ref: ^asset_ref}, &1)) do
      %Asset{relation: nil} -> {:error, :asset_relation_not_found}
      %Asset{} = asset -> normalize_relation(asset, asset.relation)
      nil -> {:error, :asset_not_found}
    end
  end

  defp resolve_relation(%RelationInspectionRequest{relation: %RelationRef{} = relation}, _version) do
    {:ok, nil, relation}
  end

  defp resolve_relation(_request, _version), do: {:error, :invalid_inspection_target}

  defp normalize_relation(asset, relation) do
    relation_ref = RelationRef.new!(relation)

    case relation_ref.connection do
      connection when is_atom(connection) and not is_nil(connection) ->
        {:ok, asset, relation_ref}

      _missing ->
        {:error, :relation_connection_missing}
    end
  rescue
    ArgumentError -> {:error, :invalid_relation}
  end

  defp inspect_with_session(request, asset, relation_ref, session) do
    include = normalize_include(request.include)

    %RelationInspectionResult{
      asset_ref: inspection_asset_ref(asset),
      relation_ref: relation_ref,
      adapter: session.adapter,
      inspected_at: DateTime.utc_now()
    }
    |> maybe_relation(session, relation_ref, include)
    |> maybe_columns(session, relation_ref, include)
    |> maybe_row_count(session, relation_ref, include)
    |> maybe_sample(session, relation_ref, include, request.sample_limit)
    |> maybe_table_metadata(session, relation_ref, include)
  end

  @spec normalize_include(term()) :: [atom()]
  defp normalize_include(include) when is_list(include) do
    include
    |> Enum.filter(&(&1 in @include_items))
    |> Enum.uniq()
  end

  defp normalize_include(_include), do: []

  defp maybe_relation(result, session, relation_ref, include) do
    if :relation in include,
      do: fetch_relation(result, session, relation_ref),
      else: result
  end

  defp fetch_relation(result, session, relation_ref) do
    case Client.relation(session, relation_ref) do
      {:ok, relation} -> %{result | relation: relation}
      {:error, reason} -> add_warning(result, :relation_failed, reason)
    end
  end

  defp maybe_columns(result, session, relation_ref, include) do
    if :columns in include,
      do: fetch_columns(result, session, relation_ref),
      else: result
  end

  defp fetch_columns(result, session, relation_ref) do
    case Client.columns(session, relation_ref) do
      {:ok, columns} -> %{result | columns: columns}
      {:error, reason} -> add_warning(result, :columns_failed, reason)
    end
  end

  defp maybe_row_count(result, session, relation_ref, include) do
    if :row_count in include,
      do: fetch_row_count(result, session, relation_ref),
      else: result
  end

  defp fetch_row_count(result, session, relation_ref) do
    case Client.row_count(session, relation_ref) do
      {:ok, row_count} -> %{result | row_count: row_count}
      {:error, reason} -> add_warning(result, :row_count_failed, reason)
    end
  end

  defp maybe_sample(result, session, relation_ref, include, limit) do
    if :sample in include,
      do: fetch_sample(result, session, relation_ref, limit),
      else: result
  end

  defp fetch_sample(result, session, relation_ref, limit) do
    case Client.sample(session, relation_ref, limit: limit) do
      {:ok, %Result{} = sample} ->
        %{
          result
          | sample: %{limit: min(max(limit, 0), 20), columns: sample.columns, rows: sample.rows}
        }

      {:error, reason} ->
        add_warning(result, :sample_failed, reason)
    end
  end

  defp maybe_table_metadata(result, session, relation_ref, include) do
    if :table_metadata in include,
      do: fetch_table_metadata(result, session, relation_ref),
      else: result
  end

  defp fetch_table_metadata(result, session, relation_ref) do
    case Client.table_metadata(session, relation_ref) do
      {:ok, metadata} -> %{result | table_metadata: metadata}
      {:error, reason} -> add_warning(result, :table_metadata_failed, reason)
    end
  end

  defp add_warning(%RelationInspectionResult{} = result, code, reason) do
    warning = %{code: code, message: inspect(reason)}
    %{result | warnings: result.warnings ++ [warning]}
  end

  defp inspection_asset_ref(%Asset{} = asset), do: asset.ref
  defp inspection_asset_ref(_asset), do: nil
end
