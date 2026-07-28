defmodule FavnRunner.Inspection do
  @moduledoc false

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest.Asset
  alias Favn.RelationRef
  alias Favn.SQL.Client
  alias Favn.SQL.Error
  alias Favn.SQL.Result

  @include_items [:relation, :columns, :row_count, :sample, :table_metadata]
  @max_batch_size 500
  @runner_registry FavnRunner.ConnectionRegistry

  @spec inspect_relation(RelationInspectionRequest.t(), Favn.Manifest.Version.t()) ::
          {:ok, RelationInspectionResult.t()} | {:error, term()}
  def inspect_relation(%RelationInspectionRequest{} = request, version) do
    with {:ok, prepared} <- prepare(request, version),
         {:ok, session} <-
           Client.connect(
             prepared.relation_ref.connection,
             connect_opts([prepared.relation_ref])
           ) do
      try do
        {:ok, inspect_prepared(prepared, session)}
      after
        Client.disconnect(session)
      end
    end
  end

  @spec inspect_relations([
          {:ok, RelationInspectionRequest.t(), Favn.Manifest.Version.t()} | {:error, term()}
        ]) ::
          {:ok, [{:ok, RelationInspectionResult.t()} | {:error, term()}]}
          | {:error, :invalid_relation_inspection_batch}
  def inspect_relations([]), do: {:ok, []}

  def inspect_relations(items) when is_list(items) and length(items) <= @max_batch_size do
    prepared =
      items
      |> Enum.with_index()
      |> Enum.map(fn
        {{:ok, %RelationInspectionRequest{} = request, version}, index} ->
          case prepare(request, version) do
            {:ok, value} -> {:ok, Map.put(value, :index, index)}
            {:error, reason} -> {:error, index, reason}
          end

        {{:error, reason}, index} ->
          {:error, index, reason}

        {_invalid, index} ->
          {:error, index, :invalid_relation_inspection_request}
      end)

    results =
      Enum.reduce(prepared, %{}, fn
        {:error, index, reason}, results -> Map.put(results, index, {:error, reason})
        {:ok, _prepared}, results -> results
      end)

    results =
      prepared
      |> Enum.flat_map(fn
        {:ok, value} -> [value]
        {:error, _index, _reason} -> []
      end)
      |> Enum.group_by(&{&1.relation_ref.connection, &1.relation_ref.catalog})
      |> Enum.reduce(results, fn {_session_key, group}, results ->
        inspect_group(group, results)
      end)

    {:ok, Enum.map(0..(length(items) - 1), &Map.fetch!(results, &1))}
  end

  def inspect_relations(_items), do: {:error, :invalid_relation_inspection_batch}

  defp prepare(request, version) do
    include = normalize_include(request.include)

    with {:ok, sample_limit} <- normalize_sample_limit(request.sample_limit, include),
         {:ok, asset, relation_ref} <- resolve_relation(request, version) do
      {:ok,
       %{
         asset: asset,
         relation_ref: relation_ref,
         include: include,
         sample_limit: sample_limit,
         required_runner_release_id: version.required_runner_release_id
       }}
    end
  end

  defp inspect_group(group, results) do
    relation_refs = Enum.map(group, & &1.relation_ref)
    connection = hd(relation_refs).connection

    case Client.connect(connection, connect_opts(relation_refs)) do
      {:ok, session} ->
        try do
          Enum.reduce(group, results, fn prepared, results ->
            Map.put(results, prepared.index, {:ok, inspect_prepared(prepared, session)})
          end)
        after
          Client.disconnect(session)
        end

      {:error, reason} ->
        Enum.reduce(group, results, fn prepared, results ->
          Map.put(results, prepared.index, {:error, reason})
        end)
    end
  end

  defp inspect_prepared(prepared, session) do
    inspect_with_session(
      prepared.asset,
      prepared.relation_ref,
      session,
      prepared.include,
      prepared.sample_limit,
      prepared.required_runner_release_id
    )
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

  defp connect_opts(relations) when is_list(relations) do
    catalogs =
      relations
      |> Enum.map(& &1.catalog)
      |> Enum.filter(&(is_binary(&1) and &1 != ""))
      |> Enum.uniq()
      |> Enum.sort()

    [registry_name: @runner_registry, required_catalogs: catalogs]
  end

  defp inspect_with_session(
         asset,
         relation_ref,
         session,
         include,
         sample_limit,
         required_runner_release_id
       ) do
    %RelationInspectionResult{
      asset_ref: inspection_asset_ref(asset),
      required_runner_release_id: required_runner_release_id,
      relation_ref: relation_ref,
      adapter: session.adapter,
      inspected_at: DateTime.utc_now()
    }
    |> maybe_relation(session, relation_ref, include)
    |> maybe_columns(session, relation_ref, include)
    |> maybe_row_count(session, relation_ref, include)
    |> maybe_sample(session, relation_ref, include, sample_limit)
    |> maybe_table_metadata(session, relation_ref, include)
  end

  @spec normalize_include(term()) :: [atom()]
  defp normalize_include(include) when is_list(include) do
    include
    |> Enum.filter(&(&1 in @include_items))
    |> Enum.uniq()
  end

  defp normalize_include(_include), do: []

  defp normalize_sample_limit(limit, include) do
    if :sample in include do
      case limit do
        int when is_integer(int) and int >= 0 -> {:ok, min(int, 20)}
        _invalid -> {:error, :invalid_sample_limit}
      end
    else
      {:ok, 20}
    end
  end

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
          | sample: %{limit: limit, columns: sample.columns, rows: sample.rows}
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
    warning = %{code: code, message: warning_message(reason)}
    %{result | warnings: result.warnings ++ [warning]}
  end

  defp warning_message(%Error{message: message}) when is_binary(message), do: message
  defp warning_message(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp warning_message(_reason), do: "inspection operation failed"

  defp inspection_asset_ref(%Asset{} = asset), do: asset.ref
  defp inspection_asset_ref(_asset), do: nil
end
