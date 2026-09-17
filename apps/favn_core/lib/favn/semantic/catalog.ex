defmodule Favn.Semantic.Catalog do
  @moduledoc """
  Pure local inspection, semantic comparison and served-contract compatibility.

  Consumers bind the ordered input records to their own source alias. Generated
  scalar macros contain aggregate expressions; they do not apply time selection,
  enforce join cardinality or validate a caller's grouping. Inspection includes
  those obligations alongside each copyable invocation.
  """

  alias Favn.Semantic.Artifact

  @doc "Inspects a complete catalog or one exact `model.metric` reference."
  @spec inspect(Artifact.t(), keyword()) :: map()
  def inspect(%Artifact{} = artifact, opts \\ []) do
    selected = Keyword.get(opts, :metric)
    alias_name = Keyword.get(opts, :source_alias, "source")

    models =
      Enum.map(artifact.models, fn model ->
        source = Enum.find(artifact.snapshot, &(&1["ref"] == model["source_asset"]))

        metrics =
          model["metrics"]
          |> Enum.filter(&(selected == nil or &1["ref"] == selected))
          |> Enum.map(fn metric ->
            Map.merge(metric, %{
              "macro" => %{
                "schema" => namespace(artifact),
                "name" => metric["macro_name"],
                "kind" => "scalar",
                "evaluation" => "aggregate_expression"
              },
              "invocation" => invocation(artifact, metric, alias_name),
              "macro_sql" => macro_sql(artifact, metric),
              "source_relation" => source["relation"],
              "source_grain" => source["contract"]["grain"],
              "time" => model["time"],
              "compatibility_requirements" => requirements(model, source)
            })
          end)

        model
        |> Map.put("metrics", metrics)
        |> Map.put("source_contract", source["contract"])
        |> Map.put("source_relation", source["relation"])
      end)
      |> Enum.filter(&(selected == nil or &1["metrics"] != []))

    %{
      "semantic_version" => artifact.semantic_version,
      "snapshot_version" => artifact.snapshot_version,
      "compiler" => artifact.compiler,
      "models" => models
    }
  end

  @doc "Returns immutable, fully qualified native macro definitions in catalog order."
  @spec macros(Artifact.t()) :: [String.t()]
  def macros(%Artifact{} = artifact),
    do: for(model <- artifact.models, metric <- model["metrics"], do: macro_sql(artifact, metric))

  @doc "Returns the immutable SQL namespace derived from the complete semantic digest."
  @spec namespace(Artifact.t()) :: String.t()
  def namespace(%Artifact{semantic_version: "sm_" <> digest}), do: "metrics_" <> digest

  @doc "Builds a quoted invocation using metadata input order and a consumer-selected alias."
  @spec invocation(Artifact.t(), map(), String.t()) :: String.t()
  def invocation(artifact, metric, source_alias) do
    args =
      Enum.map_join(
        metric["inputs"],
        ", ",
        &(quote_identifier(source_alias) <> "." <> quote_identifier(&1["column"]))
      )

    quote_identifier(namespace(artifact)) <>
      "." <> quote_identifier(metric["macro_name"]) <> "(" <> args <> ")"
  end

  @doc "Compares stable IDs and reports deterministic breaking, additive or informational changes."
  @spec diff(Artifact.t(), Artifact.t()) :: [map()]
  def diff(%Artifact{} = old, %Artifact{} = new) do
    old_entities = entities(old)
    new_entities = entities(new)

    (Map.keys(old_entities) ++ Map.keys(new_entities))
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.flat_map(fn id ->
      case {old_entities[id], new_entities[id]} do
        {nil, _} ->
          [%{"entity" => id, "classification" => "additive", "changes" => ["added"]}]

        {_, nil} ->
          [%{"entity" => id, "classification" => "breaking", "changes" => ["removed"]}]

        {same, same} ->
          []

        {before, after_value} ->
          changed = changed_fields(before, after_value, "")

          class =
            if semantic_fields(before) == semantic_fields(after_value),
              do: "informational",
              else: "breaking"

          [%{"entity" => id, "classification" => class, "changes" => changed}]
      end
    end)
  end

  @doc "Checks consumed logical contracts; missing served evidence is explicitly unknown."
  @spec compatible(Artifact.t(), [map()] | nil) :: %{
          status: :compatible | :incompatible | :unknown,
          reasons: [map()]
        }
  def compatible(%Artifact{}, nil),
    do: %{status: :unknown, reasons: [%{"reason" => "served_contract_unavailable"}]}

  def compatible(%Artifact{} = artifact, served_snapshot) when is_list(served_snapshot) do
    served = Map.new(served_snapshot, &{&1["ref"], &1})
    built = Map.new(artifact.snapshot, &{&1["ref"], &1})

    reasons =
      Enum.flat_map(artifact.models, fn model ->
        source = built[model["source_asset"]]
        expected = requirements(model, source)

        case served[model["source_asset"]] do
          nil -> [reason(model["source_asset"], "missing_source")]
          target -> compare_requirements(expected, target, served, built)
        end
      end)
      |> Enum.uniq()
      |> Enum.sort_by(&{&1["asset"], &1["reason"]})

    %{status: if(reasons == [], do: :compatible, else: :incompatible), reasons: reasons}
  end

  defp macro_sql(artifact, metric) do
    args = Enum.map_join(metric["inputs"], ", ", &quote_identifier(&1["parameter"]))

    "CREATE MACRO " <>
      quote_identifier(namespace(artifact)) <>
      "." <>
      quote_identifier(metric["macro_name"]) <>
      "(" <> args <> ") AS (" <> metric["canonical_sql"] <> ");"
  end

  defp requirements(model, source) do
    semantic_columns =
      Enum.flat_map(model["metrics"], fn metric -> Enum.map(metric["inputs"], & &1["column"]) end) ++
        if(model["time"], do: [model["time"]["column"]], else: []) ++
        if(model["dimension"],
          do: [model["dimension"]["label"] | model["dimension"]["key"]],
          else: []
        ) ++
        Enum.flat_map(model["hierarchies"], & &1["columns"]) ++
        source["contract"]["grain"] ++
        Enum.flat_map(source["contract"]["relationships"], fn role ->
          Enum.map(role["on"], & &1["source"])
        end)

    columns =
      source["contract"]["columns"]
      |> Enum.filter(&(&1["name"] in semantic_columns))
      |> Enum.map(&Map.take(&1, ["name", "type", "nullable"]))

    %{
      "source_asset" => source["ref"],
      "relation" => source["relation"],
      "columns" => columns,
      "grain" => source["contract"]["grain"],
      "relationships" => source["contract"]["relationships"]
    }
  end

  defp compare_requirements(expected, target, served, built) do
    ref = expected["source_asset"]
    contract = target["contract"]

    if contract == nil do
      [reason(ref, "missing_contract")]
    else
      columns = Map.new(contract["columns"], &{&1["name"], &1})

      column_errors =
        Enum.flat_map(expected["columns"], fn column ->
          if columns[column["name"]] != nil and
               Map.take(columns[column["name"]], ["name", "type", "nullable"]) == column,
             do: [],
             else: [reason(ref, "column_changed:" <> column["name"])]
        end)

      relation_errors =
        if target["relation"] == expected["relation"],
          do: [],
          else: [reason(ref, "relation_changed")]

      grain_errors =
        if contract["grain"] == expected["grain"], do: [], else: [reason(ref, "grain_changed")]

      roles = Map.new(contract["relationships"], &{&1["name"], &1})

      role_errors =
        Enum.flat_map(expected["relationships"], fn role ->
          if roles[role["name"]] == role,
            do: relationship_target(role, served, built),
            else: [reason(ref, "relationship_changed:" <> role["name"])]
        end)

      column_errors ++ relation_errors ++ grain_errors ++ role_errors
    end
  end

  defp relationship_target(role, served, built) do
    expected = built[role["target"]]
    actual = served[role["target"]]
    keys = Enum.map(role["on"], & &1["target"])

    if expected == nil or actual == nil or actual["contract"] == nil do
      [reason(role["target"], "missing_relationship_target")]
    else
      expected_columns = key_columns(expected["contract"], keys)
      actual_columns = key_columns(actual["contract"], keys)

      if expected["relation"] == actual["relation"] and expected_columns == actual_columns and
           keys == actual["contract"]["grain"],
         do: [],
         else: [reason(role["target"], "relationship_target_changed")]
    end
  end

  defp key_columns(contract, keys) do
    columns = Map.new(contract["columns"], &{&1["name"], &1})
    Enum.map(keys, &Map.take(Map.get(columns, &1, %{}), ["name", "type", "nullable"]))
  end

  defp semantic_fields(%{"contract" => contract} = entity) when is_map(contract) do
    contract =
      contract
      |> Map.delete("grain_description")
      |> Map.update!("columns", fn columns ->
        Enum.map(columns, &Map.delete(&1, "description"))
      end)

    Map.put(entity, "contract", contract)
  end

  defp semantic_fields(entity), do: Map.drop(entity, ["description", "format"])

  defp changed_fields(same, same, _path), do: []

  defp changed_fields(before, after_value, path) when is_map(before) and is_map(after_value) do
    (Map.keys(before) ++ Map.keys(after_value))
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.flat_map(fn key ->
      changed_fields(
        before[key],
        after_value[key],
        if(path == "", do: key, else: path <> "." <> key)
      )
    end)
  end

  defp changed_fields(before, after_value, path) when is_list(before) and is_list(after_value) do
    if Enum.all?(before ++ after_value, &(is_map(&1) and is_binary(&1["name"]))) do
      changed_fields(
        Map.new(before, &{&1["name"], &1}),
        Map.new(after_value, &{&1["name"], &1}),
        path
      )
    else
      [path]
    end
  end

  defp changed_fields(_before, _after, path), do: [path]

  defp entities(artifact) do
    models = Map.new(artifact.models, &{"model:" <> &1["name"], Map.delete(&1, "metrics")})

    metrics =
      for model <- artifact.models,
          metric <- model["metrics"],
          into: %{},
          do: {"metric:" <> metric["ref"], metric}

    contracts =
      Map.new(
        artifact.snapshot,
        &{"contract:" <> &1["ref"], Map.take(&1, ["relation", "contract", "dependencies"])}
      )

    models |> Map.merge(metrics) |> Map.merge(contracts)
  end

  defp reason(asset, value), do: %{"asset" => asset, "reason" => value}
  defp quote_identifier(value), do: "\"" <> String.replace(value, "\"", "\"\"") <> "\""
end
