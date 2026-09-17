defmodule Favn.Semantic.Schema do
  @moduledoc false

  alias Favn.Semantic.Snapshot

  @types ~w(boolean integer float decimal string binary date time datetime json uuid)
  @rules ~w(aggregate first last none)

  def validate(wire) do
    check!(
      record?(wire, %{
        "schema_version" => &(&1 == 1),
        "semantic_version" => &digest?(&1, "sm_"),
        "snapshot_version" => &digest?(&1, "dc_"),
        "snapshot" => &list?(&1, 10_000, fn x -> asset?(x) end),
        "models" => &list?(&1, 256, fn x -> model?(x) end),
        "compiler" => &compiler?/1
      })
    )

    check!(unique?(wire["snapshot"], "ref") and unique?(wire["models"], "name"))
    assets = Map.new(wire["snapshot"], &{&1["ref"], &1})
    Enum.each(wire["snapshot"], &asset_refs!(&1, assets))
    Enum.each(wire["models"], &model_refs!(&1, assets))
    macros = for model <- wire["models"], metric <- model["metrics"], do: metric["macro_name"]
    check!(macros == Enum.uniq(macros))
    :ok
  catch
    :invalid_semantic_artifact -> {:error, :invalid_semantic_artifact}
  end

  defp compiler?(value),
    do:
      record?(value, %{
        "name" => &(&1 == "favn-semantic"),
        "version" => &(&1 == 1),
        "dialect" => &(&1 == "duckdb")
      })

  defp asset?(value),
    do:
      record?(value, %{
        "ref" => &text?/1,
        "kind" => &(&1 in ~w(sql elixir source)),
        "fingerprint" => &digest?(&1, "ac_"),
        "relation" => &optional?(&1, fn x -> relation?(x) end),
        "dependencies" => &strings?(&1, 10_000),
        "contract" => &optional?(&1, fn x -> contract?(x) end)
      })

  defp relation?(value),
    do:
      record?(value, %{
        "connection_ref" => &optional_text?/1,
        "catalog" => &optional_text?/1,
        "schema" => &optional_text?/1,
        "name" => &text?/1
      })

  defp contract?(value),
    do:
      record?(value, %{
        "grain" => &strings?(&1, 1000),
        "grain_description" => &optional_text?/1,
        "columns" => &list?(&1, 1000, fn x -> column?(x) end),
        "compositions" => &list?(&1, 128, fn x -> composition?(x) end),
        "unique_keys" =>
          &list?(&1, 128, fn x -> record?(x, %{"columns" => fn y -> strings?(y, 1000) end}) end),
        "row_counts" => &list?(&1, 16, fn x -> row_count?(x) end),
        "relationships" => &list?(&1, 32, fn x -> relationship?(x) end)
      })

  defp composition?(value),
    do:
      record?(value, %{
        "module" => &text?/1,
        "start_index" => &(is_integer(&1) and &1 >= 0),
        "columns" => &strings?(&1, 1000)
      })

  defp column?(value),
    do:
      record?(value, %{
        "name" => &text?/1,
        "ordinal" => &(is_integer(&1) and &1 > 0),
        "type" => &(&1 in @types),
        "nullable" => &is_boolean/1,
        "description" => &optional_text?/1,
        "renamed_from" => &optional_text?/1,
        "tags" => &strings?(&1, 1000),
        "via" => &(&1 in [nil, "identity", "transformation", "aggregation"]),
        "sources" => &list?(&1, 10_000, fn x -> lineage?(x) end)
      })

  defp lineage?(value),
    do:
      record?(value, %{
        "kind" => &(&1 in ["asset", "external"]),
        "asset_ref" => &optional_text?/1,
        "dataset" => &optional_text?/1,
        "column" => &text?/1
      })

  defp row_count?(value),
    do:
      record?(value, %{
        "equals" => &(bound?(&1) or record?(&1, %{"parameter" => fn x -> text?(x) end})),
        "min" => &bound?/1,
        "max" => &bound?/1,
        "when" => &(&1 in [nil, "target_exists"]),
        "on_violation" => &(&1 in ~w(fail warn skip_materialization))
      })

  defp relationship?(value),
    do:
      record?(value, %{
        "name" => &text?/1,
        "target" => &text?/1,
        "cardinality" => &(&1 in ~w(many_to_one one_to_one)),
        "on_violation" => &(&1 in ~w(fail warn)),
        "on" =>
          &list?(&1, 1000, fn x ->
            record?(x, %{"source" => fn y -> text?(y) end, "target" => fn y -> text?(y) end})
          end)
      })

  defp model?(value),
    do:
      record?(value, %{
        "name" => &name?/1,
        "source_asset" => &text?/1,
        "dimension" => &optional?(&1, fn x -> dimension?(x) end),
        "time" => &optional?(&1, fn x -> time?(x) end),
        "hierarchies" =>
          &list?(&1, 32, fn x ->
            record?(x, %{"name" => fn y -> name?(y) end, "columns" => fn y -> strings?(y, 16) end})
          end),
        "metrics" => &list?(&1, 256, fn x -> metric?(x) end)
      })

  defp dimension?(value),
    do: record?(value, %{"name" => &name?/1, "label" => &text?/1, "key" => &strings?(&1, 1000)})

  defp time?(value),
    do:
      record?(value, %{
        "column" => &text?/1,
        "grain" => &(&1 in ~w(day month)),
        "timezone" => &text?/1
      })

  defp metric?(value),
    do:
      record?(value, %{
        "name" => &name?/1,
        "ref" => &text?/1,
        "macro_name" => &text?/1,
        "authored_sql" => &text?(&1, 16_384),
        "canonical_sql" => &text?(&1, 65_536),
        "formula_digest" => &digest?(&1, "fm_"),
        "description" => &text?(&1, 1024),
        "inputs" => &list?(&1, 32, fn x -> input?(x) end),
        "unit" => &unit?/1,
        "format" => &optional?(&1, fn x -> format?(x) end),
        "time_aggregate" => &(&1 in @rules),
        "minimum_grain" => &strings?(&1, 32),
        "entity_key" => &strings?(&1, 1000),
        "metric_dependencies" => &strings?(&1, 256),
        "column_dependencies" => &strings?(&1, 32),
        "logical_result_type" => &(&1 in ["unknown" | @types]),
        "nullable" => &(&1 in ["unknown", true, false]),
        "validation" => &validation?/1
      })

  defp input?(value),
    do:
      record?(value, %{
        "position" => &(is_integer(&1) and &1 in 1..32),
        "parameter" => &name?/1,
        "source_asset" => &text?/1,
        "column" => &text?/1,
        "contract_type" => &(&1 in @types),
        "nullable" => &is_boolean/1
      })

  defp unit?(value),
    do:
      record?(value, %{
        "kind" => &(&1 in ~w(count ratio percent currency custom)),
        "value" => &optional?(&1, fn x -> text?(x, 128) end)
      })

  defp format?(value),
    do:
      record?(value, %{
        "style" => &(&1 in ~w(number percent currency)),
        "decimals" => &(&1 == nil or (is_integer(&1) and &1 in 0..12))
      })

  defp validation?(value),
    do:
      record?(value, %{
        "validation_result_type" => &text?/1,
        "nullable" => &(&1 == "unknown"),
        "runtime_version" => &text?/1,
        "compiler_version" => &text?/1,
        "profile" =>
          &(is_map(&1) and map_size(&1) <= 32 and
              Enum.all?(&1, fn {key, val} -> name?(key) and text?(val, 128) end))
      })

  defp asset_refs!(asset, assets) do
    check!(asset["fingerprint"] == Snapshot.digest("ac_", Map.delete(asset, "fingerprint")))
    check!(Enum.all?(asset["dependencies"], &Map.has_key?(assets, &1)))

    if contract = asset["contract"] do
      columns = Map.new(contract["columns"], &{&1["name"], &1})
      check!(map_size(columns) == length(contract["columns"]) and map_size(columns) > 0)
      check!(Enum.map(contract["columns"], & &1["ordinal"]) == Enum.to_list(1..map_size(columns)))

      check!(
        Enum.all?(
          contract["grain"],
          &(Map.has_key?(columns, &1) and columns[&1]["nullable"] == false)
        )
      )

      Enum.each(contract["unique_keys"], fn key ->
        check!(key["columns"] != [] and Enum.all?(key["columns"], &Map.has_key?(columns, &1)))
      end)

      Enum.each(contract["columns"], fn column ->
        Enum.each(column["sources"], &lineage_refs!(&1, assets))
      end)

      check!(unique?(contract["relationships"], "name"))

      Enum.each(contract["relationships"], fn role ->
        target = assets[role["target"]]
        check!(target != nil and target["contract"] != nil and role["on"] != [])
        target_columns = Map.new(target["contract"]["columns"], &{&1["name"], &1})

        check!(
          Enum.all?(
            role["on"],
            &(columns[&1["source"]] != nil and target_columns[&1["target"]] != nil and
                columns[&1["source"]]["type"] == target_columns[&1["target"]]["type"])
          )
        )

        check!(unique?(role["on"], "source") and unique?(role["on"], "target"))
        check!(Enum.map(role["on"], & &1["target"]) == target["contract"]["grain"])
        check!(role["target"] in asset["dependencies"])
      end)
    end
  end

  defp lineage_refs!(%{"kind" => "external"} = source, _),
    do: check!(source["asset_ref"] == nil and text?(source["dataset"]))

  defp lineage_refs!(source, assets) do
    target = assets[source["asset_ref"]]
    check!(source["dataset"] == nil and target != nil)

    if target["contract"],
      do: check!(Enum.any?(target["contract"]["columns"], &(&1["name"] == source["column"])))
  end

  defp model_refs!(model, assets) do
    source = assets[model["source_asset"]]
    check!(source != nil and source["contract"] != nil and source["relation"] != nil)
    contract = source["contract"]
    columns = Map.new(contract["columns"], &{&1["name"], &1})
    roles = Enum.map(contract["relationships"], & &1["name"])
    metrics = Map.new(model["metrics"], &{&1["ref"], &1})
    check!(map_size(metrics) == length(model["metrics"]) and unique?(model["metrics"], "name"))

    if time = model["time"] do
      check!(
        columns[time["column"]] != nil and columns[time["column"]]["type"] == "date" and
          not columns[time["column"]]["nullable"]
      )

      check!(Favn.Timezone.valid_identifier?(time["timezone"]))
    end

    if dimension = model["dimension"],
      do:
        check!(
          dimension["key"] == contract["grain"] and dimension["key"] != [] and
            Map.has_key?(columns, dimension["label"])
        )

    check!(unique?(model["hierarchies"], "name"))

    Enum.each(model["hierarchies"], fn hierarchy ->
      levels = hierarchy["columns"]

      check!(
        model["dimension"] != nil and levels != [] and
          Enum.all?(levels, &Map.has_key?(columns, &1)) and
          Enum.take(levels, -length(contract["grain"])) == contract["grain"]
      )
    end)

    Enum.each(model["metrics"], fn metric ->
      check!(
        metric["ref"] == model["name"] <> "." <> metric["name"] and
          metric["macro_name"] == model["name"] <> "_" <> metric["name"]
      )

      check!(metric["formula_digest"] == Snapshot.digest("fm_", metric["canonical_sql"]))
      unit = metric["unit"]

      check!(
        case unit["kind"] do
          "currency" -> is_binary(unit["value"]) and Regex.match?(~r/\A[A-Z]{3}\z/, unit["value"])
          "custom" -> text?(unit["value"], 128)
          _ -> unit["value"] == nil
        end
      )

      if format = metric["format"] do
        check!(format["style"] != "currency" or unit["kind"] == "currency")
        check!(format["style"] != "percent" or unit["kind"] in ["ratio", "percent"])
      end

      inputs = metric["inputs"]

      check!(
        MapSet.new(Map.keys(metric["validation"]["profile"])) ==
          MapSet.new(Enum.map(inputs, & &1["parameter"]))
      )

      check!(
        inputs != [] and unique?(inputs, "parameter") and
          Enum.map(inputs, & &1["position"]) == Enum.to_list(1..length(inputs))
      )

      Enum.each(inputs, fn input ->
        column = columns[input["column"]]

        check!(
          column != nil and input["source_asset"] == model["source_asset"] and
            input["parameter"] == input["column"] and input["contract_type"] == column["type"] and
            input["nullable"] == column["nullable"]
        )
      end)

      check!(Enum.all?(metric["minimum_grain"], &(&1 in roles)))
      check!(Enum.all?(metric["column_dependencies"], &Map.has_key?(columns, &1)))
      check!(metric["time_aggregate"] == "aggregate" or model["time"] != nil)

      check!(
        metric["time_aggregate"] not in ["first", "last"] or
          model["time"]["column"] in contract["grain"]
      )

      expected_key =
        if(metric["time_aggregate"] in ["first", "last"],
          do: contract["grain"] -- [model["time"]["column"]],
          else: []
        )

      check!(metric["entity_key"] == expected_key)
    end)

    Enum.reduce(model["metrics"], %{}, fn metric, cache ->
      {_depth, cache} = dependency_refs!(metric, metrics, [], cache)
      cache
    end)
  end

  defp dependency_refs!(metric, metrics, seen, cache) do
    check!(metric["ref"] not in seen and length(seen) < 32)

    case cache[metric["ref"]] do
      nil ->
        {depth, cache} =
          Enum.reduce(metric["metric_dependencies"], {1, cache}, fn ref, {depth, cache} ->
            child = metrics[ref]
            check!(child != nil and child["time_aggregate"] == metric["time_aggregate"])
            check!(Enum.all?(child["minimum_grain"], &(&1 in metric["minimum_grain"])))
            {child_depth, cache} = dependency_refs!(child, metrics, [metric["ref"] | seen], cache)
            {max(depth, child_depth + 1), cache}
          end)

        {depth, Map.put(cache, metric["ref"], depth)}

      depth ->
        check!(length(seen) + depth <= 32)
        {depth, cache}
    end
  end

  defp record?(value, schema),
    do:
      is_map(value) and map_size(value) == map_size(schema) and
        Enum.all?(schema, fn {key, check} -> Map.has_key?(value, key) and check.(value[key]) end)

  defp list?(value, limit, check),
    do: is_list(value) and length(value) <= limit and Enum.all?(value, check)

  defp strings?(value, limit), do: list?(value, limit, &text?/1) and value == Enum.uniq(value)

  defp text?(value, limit \\ 65_536),
    do:
      is_binary(value) and byte_size(value) in 1..limit and String.valid?(value) and
        String.trim(value) != ""

  defp optional_text?(value), do: optional?(value, &text?/1)
  defp optional?(nil, _), do: true
  defp optional?(value, check), do: check.(value)
  defp name?(value), do: text?(value, 64) and Regex.match?(~r/\A[a-z][a-z0-9_]*\z/, value)

  defp digest?(value, prefix),
    do: is_binary(value) and Regex.match?(~r/\A#{prefix}[0-9a-f]{64}\z/, value)

  defp unique?(values, key), do: values |> Enum.map(& &1[key]) |> then(&(&1 == Enum.uniq(&1)))
  defp bound?(value), do: value == nil or (is_integer(value) and value >= 0)
  defp check!(true), do: :ok
  defp check!(false), do: throw(:invalid_semantic_artifact)
end
