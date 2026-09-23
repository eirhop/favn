defmodule FavnDuckdbADBC.SemanticCompiler.Grammar do
  @moduledoc false

  @base ~w(class type alias query_location)
  @aggregates ~w(sum min max avg count)
  @scalars ~w(nullif abs round)
  @operators ~w(+ - * / %)
  @types ~w(NULL BOOLEAN TINYINT SMALLINT INTEGER BIGINT HUGEINT UTINYINT USMALLINT UINTEGER UBIGINT UHUGEINT FLOAT DOUBLE DECIMAL VARCHAR DATE TIMESTAMP TIMESTAMP_TZ TIME INTERVAL)
  @profile %{
    integer: "BIGINT",
    float: "DOUBLE",
    decimal: "DECIMAL(18,2)",
    string: "VARCHAR",
    boolean: "BOOLEAN",
    date: "DATE",
    datetime: "TIMESTAMP",
    binary: "BLOB",
    time: "TIME",
    json: "JSON",
    uuid: "UUID"
  }

  @doc false
  @spec profile([map()]) :: map()
  def profile(inputs), do: Map.new(inputs, &{&1.name, Map.fetch!(@profile, &1.type)})

  @doc false
  @spec validate(map(), [map()], [non_neg_integer()] | nil) ::
          {:ok, [non_neg_integer()]} | {:error, :invalid_semantic_expression | :aggregate_limit}
  def validate(parsed, inputs, allowed) do
    names = Enum.map(inputs, & &1.name)
    locations = Map.new(inputs, fn input -> {input.name, Map.get(input, :locations)} end)

    try do
      ensure(length(names) == length(Enum.uniq_by(names, &String.downcase/1)))
      ensure(keys?(parsed, ~w(error statements)) and parsed["error"] == false)
      [statement] = parsed["statements"]
      ensure(keys?(statement, ~w(node named_param_map)) and statement["named_param_map"] == [])
      node = statement["node"]

      expected = %{
        "type" => "SELECT_NODE",
        "modifiers" => [],
        "cte_map" => %{"map" => []},
        "where_clause" => nil,
        "group_expressions" => [],
        "group_sets" => [],
        "aggregate_handling" => "STANDARD_HANDLING",
        "having" => nil,
        "sample" => nil,
        "qualify" => nil
      }

      ensure(keys?(node, Map.keys(expected) ++ ~w(select_list from_table)))
      ensure(Enum.all?(expected, fn {key, value} -> node[key] == value end))
      table = node["from_table"]
      ensure(keys?(table, ~w(type alias sample query_location)))
      ensure(table["type"] == "EMPTY" and table["alias"] == "" and table["sample"] == nil)
      [expression] = node["select_list"]
      {aggregate, offsets} = expression(expression, names, locations, MapSet.new(), 0, false)
      ensure(aggregate)
      result = offsets |> MapSet.to_list() |> Enum.sort()
      ensure(is_nil(allowed) or MapSet.new(result) == MapSet.new(allowed))
      {:ok, result}
    rescue
      _ -> {:error, :invalid_semantic_expression}
    catch
      :invalid -> {:error, :invalid_semantic_expression}
      :aggregate_limit -> {:error, :aggregate_limit}
    end
  end

  defp expression(node, names, locations, offsets, depth, inside) do
    ensure(is_map(node) and depth <= 64 and node["alias"] == "")
    kind = node["class"]
    type = node["type"]

    {extras, children, aggregate, offsets} =
      case kind do
        "COLUMN_REF" ->
          ensure(type == "COLUMN_REF" and node["column_names"] in Enum.map(names, &[&1]))
          [name] = node["column_names"]

          if specified = locations[name] do
            ensure(
              is_integer(node["query_location"]) and (node["query_location"] - 7) in specified
            )
          end

          {~w(column_names), [], false, offsets}

        "CONSTANT" ->
          ensure(type == "VALUE_CONSTANT")
          value = node["value"]
          ensure(is_map(value) and Enum.all?(Map.keys(value), &(&1 in ~w(type is_null value))))
          logical_type(value["type"])
          ensure(is_boolean(value["is_null"]))
          {~w(value), [], false, offsets}

        "FUNCTION" ->
          name = node["function_name"]
          ensure(type == "FUNCTION" and name in (@aggregates ++ @scalars ++ @operators))
          ensure(node["schema"] == "" and node["catalog"] == "" and node["export_state"] == false)
          ensure(node["order_bys"] == %{"type" => "ORDER_MODIFIER", "orders" => []})
          ensure(node["is_operator"] == name in @operators)
          aggregate = name in @aggregates
          ensure(not (aggregate and inside))
          ensure(aggregate or (node["filter"] == nil and node["distinct"] == false))
          offsets = if aggregate, do: add_offset(offsets, node["query_location"]), else: offsets
          ensure(is_list(node["children"]))
          filter = if is_nil(node["filter"]), do: [], else: [node["filter"]]

          {~w(function_name schema children filter order_bys distinct is_operator export_state catalog),
           node["children"] ++ filter, aggregate, offsets}

        "CAST" ->
          ensure(type == "OPERATOR_CAST" and node["try_cast"] == false)
          logical_type(node["cast_type"])
          {~w(child cast_type try_cast), [node["child"]], false, offsets}

        "CASE" ->
          ensure(type == "CASE_EXPR" and is_list(node["case_checks"]))

          children =
            Enum.flat_map(node["case_checks"], fn check ->
              ensure(keys?(check, ~w(when_expr then_expr)))
              [check["when_expr"], check["then_expr"]]
            end)

          {~w(case_checks else_expr), children ++ [node["else_expr"]], false, offsets}

        "COMPARISON" ->
          ensure(
            type in ~w(COMPARE_EQUAL COMPARE_NOTEQUAL COMPARE_LESSTHAN COMPARE_GREATERTHAN COMPARE_LESSTHANOREQUALTO COMPARE_GREATERTHANOREQUALTO COMPARE_DISTINCT_FROM COMPARE_NOT_DISTINCT_FROM)
          )

          {~w(left right), [node["left"], node["right"]], false, offsets}

        "CONJUNCTION" ->
          ensure(type in ~w(CONJUNCTION_AND CONJUNCTION_OR) and is_list(node["children"]))
          {~w(children), node["children"], false, offsets}

        "OPERATOR" ->
          ensure(
            type in ~w(OPERATOR_NOT OPERATOR_IS_NULL OPERATOR_IS_NOT_NULL OPERATOR_COALESCE) and
              is_list(node["children"])
          )

          {~w(children), node["children"], false, offsets}

        _ ->
          throw(:invalid)
      end

    ensure(keys?(node, @base ++ extras))

    Enum.reduce(children, {aggregate, offsets}, fn child, {found, current} ->
      {child_found, next} =
        expression(
          child,
          names,
          locations,
          current,
          depth + 1,
          inside or (aggregate and kind == "FUNCTION")
        )

      {found or child_found, next}
    end)
  end

  defp logical_type(type) do
    ensure(keys?(type, ~w(id type_info)) and type["id"] in @types)

    case type["type_info"] do
      nil ->
        :ok

      info ->
      ensure(type["id"] == "DECIMAL" and keys?(info, ~w(type alias extension_info width scale)))

      ensure(
        info["type"] == "DECIMAL_TYPE_INFO" and info["alias"] == "" and
          info["extension_info"] == nil
      )

      ensure(is_integer(info["width"]) and is_integer(info["scale"]))
      ensure(info["width"] in 1..38 and info["scale"] in 0..info["width"])
    end
  end

  defp add_offset(offsets, location) do
    ensure(is_integer(location) and (location - 7) in 0..65_535)
    result = MapSet.put(offsets, location - 7)
    if MapSet.size(result) > 1024, do: throw(:aggregate_limit)
    result
  end

  defp keys?(value, keys) when is_map(value), do: MapSet.new(Map.keys(value)) == MapSet.new(keys)
  defp keys?(_, _), do: false
  defp ensure(true), do: :ok
  defp ensure(_), do: throw(:invalid)
end
