defmodule Favn.Semantic.Compiler do
  @moduledoc """
  Compiles captured semantic declarations and public asset contracts.

  Compilation is independent of runner manifests. The injected native validator
  must implement `Favn.Semantic.Validator`; Core never opens a SQL connection or
  loads a customer module. Diagnostics retain authoring locations, while artifact
  identity depends only on the public contract and validated formula content.

  Native validation returns aggregate token locations. Inlining shifts those
  locations so a composed expression must contain exactly its children's
  aggregates, without a second SQL lexer or parser in Core.
  """

  alias Favn.Semantic.{Artifact, Diagnostic, Snapshot}
  alias Favn.SQL.Template
  alias Favn.SQL.Template.{Call, Placeholder, Text}

  @options [:unit, :description, :format, :time_aggregate, :minimum_grain, :file]
  @builtins ~w(sum min max avg count coalesce nullif abs round cast try_cast case)
  @native_errors [
    :aggregate_limit,
    :semantic_bind_failed,
    :semantic_driver_unavailable,
    :semantic_python_unavailable,
    :semantic_runtime_unsupported,
    :semantic_validation_timeout,
    :semantic_worker_cleanup_unconfirmed,
    :semantic_worker_failed,
    :semantic_worker_output_limit,
    :semantic_worker_platform_unsupported,
    :semantic_worker_protocol_error,
    :semantic_worker_start_failed,
    :invalid_semantic_input,
    :invalid_semantic_expression
  ]
  @max_build_ms 300_000

  @doc "Compiles captured models against enriched asset maps and a native validator."
  @spec compile([map()], [map()], module() | function()) ::
          {:ok, Artifact.t()} | {:error, [Diagnostic.t()]}
  def compile(models, assets, validator) when is_list(models) and length(models) <= 256 do
    with {:ok, snapshot} <- Snapshot.build(assets) do
      deadline = System.monotonic_time(:millisecond) + @max_build_ms
      context = %{assets: assets, snapshot: snapshot, validator: validator, deadline: deadline}

      {compiled, errors} =
        Enum.reduce(models, {[], []}, fn model, {acc, errors} ->
          case safely(fn -> model(model, context) end, model) do
            {:ok, value} -> {[value | acc], errors}
            {:error, error} -> {acc, [error | errors]}
          end
        end)

      case errors do
        [] ->
          if System.monotonic_time(:millisecond) < deadline,
            do: finish(compiled, snapshot),
            else:
              {:error,
               [
                 Diagnostic.new(
                   :build_timeout,
                   "Semantic build exceeded its five-minute deadline."
                 )
               ]}

        _ ->
          {:error, bound_diagnostics(Enum.reverse(errors))}
      end
    else
      {:error, reason} ->
        {:error, [Diagnostic.new(reason, "Invalid public data-contract snapshot.")]}
    end
  end

  def compile(_, _, _),
    do: {:error, [Diagnostic.new(:model_limit, "At most 256 semantic models are supported.")]}

  defp finish(models, snapshot) do
    names = Enum.map(models, & &1["name"])
    macros = for model <- models, metric <- model["metrics"], do: metric["macro_name"]

    cond do
      names != Enum.uniq(names) ->
        {:error, [Diagnostic.new(:duplicate_model, "Semantic model names must be unique.")]}

      macros != Enum.uniq(macros) ->
        {:error, [Diagnostic.new(:macro_collision, "Generated metric macro names collide.")]}

      true ->
        Artifact.new(Enum.sort_by(models, & &1["name"]), snapshot)
    end
  end

  defp model(model, context) do
    name = name!(model.name)
    source = Enum.find(context.assets, &(Map.get(&1, :module) == model.module))

    require!(
      source != nil and Map.get(source, :contract) != nil,
      :missing_contract,
      "A semantic model requires its containing SQL asset's output contract."
    )

    source_ref = Snapshot.ref(source.ref)
    public = Enum.find(context.snapshot, &(&1["ref"] == source_ref))

    require!(
      public["relation"] != nil,
      :missing_relation,
      "A semantic source requires a resolved relation."
    )

    columns = Map.new(source.contract.columns, &{&1.name, &1})
    grain = public["contract"]["grain"]
    time = time(model.time, columns)
    dimension = dimension(model.dimension, columns, grain)
    hierarchies = hierarchies(model.hierarchies, dimension, columns, grain)
    metrics = model.metrics

    require!(
      is_list(metrics) and length(metrics) <= 256,
      :metric_limit,
      "At most 256 metrics per model are supported."
    )

    metric_names = Enum.map(metrics, &name!(&1.name))

    require!(
      metric_names == Enum.uniq(metric_names),
      :duplicate_metric,
      "Metric names cannot be duplicated or overloaded."
    )

    require!(
      Enum.all?(metric_names, &(&1 not in @builtins)),
      :ambiguous_metric,
      "Metric names cannot shadow supported SQL functions."
    )

    definitions =
      Map.new(
        metrics,
        &{{&1.name, length(&1.args)},
         %{module: model.module, name: &1.name, arity: length(&1.args), shape: :expression}}
      )

    by_name = Map.new(metrics, &{&1.name, &1})
    roles = Enum.map(public["contract"]["relationships"], & &1["name"])

    context =
      Map.merge(context, %{
        name: name,
        source: source_ref,
        columns: columns,
        grain: grain,
        time: time,
        roles: roles,
        module: model.module,
        definitions: definitions,
        by_name: by_name
      })

    {compiled, _cache} =
      Enum.reduce(metrics, {[], %{}}, fn metric, {compiled, cache} ->
        case safely(
               fn -> metric(metric.name, context, [], cache) end,
               Map.merge(metric, %{model: name, metric: to_string(metric.name)})
             ) do
          {:ok, {result, cache, _depth}} -> {[result | compiled], cache}
          {:error, diagnostic} -> throw({:diagnostic, diagnostic})
        end
      end)

    %{
      "name" => name,
      "source_asset" => source_ref,
      "dimension" => dimension,
      "hierarchies" => hierarchies,
      "time" => time,
      "metrics" =>
        compiled
        |> Enum.map(&Map.drop(&1, ["_input_locations", "_aggregate_locations"]))
        |> Enum.sort_by(& &1["name"])
    }
  end

  defp metric(name, context, stack, cache) do
    require!(
      System.monotonic_time(:millisecond) < context.deadline,
      :build_timeout,
      "Semantic build exceeded its five-minute deadline."
    )

    require!(name not in stack, :metric_cycle, "Metric dependencies contain a cycle.")
    require!(length(stack) < 32, :dependency_depth, "Metric dependency depth exceeds 32.")

    case Map.fetch(cache, name) do
      {:ok, {result, depth}} ->
        require!(
          length(stack) + depth <= 32,
          :dependency_depth,
          "Metric dependency depth exceeds 32."
        )

        {result, cache, depth}

      :error ->
        source =
          context.by_name
          |> Map.fetch!(name)
          |> Map.merge(%{model: context.name, metric: to_string(name)})

        case safely(fn -> compile_metric(name, context, stack, cache) end, source) do
          {:ok, result} -> result
          {:error, diagnostic} -> throw({:diagnostic, diagnostic})
        end
    end
  end

  defp compile_metric(name, context, stack, cache) do
    metric = Map.fetch!(context.by_name, name)
    args = metric.args

    require!(
      is_list(args) and length(args) in 1..32 and args == Enum.uniq(args),
      :invalid_arguments,
      "Metrics require one to 32 distinct source-column arguments."
    )

    Enum.each(args, fn arg ->
      name!(arg)

      require!(
        Map.has_key?(context.columns, arg),
        :unknown_column,
        "A metric argument does not name a source column."
      )
    end)

    require!(
      is_binary(metric.sql) and byte_size(metric.sql) in 1..16_384,
      :formula_limit,
      "Authored metric SQL must contain one to 16,384 bytes."
    )

    require!(
      Keyword.keyword?(metric.opts) and
        Keyword.keys(metric.opts) == Enum.uniq(Keyword.keys(metric.opts)),
      :invalid_options,
      "Metric options must be unique keyword entries."
    )

    require!(
      Enum.all?(Keyword.keys(metric.opts), &(&1 in @options)),
      :unknown_option,
      "Unknown metric option."
    )

    unit = unit(Keyword.get(metric.opts, :unit))
    description = Keyword.get(metric.opts, :description)

    require!(
      is_binary(description) and byte_size(description) <= 1024 and String.trim(description) != "",
      :description_required,
      "A metric requires a nonempty description of at most 1,024 bytes."
    )

    format = format(Keyword.get(metric.opts, :format), unit)

    template =
      Template.compile!(metric.sql,
        file: metric.file,
        line: metric.line,
        module: context.module,
        scope: :definition,
        resolve_asset_refs: false,
        local_args: args,
        known_definitions: context.definitions
      )

    require!(
      template.root_kind == :expression,
      :expression_required,
      "A metric must be one aggregate expression."
    )

    calls = Enum.filter(template.nodes, &match?(%Call{}, &1))

    require!(
      calls == [] or not Enum.any?(template.nodes, &match?(%Placeholder{}, &1)),
      :mixed_composition,
      "Composed metrics cannot mix metric calls and raw column inputs."
    )

    {expanded, used, children, cache, depth, locations, aggregate_locations} =
      Enum.reduce(template.nodes, {"", [], [], cache, 1, %{}, []}, fn node,
                                                                      {sql, used, children, cache,
                                                                       depth, locations,
                                                                       aggregate_locations} ->
        {fragment, inputs, child, cache, child_depth, fragment_locations} =
          expand_node(node, args, context, [name | stack], cache)

        require!(
          byte_size(sql) + byte_size(fragment) <= 65_536,
          :expansion_limit,
          "Expanded metric SQL exceeds 65,536 bytes."
        )

        shifted = shift_locations(fragment_locations, byte_size(sql))
        locations = Map.merge(locations, shifted, fn _, a, b -> a ++ b end)

        aggregate_locations =
          if child,
            do:
              aggregate_locations ++
                Enum.map(child["_aggregate_locations"], &(&1 + byte_size(sql) + 1)),
            else: aggregate_locations

        require!(
          length(aggregate_locations) <= 1024,
          :aggregate_limit,
          "Expanded metric SQL supports at most 1,024 aggregate locations."
        )

        {sql <> fragment, used ++ inputs, if(child, do: [child | children], else: children),
         cache, max(depth, child_depth + 1), locations, aggregate_locations}
      end)

    require!(
      MapSet.new(used) == MapSet.new(args),
      :unused_argument,
      "Each metric signature argument must be used by its formula."
    )

    children = Enum.uniq_by(children, & &1["ref"])
    {selection, minimum} = usage(metric.opts, children, context)

    inputs =
      Enum.with_index(args, 1)
      |> Enum.map(fn {arg, index} ->
        column = Map.fetch!(context.columns, arg)

        %{
          "position" => index,
          "parameter" => to_string(arg),
          "source_asset" => context.source,
          "column" => to_string(arg),
          "contract_type" => to_string(column.type),
          "nullable" => column.nullable?
        }
      end)

    validation_inputs =
      Enum.map(args, fn arg ->
        column = Map.fetch!(context.columns, arg)

        %{
          name: to_string(arg),
          type: column.type,
          nullable: column.nullable?,
          locations: Map.fetch!(locations, to_string(arg))
        }
      end)

    require!(
      System.monotonic_time(:millisecond) < context.deadline,
      :build_timeout,
      "Semantic build exceeded its five-minute deadline."
    )

    allowed_aggregates = if calls == [], do: nil, else: Enum.sort(Enum.uniq(aggregate_locations))

    {validation, aggregate_locations} =
      validate(context.validator, expanded, validation_inputs,
        allowed_aggregate_locations: allowed_aggregates
      )

    require!(
      System.monotonic_time(:millisecond) < context.deadline,
      :build_timeout,
      "Semantic build exceeded its five-minute deadline."
    )

    macro_name = context.name <> "_" <> to_string(name)

    result = %{
      "name" => to_string(name),
      "ref" => context.name <> "." <> to_string(name),
      "macro_name" => macro_name,
      "authored_sql" => metric.sql,
      "canonical_sql" => expanded,
      "formula_digest" => Snapshot.digest("fm_", expanded),
      "inputs" => inputs,
      "description" => description,
      "unit" => unit,
      "format" => format,
      "time_aggregate" => selection,
      "minimum_grain" => minimum,
      "entity_key" =>
        if(selection in ["first", "last"],
          do: context.grain -- [context.time["column"]],
          else: []
        ),
      "metric_dependencies" => children |> Enum.map(& &1["ref"]) |> Enum.sort(),
      "column_dependencies" => if(children == [], do: Enum.map(args, &to_string/1), else: []),
      "logical_result_type" => "unknown",
      "nullable" => "unknown",
      "validation" => validation
    }

    result =
      result
      |> Map.put("_input_locations", locations)
      |> Map.put("_aggregate_locations", aggregate_locations)

    {result, Map.put(cache, name, {result, depth}), depth}
  end

  defp expand_node(%Text{sql: sql}, _args, _context, _stack, cache),
    do: {sql, [], nil, cache, 0, %{}}

  defp expand_node(%Placeholder{name: name}, args, _context, _stack, cache) do
    name = Enum.find(args, &(to_string(&1) == to_string(name)))
    require!(name != nil, :unknown_input, "Only declared metric arguments can supply columns.")
    {quote_identifier(to_string(name)), [name], nil, cache, 0, %{to_string(name) => [0]}}
  end

  defp expand_node(%Call{definition: definition, args: fragments}, _args, context, stack, cache) do
    child = Map.fetch!(context.by_name, definition.name)

    passed =
      Enum.map(fragments, fn fragment ->
        nodes =
          Enum.reject(fragment.nodes, fn
            %Text{sql: sql} -> String.trim(sql) == ""
            _ -> false
          end)

        case nodes do
          [%Placeholder{name: name}] ->
            to_string(name)

          _ ->
            fail(
              :invalid_metric_call,
              "Metric calls require exact source-column arguments in declaration order."
            )
        end
      end)

    require!(
      passed == Enum.map(child.args, &to_string/1),
      :invalid_metric_call,
      "Metric call arguments do not match the referenced metric's source columns and order."
    )

    {compiled, cache, depth} = metric(child.name, context, stack, cache)

    {"(" <> compiled["canonical_sql"] <> ")", child.args, compiled, cache, depth,
     shift_locations(compiled["_input_locations"], 1)}
  end

  defp expand_node(_, _, _, _, _),
    do:
      fail(
        :unsupported_sql,
        "Relations, runtime inputs and external helpers are not metric expressions."
      )

  defp usage(opts, children, context) do
    explicit = Keyword.get(opts, :time_aggregate)

    require!(
      explicit in [nil, :aggregate, :first, :last, :none],
      :invalid_time_rule,
      "Time aggregation must be aggregate, first, last or none."
    )

    child_rules = children |> Enum.map(& &1["time_aggregate"]) |> Enum.uniq()

    require!(
      length(child_rules) <= 1,
      :incompatible_time_rules,
      "Composed metrics require identical row-selection rules."
    )

    inferred = List.first(child_rules)
    rule = if(explicit, do: to_string(explicit), else: inferred)

    require!(
      rule in ["aggregate", "first", "last", "none"],
      :time_rule_required,
      "Leaf metrics require time_aggregate: aggregate, first, last or none."
    )

    require!(
      inferred == nil or inferred == rule,
      :weakened_time_rule,
      "An explicit rule cannot change a composed metric's inherited row selection."
    )

    require!(
      rule == "aggregate" or context.time != nil,
      :time_required,
      "This metric's time rule requires a model time declaration."
    )

    require!(
      rule not in ["first", "last"] or context.time["column"] in context.grain,
      :time_grain_required,
      "First/last selection requires the time column in structured source grain."
    )

    inherited = children |> Enum.flat_map(& &1["minimum_grain"]) |> Enum.uniq()
    declared = Keyword.get(opts, :minimum_grain, [])

    require!(
      is_list(declared) and Enum.all?(declared, &is_atom/1),
      :invalid_minimum_grain,
      "Minimum grain must be a list of source relationship roles."
    )

    declared = Enum.map(declared, &to_string/1)

    require!(
      declared == Enum.uniq(declared) and Enum.all?(declared, &(&1 in context.roles)),
      :invalid_minimum_grain,
      "Minimum grain contains duplicate or unknown relationship roles."
    )

    require!(
      not Keyword.has_key?(opts, :minimum_grain) or Enum.all?(inherited, &(&1 in declared)),
      :weakened_minimum_grain,
      "Explicit minimum grain cannot omit inherited role requirements."
    )

    {rule, Enum.sort(Enum.uniq(inherited ++ declared))}
  end

  defp validate(validator, sql, inputs, options) do
    require!(
      is_function(validator, 3) or
        (is_atom(validator) and Code.ensure_loaded?(validator) and
           function_exported?(validator, :validate, 3)),
      :validator_unavailable,
      "A native semantic validation capability is required to build this artifact."
    )

    result =
      if is_function(validator, 3),
        do: validator.(sql, inputs, options),
        else: validator.validate(sql, inputs, options)

    case result do
      {:ok, %{native_type: type} = validation} when is_binary(type) ->
        require!(
          is_binary(validation[:runtime_version]) and is_binary(validation[:compiler_version]) and
            is_map(validation[:validation_profile]) and
            MapSet.new(Map.keys(validation.validation_profile)) ==
              MapSet.new(Enum.map(inputs, & &1.name)),
          :invalid_validator,
          "Native validation must record its runtime, compiler and complete input type profile."
        )

        aggregate_locations = Map.get(validation, :aggregate_locations)

        require!(
          is_list(aggregate_locations) and length(aggregate_locations) in 1..1024 and
            Enum.all?(aggregate_locations, &(is_integer(&1) and &1 >= 0 and &1 < byte_size(sql))) and
            aggregate_locations == Enum.sort(Enum.uniq(aggregate_locations)),
          :invalid_validator,
          "Native validation must return one to 1,024 distinct ordered aggregate token locations."
        )

        expected = Keyword.fetch!(options, :allowed_aggregate_locations)

        require!(
          is_nil(expected) or expected == aggregate_locations,
          :mixed_composition,
          "Composed metrics must preserve exactly their referenced metrics' aggregates."
        )

        evidence = %{
          "validation_result_type" => type,
          "nullable" => "unknown",
          "runtime_version" => validation.runtime_version,
          "compiler_version" => validation.compiler_version,
          "profile" => normalize_profile(validation.validation_profile)
        }

        {evidence, aggregate_locations}

      {:error, reason} when reason in @native_errors ->
        fail(reason, "Native semantic validation failed: #{reason}.")

      {:error, {:semantic_worker_cleanup_unconfirmed, identity}} when is_map(identity) ->
        fields = [:supervisor_pid, :worker_pid, :process_group]

        require!(
          Enum.sort(Map.keys(identity)) == Enum.sort(fields) and
            Enum.all?(identity, fn {_, value} ->
              is_nil(value) or (is_integer(value) and value > 0 and value <= 4_294_967_295)
            end),
          :invalid_validator,
          "Native cleanup returned an invalid process identity."
        )

        detail =
          Enum.map_join(fields, ", ", fn field ->
            "#{field}=#{Map.get(identity, field) || "unknown"}"
          end)

        fail(
          :semantic_worker_cleanup_unconfirmed,
          "Native worker exit could not be confirmed (#{detail})."
        )

      {:error, _reason} ->
        fail(
          :native_validation_failed,
          "Native parsing or binding rejected the metric expression."
        )

      _ ->
        fail(:invalid_validator, "Native validation returned an invalid result.")
    end
  end

  defp normalize_profile(value) when is_map(value),
    do: Map.new(value, fn {key, val} -> {to_string(key), normalize_profile(val)} end)

  defp normalize_profile(value) when is_list(value), do: Enum.map(value, &normalize_profile/1)

  defp normalize_profile(value) when is_atom(value) and value not in [true, false, nil],
    do: to_string(value)

  defp normalize_profile(value), do: value

  defp time(nil, _columns), do: nil

  defp time(time, columns) do
    column = Map.get(columns, time.column)

    require!(
      column != nil and column.type == :date and not column.nullable?,
      :invalid_time_column,
      "Time requires a non-null DATE contract column."
    )

    require!(
      time.grain in [:day, :month],
      :invalid_time_grain,
      "Time grain must be day or month."
    )

    require!(
      is_binary(time.timezone) and Favn.Timezone.valid_identifier?(time.timezone),
      :invalid_timezone,
      "Time timezone must be a valid IANA identifier."
    )

    %{
      "column" => to_string(time.column),
      "grain" => to_string(time.grain),
      "timezone" => time.timezone
    }
  end

  defp dimension(nil, _, _), do: nil

  defp dimension(value, columns, grain) do
    require!(
      grain != [] and Map.has_key?(columns, value.label),
      :invalid_dimension,
      "A dimension requires structured source grain and an existing label column."
    )

    %{"name" => name!(value.name), "label" => to_string(value.label), "key" => grain}
  end

  defp hierarchies(values, dimension, columns, grain) do
    require!(
      is_list(values) and length(values) <= 32,
      :hierarchy_limit,
      "At most 32 hierarchies per model are supported."
    )

    require!(
      values == [] or dimension != nil,
      :dimension_required,
      "Hierarchies require a dimension declaration."
    )

    result =
      Enum.map(values, fn value ->
        levels = value.columns

        require!(
          is_list(levels) and length(levels) in 1..16 and levels == Enum.uniq(levels),
          :invalid_hierarchy,
          "Hierarchy levels must contain one to 16 distinct columns."
        )

        require!(
          Enum.all?(levels, &Map.has_key?(columns, &1)),
          :unknown_hierarchy_column,
          "Hierarchy levels must name source columns."
        )

        levels = Enum.map(levels, &to_string/1)

        require!(
          Enum.take(levels, -length(grain)) == grain,
          :hierarchy_key,
          "Hierarchy final levels must contain the complete ordered dimension key."
        )

        %{"name" => name!(value.name), "columns" => levels}
      end)

    names = Enum.map(result, & &1["name"])
    require!(names == Enum.uniq(names), :duplicate_hierarchy, "Hierarchy names must be unique.")
    Enum.sort_by(result, & &1["name"])
  end

  defp unit(value) when value in [:count, :ratio, :percent],
    do: %{"kind" => to_string(value), "value" => nil}

  defp unit({:currency, code}) when is_binary(code) do
    require!(
      Regex.match?(~r/\A[A-Z]{3}\z/, code),
      :invalid_unit,
      "Currency requires an uppercase three-letter code."
    )

    %{"kind" => "currency", "value" => code}
  end

  defp unit({:custom, value}) when is_binary(value) and byte_size(value) in 1..128,
    do: %{"kind" => "custom", "value" => value}

  defp unit(_), do: fail(:unit_required, "A metric requires a supported explicit unit.")

  defp format(nil, _unit), do: nil

  defp format(value, unit) when is_map(value) or is_list(value) do
    value = Map.new(value)

    require!(
      Enum.all?(Map.keys(value), &(&1 in [:decimals, :style])),
      :invalid_format,
      "Unknown display format option."
    )

    decimals = Map.get(value, :decimals)
    style = Map.get(value, :style, :number)

    require!(
      decimals == nil or (is_integer(decimals) and decimals in 0..12),
      :invalid_format,
      "Format decimals must be between zero and 12."
    )

    require!(
      style in [:number, :percent, :currency],
      :invalid_format,
      "Unsupported format style."
    )

    require!(
      style != :currency or unit["kind"] == "currency",
      :invalid_format,
      "Currency format requires a currency unit."
    )

    require!(
      style != :percent or unit["kind"] in ["ratio", "percent"],
      :invalid_format,
      "Percent format requires a ratio or percent unit."
    )

    %{"style" => to_string(style), "decimals" => decimals}
  end

  defp format(_, _),
    do: fail(:invalid_format, "Format must be a declarative map or keyword list.")

  defp name!(value) when is_atom(value) or is_binary(value) do
    name = to_string(value)

    require!(
      byte_size(name) in 1..64 and Regex.match?(~r/\A[a-z][a-z0-9_]*\z/, name),
      :invalid_name,
      "Names must be lowercase ASCII identifiers of at most 64 bytes."
    )

    name
  end

  defp name!(_), do: fail(:invalid_name, "Names must be lowercase ASCII identifiers.")

  defp shift_locations(locations, offset),
    do: Map.new(locations, fn {name, values} -> {name, Enum.map(values, &(&1 + offset))} end)

  defp quote_identifier(value), do: "\"" <> String.replace(value, "\"", "\"\"") <> "\""
  defp require!(true, _, _), do: :ok
  defp require!(false, code, message), do: fail(code, message)
  defp fail(code, message), do: throw({:semantic_error, code, message})

  defp safely(fun, source) do
    {:ok, fun.()}
  rescue
    _error in [ArgumentError, KeyError, FunctionClauseError, CompileError] ->
      {:error,
       Diagnostic.new(
         :invalid_declaration,
         "Invalid semantic declaration or SQL template.",
         if(is_map(source), do: source, else: %{})
       )}
  catch
    {:semantic_error, code, message} ->
      {:error, Diagnostic.new(code, message, if(is_map(source), do: source, else: %{}))}

    {:diagnostic, diagnostic} ->
      {:error, diagnostic}
  end

  defp bound_diagnostics(errors) when length(errors) <= 100, do: errors

  defp bound_diagnostics(errors),
    do:
      Enum.take(errors, 99) ++
        [
          Diagnostic.new(
            :diagnostics_omitted,
            "#{length(errors) - 99} additional diagnostics omitted."
          )
        ]
end
