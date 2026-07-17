defmodule Favn.SQLAsset.Renderer do
  @moduledoc false

  alias Favn.Assets.Compiler
  alias Favn.RelationRef
  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.{ParamBinding, Params, Render, Template}
  alias Favn.SQL.Contract
  alias Favn.SQL.Template.{AssetRef, Call, Placeholder, Relation, RuntimeRelation, Text}
  alias Favn.SQL.Check
  alias Favn.SQLAsset.{Definition, Error}
  alias Favn.Window.Runtime

  defmodule Fragment do
    @moduledoc false
    defstruct sql: "", bindings: [], resolved_asset_refs: []
  end

  @type opts :: [params: map(), runtime: map()]

  @spec render(Definition.t(), opts()) :: {:ok, Render.t()} | {:error, Error.t()}
  def render(%Definition{} = definition, opts \\ []) when is_list(opts) do
    with {:ok, params} <- normalize_params(opts),
         :ok <- validate_reserved_runtime_names(definition, params),
         {:ok, query_values, setting_names} <- normalize_query_values(definition, params),
         {:ok, runtime_inputs} <- normalize_runtime_inputs(definition, opts),
         {:ok, definition_catalog} <- definition_catalog(definition),
         env <-
           base_env(
             definition,
             query_values,
             setting_names,
             runtime_inputs,
             definition_catalog,
             opts
           ),
         :ok <- validate_target_relation(definition, env),
         {:ok, %Fragment{} = fragment, _env} <- render_nodes(definition.template.nodes, env),
         {:ok, %Params{} = normalized_params} <- normalize_bindings(fragment.bindings) do
      {:ok,
       %Render{
         asset_ref: definition.asset.ref,
         connection: definition.asset.relation.connection,
         relation: definition.asset.relation,
         materialization: definition.materialization,
         sql: fragment.sql,
         params: normalized_params,
         runtime: runtime_inputs.runtime,
         resolved_asset_refs: fragment.resolved_asset_refs,
         metadata: %{
           source_sql: definition.sql,
           template_root_kind: definition.template.root_kind,
           query_param_names:
             Template.query_params(definition.template) |> MapSet.to_list() |> Enum.sort(),
           runtime_input_names:
             Template.runtime_inputs(definition.template) |> MapSet.to_list() |> Enum.sort()
         }
       }}
    end
  end

  @doc false
  @spec render_check(Definition.t(), Check.t(), opts()) ::
          {:ok, Render.t()} | {:error, Error.t()}
  def render_check(%Definition{} = definition, %Check{} = check, opts) when is_list(opts) do
    raw_asset =
      (definition.raw_asset || %{})
      |> Map.put(:sql_file, check.file || definition.asset.file)

    render(
      %Definition{definition | sql: check.sql, template: check.template, raw_asset: raw_asset},
      opts
    )
  end

  @doc false
  @spec validate_contract_params(Definition.t(), opts()) :: :ok | {:error, Error.t()}
  def validate_contract_params(%Definition{} = definition, opts) when is_list(opts) do
    requirements = Contract.runtime_param_requirements(definition.contract)

    with {:ok, params} <- normalize_params(opts),
         :ok <- validate_reserved_runtime_names(definition, params) do
      Enum.reduce_while(requirements, :ok, fn {name, type}, :ok ->
        case contract_param_value(definition, params, name) do
          {:ok, value} ->
            if contract_param_type?(value, type) do
              {:cont, :ok}
            else
              {:halt, {:error, invalid_contract_param_error(definition, name, type)}}
            end

          {:error, %Error{} = error} ->
            {:halt, {:error, error}}
        end
      end)
    end
  end

  defp normalize_params(opts) do
    case Keyword.get(opts, :params, %{}) do
      params when is_map(params) ->
        {:ok, params}

      other ->
        {:error,
         %Error{
           type: :binding_failure,
           phase: :render,
           message: "render params must be a map",
           details: %{params: other}
         }}
    end
  end

  defp validate_reserved_runtime_names(%Definition{} = definition, params) do
    settings = definition.asset.settings || %{}

    Enum.reduce_while([settings: settings, params: params], :ok, fn {source, values}, :ok ->
      case Enum.find(
             Template.reserved_runtime_inputs(),
             &match?({:ok, _}, fetch_param(values, &1))
           ) do
        nil ->
          {:cont, :ok}

        name ->
          {:halt,
           {:error,
            %Error{
              type: :binding_failure,
              phase: :render,
              asset_ref: definition.asset.ref,
              message:
                "SQL input @#{name} is reserved for Favn runtime values and cannot be supplied through #{source}",
              details: %{name: name, source: source}
            }}}
      end
    end)
  end

  defp normalize_query_values(%Definition{} = definition, params) do
    settings = definition.asset.settings || %{}
    referenced = Template.query_params(definition.template)

    Enum.reduce_while(referenced, {:ok, params, MapSet.new()}, fn name,
                                                                  {:ok, values, setting_names} ->
      setting = fetch_setting(settings, name)
      param = fetch_param(params, name)

      case {setting, param} do
        {{:ok, _setting}, {:ok, _param}} ->
          {:halt,
           {:error,
            %Error{
              type: :binding_failure,
              phase: :render,
              asset_ref: definition.asset.ref,
              message: "SQL input @#{name} is declared in both asset settings and runtime params",
              details: %{name: name, sources: [:settings, :params]}
            }}}

        {{:ok, value}, :error}
        when is_nil(value) or is_boolean(value) or is_number(value) or is_binary(value) ->
          {:cont, {:ok, Map.put(values, name, value), MapSet.put(setting_names, name)}}

        {{:ok, value}, :error} ->
          {:halt,
           {:error,
            %Error{
              type: :binding_failure,
              phase: :render,
              asset_ref: definition.asset.ref,
              message: "SQL setting @#{name} must be a scalar bind value",
              details: %{name: name, value: value}
            }}}

        {:error, _param} ->
          {:cont, {:ok, values, setting_names}}
      end
    end)
  end

  defp fetch_setting(settings, name) when is_map(settings) and is_binary(name) do
    Enum.find_value(settings, :error, fn
      {key, value} when is_atom(key) -> if Atom.to_string(key) == name, do: {:ok, value}
      _entry -> nil
    end)
  end

  defp normalize_runtime_inputs(%Definition{} = definition, opts) do
    runtime = Keyword.get(opts, :runtime, %{})

    case runtime do
      runtime when is_map(runtime) ->
        with {:ok, favn_values} <- normalize_favn_runtime_values(definition, runtime) do
          case runtime_value(runtime, :window) do
            %Runtime{} = window ->
              {:ok,
               %{
                 runtime: window,
                 runtime_values:
                   Map.merge(favn_values, %{
                     window_start: window.start_at,
                     window_end: window.end_at
                   })
               }}

            nil ->
              runtime_values =
                Map.merge(favn_values, %{
                  window_start: runtime_value(runtime, :window_start),
                  window_end: runtime_value(runtime, :window_end)
                })

              {:ok, %{runtime: runtime, runtime_values: runtime_values}}

            other ->
              {:error,
               %Error{
                 type: :binding_failure,
                 phase: :render,
                 asset_ref: definition.asset.ref,
                 message: "runtime.window must be a Favn.Window.Runtime struct",
                 details: %{window: other}
               }}
          end
        end

      _other ->
        {:error,
         %Error{
           type: :binding_failure,
           phase: :render,
           asset_ref: definition.asset.ref,
           message: "render runtime must be a map",
           details: %{runtime: runtime}
         }}
    end
  end

  defp normalize_favn_runtime_values(definition, runtime) do
    run_id = runtime_value(runtime, :favn_run_id)
    run_started_at = runtime_value(runtime, :favn_run_started_at)

    cond do
      run_id != nil and (not is_binary(run_id) or run_id == "") ->
        {:error, invalid_runtime_value_error(definition, :favn_run_id, "a non-empty string")}

      run_started_at != nil and not match?(%DateTime{}, run_started_at) ->
        {:error,
         invalid_runtime_value_error(definition, :favn_run_started_at, "a DateTime struct")}

      true ->
        {:ok, %{favn_run_id: run_id, favn_run_started_at: run_started_at}}
    end
  end

  defp invalid_runtime_value_error(definition, name, expected) do
    %Error{
      type: :binding_failure,
      phase: :render,
      asset_ref: definition.asset.ref,
      message: "runtime SQL input :#{name} must be #{expected}",
      details: %{name: name, expected: expected}
    }
  end

  defp runtime_value(runtime, name) do
    case Map.fetch(runtime, name) do
      {:ok, value} -> value
      :error -> Map.get(runtime, Atom.to_string(name))
    end
  end

  defp contract_param_value(definition, params, name) do
    name_string = Atom.to_string(name)
    setting = fetch_setting(definition.asset.settings || %{}, name_string)
    param = fetch_param(params, name_string)

    case {setting, param} do
      {{:ok, _setting}, {:ok, _param}} ->
        {:error,
         %Error{
           type: :binding_failure,
           phase: :render,
           asset_ref: definition.asset.ref,
           message: "SQL input @#{name} is declared in both asset settings and runtime params",
           details: %{name: name, sources: [:settings, :params]}
         }}

      {{:ok, value}, :error} ->
        {:ok, value}

      {:error, {:ok, value}} ->
        {:ok, value}

      {:error, :error} ->
        {:error,
         %Error{
           type: :missing_query_param,
           phase: :render,
           asset_ref: definition.asset.ref,
           message: "missing SQL contract parameter @#{name}",
           details: %{name: name}
         }}
    end
  end

  defp contract_param_type?(value, :non_neg_integer),
    do: is_integer(value) and value >= 0

  defp invalid_contract_param_error(definition, name, type) do
    %Error{
      type: :binding_failure,
      phase: :render,
      asset_ref: definition.asset.ref,
      message: "SQL contract parameter @#{name} must be a non-negative integer",
      details: %{name: name, expected: type}
    }
  end

  defp definition_catalog(%Definition{} = definition) do
    catalog =
      definition.sql_definitions
      |> Enum.map(fn %SQLDefinition{} = entry -> {SQLDefinition.key(entry), entry} end)
      |> Map.new()

    {:ok, catalog}
  end

  defp base_env(definition, params, setting_names, runtime_inputs, definition_catalog, opts) do
    %{
      asset_ref: definition.asset.ref,
      root_connection: definition.asset.relation.connection,
      root_catalog: definition.asset.relation.catalog,
      root_schema: definition.asset.relation.schema,
      current_catalog: definition.asset.relation.catalog,
      current_schema: definition.asset.relation.schema,
      params: params,
      setting_names: setting_names,
      runtime_values: runtime_inputs.runtime_values,
      local_args: %{},
      definition_catalog: definition_catalog,
      stack: [],
      cache: %{},
      current_file: Map.get(definition.raw_asset || %{}, :sql_file, definition.asset.file),
      runtime_relations: Keyword.get(opts, :runtime_relations, %{}),
      manifest_relation_by_module:
        Map.get(definition.raw_asset || %{}, :manifest_relation_by_module, %{}),
      deferred_resolution:
        Map.get(definition.raw_asset || %{}, :deferred_resolution, :manifest_or_compiled)
    }
  end

  defp validate_target_relation(
         %Definition{asset: %{relation: %RelationRef{catalog: catalog, schema: nil, name: name}}},
         env
       )
       when is_binary(catalog) do
    {:error,
     %Error{
       type: :invalid_relation,
       phase: :render,
       asset_ref: env.asset_ref,
       file: env.current_file,
       message:
         "SQL asset target relations with catalog require schema; target resolved to catalog #{inspect(catalog)} and name #{inspect(name)} without schema",
       details: %{catalog: catalog, name: name}
     }}
  end

  defp validate_target_relation(%Definition{}, _env), do: :ok

  defp render_nodes(nodes, env) when is_list(nodes) do
    Enum.reduce_while(nodes, {:ok, %Fragment{}, env}, fn node, {:ok, acc, env_acc} ->
      case render_node(node, env_acc) do
        {:ok, %Fragment{} = fragment, next_env} ->
          {:cont, {:ok, merge_fragment(acc, fragment), next_env}}

        {:error, %Error{} = error} ->
          {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, fragment, next_env} -> {:ok, fragment, next_env}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp render_node(%Text{sql: sql}, env), do: {:ok, %Fragment{sql: sql}, env}

  defp render_node(%RuntimeRelation{kind: kind, span: span}, env) do
    case Map.fetch(env.runtime_relations, kind) do
      {:ok, sql} when is_binary(sql) and sql != "" ->
        {:ok, %Fragment{sql: sql}, env}

      _other ->
        {:error,
         %Error{
           type: :unresolved_runtime_relation,
           phase: :render,
           asset_ref: env.asset_ref,
           span: span,
           line: span.start_line,
           file: env.current_file,
           message: "SQL runtime relation #{kind}() is unavailable in this context",
           details: %{relation: kind}
         }}
    end
  end

  defp render_node(%Relation{} = relation, env) do
    with {:ok, relation_sql} <- plain_relation_to_sql(relation, env) do
      {:ok, %Fragment{sql: relation_sql}, env}
    end
  end

  defp render_node(%Placeholder{source: :runtime, name: name, span: span}, env) do
    case Map.fetch(env.runtime_values, name) do
      {:ok, nil} -> missing_runtime_input_error(name, span, env)
      {:ok, value} -> {:ok, value_fragment(name, :runtime, value, span), env}
      :error -> missing_runtime_input_error(name, span, env)
    end
  end

  defp render_node(%Placeholder{source: :query_param, name: name, span: span}, env) do
    case fetch_param(env.params, name) do
      {:ok, value} ->
        source = if MapSet.member?(env.setting_names, name), do: :setting, else: :query_param
        {:ok, value_fragment(name, source, value, span), env}

      :error ->
        missing_query_param_error(name, span, env)
    end
  end

  defp render_node(%Placeholder{source: {:local_arg, index}, span: span}, env) do
    case Map.fetch(env.local_args, index) do
      {:ok, %Fragment{} = fragment} ->
        {:ok, fragment, env}

      :error ->
        {:error,
         %Error{
           type: :defsql_expansion_failed,
           phase: :render,
           asset_ref: env.asset_ref,
           span: span,
           line: span.start_line,
           file: env.current_file,
           message: "failed to resolve local defsql argument @#{index}",
           stack: env.stack
         }}
    end
  end

  defp render_node(%Call{} = call, env) do
    with {:ok, definition} <- fetch_definition(call, env),
         {:ok, arg_fragments, next_env} <- render_call_args(call, env),
         callee_env <-
           env
           |> Map.put(:local_args, local_arg_map(arg_fragments))
           |> Map.put(:stack, [stack_frame(call, definition) | env.stack])
           |> Map.put(:cache, next_env.cache)
           |> Map.put(:current_file, definition.file)
           |> put_definition_relation_defaults(definition),
         {:ok, %Fragment{} = expanded, callee_env_after} <-
           render_nodes(definition.template.nodes, callee_env) do
      {:ok, expanded, %{env | cache: callee_env_after.cache}}
    else
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error ->
      {:error,
       %Error{
         type: :defsql_expansion_failed,
         phase: :render,
         asset_ref: env.asset_ref,
         message:
           "failed to expand SQL definition #{call.definition.name}/#{call.definition.arity}",
         span: call.span,
         line: call.span.start_line,
         file: env.current_file,
         stack: env.stack,
         cause: error
       }}
  end

  defp render_node(%AssetRef{} = asset_ref, env) do
    with {:ok, relation_ref, next_env} <- resolve_asset_ref(asset_ref, env),
         {:ok, relation_sql} <- relation_to_sql(relation_ref, asset_ref, env) do
      fragment = %Fragment{
        sql: relation_sql,
        resolved_asset_refs: [resolved_ref(asset_ref, relation_ref)]
      }

      {:ok, fragment, next_env}
    end
  end

  defp fetch_param(params, name) when is_binary(name) do
    case Map.fetch(params, name) do
      {:ok, value} -> {:ok, value}
      :error -> fetch_atom_named_param(params, name)
    end
  end

  defp fetch_param(params, name) when is_atom(name) do
    case Map.fetch(params, name) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(params, Atom.to_string(name))
    end
  end

  defp fetch_atom_named_param(params, name) do
    Enum.find_value(params, :error, fn
      {key, value} when is_atom(key) ->
        if Atom.to_string(key) == name, do: {:ok, value}, else: false

      _other ->
        false
    end)
  end

  defp fetch_definition(%Call{definition: %{name: name, arity: arity}, span: span}, env) do
    case Map.fetch(env.definition_catalog, {name, arity}) do
      {:ok, %SQLDefinition{} = definition} ->
        {:ok, definition}

      :error ->
        {:error,
         %Error{
           type: :defsql_expansion_failed,
           phase: :render,
           asset_ref: env.asset_ref,
           span: span,
           line: span.start_line,
           file: env.current_file,
           message: "failed to find SQL definition #{name}/#{arity} during render",
           stack: env.stack,
           details: %{name: name, arity: arity}
         }}
    end
  end

  defp render_call_args(call, env) do
    Enum.reduce_while(call.args, {:ok, [], env}, fn fragment, {:ok, acc, env_acc} ->
      case render_nodes(fragment.nodes, env_acc) do
        {:ok, rendered_fragment, env_after_arg} ->
          {:cont, {:ok, [rendered_fragment | acc], env_after_arg}}

        {:error, %Error{} = error} ->
          {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, reversed, env_after} -> {:ok, Enum.reverse(reversed), env_after}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp local_arg_map(arg_fragments) do
    arg_fragments
    |> Enum.with_index()
    |> Map.new(fn {fragment, index} -> {index, fragment} end)
  end

  defp stack_frame(call, %SQLDefinition{} = definition) do
    %{
      definition: {definition.module, definition.name, definition.arity},
      call_span: call.span,
      file: definition.file,
      line: definition.line
    }
  end

  defp put_definition_relation_defaults(env, %SQLDefinition{relation_defaults: defaults})
       when is_map(defaults) do
    env
    |> Map.put(
      :current_catalog,
      Map.get(defaults, :catalog) || Map.get(defaults, "catalog") || env.root_catalog
    )
    |> Map.put(
      :current_schema,
      Map.get(defaults, :schema) || Map.get(defaults, "schema") || env.root_schema
    )
  end

  defp put_definition_relation_defaults(env, %SQLDefinition{}), do: env

  defp resolve_asset_ref(
         %AssetRef{resolution: :resolved, relation: %RelationRef{} = relation_ref} =
           asset_ref,
         env
       ) do
    with :ok <- ensure_same_connection(asset_ref, relation_ref, env) do
      {:ok, relation_ref, env}
    end
  end

  defp resolve_asset_ref(%AssetRef{resolution: :deferred, module: module} = asset_ref, env) do
    case Map.fetch(env.cache, module) do
      {:ok, %RelationRef{} = relation_ref} ->
        with :ok <- ensure_same_connection(asset_ref, relation_ref, env) do
          {:ok, relation_ref, env}
        end

      :error ->
        with {:ok, relation_ref} <-
               resolve_deferred_module(
                 module,
                 env.asset_ref,
                 asset_ref.span,
                 env.stack,
                 env.current_file,
                 env.manifest_relation_by_module,
                 env.deferred_resolution
               ),
             :ok <- ensure_same_connection(asset_ref, relation_ref, env) do
          {:ok, relation_ref, %{env | cache: Map.put(env.cache, module, relation_ref)}}
        end
    end
  end

  defp resolve_deferred_module(
         module,
         asset_ref,
         span,
         stack,
         current_file,
         relation_map,
         deferred_resolution
       ) do
    case manifest_relation(module, relation_map) do
      {:ok, relation_ref} ->
        {:ok, relation_ref}

      :error ->
        resolve_missing_deferred_module(
          module,
          asset_ref,
          span,
          stack,
          current_file,
          deferred_resolution
        )
    end
  end

  defp resolve_missing_deferred_module(
         module,
         asset_ref,
         span,
         stack,
         current_file,
         :manifest_only
       ) do
    {:error,
     %Error{
       type: :unresolved_asset_ref,
       phase: :render,
       asset_ref: asset_ref,
       span: span,
       line: span.start_line,
       file: current_file,
       message:
         "SQL asset reference #{inspect(module)} is not present in the pinned manifest relation map",
       stack: stack,
       details: %{module: module}
     }}
  end

  defp resolve_missing_deferred_module(
         module,
         asset_ref,
         span,
         stack,
         current_file,
         _deferred_resolution
       ) do
    resolve_compiled_module(module, asset_ref, span, stack, current_file)
  end

  defp manifest_relation(module, relation_map) when is_map(relation_map) do
    case Map.fetch(relation_map, module) do
      {:ok, %RelationRef{} = relation_ref} -> {:ok, relation_ref}
      _ -> :error
    end
  end

  defp manifest_relation(_module, _relation_map), do: :error

  defp resolve_compiled_module(module, asset_ref, span, stack, current_file) do
    case Code.ensure_compiled(module) do
      {:module, _compiled} ->
        case Compiler.compile_module_assets(module) do
          {:ok, [%{relation: %RelationRef{} = relation_ref}]} ->
            {:ok, relation_ref}

          {:ok, [%{relation: nil}]} ->
            {:error,
             %Error{
               type: :invalid_relation,
               phase: :render,
               asset_ref: asset_ref,
               span: span,
               line: span.start_line,
               file: current_file,
               message:
                 "SQL asset reference #{inspect(module)} resolved, but it does not have a relation",
               stack: stack,
               details: %{module: module}
             }}

          {:ok, _many} ->
            {:error,
             %Error{
               type: :invalid_relation,
               phase: :render,
               asset_ref: asset_ref,
               span: span,
               line: span.start_line,
               file: current_file,
               message:
                 "invalid SQL asset reference #{inspect(module)}; expected a compiled single-asset module",
               stack: stack,
               details: %{module: module}
             }}

          {:error, _reason} ->
            {:error,
             %Error{
               type: :unresolved_asset_ref,
               phase: :render,
               asset_ref: asset_ref,
               span: span,
               line: span.start_line,
               file: current_file,
               message:
                 "SQL asset reference #{inspect(module)} could not be resolved at render time",
               stack: stack,
               details: %{module: module}
             }}
        end

      {:error, _reason} ->
        {:error,
         %Error{
           type: :unresolved_asset_ref,
           phase: :render,
           asset_ref: asset_ref,
           span: span,
           line: span.start_line,
           file: current_file,
           message: "SQL asset reference #{inspect(module)} could not be resolved at render time",
           stack: stack,
           details: %{module: module}
         }}
    end
  end

  defp ensure_same_connection(
         %AssetRef{module: _module, span: _span},
         %RelationRef{connection: connection},
         env
       )
       when connection == env.root_connection,
       do: :ok

  defp ensure_same_connection(
         %AssetRef{module: module, span: span},
         %RelationRef{} = relation_ref,
         env
       ) do
    {:error,
     %Error{
       type: :cross_connection_asset_ref,
       phase: :render,
       asset_ref: env.asset_ref,
       span: span,
       line: span.start_line,
       file: env.current_file,
       message:
         "SQL asset reference #{inspect(module)} resolves to connection #{inspect(relation_ref.connection)}, expected #{inspect(env.root_connection)}",
       stack: env.stack,
       details: %{
         module: module,
         expected_connection: env.root_connection,
         actual_connection: relation_ref.connection
       }
     }}
  end

  defp normalize_bindings(bindings) do
    numbered =
      bindings
      |> Enum.with_index(1)
      |> Enum.map(fn {%ParamBinding{} = binding, ordinal} ->
        %ParamBinding{binding | ordinal: ordinal}
      end)

    {:ok, %Params{format: :positional, bindings: numbered}}
  end

  defp merge_fragment(%Fragment{} = left, %Fragment{} = right) do
    %Fragment{
      sql: left.sql <> right.sql,
      bindings: left.bindings ++ right.bindings,
      resolved_asset_refs: left.resolved_asset_refs ++ right.resolved_asset_refs
    }
  end

  defp value_fragment(name, source, value, span) do
    %Fragment{
      sql: "?",
      bindings: [%ParamBinding{ordinal: 0, name: name, source: source, value: value, span: span}]
    }
  end

  defp plain_relation_to_sql(%Relation{segments: [name]}, env) do
    %RelationRef{catalog: env.current_catalog, schema: env.current_schema, name: name}
    |> relation_ref_to_sql(env)
  end

  defp plain_relation_to_sql(%Relation{segments: [schema, name]}, env) do
    %RelationRef{catalog: env.current_catalog, schema: schema, name: name}
    |> relation_ref_to_sql(env)
  end

  defp plain_relation_to_sql(%Relation{segments: [catalog, schema, name]}, env) do
    %RelationRef{catalog: catalog, schema: schema, name: name}
    |> relation_ref_to_sql(env)
  end

  defp relation_ref_to_sql(%RelationRef{catalog: nil, schema: nil, name: name}, _env),
    do: {:ok, name}

  defp relation_ref_to_sql(%RelationRef{catalog: nil, schema: schema, name: name}, _env),
    do: {:ok, Enum.join([schema, name], ".")}

  defp relation_ref_to_sql(%RelationRef{catalog: catalog, schema: nil, name: name}, env) do
    {:error,
     %Error{
       type: :invalid_relation,
       phase: :render,
       asset_ref: env.asset_ref,
       file: env.current_file,
       message:
         "catalog-qualified SQL references require schema; relation resolved to catalog #{inspect(catalog)} and name #{inspect(name)} without schema",
       stack: env.stack,
       details: %{catalog: catalog, name: name}
     }}
  end

  defp relation_ref_to_sql(%RelationRef{catalog: catalog, schema: schema, name: name}, _env),
    do: {:ok, Enum.join([catalog, schema, name], ".")}

  defp relation_to_sql(%RelationRef{catalog: nil, schema: nil, name: name}, _asset_ref, _env),
    do: {:ok, name}

  defp relation_to_sql(%RelationRef{catalog: nil, schema: schema, name: name}, _asset_ref, _env),
    do: {:ok, Enum.join([schema, name], ".")}

  defp relation_to_sql(
         %RelationRef{catalog: catalog, schema: nil, name: name} = relation_ref,
         %AssetRef{module: module, span: span},
         env
       ) do
    {:error,
     %Error{
       type: :invalid_relation,
       phase: :render,
       asset_ref: env.asset_ref,
       span: span,
       line: span.start_line,
       file: env.current_file,
       message:
         "catalog-qualified SQL references require schema; #{inspect(module)} resolved to catalog #{inspect(catalog)} and name #{inspect(name)} without schema",
       stack: env.stack,
       details: %{module: module, relation: relation_ref}
     }}
  end

  defp relation_to_sql(
         %RelationRef{catalog: catalog, schema: schema, name: name},
         _asset_ref,
         _env
       ),
       do: {:ok, Enum.join([catalog, schema, name], ".")}

  defp resolved_ref(%AssetRef{} = asset_ref, %RelationRef{} = relation_ref) do
    %{
      module: asset_ref.module,
      relation: relation_ref,
      span: asset_ref.span,
      resolution: :resolved
    }
  end

  defp missing_runtime_input_error(name, span, env) do
    {:error,
     %Error{
       type: :missing_runtime_input,
       phase: :render,
       asset_ref: env.asset_ref,
       span: span,
       line: span.start_line,
       file: env.current_file,
       message: "missing runtime SQL input :#{name} for #{inspect(env.asset_ref)}",
       stack: env.stack,
       details: %{name: name}
     }}
  end

  defp missing_query_param_error(name, span, env) do
    {:error,
     %Error{
       type: :missing_query_param,
       phase: :render,
       asset_ref: env.asset_ref,
       span: span,
       line: span.start_line,
       file: env.current_file,
       message: "missing SQL param :#{name} for #{inspect(env.asset_ref)}",
       stack: env.stack,
       details: %{name: name}
     }}
  end
end
