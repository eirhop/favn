defmodule Favn.SQLAsset.Renderer do
  @moduledoc false

  alias Favn.Assets.Compiler
  alias Favn.RelationRef
  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.{ParamBinding, Params, Render, Template}
  alias Favn.SQL.Template.{AssetRef, Call, Placeholder, Relation, Text}
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
         {:ok, runtime_inputs} <- normalize_runtime_inputs(definition, opts),
         {:ok, definition_catalog} <- definition_catalog(definition),
         env <- base_env(definition, params, runtime_inputs, definition_catalog),
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

  defp normalize_runtime_inputs(%Definition{} = definition, opts) do
    runtime = Keyword.get(opts, :runtime, %{})

    case runtime do
      runtime when is_map(runtime) ->
        case Map.get(runtime, :window) || Map.get(runtime, "window") do
          %Runtime{} = window ->
            {:ok,
             %{
               runtime: window,
               runtime_values: %{window_start: window.start_at, window_end: window.end_at}
             }}

          nil ->
            runtime_values = %{
              window_start: runtime[:window_start] || runtime["window_start"],
              window_end: runtime[:window_end] || runtime["window_end"]
            }

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

  defp definition_catalog(%Definition{} = definition) do
    catalog =
      definition.sql_definitions
      |> Enum.map(fn %SQLDefinition{} = entry -> {SQLDefinition.key(entry), entry} end)
      |> Map.new()

    {:ok, catalog}
  end

  defp base_env(definition, params, runtime_inputs, definition_catalog) do
    %{
      asset_ref: definition.asset.ref,
      root_connection: definition.asset.relation.connection,
      params: params,
      runtime_values: runtime_inputs.runtime_values,
      local_args: %{},
      definition_catalog: definition_catalog,
      stack: [],
      cache: %{},
      current_file: Map.get(definition.raw_asset || %{}, :sql_file, definition.asset.file),
      manifest_relation_by_module:
        Map.get(definition.raw_asset || %{}, :manifest_relation_by_module, %{})
    }
  end

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

  defp render_node(%Relation{raw: raw}, env), do: {:ok, %Fragment{sql: raw}, env}

  defp render_node(%Placeholder{source: :runtime, name: name, span: span}, env) do
    case Map.fetch(env.runtime_values, name) do
      {:ok, nil} -> missing_runtime_input_error(name, span, env)
      {:ok, value} -> {:ok, value_fragment(name, :runtime, value, span), env}
      :error -> missing_runtime_input_error(name, span, env)
    end
  end

  defp render_node(%Placeholder{source: :query_param, name: name, span: span}, env) do
    if Map.has_key?(env.params, name) do
      {:ok, value_fragment(name, :query_param, Map.fetch!(env.params, name), span), env}
    else
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
           |> Map.put(:current_file, definition.file),
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
    with {:ok, relation_ref, next_env} <- resolve_asset_ref(asset_ref, env) do
      fragment = %Fragment{
        sql: relation_to_sql(relation_ref),
        resolved_asset_refs: [resolved_ref(asset_ref, relation_ref)]
      }

      {:ok, fragment, next_env}
    end
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
                 env.manifest_relation_by_module
               ),
             :ok <- ensure_same_connection(asset_ref, relation_ref, env) do
          {:ok, relation_ref, %{env | cache: Map.put(env.cache, module, relation_ref)}}
        end
    end
  end

  defp resolve_deferred_module(module, asset_ref, span, stack, current_file, relation_map) do
    case manifest_relation(module, relation_map) do
      {:ok, relation_ref} ->
        {:ok, relation_ref}

      :error ->
        resolve_compiled_module(module, asset_ref, span, stack, current_file)
    end
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

  defp relation_to_sql(%RelationRef{} = relation_ref) do
    [relation_ref.catalog, relation_ref.schema, relation_ref.name]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(".")
  end

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
