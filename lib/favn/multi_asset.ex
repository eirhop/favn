defmodule Favn.MultiAsset do
  @moduledoc """
  Advanced Elixir DSL for compiling one module into many generated assets.

  Use this module when many assets share the same runtime implementation but
  differ by declarative config. Each declared `asset :name do ... end` compiles
  to its own canonical `%Favn.Asset{}` while the module keeps one shared public
  `asset/1` runtime function.

  ## When to use it

  Use `Favn.MultiAsset` when:

  - you are generating many similar extraction assets
  - the runtime implementation is shared
  - per-asset differences are mostly config, metadata, relation ownership, or dependencies

  ## Minimal example

      defmodule MyApp.Raw.Shopify do
        use Favn.MultiAsset

        defaults do
          meta owner: "data-platform", category: :shopify, tags: [:raw]

          rest do
            primary_key "id"
            paginator :cursor, cursor_path: "links.next"
          end
        end

        @doc "Extract orders"
        @relation true
        asset :orders do
          rest do
            path "/orders.json"
            data_path "orders"
          end
        end

        def asset(ctx) do
          MyApp.Shopify.Client.extract(ctx.asset.config, ctx)
        end
      end

  ## Authoring contract

  - define exactly one public `asset/1` runtime function
  - define at least one `asset :name do ... end` declaration
  - use at most one `defaults do ... end` block
  - attach `@doc`, `@meta`, `@depends`, `@window`, and `@relation` directly above each declared asset

  ## Supported attributes and blocks

  Per generated asset you can use:

  - `@doc`
  - `@meta`
  - `@depends`
  - `@window`
  - `@relation`
  - `asset :name do ... end`

  `defaults do ... end` currently supports:

  - `meta ...`
  - `window Favn.Window.*(...)`
  - `rest do ... end`

  `asset :name do ... end` currently supports:

  - `rest do ... end`

  `rest` currently supports these entries:

  - `path "/path"`: request path or endpoint path
  - `data_path "items"`: field path containing extracted records
  - `params %{...}` or keyword list: static request params
  - `primary_key "id"`: identifier field for downstream extraction logic
  - `paginator kind, opts`: paginator config map with added `:kind`
  - `incremental opts`: incremental extraction config, defaults `kind: :cursor`
  - `method :get` or `"GET"`: request method
  - `extra %{...}` or keyword list: adapter-specific extra config

  `@depends` supports:

  - `:same_module_asset_name`
  - `{OtherModule, :asset_name}`

  ## What gets compiled

  Each declaration becomes one canonical `%Favn.Asset{}` with:

  - `ref: {Module, :name}`
  - `entrypoint: :asset`
  - merged defaults plus per-asset config in `ctx.asset.config`

  ## Runtime context notes

  The shared runtime usually reads `ctx.asset.config` to decide what to extract.
  Relation ownership, metadata, and window specs remain per generated asset.

  ## Common mistakes

  - forgetting the shared `asset/1` runtime function
  - declaring multiple `defaults` blocks
  - using duplicate asset names
  - expecting asset blocks to support arbitrary clauses beyond the current DSL

  ## See also

  - `Favn.AgentGuide`
  - `Favn.Asset`
  - `Favn.Namespace`
  """

  alias Favn.Asset
  alias Favn.Asset.RelationResolver
  alias Favn.Namespace
  alias Favn.Ref
  alias Favn.Window.Spec

  @doc false
  defmacro __using__(_opts) do
    quote do
      Module.register_attribute(__MODULE__, :depends, accumulate: true)
      Module.register_attribute(__MODULE__, :meta, persist: false)
      Module.register_attribute(__MODULE__, :relation, accumulate: true)
      Module.register_attribute(__MODULE__, :window, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_multi_asset_defaults_raw, persist: true)

      Module.register_attribute(__MODULE__, :favn_multi_assets_raw, persist: true)
      Module.register_attribute(__MODULE__, :favn_multi_asset_decls, persist: true)

      Module.register_attribute(__MODULE__, :favn_multi_asset_runtime_count, persist: false)
      Module.register_attribute(__MODULE__, :favn_multi_asset_generating, persist: false)

      @favn_multi_asset_runtime_count 0
      @favn_multi_assets_raw []
      @favn_multi_asset_decls []

      @on_definition Favn.MultiAsset
      @before_compile Favn.MultiAsset

      import Favn.MultiAsset, only: [defaults: 1, asset: 2]
    end
  end

  @doc false
  def __on_definition__(env, kind, name, args, _guards, _body) do
    arity = length(args || [])

    generated_definition? =
      Module.get_attribute(env.module, :favn_multi_asset_generating) == true and
        kind == :def and
        name in [:__favn_assets__, :__favn_assets_raw__]

    if generated_definition? do
      :ok
    else
      case {kind, name, arity} do
        {:def, :asset, 1} ->
          validate_no_stray_asset_attributes!(env, kind, name, arity)
          increment_runtime_count!(env)

        {:defp, name, 1} when name != :asset ->
          if is_generated_decl_name?(name) do
            capture_generated_asset_definition!(env, name)
          else
            validate_no_stray_asset_attributes!(env, kind, name, arity)
          end

        {:def, :asset, _other_arity} ->
          compile_error!(
            env.file,
            env.line,
            "Favn.MultiAsset requires exactly one public asset/1 function"
          )

        {:defp, :asset, _arity} ->
          compile_error!(
            env.file,
            env.line,
            "Favn.MultiAsset requires a public def asset(ctx)"
          )

        {kind, _name, _arity} when kind in [:def, :defp] ->
          validate_no_stray_asset_attributes!(env, kind, name, arity)

        _ ->
          :ok
      end
    end
  end

  @doc """
  Declares defaults shared by every generated asset in the module.

  Defaults are merged with per-asset declarations. In v0.4 this block supports
  `meta`, `window`, and `rest`.

  Supported entries:

  - `meta owner: ..., category: ..., tags: ...`
  - `window Favn.Window.daily(...)`
  - `rest do ... end`

  ## Example

      defaults do
        meta owner: "data-platform", category: :shopify, tags: [:raw]

        rest do
          primary_key "id"
          paginator :cursor, cursor_path: "links.next"
        end
      end
  """
  defmacro defaults(do: block) do
    ensure_no_pending_attributes!(__CALLER__)

    current = Module.get_attribute(__CALLER__.module, :favn_multi_asset_defaults_raw)

    if current do
      compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "multiple defaults blocks are not allowed; use at most one defaults do ... end"
      )
    end

    defaults = normalize_defaults_block!(block, __CALLER__)
    Module.put_attribute(__CALLER__.module, :favn_multi_asset_defaults_raw, defaults)

    marker_fun = defaults_marker_fun_name(__CALLER__.line)

    quote do
      defp unquote(marker_fun)(), do: :ok
      :ok
    end
  end

  @doc """
  Declares one generated asset inside a `Favn.MultiAsset` module.

  Attach standard asset attributes such as `@doc`, `@meta`, `@depends`,
  `@window`, and `@relation` immediately above the declaration.

  The asset block currently supports only `rest do ... end`.

      ## Example

      @doc "Extract orders"
      @depends {MyApp.Raw.Shopify, :customers}
      @relation true
      asset :orders do
        rest do
          path "/orders.json"
          data_path "orders"
        end
      end
  """
  defmacro asset(name, do: block) do
    if not is_atom(name) do
      compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "asset name must be an atom, got: #{Macro.to_string(name)}"
      )
    end

    asset_rest = normalize_asset_block!(block, __CALLER__, name)

    raw_decl = %{
      name: name,
      file: normalize_file(__CALLER__.file),
      line: __CALLER__.line,
      rest: asset_rest
    }

    decl_fun = decl_fun_name(name)

    quote do
      @favn_multi_asset_decls [
        {unquote(decl_fun), unquote(Macro.escape(raw_decl))} | @favn_multi_asset_decls
      ]

      defp unquote(decl_fun)(_ctx), do: :ok
      :ok
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    runtime_count = Module.get_attribute(env.module, :favn_multi_asset_runtime_count) || 0

    if runtime_count != 1 do
      compile_error!(
        env.file,
        env.line,
        "Favn.MultiAsset modules must define exactly one public asset/1 function"
      )
    end

    ensure_no_pending_attributes!(env)

    raw_assets =
      env.module
      |> Module.get_attribute(:favn_multi_assets_raw)
      |> Enum.reverse()

    if raw_assets == [] do
      compile_error!(
        env.file,
        env.line,
        "Favn.MultiAsset modules must declare at least one asset :name do ... end"
      )
    end

    _ = validate_unique_names!(raw_assets)
    assets = raw_assets |> Enum.map(&build_asset!/1) |> resolve_relations!(env.module, env)
    _ = ensure_unique_relation_owners!(assets, env)

    Module.put_attribute(env.module, :favn_multi_asset_generating, true)

    quote do
      @doc false
      @spec __favn_assets__() :: [Favn.Asset.t()]
      def __favn_assets__, do: unquote(Macro.escape(assets))

      @doc false
      def __favn_assets_raw__, do: unquote(Macro.escape(raw_assets))
    end
  end

  defp increment_runtime_count!(env) do
    count = Module.get_attribute(env.module, :favn_multi_asset_runtime_count) || 0
    Module.put_attribute(env.module, :favn_multi_asset_runtime_count, count + 1)
  end

  defp is_generated_decl_name?(name) when is_atom(name) do
    name
    |> Atom.to_string()
    |> String.starts_with?("__favn_multi_asset_decl__")
  end

  defp decl_fun_name(name) when is_atom(name),
    do: String.to_atom("__favn_multi_asset_decl__#{name}")

  defp defaults_marker_fun_name(line),
    do: String.to_atom("__favn_multi_asset_defaults_marker__#{line}")

  defp capture_generated_asset_definition!(env, decl_fun) do
    decl = fetch_decl!(env.module, decl_fun, env)

    depends = env.module |> fetch_accum_attribute(:depends) |> Enum.reverse()
    meta = Module.get_attribute(env.module, :meta)
    window = env.module |> fetch_accum_attribute(:window) |> Enum.reverse()
    relation = env.module |> fetch_accum_attribute(:relation) |> Enum.reverse()
    doc = normalize_doc(Module.get_attribute(env.module, :doc))

    validate_relation_attr!(relation, env)

    Module.delete_attribute(env.module, :depends)
    Module.delete_attribute(env.module, :meta)
    Module.delete_attribute(env.module, :window)
    Module.delete_attribute(env.module, :relation)
    clear_doc!(env.module, env.line)

    defaults =
      Module.get_attribute(env.module, :favn_multi_asset_defaults_raw) ||
        %{meta: %{}, window_spec: nil, rest: nil}

    merged_meta =
      defaults.meta
      |> Map.merge(normalize_meta!(meta, env))

    merged_window = normalize_window!(window, env) || defaults.window_spec
    merged_rest = merge_rest(defaults.rest, decl.rest)
    merged_config = if is_nil(merged_rest), do: %{}, else: %{rest: merged_rest}

    raw_asset = %{
      module: env.module,
      name: decl.name,
      entrypoint: :asset,
      arity: 1,
      doc: doc,
      file: decl.file,
      line: decl.line,
      depends: depends,
      meta: merged_meta,
      window_spec: merged_window,
      relation: relation,
      config: merged_config
    }

    raw_assets = Module.get_attribute(env.module, :favn_multi_assets_raw) || []
    Module.put_attribute(env.module, :favn_multi_assets_raw, [raw_asset | raw_assets])
  end

  defp fetch_decl!(module, decl_fun, env) do
    decls = Module.get_attribute(module, :favn_multi_asset_decls) || []

    case Enum.find(decls, fn {name, _decl} -> name == decl_fun end) do
      {_name, decl} ->
        decl

      nil ->
        compile_error!(env.file, env.line, "internal error: missing declaration for #{decl_fun}")
    end
  end

  defp validate_unique_names!(raw_assets) do
    raw_assets
    |> Enum.group_by(& &1.name)
    |> Enum.each(fn {name, assets} ->
      case assets do
        [_single] ->
          :ok

        [first | _rest] ->
          compile_error!(
            first.file,
            first.line,
            "duplicate asset name #{inspect(name)}; asset names must be unique within a module"
          )
      end
    end)

    raw_assets
  end

  defp build_asset!(raw_asset) do
    depends_on = normalize_depends!(raw_asset.depends, raw_asset)

    asset = %Asset{
      module: raw_asset.module,
      name: raw_asset.name,
      entrypoint: raw_asset.entrypoint,
      ref: Ref.new(raw_asset.module, raw_asset.name),
      arity: raw_asset.arity,
      type: :elixir,
      title: nil,
      doc: raw_asset.doc,
      file: raw_asset.file,
      line: raw_asset.line,
      meta: raw_asset.meta,
      depends_on: depends_on,
      config: raw_asset.config,
      window_spec: raw_asset.window_spec
    }

    try do
      Asset.validate!(asset)
    rescue
      error in ArgumentError ->
        compile_error!(raw_asset.file, raw_asset.line, error.message)
    end
  end

  defp normalize_defaults_block!(block, env) do
    {meta, window_spec, rest, rest_count} =
      block_expressions(block)
      |> Enum.reduce({%{}, nil, nil, 0}, fn expression, {meta, window_spec, rest, rest_count} ->
        case expression do
          {:meta, _meta, [meta_ast]} ->
            {normalize_meta!(eval_quoted!(meta_ast, env), env), window_spec, rest, rest_count}

          {:window, _meta, [window_ast]} ->
            {meta, normalize_window_value!(eval_quoted!(window_ast, env), env), rest, rest_count}

          {:rest, _meta, [[do: rest_block]]} ->
            if rest_count > 0 do
              compile_error!(
                env.file,
                env.line,
                "multiple rest blocks are not allowed inside defaults"
              )
            end

            {meta, window_spec, normalize_rest_block!(rest_block, env), rest_count + 1}

          other ->
            compile_error!(
              env.file,
              env.line,
              "defaults only supports meta, window, and rest blocks; got: #{Macro.to_string(other)}"
            )
        end
      end)

    _ = rest_count
    %{meta: meta, window_spec: window_spec, rest: rest}
  end

  defp normalize_asset_block!(block, env, name) do
    {rest, rest_count} =
      block_expressions(block)
      |> Enum.reduce({nil, 0}, fn expression, {_rest, rest_count} ->
        case expression do
          {:rest, _meta, [[do: rest_block]]} ->
            if rest_count > 0 do
              compile_error!(
                env.file,
                env.line,
                "multiple rest blocks are not allowed inside asset #{inspect(name)}"
              )
            end

            {normalize_rest_block!(rest_block, env), rest_count + 1}

          other ->
            compile_error!(
              env.file,
              env.line,
              "asset blocks only support rest do ... end in v0.4; got: #{Macro.to_string(other)}"
            )
        end
      end)

    _ = rest_count
    rest
  end

  defp normalize_rest_block!(block, env) do
    block_expressions(block)
    |> Enum.reduce(%{}, fn expression, acc ->
      case expression do
        {:path, _meta, [value_ast]} ->
          put_unique_rest_slot!(
            acc,
            :path,
            normalize_binary!(eval_quoted!(value_ast, env), :path, env),
            env
          )

        {:data_path, _meta, [value_ast]} ->
          put_unique_rest_slot!(
            acc,
            :data_path,
            normalize_binary!(eval_quoted!(value_ast, env), :data_path, env),
            env
          )

        {:params, _meta, [value_ast]} ->
          put_unique_rest_slot!(
            acc,
            :params,
            normalize_map_like!(eval_quoted!(value_ast, env), "rest.params", env),
            env
          )

        {:primary_key, _meta, [value_ast]} ->
          put_unique_rest_slot!(
            acc,
            :primary_key,
            normalize_binary!(eval_quoted!(value_ast, env), :primary_key, env),
            env
          )

        {:paginator, _meta, [kind_ast, opts_ast]} ->
          kind = eval_quoted!(kind_ast, env)

          if not is_atom(kind) do
            compile_error!(
              env.file,
              env.line,
              "rest.paginator kind must be an atom, got: #{inspect(kind)}"
            )
          end

          opts = normalize_map_like!(eval_quoted!(opts_ast, env), "rest.paginator options", env)

          put_unique_rest_slot!(acc, :paginator, Map.put(opts, :kind, kind), env)

        {:incremental, _meta, [opts_ast]} ->
          opts = normalize_map_like!(eval_quoted!(opts_ast, env), "rest.incremental options", env)
          put_unique_rest_slot!(acc, :incremental, Map.put_new(opts, :kind, :cursor), env)

        {:method, _meta, [value_ast]} ->
          value = eval_quoted!(value_ast, env)

          if not (is_atom(value) or is_binary(value)) do
            compile_error!(
              env.file,
              env.line,
              "rest.method must be an atom or string, got: #{inspect(value)}"
            )
          end

          put_unique_rest_slot!(acc, :method, value, env)

        {:extra, _meta, [value_ast]} ->
          put_unique_rest_slot!(
            acc,
            :extra,
            normalize_map_like!(eval_quoted!(value_ast, env), "rest.extra", env),
            env
          )

        other ->
          compile_error!(
            env.file,
            env.line,
            "rest only supports path, data_path, params, primary_key, paginator, incremental, method, and extra; got: #{Macro.to_string(other)}"
          )
      end
    end)
  end

  defp put_unique_rest_slot!(acc, key, value, env) do
    if Map.has_key?(acc, key) do
      compile_error!(env.file, env.line, "multiple rest.#{key} entries are not allowed")
    end

    Map.put(acc, key, value)
  end

  defp normalize_depends!(depends, raw_asset) do
    Enum.map(depends, fn
      name when is_atom(name) ->
        if module_atom?(name) do
          compile_error!(
            raw_asset.file,
            raw_asset.line,
            "invalid @depends entry #{inspect(name)}; expected :asset_name or {Module, :asset_name}; module shorthand is not supported in Favn.MultiAsset"
          )
        else
          Ref.new(raw_asset.module, name)
        end

      {module, name} when is_atom(module) and is_atom(name) ->
        Ref.new(module, name)

      dependency ->
        compile_error!(
          raw_asset.file,
          raw_asset.line,
          "invalid @depends entry #{inspect(dependency)}; expected :asset_name or {Module, :asset_name}"
        )
    end)
  end

  defp resolve_relations!(assets, module, env) do
    defaults = Namespace.resolve_relation(module)

    Enum.map(assets, fn %Asset{} = asset ->
      inferred_name = asset.name

      relation =
        case fetch_raw_relation(module, asset.name) do
          nil ->
            asset.relation

          authored_value ->
            RelationResolver.resolve_explicit_relation!(authored_value, defaults, inferred_name)
        end

      %{asset | relation: relation}
    end)
  rescue
    error in ArgumentError ->
      compile_error!(env.file, env.line, error.message)
  end

  defp fetch_raw_relation(module, name) do
    with entries when is_list(entries) <- Module.get_attribute(module, :favn_multi_assets_raw),
         %{relation: relation} <- Enum.find(entries, &(&1.name == name)) do
      case relation do
        [] -> nil
        [value] -> value
      end
    else
      _ -> nil
    end
  end

  defp ensure_unique_relation_owners!(assets, env) do
    :ok = RelationResolver.ensure_unique_relation_owners!(assets)

    assets
  rescue
    error in ArgumentError ->
      compile_error!(env.file, env.line, error.message)
  end

  defp normalize_meta!(meta, _env) when is_nil(meta), do: %{}

  defp normalize_meta!(meta, env) do
    Asset.normalize_meta!(meta)
  rescue
    error in ArgumentError ->
      compile_error!(env.file, env.line, error.message)
  end

  defp normalize_window!([], _env), do: nil
  defp normalize_window!([%Spec{} = spec], _env), do: spec

  defp normalize_window!([_a, _b | _rest], env) do
    compile_error!(
      env.file,
      env.line,
      "multiple @window attributes are not allowed; use at most one @window per asset declaration"
    )
  end

  defp normalize_window!(value, env) do
    compile_error!(
      env.file,
      env.line,
      "invalid @window value #{inspect(value)}; expected Favn.Window spec like Favn.Window.daily()"
    )
  end

  defp normalize_window_value!(%Spec{} = spec, _env), do: spec

  defp normalize_window_value!(value, env) do
    compile_error!(
      env.file,
      env.line,
      "invalid defaults window value #{inspect(value)}; expected Favn.Window spec like Favn.Window.daily()"
    )
  end

  defp validate_relation_attr!([], _env), do: :ok

  defp validate_relation_attr!([relation], env) do
    valid? =
      relation == true or (is_list(relation) and Keyword.keyword?(relation)) or is_map(relation)

    if valid? do
      :ok
    else
      compile_error!(
        env.file,
        env.line,
        "invalid @relation value #{inspect(relation)}; expected true, a keyword list, or a map"
      )
    end
  end

  defp validate_relation_attr!([_a, _b | _rest], env) do
    compile_error!(
      env.file,
      env.line,
      "multiple @relation attributes are not allowed; use at most one @relation per asset declaration"
    )
  end

  defp validate_no_stray_asset_attributes!(env, kind, name, arity) do
    depends = fetch_accum_attribute(env.module, :depends)
    meta = Module.get_attribute(env.module, :meta)
    window = fetch_accum_attribute(env.module, :window)
    relation = fetch_accum_attribute(env.module, :relation)
    doc = normalize_doc(Module.get_attribute(env.module, :doc))

    if depends != [] or not is_nil(meta) or window != [] or relation != [] or not is_nil(doc) do
      Module.delete_attribute(env.module, :depends)
      Module.delete_attribute(env.module, :meta)
      Module.delete_attribute(env.module, :window)
      Module.delete_attribute(env.module, :relation)
      clear_doc!(env.module, env.line)

      compile_error!(
        env.file,
        env.line,
        "@doc/@depends/@meta/@window/@relation on #{kind} #{name}/#{arity} requires asset :name do immediately below those attributes"
      )
    else
      :ok
    end
  end

  defp ensure_no_pending_attributes!(env) do
    depends = fetch_accum_attribute(env.module, :depends)
    meta = Module.get_attribute(env.module, :meta)
    window = fetch_accum_attribute(env.module, :window)
    relation = fetch_accum_attribute(env.module, :relation)
    pending_doc? = pending_doc?(env.module)

    if depends != [] or not is_nil(meta) or window != [] or relation != [] or pending_doc? do
      Module.delete_attribute(env.module, :depends)
      Module.delete_attribute(env.module, :meta)
      Module.delete_attribute(env.module, :window)
      Module.delete_attribute(env.module, :relation)
      clear_doc!(env.module, env.line)

      compile_error!(
        env.file,
        env.line,
        "@doc/@meta/@depends/@window/@relation must be attached to an immediately following asset :name do"
      )
    else
      :ok
    end
  end

  defp pending_doc?(module) do
    case Module.get_attribute(module, :doc) do
      nil -> false
      false -> false
      {_line, false} -> false
      _ -> true
    end
  end

  defp merge_rest(nil, nil), do: nil
  defp merge_rest(nil, asset_rest) when is_map(asset_rest), do: asset_rest
  defp merge_rest(default_rest, nil) when is_map(default_rest), do: default_rest

  defp merge_rest(default_rest, asset_rest) do
    merged = Map.merge(default_rest, asset_rest)
    merged = merge_nested_map(merged, default_rest, asset_rest, :params)
    merged = merge_nested_map(merged, default_rest, asset_rest, :extra)

    if map_size(merged) == 0 do
      nil
    else
      merged
    end
  end

  defp merge_nested_map(merged, defaults, overrides, key) do
    default_map = Map.get(defaults, key)
    override_map = Map.get(overrides, key)

    map =
      case {default_map, override_map} do
        {nil, nil} -> nil
        {nil, override_map} -> override_map
        {default_map, nil} -> default_map
        {default_map, override_map} -> Map.merge(default_map, override_map)
      end

    if is_nil(map) do
      Map.delete(merged, key)
    else
      Map.put(merged, key, map)
    end
  end

  defp normalize_map_like!(value, label, env) when is_list(value) do
    if Keyword.keyword?(value) do
      Map.new(value)
    else
      compile_error!(
        env.file,
        env.line,
        "#{label} must be a keyword list or map, got: #{inspect(value)}"
      )
    end
  end

  defp normalize_map_like!(value, _label, _env) when is_map(value), do: value

  defp normalize_map_like!(value, label, env) do
    compile_error!(
      env.file,
      env.line,
      "#{label} must be a keyword list or map, got: #{inspect(value)}"
    )
  end

  defp normalize_binary!(value, _field, _env) when is_binary(value), do: value

  defp normalize_binary!(value, field, env) do
    compile_error!(
      env.file,
      env.line,
      "rest.#{field} must be a string, got: #{inspect(value)}"
    )
  end

  defp eval_quoted!(ast, env) do
    {value, _bindings} = Code.eval_quoted(ast, [], env)
    value
  rescue
    error in [CompileError, SyntaxError, ArgumentError] ->
      compile_error!(env.file, env.line, Exception.message(error))
  end

  defp block_expressions({:__block__, _meta, expressions}), do: expressions
  defp block_expressions(nil), do: []
  defp block_expressions(expression), do: [expression]

  defp fetch_accum_attribute(module, attribute) do
    case Module.get_attribute(module, attribute) do
      nil -> []
      value when is_list(value) -> value
    end
  end

  defp normalize_doc({_line, false}), do: nil
  defp normalize_doc({_line, doc}) when is_binary(doc), do: doc
  defp normalize_doc(false), do: nil
  defp normalize_doc(doc) when is_binary(doc), do: doc
  defp normalize_doc(_), do: nil

  defp normalize_file(file) do
    file
    |> to_string()
    |> Path.relative_to_cwd()
  end

  defp compile_error!(file, line, description) do
    raise CompileError, file: file, line: line, description: description
  end

  defp clear_doc!(module, line) do
    Module.put_attribute(module, :doc, {line, false})
  end

  defp module_atom?(module) when is_atom(module) do
    module
    |> Atom.to_string()
    |> String.starts_with?("Elixir.")
  end
end
