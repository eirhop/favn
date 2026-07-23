defmodule Favn.MultiAsset do
  @moduledoc """
  Defines multiple generated assets that share one Elixir `asset/1` runtime.

  Module-level declarations are defaults for every generated asset. Declarations
  inside an `asset` block add to or override those defaults. Settings and meta
  are shallow-merged, dependencies are combined, and scalar declarations are
  replaced by the child value.

      defmodule MyApp.Mercatus do
        use Favn.MultiAsset

        settings method: "GET"
        meta owner: "data-platform"
        meta category: "mercatus"
        window Favn.Window.monthly()
        freshness :always
        execution_pool :mercatus_api
        relation true

        asset :orders do
          description "Extract orders"
          settings path: "/orders"
          meta tags: ["orders"]
        end

        asset :events do
          settings path: "/events"
          freshness :daily
        end

        @doc "Execute one generated extraction."
        def asset(ctx), do: MyApp.Client.extract(ctx.asset.settings, ctx)
      end

  Shared declarations must appear before the first `asset` block. Child
  descriptions use `description/1` because generated children are manifest
  entries, not Elixir functions; `@doc` remains attached to the real `asset/1`
  function.

  Structural ancestor `Favn.Namespace` modules may provide `settings`, `meta`,
  `runtime_config`, `window`, `coverage`, and `freshness` defaults. Resolution order is
  namespace root-to-leaf, this module's shared declarations, then each child.
  Settings and metadata shallow-merge, runtime configuration composes through
  normal conflict validation, and child scalar declarations win. Use `nil` in a
  child to clear an inherited optional scalar. The module uses only
  `Favn.MultiAsset`; module ancestry selects namespace defaults automatically.
  """

  alias Favn.Asset
  alias Favn.Asset.RelationResolver
  alias Favn.Coverage.Spec, as: CoverageSpec
  alias Favn.DSL.AssetDeclarations
  alias Favn.DSL.Compiler, as: DSLCompiler
  alias Favn.Namespace
  alias Favn.Ref
  alias Favn.RuntimeConfig.Bundle
  alias Favn.RuntimeConfig.Requirements
  alias Favn.Window.Spec

  @shared_declarations [
    :settings,
    :meta,
    :depends,
    :window,
    :coverage,
    :freshness,
    :retry,
    :execution_pool,
    :relation,
    :runtime_config
  ]

  @doc false
  defmacro __using__(_opts) do
    env = __CALLER__

    quote bind_quoted: [file: env.file, line: env.line] do
      Favn.DSL.AssetDeclarations.claim_module!(__MODULE__, :multi_asset, file, line)
      Favn.DSL.AssetDeclarations.register!(__MODULE__)
      Module.register_attribute(__MODULE__, :favn_multi_assets_raw, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_multi_asset_current_decl, persist: false)
      Module.register_attribute(__MODULE__, :favn_multi_assets_started, persist: false)
      Module.register_attribute(__MODULE__, :favn_multi_asset_runtime_count, persist: false)
      Module.register_attribute(__MODULE__, :favn_multi_asset_generating, persist: false)

      @favn_multi_assets_started false
      @favn_multi_asset_runtime_count 0

      import Favn.MultiAsset,
        only: [
          asset: 2,
          settings: 1,
          meta: 1,
          depends: 1,
          window: 1,
          coverage: 1,
          freshness: 1,
          retry: 1,
          execution_pool: 1,
          relation: 1,
          runtime_config: 1,
          runtime_config: 2,
          env!: 1,
          env!: 2,
          secret_env!: 1,
          secret_env!: 2
        ]

      @on_definition Favn.MultiAsset
      @before_compile Favn.MultiAsset
    end
  end

  for declaration <- [
        :settings,
        :meta,
        :depends,
        :window,
        :coverage,
        :freshness,
        :retry,
        :execution_pool,
        :relation
      ] do
    @doc false
    defmacro unquote(declaration)(value) do
      declaration = unquote(declaration)

      quote do
        Favn.MultiAsset.put_shared_declaration!(__MODULE__, unquote(declaration), unquote(value))
      end
    end
  end

  @doc false
  defmacro runtime_config(bundle) do
    quote do
      Favn.MultiAsset.put_shared_declaration!(
        __MODULE__,
        :runtime_config,
        Favn.RuntimeConfig.Bundle.validate!(unquote(bundle))
      )
    end
  end

  @doc false
  defmacro runtime_config(scope, fields) do
    caller = __CALLER__
    file = DSLCompiler.normalize_file(caller.file)

    quote do
      Favn.MultiAsset.put_shared_declaration!(
        __MODULE__,
        :runtime_config,
        Favn.RuntimeConfig.Bundle.inline!(unquote(scope), unquote(fields),
          module: unquote(caller.module),
          file: unquote(file),
          line: unquote(caller.line)
        )
      )
    end
  end

  @doc false
  defmacro env!(key, opts \\ []) do
    quote do
      Favn.RuntimeConfig.Ref.env!(unquote(key), unquote(opts))
    end
  end

  @doc false
  defmacro secret_env!(key, opts \\ []) do
    quote do
      Favn.RuntimeConfig.Ref.secret_env!(unquote(key), unquote(opts))
    end
  end

  @doc false
  @spec put_shared_declaration!(module(), atom(), term()) :: :ok
  def put_shared_declaration!(module, declaration, value) do
    if Module.get_attribute(module, :favn_multi_assets_started) do
      raise CompileError,
        description:
          "shared #{declaration} must be declared before the first Favn.MultiAsset asset block"
    end

    AssetDeclarations.put(module, declaration, value)
  end

  @doc "Declares one generated child asset."
  defmacro asset(name, do: block) do
    env = __CALLER__

    unless is_atom(name) do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "asset name must be an atom, got: #{Macro.to_string(name)}"
      )
    end

    child = parse_child_block!(block, env, name)

    declaration = %{
      module: env.module,
      name: name,
      entrypoint: :asset,
      arity: 1,
      file: DSLCompiler.normalize_file(env.file),
      line: env.line,
      child: child
    }

    marker = marker_name(name)

    quote do
      Module.put_attribute(
        __MODULE__,
        :favn_multi_asset_current_decl,
        unquote(Macro.escape(declaration))
      )

      defp unquote(marker)(), do: :ok
      :ok
    end
  end

  @doc false
  def __on_definition__(env, kind, name, args, _guards, _body) do
    if Module.get_attribute(env.module, :favn_multi_asset_generating) do
      :ok
    else
      AssetDeclarations.reject_legacy_attributes!(env.module, env.file, env.line)
      arity = length(args || [])

      case {kind, name, arity} do
        {:defp, marker, 0} ->
          if marker_name?(marker) do
            capture_child_declaration!(env, marker)
          else
            :ok
          end

        {:def, :asset, 1} ->
          count = Module.get_attribute(env.module, :favn_multi_asset_runtime_count) || 0
          Module.put_attribute(env.module, :favn_multi_asset_runtime_count, count + 1)

        {:def, :asset, _arity} ->
          DSLCompiler.compile_error!(
            env.file,
            env.line,
            "Favn.MultiAsset requires exactly one public asset/1 function"
          )

        {:defp, :asset, _arity} ->
          DSLCompiler.compile_error!(
            env.file,
            env.line,
            "Favn.MultiAsset requires a public def asset(ctx)"
          )

        _other ->
          :ok
      end
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    AssetDeclarations.reject_legacy_attributes!(env.module, env.file, env.line)

    if pending_doc?(env.module) do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "@doc must document the public asset/1 function"
      )
    end

    if Module.get_attribute(env.module, :favn_multi_asset_runtime_count) != 1 do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "Favn.MultiAsset modules must define exactly one public asset/1 function"
      )
    end

    raw_declarations =
      env.module
      |> Module.get_attribute(:favn_multi_assets_raw)
      |> List.wrap()
      |> Enum.reverse()

    if raw_declarations == [] do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "Favn.MultiAsset modules must declare at least one asset :name do ... end"
      )
    end

    validate_unique_names!(raw_declarations)
    shared = shared_declarations(env.module)
    context = %{module: env.module, file: DSLCompiler.normalize_file(env.file), line: env.line}
    {_assets, raw_assets} = finalize_raw_assets(raw_declarations, shared, context)
    Module.put_attribute(env.module, :favn_multi_asset_generating, true)

    quote do
      @doc false
      @spec __favn_assets__() :: [Favn.Asset.t()]
      def __favn_assets__ do
        {assets, _raw_assets} =
          Favn.MultiAsset.finalize_raw_assets(
            unquote(Macro.escape(raw_declarations)),
            unquote(Macro.escape(shared)),
            unquote(Macro.escape(context))
          )

        assets
      end

      @doc false
      def __favn_assets_raw__, do: unquote(Macro.escape(raw_assets))
    end
  end

  @doc false
  @spec finalize_raw_assets([map()], map(), map()) :: {[Asset.t()], [map()]}
  def finalize_raw_assets(raw_declarations, shared, context)
      when is_list(raw_declarations) and is_map(shared) and is_map(context) do
    namespace = Namespace.resolve(context.module)

    {assets, raw_assets} =
      Enum.map_reduce(raw_declarations, [], fn declaration, raw_assets ->
        raw_asset = merge_declarations!(declaration, shared, namespace, context)
        {build_asset!(raw_asset, namespace.relation, context), [raw_asset | raw_assets]}
      end)

    :ok = ensure_unique_relation_owners!(assets, context)
    {assets, Enum.reverse(raw_assets)}
  end

  defp shared_declarations(module) do
    Map.new(@shared_declarations, &{&1, AssetDeclarations.values(module, &1)})
  end

  defp capture_child_declaration!(env, marker) do
    AssetDeclarations.reject_legacy_attributes!(env.module, env.file, env.line)
    reject_pending_doc!(env)

    declaration = Module.get_attribute(env.module, :favn_multi_asset_current_decl)

    if is_nil(declaration) do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "internal error: missing declaration for #{marker}"
      )
    end

    Module.put_attribute(env.module, :favn_multi_assets_started, true)
    Module.put_attribute(env.module, :favn_multi_assets_raw, declaration)
    Module.delete_attribute(env.module, :favn_multi_asset_current_decl)
  end

  defp marker_name(name), do: String.to_atom("__favn_multi_asset_marker_#{name}")

  defp marker_name?(name) do
    name
    |> Atom.to_string()
    |> String.starts_with?("__favn_multi_asset_marker_")
  end

  defp parse_child_block!(block, env, name) do
    block
    |> block_expressions()
    |> Enum.reduce(empty_child(), fn expression, child ->
      case expression do
        {:description, _meta, [value]} ->
          put_single_child!(child, :description, eval_quoted!(value, env), env, name)

        {declaration, _meta, [value]}
        when declaration in [
               :settings,
               :meta,
               :depends,
               :window,
               :coverage,
               :freshness,
               :retry,
               :execution_pool,
               :relation
             ] ->
          Map.update!(child, declaration, &(&1 ++ [eval_quoted!(value, env)]))

        {:runtime_config, _meta, [bundle]} ->
          Map.update!(
            child,
            :runtime_config,
            &(&1 ++ [Bundle.validate!(eval_quoted!(bundle, env))])
          )

        {:runtime_config, _meta, [scope, fields]} ->
          declaration =
            Bundle.inline!(eval_quoted!(scope, env), eval_quoted!(fields, env),
              module: env.module,
              file: DSLCompiler.normalize_file(env.file),
              line: env.line
            )

          Map.update!(child, :runtime_config, &(&1 ++ [declaration]))

        {:@, _meta, _args} ->
          DSLCompiler.compile_error!(
            env.file,
            env.line,
            "asset #{inspect(name)} uses an @ attribute; use child DSL macros without @"
          )

        other ->
          DSLCompiler.compile_error!(
            env.file,
            env.line,
            "unsupported declaration in asset #{inspect(name)}: #{Macro.to_string(other)}"
          )
      end
    end)
  rescue
    error in ArgumentError -> DSLCompiler.compile_error!(env.file, env.line, error.message)
  end

  defp empty_child do
    %{
      description: [],
      settings: [],
      meta: [],
      depends: [],
      window: [],
      coverage: [],
      freshness: [],
      retry: [],
      execution_pool: [],
      relation: [],
      runtime_config: []
    }
  end

  defp put_single_child!(child, declaration, value, env, name) do
    if Map.fetch!(child, declaration) != [] do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "multiple #{declaration} declarations are not allowed in asset #{inspect(name)}"
      )
    end

    Map.put(child, declaration, [value])
  end

  defp merge_declarations!(declaration, shared, namespace, env) do
    child = declaration.child

    settings =
      namespace
      |> Namespace.effective_declarations(:settings, shared.settings ++ child.settings)
      |> Favn.Settings.merge_all!()

    meta =
      namespace
      |> Namespace.effective_declarations(:meta, shared.meta ++ child.meta)
      |> merge_meta!()

    depends = Enum.uniq(shared.depends ++ child.depends)
    window_values = select_scalar(namespace, :window, shared.window, child.window)
    window = scalar_value!(:window, [], window_values, env, nil)
    validate_window!(window)
    window = mark_window_source(window, shared.window, child.window)

    coverage_values = select_scalar(namespace, :coverage, shared.coverage, child.coverage)
    coverage = normalize_coverage!(coverage_values, window, env)

    freshness_values = select_scalar(namespace, :freshness, shared.freshness, child.freshness)

    freshness = normalize_freshness!(freshness_values, window, env)
    freshness = mark_freshness_source(freshness, shared.freshness, child.freshness)
    retry = scalar_value!(:retry, shared.retry, child.retry, env, nil)

    execution_pool =
      scalar_value!(:execution_pool, shared.execution_pool, child.execution_pool, env, nil)

    relation = scalar_value!(:relation, shared.relation, child.relation, env, nil)
    description = scalar_value!(:description, [], child.description, env, nil)

    runtime_config =
      namespace
      |> Namespace.effective_declarations(
        :runtime_config,
        shared.runtime_config ++ child.runtime_config
      )
      |> Requirements.merge_all!(consumer: declaration.module)

    validate_description!(description)
    validate_execution_pool!(execution_pool)
    validate_relation!(relation)

    Map.merge(declaration, %{
      doc: description,
      settings: settings,
      meta: meta,
      depends: depends,
      window_spec: window,
      coverage_spec: coverage,
      freshness: freshness,
      retry_policy: normalize_retry!(retry),
      execution_pool: execution_pool,
      relation: relation,
      runtime_config: runtime_config
    })
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(declaration.file, declaration.line, error.message)
  end

  defp scalar_value!(name, shared, child, env, default) do
    selected = if child == [], do: shared, else: child

    case selected do
      [] ->
        default

      [value] ->
        value

      _ ->
        DSLCompiler.compile_error!(
          env.file,
          env.line,
          "multiple #{name} declarations are not allowed"
        )
    end
  end

  defp select_scalar(namespace, field, shared, child) do
    local = if child == [], do: shared, else: child
    Namespace.effective_declarations(namespace, field, local)
  end

  defp merge_meta!(declarations) do
    Enum.reduce(declarations, %{}, fn declaration, acc ->
      Map.merge(acc, Asset.normalize_meta!(declaration))
    end)
  end

  defp normalize_freshness!([nil], _window, _env), do: nil

  defp normalize_freshness!(values, window, env) do
    Asset.normalize_freshness!(values, window, "per generated asset")
  rescue
    error in ArgumentError -> DSLCompiler.compile_error!(env.file, env.line, error.message)
  end

  defp normalize_coverage!([], _window, _env), do: nil
  defp normalize_coverage!([nil], _window, _env), do: nil

  defp normalize_coverage!([value], %Spec{}, env) do
    case CoverageSpec.from_value(value) do
      {:ok, %CoverageSpec{} = spec} -> spec
      {:error, reason} -> DSLCompiler.compile_error!(env.file, env.line, inspect(reason))
    end
  end

  defp normalize_coverage!([_value], nil, env) do
    DSLCompiler.compile_error!(env.file, env.line, "coverage requires an effective asset window")
  end

  defp normalize_coverage!(_values, _window, env) do
    DSLCompiler.compile_error!(
      env.file,
      env.line,
      "multiple coverage declarations are not allowed"
    )
  end

  defp mark_window_source(%Spec{} = spec, [], []),
    do: Spec.with_declaration_source(spec, :namespace)

  defp mark_window_source(%Spec{} = spec, _shared, _child),
    do: Spec.with_declaration_source(spec, :local)

  defp mark_window_source(nil, _shared, _child), do: nil

  defp mark_freshness_source(%Favn.Freshness.Policy{} = policy, [], []),
    do: Favn.Freshness.Policy.with_declaration_source(policy, :namespace)

  defp mark_freshness_source(%Favn.Freshness.Policy{} = policy, _shared, _child),
    do: Favn.Freshness.Policy.with_declaration_source(policy, :local)

  defp mark_freshness_source(nil, _shared, _child), do: nil

  defp normalize_retry!(nil), do: nil
  defp normalize_retry!(value), do: Favn.Retry.Policy.new!(value)

  defp validate_description!(nil), do: :ok
  defp validate_description!(value) when is_binary(value), do: :ok

  defp validate_description!(value),
    do: raise(ArgumentError, "description must be a string or nil, got: #{inspect(value)}")

  defp validate_window!(nil), do: :ok
  defp validate_window!(%Spec{}), do: :ok

  defp validate_window!(value),
    do: raise(ArgumentError, "window must be a Favn.Window.Spec or nil, got: #{inspect(value)}")

  defp validate_execution_pool!(nil), do: :ok
  defp validate_execution_pool!(value) when is_atom(value), do: :ok

  defp validate_execution_pool!(value),
    do: raise(ArgumentError, "execution_pool must be an atom or nil, got: #{inspect(value)}")

  defp validate_relation!(nil), do: :ok

  defp validate_relation!(value) do
    unless DSLCompiler.valid_relation_attr_value?(value) do
      raise ArgumentError, "relation must be true, a keyword list, a map, or nil"
    end
  end

  defp build_asset!(raw_asset, relation_defaults, env) do
    relation = resolve_relation!(raw_asset, relation_defaults, env)

    asset = %Asset{
      module: raw_asset.module,
      name: raw_asset.name,
      entrypoint: :asset,
      ref: Ref.new(raw_asset.module, raw_asset.name),
      arity: 1,
      type: :elixir,
      doc: raw_asset.doc,
      file: raw_asset.file,
      line: raw_asset.line,
      meta: raw_asset.meta,
      depends_on: normalize_depends!(raw_asset.depends, raw_asset),
      settings: raw_asset.settings,
      runtime_config: raw_asset.runtime_config,
      window_spec: raw_asset.window_spec,
      coverage_spec: raw_asset.coverage_spec,
      freshness: raw_asset.freshness,
      retry_policy: raw_asset.retry_policy,
      execution_pool: raw_asset.execution_pool,
      relation: relation
    }

    Asset.validate!(asset)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_asset.file, raw_asset.line, error.message)
  end

  defp normalize_depends!(depends, raw_asset) do
    depends
    |> Enum.map(fn
      name when is_atom(name) ->
        if DSLCompiler.module_atom?(name) do
          DSLCompiler.compile_error!(
            raw_asset.file,
            raw_asset.line,
            "invalid depends entry #{inspect(name)}; use :asset_name or {Module, :asset_name}"
          )
        end

        Ref.new(raw_asset.module, name)

      {module, name} when is_atom(module) and is_atom(name) ->
        Ref.new(module, name)

      dependency ->
        DSLCompiler.compile_error!(
          raw_asset.file,
          raw_asset.line,
          "invalid depends entry #{inspect(dependency)}; use :asset_name or {Module, :asset_name}"
        )
    end)
    |> Enum.uniq()
    |> Enum.sort_by(fn {module, name} -> {Atom.to_string(module), Atom.to_string(name)} end)
  end

  defp resolve_relation!(%{relation: nil}, _relation_defaults, _env), do: nil

  defp resolve_relation!(raw_asset, relation_defaults, env) do
    RelationResolver.resolve_explicit_relation!(
      raw_asset.relation,
      relation_defaults,
      raw_asset.name
    )
  rescue
    error in ArgumentError -> DSLCompiler.compile_error!(env.file, env.line, error.message)
  end

  defp ensure_unique_relation_owners!(assets, env) do
    RelationResolver.ensure_unique_relation_owners!(assets)
  rescue
    error in ArgumentError -> DSLCompiler.compile_error!(env.file, env.line, error.message)
  end

  defp validate_unique_names!(declarations) do
    declarations
    |> Enum.group_by(& &1.name)
    |> Enum.each(fn
      {_name, [_single]} ->
        :ok

      {name, [first | _]} ->
        DSLCompiler.compile_error!(
          first.file,
          first.line,
          "duplicate asset name #{inspect(name)}; names must be unique within a module"
        )
    end)
  end

  defp reject_pending_doc!(env) do
    if pending_doc?(env.module) do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "@doc cannot document a generated child; use description inside the asset block"
      )
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

  defp eval_quoted!(ast, env) do
    {value, _bindings} = Code.eval_quoted(ast, [], env)
    value
  rescue
    error in [CompileError, SyntaxError, ArgumentError] ->
      DSLCompiler.compile_error!(env.file, env.line, Exception.message(error))
  end

  defp block_expressions({:__block__, _meta, expressions}), do: expressions
  defp block_expressions(nil), do: []
  defp block_expressions(expression), do: [expression]
end
