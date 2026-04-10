defmodule Favn.Assets do
  @moduledoc """
  Compile-time DSL for authoring Favn assets inside a module.

  `use Favn.Assets` lets a module mark public functions with `@asset` so Favn
  can capture canonical `%Favn.Asset{}` metadata for later introspection.

  This module is intentionally focused on compile-time authoring behavior:

    * collecting metadata from `@asset`
    * enforcing authoring rules
    * normalizing DSL-friendly dependency declarations
    * emitting `__favn_assets__/0` for later runtime inspection

  Asset authoring contract:

    * asset functions must have arity 1 and accept one runtime context argument
    * use repeatable `@depends` attributes for dependencies
    * use `@meta` for non-execution metadata
    * use optional `@window Favn.Window.daily()` to define asset windowing
  """

  alias Favn.Asset
  alias Favn.Ref

  @doc false
  defmacro __using__(_opts) do
    quote do
      Module.register_attribute(__MODULE__, :asset, persist: false)
      Module.register_attribute(__MODULE__, :depends, accumulate: true)
      Module.register_attribute(__MODULE__, :meta, persist: false)
      Module.register_attribute(__MODULE__, :produces, accumulate: true)
      Module.register_attribute(__MODULE__, :window, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_assets_raw, accumulate: true)

      @on_definition Favn.Assets
      @before_compile Favn.Assets
    end
  end

  @doc false
  def __on_definition__(env, kind, name, args, _guards, _body) do
    case Module.get_attribute(env.module, :asset) do
      nil ->
        validate_no_stray_asset_attributes!(env, kind, name, args)

      asset_opts ->
        Module.delete_attribute(env.module, :asset)

        case kind do
          :def ->
            arity = length(args || [])

            if arity == 1 do
              depends =
                env.module
                |> Module.get_attribute(:depends)
                |> Enum.reverse()

              meta = Module.get_attribute(env.module, :meta)

              window =
                env.module
                |> Module.get_attribute(:window)
                |> Enum.reverse()

              produces = env.module |> Module.get_attribute(:produces) |> Enum.reverse()
              validate_produces_attr!(produces, env)
              Module.delete_attribute(env.module, :depends)
              Module.delete_attribute(env.module, :meta)
              Module.delete_attribute(env.module, :window)
              Module.delete_attribute(env.module, :produces)

              Module.put_attribute(env.module, :favn_assets_raw, %{
                module: env.module,
                name: name,
                arity: arity,
                doc: normalize_doc(Module.get_attribute(env.module, :doc)),
                file: normalize_file(env.file),
                line: env.line,
                asset_decl: asset_opts,
                depends: depends,
                meta: meta,
                window: window,
                produces: produces
              })
            else
              compile_error!(
                env.file,
                env.line,
                "@asset functions must have arity 1 and accept one runtime context argument"
              )
            end

          :defp ->
            compile_error!(env.file, env.line, "@asset can only be used on public functions")
        end
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    case Module.get_attribute(env.module, :asset) do
      nil ->
        :ok

      _ ->
        compile_error!(
          env.file,
          env.line,
          "@asset must be followed by a public function definition"
        )
    end

    case {
      Module.get_attribute(env.module, :depends),
      Module.get_attribute(env.module, :meta),
      Module.get_attribute(env.module, :window),
      Module.get_attribute(env.module, :produces)
    } do
      {[], nil, [], []} ->
        :ok

      _ ->
        compile_error!(
          env.file,
          env.line,
          "@depends/@meta/@window/@produces must be attached to an immediately following @asset function"
        )
    end

    raw_assets =
      env.module
      |> Module.get_attribute(:favn_assets_raw)
      |> Enum.reverse()

    assets =
      raw_assets
      |> validate_unique_names!()
      |> Enum.map(&build_asset!/1)

    quote do
      @doc false
      @spec __favn_assets__() :: [Favn.Asset.t()]
      def __favn_assets__, do: unquote(Macro.escape(assets))

      @doc false
      def __favn_assets_raw__, do: unquote(Macro.escape(raw_assets))
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
    title = normalize_asset_decl!(raw_asset.asset_decl, raw_asset)
    meta = normalize_meta!(raw_asset.meta, raw_asset)
    depends_on = normalize_depends!(raw_asset.depends, raw_asset)
    window_spec = normalize_window!(raw_asset.window, raw_asset)

    asset = %Asset{
      module: raw_asset.module,
      name: raw_asset.name,
      ref: Ref.new(raw_asset.module, raw_asset.name),
      arity: raw_asset.arity,
      doc: raw_asset.doc,
      file: raw_asset.file,
      line: raw_asset.line,
      title: title,
      meta: meta,
      depends_on: depends_on,
      window_spec: window_spec
    }

    try do
      Asset.validate!(asset)
    rescue
      error in ArgumentError ->
        compile_error!(raw_asset.file, raw_asset.line, error.message)
    end
  end

  defp normalize_depends!(depends, raw_asset) do
    Enum.map(depends, fn
      name when is_atom(name) ->
        Ref.new(raw_asset.module, name)

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

  defp normalize_meta!(meta, raw_asset) do
    Asset.normalize_meta!(meta)
  rescue
    error in ArgumentError ->
      compile_error!(raw_asset.file, raw_asset.line, error.message)
  end

  defp normalize_window!([], _raw_asset), do: nil

  defp normalize_window!([%Favn.Window.Spec{} = spec], _raw_asset), do: spec

  defp normalize_window!([_a, _b | _rest], raw_asset) do
    compile_error!(
      raw_asset.file,
      raw_asset.line,
      "multiple @window attributes are not allowed; use at most one @window per @asset function"
    )
  end

  defp normalize_window!(value, raw_asset) do
    compile_error!(
      raw_asset.file,
      raw_asset.line,
      "invalid @window value #{inspect(value)}; expected Favn.Window spec like Favn.Window.daily()"
    )
  end

  defp validate_produces_attr!([], _env), do: :ok

  defp validate_produces_attr!([produces], env) do
    valid? =
      produces == true or (is_list(produces) and Keyword.keyword?(produces)) or is_map(produces)

    if valid? do
      :ok
    else
      compile_error!(
        env.file,
        env.line,
        "invalid @produces value #{inspect(produces)}; expected true, a keyword list, or a map"
      )
    end
  end

  defp validate_produces_attr!([_a, _b | _rest], env) do
    compile_error!(
      env.file,
      env.line,
      "multiple @produces attributes are not allowed; use at most one @produces per @asset function"
    )
  end

  defp normalize_doc({_line, false}), do: nil
  defp normalize_doc({_line, doc}) when is_binary(doc), do: doc
  defp normalize_doc(false), do: nil
  defp normalize_doc(doc) when is_binary(doc), do: doc
  defp normalize_doc(_), do: nil

  defp normalize_asset_decl!(true, _raw_asset), do: nil
  defp normalize_asset_decl!(name, _raw_asset) when is_binary(name), do: name

  defp normalize_asset_decl!(other, raw_asset),
    do:
      compile_error!(
        raw_asset.file,
        raw_asset.line,
        "@asset must be true or a display-name string, got: #{inspect(other)}"
      )

  defp normalize_file(file) do
    file
    |> to_string()
    |> Path.relative_to_cwd()
  end

  defp compile_error!(file, line, description) do
    raise CompileError, file: file, line: line, description: description
  end

  defp validate_no_stray_asset_attributes!(env, kind, name, args)
       when kind in [:def, :defp] do
    depends = Module.get_attribute(env.module, :depends)
    meta = Module.get_attribute(env.module, :meta)
    window = Module.get_attribute(env.module, :window)
    produces = Module.get_attribute(env.module, :produces)

    if depends != [] or not is_nil(meta) or window != [] or produces != [] do
      Module.delete_attribute(env.module, :depends)
      Module.delete_attribute(env.module, :meta)
      Module.delete_attribute(env.module, :window)
      Module.delete_attribute(env.module, :produces)

      arity = length(args || [])

      compile_error!(
        env.file,
        env.line,
        "@depends/@meta/@window/@produces on #{kind} #{name}/#{arity} requires @asset immediately above that function"
      )
    else
      :ok
    end
  end

  defp validate_no_stray_asset_attributes!(_env, _kind, _name, _args), do: :ok
end
