defmodule Favn.Source do
  @moduledoc """
  Public DSL for external upstream relations that Favn can reason about but not run.

  Use `Favn.Source` for warehouse objects that participate in lineage and SQL
  dependency inference but are not executed as materializing assets.

  ## When to use it

  Use this module when a relation is external to Favn-managed execution but you
  still want it to be explicit and typed in the catalog.

  ## Minimal example

      defmodule MyApp.Raw.Stripe.Charges do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "stripe"]
        use Favn.Source

        @doc "External raw Stripe charges table"
        @meta owner: "data-platform", category: :payments, tags: [:raw]
        @relation true
      end

  ## Authoring contract

  - define no user functions
  - declare exactly one `@relation`
  - optionally add `@doc` and `@meta`

  ## Supported attributes

  - `@doc`: source documentation
  - `@meta`: keyword or map metadata such as `owner`, `category`, and `tags`
  - `@relation`: required relation declaration

  `@relation` supports:

  - `true` to infer from module name plus namespace defaults
  - keyword or map relation overrides such as `connection`, `catalog`, and `schema`

  ## What gets compiled

  A source compiles to one canonical `%Favn.Asset{}` with `type: :source` and no
  executable entrypoint.

  ## See also

  - `Favn.SQLAsset`
  - `Favn.Namespace`
  """

  alias Favn.Asset
  alias Favn.Ref

  @doc false
  defmacro __using__(_opts) do
    quote do
      Module.register_attribute(__MODULE__, :meta, persist: false)
      Module.register_attribute(__MODULE__, :relation, accumulate: true)

      @before_compile Favn.Source
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    relation = Module.get_attribute(env.module, :relation) |> Enum.reverse()
    meta = Module.get_attribute(env.module, :meta)
    doc = normalize_doc(Module.get_attribute(env.module, :doc))

    if relation == [] do
      compile_error!(
        env.file,
        env.line,
        "Favn.Source modules require @relation attribute"
      )
    end

    validate_no_user_definitions!(env)
    validate_relation_attr!(relation, env)

    raw_asset = %{
      module: env.module,
      name: :asset,
      arity: 0,
      doc: doc,
      file: normalize_file(env.file),
      line: env.line,
      depends: [],
      meta: meta,
      window: [],
      relation: relation
    }

    asset = %Asset{
      module: env.module,
      name: :asset,
      entrypoint: nil,
      ref: Ref.new(env.module, :asset),
      arity: 0,
      type: :source,
      title: nil,
      doc: doc,
      file: normalize_file(env.file),
      line: env.line,
      meta: meta || %{},
      depends_on: [],
      dependencies: [],
      config: %{},
      window_spec: nil,
      relation: nil,
      materialization: nil,
      relation_inputs: [],
      diagnostics: []
    }

    asset = ensure_valid_asset!(asset, env)

    quote do
      @doc false
      @spec __favn_assets__() :: [Favn.Asset.t()]
      def __favn_assets__, do: [unquote(Macro.escape(asset))]

      @doc false
      def __favn_assets_raw__, do: [unquote(Macro.escape(raw_asset))]

      @doc false
      def __favn_single_asset__, do: true

      @doc false
      def __favn_source__, do: true
    end
  end

  defp validate_relation_attr!([relation], env) do
    valid? =
      relation == true or (is_list(relation) and Keyword.keyword?(relation)) or is_map(relation)

    if not valid? do
      compile_error!(
        env.file,
        env.line,
        "invalid @relation value #{inspect(relation)}; expected true, a keyword list, or a map"
      )
    end

    :ok
  end

  defp validate_relation_attr!(_relation, env) do
    compile_error!(
      env.file,
      env.line,
      "multiple @relation attributes are not allowed; use at most one @relation"
    )
  end

  defp validate_no_user_definitions!(env) do
    definitions =
      env.module
      |> Module.definitions_in()
      |> Enum.reject(&allowed_generated_definition?/1)

    case definitions do
      [] ->
        :ok

      definitions ->
        formatted = Enum.map_join(definitions, ", ", fn {name, arity} -> "#{name}/#{arity}" end)

        compile_error!(
          env.file,
          env.line,
          "Favn.Source modules are declarative and cannot define functions; found: #{formatted}"
        )
    end
  end

  defp allowed_generated_definition?({:__favn_namespace_config__, 0}), do: true
  defp allowed_generated_definition?(_definition), do: false

  defp ensure_valid_asset!(%Asset{} = asset, env) do
    Asset.validate!(asset)
  rescue
    error in ArgumentError -> compile_error!(env.file, env.line, error.message)
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
end
