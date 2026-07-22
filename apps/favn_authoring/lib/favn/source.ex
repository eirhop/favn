defmodule Favn.Source do
  @moduledoc """
  Public DSL for external upstream relations that Favn can reason about but not run.

  Use `Favn.Source` for lakehouse objects that participate in lineage and SQL
  dependency inference but are not executed as materializing assets.

  ## When to use it

  Use this module when a relation is external to Favn-managed execution but you
  still want it to be explicit and typed in the catalog.

  ## Minimal example

      # lib/my_app/lakehouse/raw/payments/stripe_charges.ex
      defmodule MyApp.Lakehouse.Raw.Payments.StripeCharges do
        @moduledoc "External raw Stripe charges table."

        use Favn.Source

        meta owner: "data-platform"
        meta category: :payments
        meta tags: [:raw]
        relation connection: :important_lakehouse,
                 catalog: "raw",
                 schema: "payments",
                 name: "stripe_charges"
      end

  ## Authoring contract

  - define no user functions
  - declare exactly one `relation`
  - use `@moduledoc` for the source description and optionally add `meta`

  ## Supported declarations

  - `@moduledoc`: real Elixir module documentation and the manifest description
  - `meta`: keyword or map metadata such as `owner`, `category`, and `tags`
  - `relation`: required relation declaration

  Structural ancestor `Favn.Namespace` modules provide relation defaults and
  inherited metadata. Relation fields merge by key during asset compilation;
  metadata shallow-merges root-to-leaf and then applies this source's metadata.
  The source module uses only `Favn.Source`; module ancestry selects namespace
  defaults automatically.

  `relation` supports:

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
  alias Favn.DSL.AssetDeclarations
  alias Favn.Namespace
  alias Favn.Ref

  @doc false
  defmacro __using__(_opts) do
    env = __CALLER__

    quote bind_quoted: [file: env.file, line: env.line] do
      Favn.DSL.AssetDeclarations.claim_module!(__MODULE__, :source, file, line)
      Favn.DSL.AssetDeclarations.register!(__MODULE__, [:meta, :relation])
      import Favn.DSL.AssetDeclarations, only: [meta: 1, relation: 1]

      @before_compile Favn.Source
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    AssetDeclarations.reject_legacy_attributes!(env.module, env.file, env.line)
    relation = AssetDeclarations.values(env.module, :relation)
    meta = AssetDeclarations.values(env.module, :meta)
    doc = normalize_doc(Module.get_attribute(env.module, :moduledoc))

    if relation == [] do
      compile_error!(
        env.file,
        env.line,
        "Favn.Source modules require a relation declaration"
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

    _asset = finalize_raw_asset(raw_asset)

    quote do
      @doc false
      @spec __favn_assets__() :: [Favn.Asset.t()]
      def __favn_assets__ do
        [Favn.Source.finalize_raw_asset(unquote(Macro.escape(raw_asset)))]
      end

      @doc false
      def __favn_assets_raw__, do: [unquote(Macro.escape(raw_asset))]

      @doc false
      def __favn_single_asset__, do: true

      @doc false
      def __favn_source__, do: true
    end
  end

  @doc false
  @spec finalize_raw_asset(map()) :: Asset.t()
  def finalize_raw_asset(raw_asset) when is_map(raw_asset) do
    namespace = Namespace.resolve(raw_asset.module)
    validate_no_inherited_coverage!(namespace)

    meta =
      namespace
      |> Namespace.effective_declarations(:meta, raw_asset.meta)
      |> Enum.reduce(%{}, fn declaration, acc ->
        Map.merge(acc, Asset.normalize_meta!(declaration))
      end)

    asset = %Asset{
      module: raw_asset.module,
      name: :asset,
      entrypoint: nil,
      ref: Ref.new(raw_asset.module, :asset),
      arity: 0,
      type: :source,
      doc: raw_asset.doc,
      file: raw_asset.file,
      line: raw_asset.line,
      meta: meta,
      depends_on: [],
      dependencies: [],
      settings: %{},
      window_spec: nil,
      relation: nil,
      materialization: nil,
      relation_inputs: [],
      diagnostics: []
    }

    ensure_valid_asset!(asset, raw_asset)
  rescue
    error in ArgumentError -> compile_error!(raw_asset.file, raw_asset.line, error.message)
  end

  defp validate_relation_attr!([relation], env) do
    valid? =
      relation == true or (is_list(relation) and Keyword.keyword?(relation)) or is_map(relation)

    if not valid? do
      compile_error!(
        env.file,
        env.line,
        "invalid relation value #{inspect(relation)}; expected true, a keyword list, or a map"
      )
    end

    :ok
  end

  defp validate_relation_attr!(_relation, env) do
    compile_error!(
      env.file,
      env.line,
      "multiple relation declarations are not allowed; use at most one relation"
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

  defp allowed_generated_definition?(_definition), do: false

  defp ensure_valid_asset!(%Asset{} = asset, env) do
    Asset.validate!(asset)
  rescue
    error in ArgumentError -> compile_error!(env.file, env.line, error.message)
  end

  defp validate_no_inherited_coverage!(%Favn.Namespace.Config{coverage: {:set, nil}}), do: :ok
  defp validate_no_inherited_coverage!(%Favn.Namespace.Config{coverage: :unset}), do: :ok

  defp validate_no_inherited_coverage!(%Favn.Namespace.Config{coverage: {:set, _coverage}}) do
    raise ArgumentError, "coverage requires an effective asset window"
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
