defmodule Favn.Asset do
  @moduledoc """
  Preferred single-asset Elixir DSL and the canonical `%Favn.Asset{}` struct.

  Use this module when one module should define exactly one executable Elixir
  asset. That asset is always declared as `def asset(ctx)` and compiles to the
  canonical ref `{Module, :asset}`.

  ## When to use it

  Use `Favn.Asset` when:

  - one module should represent one asset
  - the runtime logic is normal Elixir code
  - you want the clearest public authoring surface for humans and AI agents

  Use `Favn.MultiAsset` for repetitive generated assets, and `Favn.SQLAsset`
  when the primary body is SQL.

  ## Minimal example

      defmodule MyApp.Raw.Sales.Orders do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "sales"]
        use Favn.Asset

        @doc "Extract raw orders"
        @meta owner: "data-platform", category: :sales, tags: [:raw]
        @depends MyApp.Raw.Sales.Customers
        @window Favn.Window.daily()
        @relation true
        def asset(ctx) do
          _asset = ctx.asset
          _window = ctx.window
          :ok
        end
      end

  ## Contract

  - define exactly one public `asset/1`
  - attach `@doc`, `@meta`, `@depends`, `@window`, and `@relation` directly above `def asset(ctx)`
  - repeat `@depends` for multiple upstream dependencies
  - use module shorthand in `@depends` for another single-asset module

  ## Attributes

  - `@doc`: asset documentation shown in compiled docs and metadata
  - `@meta`: keyword or map metadata such as `owner`, `category`, and `tags`
  - `@depends`: repeatable dependency declaration
  - `@window`: one `Favn.Window.*` spec
  - `@relation`: optional owned relation declaration

  ## Runtime Config

  Use `source_config/2` for runtime values that must be resolved by the runner
  instead of read directly with `System.get_env/1` inside asset code:

      source_config :source_system,
        segment_id: env!("SOURCE_SYSTEM_SEGMENT_ID"),
        token: secret_env!("SOURCE_SYSTEM_TOKEN")

      def asset(ctx) do
        segment_id = ctx.config.source_system.segment_id
        token = ctx.config.source_system.token

        MyApp.Client.fetch_orders(segment_id, token)
        :ok
      end

  The manifest records required environment variable names and secret flags, but
  not resolved values. Missing required environment variables fail before asset
  code runs with a structured error such as `missing_env SOURCE_SYSTEM_TOKEN`.

  ## Source-system raw landing pattern

  A common Elixir asset shape is landing records from an external source system
  into a raw SQL relation before downstream `Favn.SQLAsset` transformations run.

  Keep the source client and SQL landing helper in your own application, not in
  Favn. The asset should coordinate the boundary:

      defmodule MyApp.Warehouse.Raw.SourceItems do
        use Favn.Namespace
        use Favn.Asset

        alias MyApp.SourceClient
        alias MyApp.RawLanding

        source_config :source_system,
          segment_id: env!("SOURCE_SYSTEM_SEGMENT_ID"),
          token: secret_env!("SOURCE_SYSTEM_TOKEN")

        @relation true
        def asset(ctx) do
          relation = ctx.asset.relation
          source_config = ctx.config.source_system

          with {:ok, rows} <- SourceClient.fetch_all(source_config),
               :ok <- RawLanding.replace_rows(relation, rows) do
            {:ok,
             %{
               rows_written: length(rows),
               mode: :full_refresh,
               relation: Enum.join([relation.schema, relation.name], "."),
               loaded_at: DateTime.utc_now(),
               source: %{
                 system: :source_system,
                 segment_id_hash: hash_identity(source_config.segment_id)
               }
             }}
          end
        end
      end

  Important rules for this pattern:

  - read source IDs and tokens from `ctx.config`, not `System.get_env/1`
  - pass only the narrow source config to source clients, not the full `ctx`
  - write raw rows through `Favn.SQLClient` in your landing helper
  - return structured metadata that run inspection can display
  - hash or redact source identities; never return raw segment IDs or tokens

  See `examples/basic-workflow-tutorial` for the canonical working example.

  `@depends` supports:

  - `Other.SingleAssetModule`
  - `{Other.MultiAssetModule, :asset_name}`

  `@relation` supports:

  - `true` to infer from module name plus namespace defaults
  - keyword or map relation overrides such as `connection`, `catalog`, and `schema`

  ## Compiles To

  The DSL compiles into one canonical `%Favn.Asset{}` with:

  - `ref: {Module, :asset}`
  - `type: :elixir`
  - normalized metadata and dependency refs
  - optional window and relation ownership metadata

  ## Runtime Context

  `ctx` is a `Favn.Run.Context`. In practice, authors most often read:

  - `ctx.asset` for canonical asset metadata
  - `ctx.asset.config` for compiled config when present
  - `ctx.config` for resolved runtime config declared with `source_config/2`
  - `ctx.asset.relation` for owned relation identity
  - `ctx.window` for resolved runtime windows on windowed assets

  ## Common Mistakes

  - defining more than one `asset/1`
  - attaching DSL attributes to another function
  - using an invalid `@depends` shape
  - declaring multiple `@window` or `@relation` attributes

  ## See also

  - `Favn.SQLAsset`
  - `Favn.MultiAsset`
  - `Favn.Namespace`
  - `Favn.SQLClient`
  """

  alias Favn.Asset.Dependency
  alias Favn.Asset.RelationInput
  alias Favn.Diagnostic
  alias Favn.DSL.Compiler, as: DSLCompiler
  alias Favn.Ref
  alias Favn.RelationRef
  alias Favn.RuntimeConfig.Requirements
  alias Favn.SQLAsset.Materialization
  alias Favn.Window.Spec

  @doc false
  defmacro __using__(_opts) do
    quote do
      Module.register_attribute(__MODULE__, :depends, accumulate: true)
      Module.register_attribute(__MODULE__, :meta, persist: false)
      Module.register_attribute(__MODULE__, :relation, accumulate: true)
      Module.register_attribute(__MODULE__, :runtime_config, accumulate: true)
      Module.register_attribute(__MODULE__, :window, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_single_asset_raw, persist: false)

      import Favn.Asset, only: [source_config: 2, env!: 1, secret_env!: 1]

      @on_definition Favn.Asset
      @before_compile Favn.Asset
    end
  end

  @doc """
  Declares runtime configuration required by this asset.

  Values are resolved by the runner at execution time and exposed through
  `ctx.config`. Runtime values are not embedded in the manifest.
  """
  defmacro source_config(scope, fields) do
    quote bind_quoted: [scope: scope, fields: fields] do
      {runtime_config_scope, runtime_config_fields} =
        if is_atom(scope), do: {scope, fields}, else: {fields, scope}

      Module.put_attribute(
        __MODULE__,
        :runtime_config,
        %{runtime_config_scope => runtime_config_fields}
      )
    end
  end

  @doc """
  Declares a required environment variable runtime config value.
  """
  defmacro env!(key) when is_binary(key) do
    quote do
      Favn.RuntimeConfig.Ref.env!(unquote(key))
    end
  end

  @doc """
  Declares a required secret environment variable runtime config value.
  """
  defmacro secret_env!(key) when is_binary(key) do
    quote do
      Favn.RuntimeConfig.Ref.secret_env!(unquote(key))
    end
  end

  @doc false
  def __on_definition__(env, kind, name, args, _guards, _body) do
    arity = length(args || [])

    case {kind, name, arity} do
      {:def, :asset, 1} ->
        capture_single_asset_definition!(env)

      {:def, :asset, _other_arity} ->
        DSLCompiler.compile_error!(
          env.file,
          env.line,
          "Favn.Asset requires exactly one public asset/1 function"
        )

      {:defp, :asset, _arity} ->
        DSLCompiler.compile_error!(
          env.file,
          env.line,
          "Favn.Asset requires a public def asset(ctx)"
        )

      {kind, _name, _arity} when kind in [:def, :defp] ->
        validate_no_stray_asset_attributes!(env, kind, name, arity)

      _ ->
        :ok
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    raw_asset = Module.get_attribute(env.module, :favn_single_asset_raw)

    if is_nil(raw_asset) do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "Favn.Asset modules must define exactly one public asset/1 function"
      )
    end

    case {
      Module.get_attribute(env.module, :depends),
      Module.get_attribute(env.module, :meta),
      Module.get_attribute(env.module, :runtime_config),
      Module.get_attribute(env.module, :window),
      Module.get_attribute(env.module, :relation)
    } do
      {[], nil, [], [], []} ->
        :ok

      _ ->
        DSLCompiler.compile_error!(
          env.file,
          env.line,
          "@depends/@meta/@window/@relation and source_config/2 must be attached to def asset(ctx)"
        )
    end

    asset = build_single_asset!(raw_asset)

    quote do
      @doc false
      @spec __favn_assets__() :: [Favn.Asset.t()]
      def __favn_assets__, do: [unquote(Macro.escape(asset))]

      @doc false
      def __favn_assets_raw__, do: [unquote(Macro.escape(raw_asset))]

      @doc false
      def __favn_single_asset__, do: true
    end
  end

  @type t :: %__MODULE__{
          module: module(),
          name: atom(),
          entrypoint: atom() | nil,
          ref: Ref.t(),
          arity: non_neg_integer(),
          type: :elixir | :sql | :source,
          title: String.t() | nil,
          doc: String.t() | nil,
          file: String.t(),
          line: pos_integer(),
          meta: map(),
          depends_on: [Ref.t()],
          dependencies: [Dependency.t()],
          config: map(),
          window_spec: Spec.t() | nil,
          relation: RelationRef.t() | nil,
          materialization: Favn.SQLAsset.Materialization.t() | nil,
          relation_inputs: [RelationInput.t()],
          runtime_config: Requirements.declarations(),
          diagnostics: [Diagnostic.t()]
        }

  @typedoc """
  Canonical return shape expected from asset function execution.
  """
  @type return_value :: :ok | {:ok, map()} | {:error, term()}

  defstruct [
    :module,
    :name,
    :entrypoint,
    :ref,
    :arity,
    :title,
    :doc,
    :file,
    :line,
    type: :elixir,
    meta: %{},
    depends_on: [],
    dependencies: [],
    config: %{},
    window_spec: nil,
    relation: nil,
    materialization: nil,
    relation_inputs: [],
    runtime_config: %{},
    diagnostics: []
  ]

  @doc """
  Validate a canonical `%Favn.Asset{}`.

  This function expects an already-built asset struct. In particular,
  `depends_on` must already be a list of `Favn.Ref.t()` values.

  ## Raises

    * `ArgumentError` when `meta` is not a map
    * `ArgumentError` when `depends_on` is not a list of canonical refs
  """
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = asset) do
    meta = normalize_meta!(asset.meta)
    validate_depends_on!(asset.depends_on)
    validate_entrypoint!(asset.entrypoint)
    validate_config!(asset.config)
    validate_window_spec!(asset.window_spec)
    validate_relation!(asset.relation)
    validate_runtime_config!(asset.runtime_config)
    validate_type!(asset.type)
    validate_materialization!(asset.materialization)

    %{asset | meta: meta}
  end

  @doc """
  Normalize and validate authored asset metadata (`@meta`).

  This is for DSL/catalog metadata only and is separate from runtime success
  return metadata, which must be a map.
  """
  @spec normalize_meta!(map() | keyword() | nil) :: map()
  def normalize_meta!(nil), do: %{}

  def normalize_meta!(meta) when is_list(meta) do
    if Keyword.keyword?(meta) do
      normalize_meta!(Map.new(meta))
    else
      raise ArgumentError, "asset meta must be a keyword list or map, got: #{inspect(meta)}"
    end
  end

  def normalize_meta!(meta) when is_map(meta) do
    supported = [:owner, :category, :tags]

    Enum.each(meta, fn
      {:owner, owner} when is_binary(owner) ->
        :ok

      {:owner, value} ->
        raise ArgumentError, "asset meta owner must be a string, got: #{inspect(value)}"

      {:category, category} when is_atom(category) ->
        :ok

      {:category, value} ->
        raise ArgumentError, "asset meta category must be an atom, got: #{inspect(value)}"

      {:tags, tags} when is_list(tags) ->
        Enum.each(tags, fn
          tag when is_atom(tag) or is_binary(tag) ->
            :ok

          tag ->
            raise ArgumentError,
                  "asset meta tags entries must be atoms or strings, got: #{inspect(tag)}"
        end)

      {:tags, value} ->
        raise ArgumentError, "asset meta tags must be a list, got: #{inspect(value)}"

      {key, _value} ->
        if key in supported do
          :ok
        else
          raise ArgumentError,
                "asset meta contains unsupported key #{inspect(key)}; allowed keys: [:owner, :category, :tags]"
        end
    end)

    meta
  end

  def normalize_meta!(meta),
    do: raise(ArgumentError, "asset meta must be a keyword list or map, got: #{inspect(meta)}")

  defp validate_depends_on!(depends_on) when is_list(depends_on) do
    Enum.each(depends_on, fn
      {module, name} when is_atom(module) and is_atom(name) ->
        :ok

      dependency ->
        raise ArgumentError,
              "asset depends_on must be a list of Favn.Ref values, got: #{inspect(dependency)}"
    end)
  end

  defp validate_depends_on!(depends_on) do
    raise ArgumentError,
          "asset depends_on must be a list of Favn.Ref values, got: #{inspect(depends_on)}"
  end

  defp validate_entrypoint!(entrypoint) when is_atom(entrypoint) or is_nil(entrypoint), do: :ok

  defp validate_entrypoint!(entrypoint) do
    raise ArgumentError,
          "asset entrypoint must be an atom or nil, got: #{inspect(entrypoint)}"
  end

  defp validate_config!(config) when is_map(config), do: :ok

  defp validate_config!(config) do
    raise ArgumentError, "asset config must be a map, got: #{inspect(config)}"
  end

  defp validate_runtime_config!(runtime_config) when is_map(runtime_config) do
    Requirements.normalize!(runtime_config)
    :ok
  end

  defp validate_runtime_config!(runtime_config) do
    raise ArgumentError,
          "asset runtime_config must be a map, got: #{inspect(runtime_config)}"
  end

  defp validate_window_spec!(nil), do: :ok

  defp validate_window_spec!(%Spec{} = spec) do
    case Spec.validate(spec) do
      :ok -> :ok
      {:error, reason} -> raise ArgumentError, "invalid asset window_spec: #{inspect(reason)}"
    end
  end

  defp validate_window_spec!(value) do
    raise ArgumentError,
          "asset window_spec must be a Favn.Window.Spec or nil, got: #{inspect(value)}"
  end

  defp validate_relation!(nil), do: :ok
  defp validate_relation!(%RelationRef{} = relation_ref), do: RelationRef.validate!(relation_ref)

  defp validate_relation!(value) do
    raise ArgumentError,
          "asset relation must be a Favn.RelationRef or nil, got: #{inspect(value)}"
  end

  defp validate_type!(type) when type in [:elixir, :sql, :source], do: :ok

  defp validate_type!(value) do
    raise ArgumentError, "asset type must be :elixir, :sql, or :source, got: #{inspect(value)}"
  end

  defp validate_materialization!(nil), do: :ok

  defp validate_materialization!(materialization) do
    case Materialization.normalize!(materialization) do
      normalized when normalized == materialization -> :ok
      _normalized -> raise ArgumentError, "asset materialization must already be normalized"
    end
  end

  defp capture_single_asset_definition!(env) do
    if Module.get_attribute(env.module, :favn_single_asset_raw) do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "Favn.Asset modules can define only one asset/1 function"
      )
    end

    depends = env.module |> Module.get_attribute(:depends) |> Enum.reverse()
    meta = Module.get_attribute(env.module, :meta)
    runtime_config = env.module |> Module.get_attribute(:runtime_config) |> Enum.reverse()
    window = env.module |> Module.get_attribute(:window) |> Enum.reverse()
    relation = env.module |> Module.get_attribute(:relation) |> Enum.reverse()
    validate_relation_attr!(relation, env)

    Module.delete_attribute(env.module, :depends)
    Module.delete_attribute(env.module, :meta)
    Module.delete_attribute(env.module, :runtime_config)
    Module.delete_attribute(env.module, :window)
    Module.delete_attribute(env.module, :relation)

    Module.put_attribute(env.module, :favn_single_asset_raw, %{
      module: env.module,
      name: :asset,
      arity: 1,
      doc: DSLCompiler.normalize_doc(Module.get_attribute(env.module, :doc)),
      file: DSLCompiler.normalize_file(env.file),
      line: env.line,
      depends: depends,
      meta: meta,
      runtime_config: runtime_config,
      window: window,
      relation: relation
    })
  end

  defp build_single_asset!(raw_asset) do
    depends_on = normalize_single_asset_depends!(raw_asset.depends, raw_asset)
    meta = normalize_single_asset_meta!(raw_asset.meta, raw_asset)
    runtime_config = normalize_single_asset_runtime_config!(raw_asset.runtime_config, raw_asset)
    window_spec = normalize_single_asset_window!(raw_asset.window, raw_asset)

    asset = %__MODULE__{
      module: raw_asset.module,
      name: :asset,
      entrypoint: :asset,
      ref: Ref.new(raw_asset.module, :asset),
      arity: 1,
      type: :elixir,
      title: nil,
      doc: raw_asset.doc,
      file: raw_asset.file,
      line: raw_asset.line,
      meta: meta,
      depends_on: depends_on,
      config: %{},
      runtime_config: runtime_config,
      window_spec: window_spec
    }

    try do
      validate!(asset)
    rescue
      error in ArgumentError ->
        DSLCompiler.compile_error!(raw_asset.file, raw_asset.line, error.message)
    end
  end

  defp normalize_single_asset_depends!(depends, raw_asset) do
    Enum.map(depends, fn
      module when is_atom(module) ->
        if DSLCompiler.module_atom?(module) do
          Ref.new(module, :asset)
        else
          DSLCompiler.compile_error!(
            raw_asset.file,
            raw_asset.line,
            "invalid @depends entry #{inspect(module)}; expected Module or {Module, :asset_name}"
          )
        end

      {module, name} when is_atom(module) and is_atom(name) ->
        if DSLCompiler.module_atom?(module) do
          Ref.new(module, name)
        else
          DSLCompiler.compile_error!(
            raw_asset.file,
            raw_asset.line,
            "invalid @depends entry #{inspect({module, name})}; expected Module or {Module, :asset_name}"
          )
        end

      dependency ->
        DSLCompiler.compile_error!(
          raw_asset.file,
          raw_asset.line,
          "invalid @depends entry #{inspect(dependency)}; expected Module or {Module, :asset_name}"
        )
    end)
  end

  defp normalize_single_asset_meta!(meta, raw_asset) do
    normalize_meta!(meta)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_asset.file, raw_asset.line, error.message)
  end

  defp normalize_single_asset_runtime_config!([], _raw_asset), do: %{}

  defp normalize_single_asset_runtime_config!(entries, raw_asset) do
    entries
    |> Enum.reduce(%{}, &Map.merge(&2, &1))
    |> normalize_runtime_config_entry_order()
    |> Requirements.normalize!()
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_asset.file, raw_asset.line, error.message)
  end

  defp normalize_runtime_config_entry_order(%{} = declarations) do
    Map.new(declarations, fn
      {scope, fields} when is_atom(scope) -> {scope, fields}
      {fields, scope} when is_atom(scope) -> {scope, fields}
    end)
  end

  defp normalize_single_asset_window!([], _raw_asset), do: nil
  defp normalize_single_asset_window!([%Spec{} = spec], _raw_asset), do: spec

  defp normalize_single_asset_window!([_a, _b | _rest], raw_asset) do
    DSLCompiler.compile_error!(
      raw_asset.file,
      raw_asset.line,
      "multiple @window attributes are not allowed; use at most one @window for def asset(ctx)"
    )
  end

  defp normalize_single_asset_window!(value, raw_asset) do
    DSLCompiler.compile_error!(
      raw_asset.file,
      raw_asset.line,
      "invalid @window value #{inspect(value)}; expected Favn.Window spec like Favn.Window.daily()"
    )
  end

  defp validate_relation_attr!([], _env), do: :ok

  defp validate_relation_attr!([relation], env) do
    valid? = DSLCompiler.valid_relation_attr_value?(relation)

    if valid? do
      :ok
    else
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "invalid @relation value #{inspect(relation)}; expected true, a keyword list, or a map"
      )
    end
  end

  defp validate_relation_attr!([_a, _b | _rest], env) do
    DSLCompiler.compile_error!(
      env.file,
      env.line,
      "multiple @relation attributes are not allowed; use at most one @relation for def asset(ctx)"
    )
  end

  defp validate_no_stray_asset_attributes!(env, kind, name, arity) do
    depends = Module.get_attribute(env.module, :depends)
    meta = Module.get_attribute(env.module, :meta)
    window = Module.get_attribute(env.module, :window)
    relation = Module.get_attribute(env.module, :relation)

    if depends != [] or not is_nil(meta) or window != [] or relation != [] do
      Module.delete_attribute(env.module, :depends)
      Module.delete_attribute(env.module, :meta)
      Module.delete_attribute(env.module, :window)
      Module.delete_attribute(env.module, :relation)

      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "@depends/@meta/@window/@relation on #{kind} #{name}/#{arity} requires def asset(ctx) immediately below those attributes"
      )
    else
      :ok
    end
  end
end
