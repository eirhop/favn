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

  Asset modules should carry a business-oriented `@moduledoc`. Explain the data
  grain, source, filtering/retention rules, key transformations, and downstream
  purpose. Use the function `@doc` for the operational action performed by
  `asset/1`.

  Use `Favn.MultiAsset` for repetitive generated assets, and `Favn.SQLAsset`
  when the primary body is SQL.

  ## Minimal example

      # lib/my_app/lakehouse/raw/sales/orders.ex
      defmodule MyApp.Lakehouse.Raw.Sales.Orders do
        @moduledoc \"\"\"
        Raw commerce orders as received from the source platform.

        One row represents one source order. Cancelled orders are retained, order
        timestamps are normalized to UTC, and source monetary values are kept in
        the original currency for downstream modeling.
        \"\"\"

        use Favn.Asset

        @doc "Fetch, normalize, and write raw commerce orders."
        meta owner: "data-platform"
        meta category: :sales
        meta tags: [:raw]
        depends MyApp.Lakehouse.Raw.Sales.Customers
        window Favn.Window.daily()
        freshness :daily
        relation true
        def asset(ctx) do
          _asset = ctx.asset
          _window = ctx.window
          :ok
        end
      end

  ## Contract

  - define exactly one public `asset/1`
  - use `@doc` for the real `asset/1` function
  - declare `settings`, `meta`, `depends`, `window`, `freshness`, `retry`,
    `execution_pool`, `relation`, and `runtime_config` before `asset/1`
  - repeat `depends` for multiple upstream dependencies
  - repeat `settings` or `meta` to shallow-merge declarations from left to right
  - use module shorthand in `depends` for another single-asset module

  ## Declarations

  - `@doc`: asset documentation shown in compiled docs and metadata
  - `settings`: non-secret, JSON-like static values compiled into the manifest
  - `meta`: keyword or map metadata such as `owner`, `category`, and `tags`;
    category and tag labels may be atoms or strings and normalize to manifest
    strings
  - `depends`: repeatable dependency declaration
  - `window`: one `Favn.Window.*` spec
  - `freshness`: optional asset freshness policy
  - `retry`: optional node-attempt retry policy overriding the pipeline default
  - `execution_pool`: optional orchestrator admission pool
  - `relation`: optional owned relation declaration

  Top-level settings keys remain atoms for ergonomic runtime access. Nested maps
  are JSON-shaped and therefore use string keys. Later declarations replace an
  earlier value at the same top-level key, including with `nil`:

      settings source: "orders"
      settings request: %{path: "/v1/orders", timeout_ms: 5_000}

      def asset(ctx) do
        source = ctx.asset.settings.source
        path = ctx.asset.settings.request["path"]
        MyApp.Client.fetch(source, path)
      end

  Put secrets, environment-dependent values, and deployment configuration in
  `runtime_config`, not `settings`. They are resolved separately into
  `ctx.runtime_config` and are never persisted as resolved manifest values.

  ## Retry policy

  Attach at most one `retry` directly above `asset/1`. `max_attempts` includes
  the initial attempt and defaults to one. Fixed and bounded exponential
  backoff use the same `%Favn.Retry.Policy{}` contract as pipelines and SQL
  assets:

      retry max_attempts: 4,
             backoff: {:exponential, initial: 5_000, max: 120_000, jitter: 0.2}
      def asset(ctx), do: fetch_orders(ctx)

  An explicit operator override takes precedence over `retry`; `retry` takes
  precedence over the pipeline `retry` default. Policy controls attempt count
  and timing only. Favn repeats a node only for an explicitly retryable,
  known-safe failure. Custom Elixir side effects are not rolled back, and an
  unknown-outcome external write is terminal regardless of `max_attempts`.
  Successful sibling nodes are preserved while this node retries.

  Read [Retries, Replay, And Runtime-Input Pins](retries-and-replay.html) before
  retrying API calls, message publication, files, or other external effects.

  ## Execution Pool

  Use `execution_pool` when the asset body talks to a rate-limited API, source
  database, SFTP server, memory-heavy transform, or another shared resource that
  should be admitted by the orchestrator before code starts running.

      execution_pool :github_api
      def asset(ctx), do: fetch_from_github(ctx)

  Asset-level pools override any pipeline-level default `execution_pool`. The
  pool itself is configured by the orchestrator runtime with `config :favn,
  execution_pools: [...]`. SQL/database `write_concurrency` remains separate and
  protects writer/backend admission only after the asset body has started.

  ## Freshness

  Use `freshness` when a previous successful asset result can satisfy future
  runs without executing the asset again. Attach at most one `freshness` directly
  above `def asset(ctx)`.

  Supported V1 values are:

  - `:daily` or `:day`: one success per local calendar day in `"Etc/UTC"`
  - `{:daily, timezone: "Europe/Oslo"}`: one success per local day in the given timezone
  - `[max_age: {:hours, 6}]`: rolling max age, using `{unit, amount}`
  - `[window_success: true]`: one success for the exact runtime window
  - `:always`: always run when planned

  Windowed assets default to `[window_success: true]` when no explicit freshness
  is declared. Non-windowed assets have no implicit freshness. `:always` is the
  explicit opt-out from that window default.

      window Favn.Window.daily()
      freshness {:daily, timezone: "Europe/Oslo"}
      def asset(ctx), do: build_daily(ctx)

  At runtime, the orchestrator records successful freshness state and can skip
  fresh nodes under the selected refresh policy. Read `Favn.Freshness.Policy` for
  the complete input contract and `Favn.Freshness.Key` for stored state keys.

  ## Runtime Config

  Use `runtime_config/1,2` for runtime values that must be resolved by the runner
  instead of read directly with `System.get_env/1` inside asset code. Reusable
  bundles are defined with `Favn.RuntimeConfig`:

      defmodule MyApp.RuntimeConfigs do
        use Favn.RuntimeConfig

        bundle :source_system,
          segment_id: env!("SOURCE_SYSTEM_SEGMENT_ID"),
          token: secret_env!("SOURCE_SYSTEM_TOKEN")
      end

      runtime_config MyApp.RuntimeConfigs.source_system()

  One-off requirements can be declared inline:

      runtime_config :source_system,
        segment_id: env!("SOURCE_SYSTEM_SEGMENT_ID"),
        token: secret_env!("SOURCE_SYSTEM_TOKEN")

      def asset(ctx) do
        segment_id = ctx.runtime_config.source_system.segment_id
        token = ctx.runtime_config.source_system.token

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

      defmodule MyApp.Lakehouse.Raw.Sales.SourceItems do
        use Favn.Namespace
        use Favn.Asset

        alias MyApp.SourceClient
        alias MyApp.RawLanding

        runtime_config :source_system,
          segment_id: env!("SOURCE_SYSTEM_SEGMENT_ID"),
          token: secret_env!("SOURCE_SYSTEM_TOKEN")

        relation true
        def asset(ctx) do
          relation = ctx.asset.relation
          runtime_config = ctx.runtime_config.source_system

          with {:ok, rows} <- SourceClient.fetch_all(runtime_config),
               :ok <- RawLanding.replace_rows(relation, rows) do
            {:ok,
             %{
               rows_written: length(rows),
               mode: :full_refresh,
                relation: Enum.join([relation.catalog, relation.schema, relation.name], "."),
               loaded_at: DateTime.utc_now(),
               source: %{
                 system: :source_system,
                 segment_id_hash: hash_identity(runtime_config.segment_id)
               }
             }}
          end
        end
      end

  Important rules for this pattern:

  - read source IDs and tokens from `ctx.runtime_config`, not `System.get_env/1`
  - pass only the narrow source config to source clients, not the full `ctx`
  - write raw rows through `Favn.SQLClient` in your landing helper
  - return structured metadata that run inspection can display
  - hash or redact source identities; never return raw segment IDs or tokens

  See `examples/basic-workflow-tutorial` for the canonical working example.

  `depends` supports:

  - `Other.SingleAssetModule`
  - `{Other.MultiAssetModule, :asset_name}`

  `relation` supports:

  - `true` to infer from module name plus namespace defaults
  - keyword or map relation overrides such as `connection`, `catalog`, and `schema`

  ## Compiles To

  The DSL compiles into one canonical `%Favn.Asset{}` with:

  - `ref: {Module, :asset}`
  - `type: :elixir`
  - normalized metadata and dependency refs
  - optional window and relation ownership metadata
  - optional normalized freshness policy
  - optional orchestrator execution pool

  ## Runtime Context

  `ctx` is a `Favn.Run.Context`. In practice, authors most often read:

  - `ctx.asset` for canonical asset metadata
  - `ctx.asset.settings` for compiled non-secret settings
  - `ctx.runtime_config` for resolved runtime config declared with `runtime_config/1,2`
  - `ctx.asset.relation` for owned relation identity
  - `ctx.window` for resolved runtime windows on windowed assets

  ## Common Mistakes

  - defining more than one `asset/1`
  - placing declarations after `asset/1`
  - using an invalid `depends` shape
  - declaring multiple `window` or `relation` attributes
  - declaring multiple `freshness` attributes

  ## See also

  - `Favn.SQLAsset`
  - `Favn.MultiAsset`
  - `Favn.Namespace`
  - `Favn.Freshness.Policy`
  - `Favn.SQLClient`
  """

  alias Favn.Asset.Dependency
  alias Favn.Asset.RelationInput
  alias Favn.Diagnostic
  alias Favn.DSL.AssetDeclarations
  alias Favn.DSL.Compiler, as: DSLCompiler
  alias Favn.Freshness.Policy, as: FreshnessPolicy
  alias Favn.Manifest.Labels
  alias Favn.Namespace
  alias Favn.Ref
  alias Favn.RelationRef
  alias Favn.RuntimeConfig.Requirements
  alias Favn.SQL.SessionRequirements
  alias Favn.SQLAsset.Materialization
  alias Favn.Window.Spec

  @doc false
  defmacro __using__(_opts) do
    quote do
      Favn.DSL.AssetDeclarations.register!(__MODULE__)
      Module.register_attribute(__MODULE__, :favn_single_asset_raw, persist: false)

      import Favn.DSL.AssetDeclarations,
        only: [
          settings: 1,
          meta: 1,
          depends: 1,
          window: 1,
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

      @on_definition Favn.Asset
      @before_compile Favn.Asset
    end
  end

  @doc false
  def __on_definition__(env, kind, name, args, _guards, _body) do
    AssetDeclarations.reject_legacy_attributes!(env.module, env.file, env.line)
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

    AssetDeclarations.reject_legacy_attributes!(env.module, env.file, env.line)

    case Enum.flat_map(
           [
             :settings,
             :meta,
             :depends,
             :freshness,
             :retry,
             :execution_pool,
             :runtime_config,
             :window,
             :relation
           ],
           &AssetDeclarations.values(env.module, &1)
         ) do
      [] ->
        :ok

      _ ->
        DSLCompiler.compile_error!(
          env.file,
          env.line,
          "asset declarations must appear before def asset(ctx)"
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
          doc: String.t() | nil,
          file: String.t(),
          line: pos_integer(),
          meta: map(),
          depends_on: [Ref.t()],
          dependencies: [Dependency.t()],
          settings: Favn.Settings.t(),
          window_spec: Spec.t() | nil,
          freshness: FreshnessPolicy.t() | nil,
          retry_policy: Favn.Retry.Policy.t() | nil,
          execution_pool: atom() | nil,
          relation: RelationRef.t() | nil,
          materialization: Favn.SQLAsset.Materialization.t() | nil,
          relation_inputs: [RelationInput.t()],
          runtime_config: Requirements.declarations(),
          session_requirements: SessionRequirements.t(),
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
    :doc,
    :file,
    :line,
    type: :elixir,
    meta: %{},
    depends_on: [],
    dependencies: [],
    settings: %{},
    window_spec: nil,
    freshness: nil,
    retry_policy: nil,
    execution_pool: nil,
    relation: nil,
    materialization: nil,
    relation_inputs: [],
    runtime_config: %{},
    session_requirements: %SessionRequirements{version: 1},
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
    settings = Favn.Settings.normalize!(asset.settings)
    validate_window_spec!(asset.window_spec)
    validate_freshness!(asset.freshness)
    validate_retry_policy!(asset.retry_policy)
    validate_execution_pool!(asset.execution_pool)
    validate_relation!(asset.relation)
    validate_runtime_config!(asset.runtime_config)
    validate_session_requirements!(asset.session_requirements)
    validate_type!(asset.type)
    validate_materialization!(asset.materialization)

    %{asset | meta: meta, settings: settings}
  end

  @doc """
  Normalize and validate authored asset metadata (`meta`).

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

    Enum.reduce(meta, %{}, fn {key, value}, acc ->
      case normalize_meta_key(key) do
        :owner when is_binary(value) ->
          Map.put(acc, :owner, value)

        :owner ->
          raise ArgumentError, "asset meta owner must be a string, got: #{inspect(value)}"

        :category when is_atom(value) or is_binary(value) ->
          Map.put(acc, :category, Labels.normalize_label!(value))

        :category ->
          raise ArgumentError,
                "asset meta category must be an atom or string, got: #{inspect(value)}"

        :tags when is_list(value) ->
          Map.put(acc, :tags, Labels.normalize_labels!(value))

        :tags ->
          raise ArgumentError, "asset meta tags must be a list, got: #{inspect(value)}"

        nil ->
          raise ArgumentError,
                "asset meta contains unsupported key #{inspect(key)}; allowed keys: #{inspect(supported)}"
      end
    end)
  end

  def normalize_meta!(meta),
    do: raise(ArgumentError, "asset meta must be a keyword list or map, got: #{inspect(meta)}")

  defp normalize_meta_key(:owner), do: :owner
  defp normalize_meta_key(:category), do: :category
  defp normalize_meta_key(:tags), do: :tags
  defp normalize_meta_key("owner"), do: :owner
  defp normalize_meta_key("category"), do: :category
  defp normalize_meta_key("tags"), do: :tags
  defp normalize_meta_key(_other), do: nil

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

  defp validate_runtime_config!(runtime_config) when is_map(runtime_config) do
    Requirements.normalize!(runtime_config)
    :ok
  end

  defp validate_runtime_config!(runtime_config) do
    raise ArgumentError,
          "asset runtime_config must be a map, got: #{inspect(runtime_config)}"
  end

  defp validate_session_requirements!(%SessionRequirements{} = requirements) do
    SessionRequirements.validate!(requirements)
    :ok
  end

  defp validate_session_requirements!(requirements) do
    raise ArgumentError,
          "asset session_requirements must be a Favn.SQL.SessionRequirements struct, got: #{inspect(requirements)}"
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

  defp validate_freshness!(nil), do: :ok

  defp validate_freshness!(%FreshnessPolicy{} = policy) do
    case FreshnessPolicy.validate(policy) do
      {:ok, ^policy} -> :ok
      {:ok, _normalized} -> raise ArgumentError, "asset freshness must already be normalized"
      {:error, reason} -> raise ArgumentError, "invalid asset freshness: #{inspect(reason)}"
    end
  end

  defp validate_freshness!(value) do
    raise ArgumentError,
          "asset freshness must be a Favn.Freshness.Policy or nil, got: #{inspect(value)}"
  end

  defp validate_execution_pool!(nil), do: :ok
  defp validate_execution_pool!(value) when is_atom(value), do: :ok

  defp validate_execution_pool!(value) do
    raise ArgumentError,
          "asset execution_pool must be an atom or nil, got: #{inspect(value)}"
  end

  defp validate_retry_policy!(nil), do: :ok

  defp validate_retry_policy!(%Favn.Retry.Policy{} = policy) do
    case Favn.Retry.Policy.validate(policy) do
      {:ok, ^policy} -> :ok
      {:ok, _normalized} -> raise ArgumentError, "asset retry policy must already be normalized"
      {:error, reason} -> raise ArgumentError, "invalid asset retry policy: #{inspect(reason)}"
    end
  end

  defp validate_retry_policy!(value) do
    raise ArgumentError,
          "asset retry_policy must be a Favn.Retry.Policy or nil, got: #{inspect(value)}"
  end

  @doc false
  @spec normalize_freshness!([term()], Spec.t() | nil, String.t()) :: FreshnessPolicy.t() | nil
  def normalize_freshness!([], nil, _attachment), do: nil

  def normalize_freshness!([], %Spec{}, _attachment) do
    {:ok, freshness} = FreshnessPolicy.window_success()
    freshness
  end

  def normalize_freshness!([value], _window_spec, _attachment),
    do: FreshnessPolicy.from_value!(value)

  def normalize_freshness!([_a, _b | _rest], _window_spec, attachment) do
    raise ArgumentError,
          "multiple freshness declarations are not allowed; use at most one freshness #{attachment}"
  end

  def normalize_freshness!(value, _window_spec, _attachment) do
    raise ArgumentError,
          "invalid freshness value #{inspect(value)}; expected a Favn.Freshness.Policy V1 value"
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

    settings = AssetDeclarations.take(env.module, :settings)
    depends = AssetDeclarations.take(env.module, :depends)
    freshness = AssetDeclarations.take(env.module, :freshness)
    retry = AssetDeclarations.take(env.module, :retry)
    execution_pool = AssetDeclarations.take(env.module, :execution_pool)
    meta = AssetDeclarations.take(env.module, :meta)
    runtime_config = AssetDeclarations.take(env.module, :runtime_config)
    window = AssetDeclarations.take(env.module, :window)
    relation = AssetDeclarations.take(env.module, :relation)
    validate_relation_attr!(relation, env)

    Module.put_attribute(env.module, :favn_single_asset_raw, %{
      module: env.module,
      name: :asset,
      arity: 1,
      doc: DSLCompiler.normalize_doc(Module.get_attribute(env.module, :doc)),
      file: DSLCompiler.normalize_file(env.file),
      line: env.line,
      settings: settings,
      depends: depends,
      freshness: freshness,
      retry: retry,
      execution_pool: execution_pool,
      meta: meta,
      runtime_config: runtime_config,
      window: window,
      relation: relation
    })
  end

  defp build_single_asset!(raw_asset) do
    depends_on = normalize_single_asset_depends!(raw_asset.depends, raw_asset)
    settings = normalize_single_asset_settings!(raw_asset.settings, raw_asset)
    meta = normalize_single_asset_meta!(raw_asset.meta, raw_asset)
    runtime_config = normalize_single_asset_runtime_config!(raw_asset.runtime_config, raw_asset)
    window_spec = normalize_single_asset_window!(raw_asset.window, raw_asset)
    freshness = normalize_single_asset_freshness!(raw_asset.freshness, window_spec, raw_asset)
    retry_policy = normalize_single_asset_retry!(raw_asset.retry, raw_asset)
    execution_pool = normalize_execution_pool!(raw_asset.execution_pool, raw_asset)

    asset = %__MODULE__{
      module: raw_asset.module,
      name: :asset,
      entrypoint: :asset,
      ref: Ref.new(raw_asset.module, :asset),
      arity: 1,
      type: :elixir,
      doc: raw_asset.doc,
      file: raw_asset.file,
      line: raw_asset.line,
      meta: meta,
      depends_on: depends_on,
      settings: settings,
      runtime_config: runtime_config,
      window_spec: window_spec,
      freshness: freshness,
      retry_policy: retry_policy,
      execution_pool: execution_pool
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
            "invalid depends entry #{inspect(module)}; expected Module or {Module, :asset_name}"
          )
        end

      {module, name} when is_atom(module) and is_atom(name) ->
        if DSLCompiler.module_atom?(module) do
          Ref.new(module, name)
        else
          DSLCompiler.compile_error!(
            raw_asset.file,
            raw_asset.line,
            "invalid depends entry #{inspect({module, name})}; expected Module or {Module, :asset_name}"
          )
        end

      dependency ->
        DSLCompiler.compile_error!(
          raw_asset.file,
          raw_asset.line,
          "invalid depends entry #{inspect(dependency)}; expected Module or {Module, :asset_name}"
        )
    end)
  end

  defp normalize_single_asset_meta!(meta, raw_asset) do
    Enum.reduce(meta, %{}, fn declaration, acc -> Map.merge(acc, normalize_meta!(declaration)) end)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_asset.file, raw_asset.line, error.message)
  end

  defp normalize_single_asset_settings!(settings, raw_asset) do
    Favn.Settings.merge_all!(settings)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_asset.file, raw_asset.line, error.message)
  end

  defp normalize_single_asset_runtime_config!(entries, raw_asset) do
    inherited = Namespace.resolve_runtime_config(raw_asset.module)
    Requirements.merge_all!(inherited ++ entries, consumer: raw_asset.module)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_asset.file, raw_asset.line, error.message)
  end

  defp normalize_single_asset_window!([], _raw_asset), do: nil
  defp normalize_single_asset_window!([%Spec{} = spec], _raw_asset), do: spec

  defp normalize_single_asset_window!([_a, _b | _rest], raw_asset) do
    DSLCompiler.compile_error!(
      raw_asset.file,
      raw_asset.line,
      "multiple window attributes are not allowed; use at most one window for def asset(ctx)"
    )
  end

  defp normalize_single_asset_window!(value, raw_asset) do
    DSLCompiler.compile_error!(
      raw_asset.file,
      raw_asset.line,
      "invalid window value #{inspect(value)}; expected Favn.Window spec like Favn.Window.daily()"
    )
  end

  defp normalize_single_asset_freshness!(freshness, window_spec, raw_asset) do
    normalize_freshness!(freshness, window_spec, "for def asset(ctx)")
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_asset.file, raw_asset.line, error.message)
  end

  defp normalize_single_asset_retry!([], _raw_asset), do: nil
  defp normalize_single_asset_retry!([value], _raw_asset), do: Favn.Retry.Policy.new!(value)

  defp normalize_single_asset_retry!([_first, _second | _rest], raw_asset) do
    DSLCompiler.compile_error!(
      raw_asset.file,
      raw_asset.line,
      "multiple retry attributes are not allowed; use at most one retry for def asset(ctx)"
    )
  end

  defp normalize_single_asset_retry!(value, raw_asset) do
    DSLCompiler.compile_error!(
      raw_asset.file,
      raw_asset.line,
      "invalid retry value #{inspect(value)}; expected a retry keyword list"
    )
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_asset.file, raw_asset.line, error.message)
  end

  defp normalize_execution_pool!([], _raw_asset), do: nil

  defp normalize_execution_pool!([value], _raw_asset) when is_atom(value) and not is_nil(value),
    do: value

  defp normalize_execution_pool!([_first, _second | _rest], raw_asset) do
    DSLCompiler.compile_error!(
      raw_asset.file,
      raw_asset.line,
      "multiple execution_pool declarations are not allowed"
    )
  end

  defp normalize_execution_pool!(value, raw_asset) do
    DSLCompiler.compile_error!(
      raw_asset.file,
      raw_asset.line,
      "invalid execution_pool value #{inspect(value)}; expected a non-nil atom"
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
        "invalid relation value #{inspect(relation)}; expected true, a keyword list, or a map"
      )
    end
  end

  defp validate_relation_attr!([_a, _b | _rest], env) do
    DSLCompiler.compile_error!(
      env.file,
      env.line,
      "multiple relation attributes are not allowed; use at most one relation for def asset(ctx)"
    )
  end

  defp validate_no_stray_asset_attributes!(env, kind, name, arity) do
    declarations =
      Enum.flat_map(
        [
          :settings,
          :meta,
          :depends,
          :freshness,
          :retry,
          :execution_pool,
          :runtime_config,
          :window,
          :relation
        ],
        &AssetDeclarations.values(env.module, &1)
      )

    if declarations != [] do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "asset declarations before #{kind} #{name}/#{arity} require def asset(ctx) immediately below them"
      )
    else
      :ok
    end
  end
end
