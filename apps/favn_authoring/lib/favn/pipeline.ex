defmodule Favn.Pipeline do
  @moduledoc """
  Public DSL for pipeline composition.

  Pipelines select assets and attach orchestration configuration without
  redefining dependency logic. Favn still derives execution order from the asset
  graph.

  ## When to use it

  Use `Favn.Pipeline` when you want a named operational unit such as a daily
  run, a scheduled sync, or a curated asset subset.

  ## Minimal example

      defmodule MyApp.Pipelines.DailySales do
        use Favn.Pipeline

        pipeline :daily_sales do
          asset MyApp.Lakehouse.Mart.Sales.FctOrders
          deps :all
          schedule cron: "0 2 * * *", timezone: "Etc/UTC"
        end
      end

  ## Clauses

  A pipeline block supports these clauses:

  - `asset ref_or_module`: add one target asset
  - `assets refs_or_modules`: add many target assets
  - `select do ... end`: additive selectors using `asset`, `tag`, `category`, and `module`
  - `deps :all | :none`: include upstream dependencies or not
  - `settings map_or_keyword`: non-secret static values exposed through `ctx.pipeline.settings`
  - `meta map_or_keyword`: descriptive metadata for operators and tooling
  - `schedule {Module, :name}`: reference a named schedule
  - `schedule cron: ..., ...`: declare an inline schedule
  - `window atom`: attach a pipeline window policy
  - `retry keyword`: default node-attempt retry policy for selected assets
  - `max_concurrency positive_integer`: limit asset steps admitted from one run
  - `execution_pool atom`: default shared execution pool for selected assets
  - `resource_recovery :retry_remaining, opts`: opt into linked recovery runs after a resource probe succeeds
  - `source atom`: attach a named pipeline source
  - `outputs [atom, ...]`: attach named outputs

  ## Execution Concurrency

  `max_concurrency` is a per-run orchestrator admission limit. It limits how many
  runnable asset steps from one pipeline run may execute at once without changing
  dependency graph semantics.

      pipeline :raw_api_ingestion do
        assets MyApp.Lakehouse.Raw.ExternalApi
        max_concurrency 1
      end

  `execution_pool` declares the default shared pool for assets in the pipeline.
  Asset-level `execution_pool` declarations override this default for that
  asset. Pools are configured at runtime with `config :favn, execution_pools:
  [...]`; the orchestrator owns admission globally, not the runner.

      pipeline :raw_github_ingestion do
        assets MyApp.Lakehouse.Raw.GitHub
        execution_pool :github_api
        max_concurrency 2
      end

  Execution concurrency applies before the asset body starts. SQL
  `write_concurrency` remains separate and protects only SQL/backend writer
  admission after asset execution has begun.

  ## Retry policy

  `retry` defines the default node-attempt policy for assets selected by this
  pipeline. `max_attempts` includes the initial attempt and defaults to one when
  no policy is declared:

      pipeline :raw_api_ingestion do
        assets MyApp.Lakehouse.Raw.ExternalApi

        retry max_attempts: 3,
              backoff: {:exponential, initial: 5_000, max: 300_000, jitter: 0.2}
      end

  The effective precedence is explicit operator override, asset `retry`, this
  pipeline default, then one attempt. It is frozen into each planned node.
  Retry policy controls count and timing; it never makes an unsafe or
  unknown-outcome write retryable. A safely failed node can repeat while its
  successful siblings remain complete.

  Schedules remain separate: overlap and missed-occurrence settings decide
  whether another run is admitted. They do not retry this run or share its
  attempt count/runtime-input pins. Read
  [Retries, Replay, And Runtime-Input Pins](retries-and-replay.html) for the
  schedule timeline, input-mode matrix, recovery behavior, and side-effect
  warnings.

  ## Schedule Options

  Inline `schedule` supports:

  - `cron`: required 5-field cron expression, or a 6-field expression with a
  leading seconds field
  - `timezone`: optional IANA timezone string
  - `missed`: `:skip | :one | :all`, defaults to `:skip`. Runtime `:all`
    catch-up is capped per schedule entry per tick to avoid unbounded high-frequency
    backlog submission. The orchestrator default cap is 1,000 occurrences per
    schedule entry per tick.
  - `overlap`: `:forbid | :allow | :queue_one`, defaults to `:forbid`
  - `active`: boolean, defaults to `true`

  ## Window Policies

  `window` supports `:hourly`, `:daily`, `:monthly`, and `:yearly` aliases plus
  canonical `:hour`, `:day`, `:month`, and `:year` values.

  A windowed pipeline expects manual runs to provide a concrete window request,
  for example `mix favn.run MyApp.Pipelines.Monthly --window month:2026-03`.
  Scheduled windowed runs use the explicit anchor policy in the effective
  timezone. `:previous_complete_period` is the default and selects the period
  immediately before the occurrence. `:current_period` selects the possibly
  incomplete period containing the occurrence. Schedule cadence remains
  independent: a daily schedule may use monthly anchor windows. Pipelines
  without a `window` clause submit no anchor window and are the normal full-load
  path.

      schedule cron: "0 2 * * *", timezone: "Europe/Oslo"
      window :monthly, anchor: :current_period

  Manual run and backfill requests remain constrained to the pipeline window
  kind. A monthly pipeline therefore accepts monthly requests regardless of its
  schedule cadence.

  Assets can mark their asset-level window spec as required with
  `window Favn.Window.monthly(required: true)`. Planning fails before runner
  execution if a required-window asset is selected without a resolved anchor
  window.

  ## Expanded Example

      defmodule MyApp.Lakehouse do
        use Favn.Namespace
        relation connection: :important_lakehouse
      end

      defmodule MyApp.Lakehouse.Raw do
        use Favn.Namespace
        relation catalog: "raw"
      end

      defmodule MyApp.Lakehouse.Raw.Sales do
        use Favn.Namespace
        relation schema: "sales"
      end

      defmodule MyApp.Lakehouse.Raw.Sales.Orders do
        use Favn.Asset

        meta owner: "data-platform"
        meta category: :sales
        meta tags: [:raw, :daily]
        relation true
        def asset(_ctx), do: :ok
      end

      defmodule MyApp.Lakehouse.Raw.Sales.OrderLines do
        use Favn.Asset

        meta owner: "data-platform"
        meta category: :sales
        meta tags: [:raw, :daily]
        relation [name: "order_line_items"]
        def asset(_ctx), do: :ok
      end

      defmodule MyApp.Lakehouse.Mart do
        use Favn.Namespace
        relation catalog: "mart"
      end

      defmodule MyApp.Lakehouse.Mart.Sales do
        use Favn.Namespace
        relation schema: "sales"
      end

      defmodule MyApp.Lakehouse.Mart.Sales.OrderSummary do
        use Favn.SQLAsset

        meta owner: "analytics"
        meta category: :sales
        meta tags: [:mart, :daily]
        depends MyApp.Lakehouse.Raw.Sales.Orders
        materialized :view

        query do
          ~SQL\"""
          select *
          from raw.sales.orders
          \"""
        end
      end

      defmodule MyApp.Pipelines.DailySales do
        use Favn.Pipeline

        pipeline :daily_sales do
          select do
            module MyApp.Lakehouse.Mart
            tag :daily
            category :sales
          end

          deps :all
          settings requested_by: "scheduler", priority: :normal
          meta owner: "analytics"
          meta purpose: :daily_refresh
          schedule cron: "0 2 * * *", timezone: "Europe/Oslo", missed: :one
          window :daily
          source :scheduler
          outputs [:important_lakehouse, :metrics]
        end
      end

  Namespace defaults are inherited from structural parent modules. Leaf asset
  modules use only their asset DSL and add or override declarations there.
  `relation true` is the normal path when the module leaf should become the
  relation name, while `relation [name: "..."]` overrides only that name.

  ## Rules

  - declare exactly one `pipeline :name do ... end`
  - use either shorthand selection (`asset`, `assets`) or `select do ... end`
  - selector semantics are additive and deduplicated

  ## See also

  - `Favn`
  - `Favn.Window`
  - `Favn.Window.Policy`
  - `Favn.Window.Request`
  - `Favn.Triggers.Schedules`
  """

  alias Favn.Pipeline.Definition
  alias Favn.Triggers.Schedule
  alias Favn.Window.Policy

  @type fetch_error :: :not_pipeline_module | :pipeline_not_defined

  defmacro __using__(_opts) do
    quote do
      import Favn.Pipeline

      Module.register_attribute(__MODULE__, :favn_pipeline_name, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_declared, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_block_open, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_selectors, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_pipeline_selection_mode, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_deps, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_settings, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_pipeline_meta, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_pipeline_schedule, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_window, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_retry, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_max_concurrency, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_execution_pool, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_resource_recovery, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_source, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_outputs, persist: false)

      @before_compile Favn.Pipeline
    end
  end

  @doc """
  Declares the pipeline name and body for a pipeline module.

  Each module may declare exactly one pipeline block.
  """
  defmacro pipeline(name, do: block) when is_atom(name) do
    if Module.get_attribute(__CALLER__.module, :favn_pipeline_declared) do
      raise ArgumentError, "pipeline can only be declared once per module"
    end

    Module.put_attribute(__CALLER__.module, :favn_pipeline_declared, true)

    quote do
      @favn_pipeline_name unquote(name)
      Module.put_attribute(__MODULE__, :favn_pipeline_block_open, true)
      unquote(block)
      Module.put_attribute(__MODULE__, :favn_pipeline_block_open, false)
    end
  end

  @doc """
  Chooses whether pipeline runs include upstream dependencies.

  Supported values:

  - `:all`: include upstream dependencies
  - `:none`: run only the selected targets

  ## Example

      deps :all
  """
  defmacro deps(mode) do
    quote bind_quoted: [mode: mode] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "deps")
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_deps, "deps")
      Favn.Pipeline.validate_deps!(mode)
      @favn_pipeline_deps mode
    end
  end

  @doc """
  Attaches non-secret pipeline settings as a map or keyword list.

  Repeated declarations shallow-merge from left to right. Assets read the
  result through `ctx.pipeline.settings`. Per-run inputs remain in `ctx.params`.

  ## Examples

      settings requested_by: "scheduler", priority: :high
      settings %{requested_by: "operator", dry_run: true}
  """
  defmacro settings(opts) do
    quote bind_quoted: [opts: opts] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "settings")
      Favn.Pipeline.validate_map_like_clause!(opts, "settings")
      @favn_pipeline_settings opts
    end
  end

  @doc """
  Attaches pipeline metadata as a map or keyword list.

  `meta` is intended for descriptive or classification data rather than runtime
  control fields. Repeated declarations shallow-merge from left to right. Keys
  are normalized to strings so metadata remains stable through JSON persistence.

  ## Examples

      meta owner: "analytics"
      meta purpose: :daily_refresh
      meta %{team: "data-platform", tier: :mart}
  """
  defmacro meta(opts) do
    quote bind_quoted: [opts: opts] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "meta")
      Favn.Pipeline.validate_map_like_clause!(opts, "meta")
      @favn_pipeline_meta opts
    end
  end

  @doc """
  Attaches a schedule reference or an inline schedule definition.

  Supported forms:

  - `{Module, :name}` for a named schedule from `Favn.Triggers.Schedules`
  - keyword options for an inline schedule

  Inline options:

  - `cron` required, using either a 5-field expression or a 6-field expression
    with a leading seconds field
  - `timezone` optional
  - `missed` optional, defaults to `:skip`; runtime `:all` catch-up is capped
    per schedule entry per tick
  - `overlap` optional, defaults to `:forbid`
  - `active` optional, defaults to `true`

  ## Examples

      schedule {MyApp.Schedules, :daily}

      schedule cron: "0 2 * * *",
               timezone: "Europe/Oslo",
               missed: :one,
               overlap: :queue_one,
               active: true
  """
  defmacro schedule(value) do
    quote bind_quoted: [value: value] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "schedule")
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_schedule, "schedule")
      @favn_pipeline_schedule Favn.Pipeline.normalize_schedule_clause!(value)
    end
  end

  @doc """
  Declares the pipeline window policy to use at runtime.

  Supported values are `:hourly`, `:daily`, `:monthly`, and `:yearly`
  plus their canonical forms `:hour`, `:day`, `:month`, and `:year`.

  ## Example

      window :daily
  """
  defmacro window(name) do
    quote bind_quoted: [name: name] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "window")
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_window, "window")
      @favn_pipeline_window Favn.Pipeline.normalize_window_clause!(name, [])
    end
  end

  @doc """
  Declares a pipeline window policy with options.

  Supported options:

  - `anchor: :previous_complete_period | :current_period`
  - `lookback: non_neg_integer`
  - `timezone: "Etc/UTC"`
  - `allow_full_load: true | false`
  """
  defmacro window(name, opts) do
    quote bind_quoted: [name: name, opts: opts] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "window")
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_window, "window")
      @favn_pipeline_window Favn.Pipeline.normalize_window_clause!(name, opts)
    end
  end

  @doc """
  Limits how many asset steps may execute concurrently within one pipeline run.

  The limit is enforced by the orchestrator admission layer and does not alter
  the dependency graph. Use this for source systems or transforms that should not
  run all independent assets at once.

  ## Example

      max_concurrency 1
  """
  defmacro max_concurrency(value) do
    quote bind_quoted: [value: value] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "max_concurrency")

      Favn.Pipeline.ensure_singleton_clause!(
        __MODULE__,
        :favn_pipeline_max_concurrency,
        "max_concurrency"
      )

      Favn.Pipeline.validate_max_concurrency!(value)
      @favn_pipeline_max_concurrency value
    end
  end

  @doc """
  Declares the default automatic node-attempt retry policy.

  `max_attempts` includes the initial attempt. An asset-level `retry` overrides
  this default, and an explicit operator policy overrides both. A retry policy
  never makes an unsafe or unknown-outcome failure retryable.

      retry max_attempts: 3,
            backoff: {:exponential, initial: 5_000, max: 300_000}
  """
  defmacro retry(opts) do
    quote bind_quoted: [opts: opts] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "retry")
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_retry, "retry")
      @favn_pipeline_retry Favn.Retry.Policy.new!(opts)
    end
  end

  @doc """
  Declares the default shared execution pool for selected pipeline assets.

  Asset-level `execution_pool` declarations override this default. The pool must
  be configured in the orchestrator runtime; unknown pools fail closed instead of
  running unprotected.

  ## Example

      execution_pool :github_api
  """
  defmacro execution_pool(name) do
    quote bind_quoted: [name: name] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "execution_pool")

      Favn.Pipeline.ensure_singleton_clause!(
        __MODULE__,
        :favn_pipeline_execution_pool,
        "execution_pool"
      )

      Favn.Pipeline.validate_execution_pool!(name)
      @favn_pipeline_execution_pool name
    end
  end

  @doc """
  Opts a pipeline into linked recovery runs after a resource circuit closes.

  `:retry_remaining` submits a new run for explicitly safe failed nodes and
  nodes that never started because the resource circuit was open. The terminal
  source run is never reopened or mutated.

  Automatic recovery is intentionally opt-in. `max_age_ms` bounds how long a
  blocked source run remains eligible and defaults to six hours.

  ## Example

      resource_recovery :retry_remaining,
        max_age_ms: :timer.hours(6)
  """
  defmacro resource_recovery(mode, opts \\ []) do
    quote bind_quoted: [mode: mode, opts: opts] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "resource_recovery")

      Favn.Pipeline.ensure_singleton_clause!(
        __MODULE__,
        :favn_pipeline_resource_recovery,
        "resource_recovery"
      )

      @favn_pipeline_resource_recovery Favn.ResourceRecovery.Policy.new!(mode, opts)
    end
  end

  @doc """
  Declares the named pipeline source.

  The value must be an atom understood by your runtime or surrounding app.

  ## Example

      source :scheduler
  """
  defmacro source(name) do
    quote bind_quoted: [name: name] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "source")
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_source, "source")
      Favn.Pipeline.validate_atom_clause!(name, "source")
      @favn_pipeline_source name
    end
  end

  @doc """
  Declares named pipeline outputs.

  The value must be a list of atoms.

  ## Example

      outputs [:warehouse, :metrics]
  """
  defmacro outputs(value) do
    quote bind_quoted: [value: value] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "outputs")
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_outputs, "outputs")
      Favn.Pipeline.validate_outputs!(value)
      @favn_pipeline_outputs value
    end
  end

  @doc """
  Adds one asset ref or single-asset module to the pipeline selection.

  Supported values:

  - `{Module, :asset_name}`
  - `SingleAssetModule` for modules compiled as `{Module, :asset}`

  ## Examples

      asset {MyApp.Raw.Shopify, :orders}
      asset MyApp.Lakehouse.Mart.Sales.FctOrders
  """
  defmacro asset(ref) do
    quote bind_quoted: [ref: ref] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "asset")
      current_mode = Module.get_attribute(__MODULE__, :favn_pipeline_selection_mode)

      if current_mode in [nil, :shorthand] do
        Module.put_attribute(__MODULE__, :favn_pipeline_selection_mode, :shorthand)
        Module.put_attribute(__MODULE__, :favn_pipeline_selectors, {:asset, ref})
      else
        raise ArgumentError, "pipeline cannot mix shorthand selection with `select do ... end`"
      end
    end
  end

  @doc """
  Adds many asset refs or single-asset modules to the pipeline selection.

  ## Example

      assets [
        {MyApp.Raw.Shopify, :orders},
        MyApp.Lakehouse.Mart.Sales.FctOrders
      ]
  """
  defmacro assets(refs) do
    quote bind_quoted: [refs: refs] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "assets")
      current_mode = Module.get_attribute(__MODULE__, :favn_pipeline_selection_mode)

      if current_mode in [nil, :shorthand] do
        Module.put_attribute(__MODULE__, :favn_pipeline_selection_mode, :shorthand)

        Enum.each(refs, fn ref ->
          Module.put_attribute(__MODULE__, :favn_pipeline_selectors, {:asset, ref})
        end)
      else
        raise ArgumentError, "pipeline cannot mix shorthand selection with `select do ... end`"
      end
    end
  end

  @doc """
  Declares additive selectors such as `asset`, `tag`, `category`, or `module`.

  Supported selector forms inside the block:

  - `asset ref_or_module`
  - `tag atom_or_string`
  - `category atom_or_string`
  - `module Some.Namespace`

  Tag and category selectors are manifest labels. Authored atoms and strings are
  normalized to strings so selector behavior is stable after JSON persistence.

  ## Example

      select do
        module MyApp.Lakehouse.Mart
        tag :daily
        category :sales
      end
  """
  defmacro select(do: block) do
    selectors = __extract_selectors__(block)

    quote bind_quoted: [selectors: selectors] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "select")
      current_mode = Module.get_attribute(__MODULE__, :favn_pipeline_selection_mode)

      if current_mode in [nil, :select] do
        Module.put_attribute(__MODULE__, :favn_pipeline_selection_mode, :select)

        Enum.each(selectors, fn selector ->
          Module.put_attribute(__MODULE__, :favn_pipeline_selectors, selector)
        end)
      else
        raise ArgumentError, "pipeline cannot mix shorthand selection with `select do ... end`"
      end
    end
  end

  @doc false
  def __extract_selectors__({:__block__, _meta, entries}) when is_list(entries) do
    Enum.map(entries, &selector_from_ast!/1)
  end

  def __extract_selectors__(entry), do: [selector_from_ast!(entry)]

  defp selector_from_ast!({:asset, _meta, [ref]}), do: {:asset, ref}
  defp selector_from_ast!({:tag, _meta, [value]}), do: {:tag, value}
  defp selector_from_ast!({:category, _meta, [value]}), do: {:category, value}
  defp selector_from_ast!({:module, _meta, [value]}), do: {:module, value}

  defp selector_from_ast!(other) do
    raise ArgumentError,
          "unsupported selector expression inside select block: #{Macro.to_string(other)}"
  end

  defmacro __before_compile__(env) do
    Favn.DSL.AssetDeclarations.reject_legacy_attributes!(env.module, env.file, env.line)
    name = Module.get_attribute(env.module, :favn_pipeline_name)

    if is_nil(name) do
      raise ArgumentError,
            "pipeline module #{inspect(env.module)} must define one `pipeline ... do` block"
    end

    selectors = Module.get_attribute(env.module, :favn_pipeline_selectors) |> Enum.reverse()
    mode = Module.get_attribute(env.module, :favn_pipeline_selection_mode)

    definition =
      %Definition{
        module: env.module,
        name: name,
        selectors: selectors,
        selection_mode: mode,
        deps: Module.get_attribute(env.module, :favn_pipeline_deps) || :all,
        settings:
          env.module
          |> Module.get_attribute(:favn_pipeline_settings)
          |> Enum.reverse()
          |> Favn.Settings.merge_all!(),
        meta:
          env.module
          |> Module.get_attribute(:favn_pipeline_meta)
          |> Enum.reverse()
          |> normalize_meta_declarations!(),
        schedule: Module.get_attribute(env.module, :favn_pipeline_schedule),
        window: Module.get_attribute(env.module, :favn_pipeline_window),
        retry_policy: Module.get_attribute(env.module, :favn_pipeline_retry),
        max_concurrency: Module.get_attribute(env.module, :favn_pipeline_max_concurrency),
        execution_pool: Module.get_attribute(env.module, :favn_pipeline_execution_pool),
        resource_recovery: Module.get_attribute(env.module, :favn_pipeline_resource_recovery),
        source: Module.get_attribute(env.module, :favn_pipeline_source),
        outputs: Module.get_attribute(env.module, :favn_pipeline_outputs) || []
      }

    quote do
      @doc false
      @spec __favn_pipeline__() :: Favn.Pipeline.Definition.t()
      def __favn_pipeline__, do: unquote(Macro.escape(definition))
    end
  end

  @spec fetch(module()) :: {:ok, Definition.t()} | {:error, fetch_error()}
  def fetch(module) when is_atom(module) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- function_exported?(module, :__favn_pipeline__, 0) do
      case module.__favn_pipeline__() do
        %Definition{} = definition -> {:ok, definition}
        _other -> {:error, :pipeline_not_defined}
      end
    else
      _ -> {:error, :not_pipeline_module}
    end
  end

  def fetch(_invalid), do: {:error, :not_pipeline_module}

  @doc false
  @spec normalize_meta_declarations!([map() | keyword()]) :: map()
  def normalize_meta_declarations!(declarations) when is_list(declarations) do
    merged =
      Enum.reduce(declarations, %{}, fn declaration, acc ->
        normalized = Favn.Settings.normalize!(metadata: Map.new(declaration)).metadata
        Map.merge(acc, normalized)
      end)

    Favn.Settings.normalize!(metadata: merged).metadata
  end

  @doc false
  def ensure_singleton_clause!(module, attribute, label) do
    if Module.get_attribute(module, attribute) != nil do
      raise ArgumentError, "pipeline clause `#{label}` can only be declared once"
    end
  end

  @doc false
  def ensure_in_pipeline_block!(module, label) do
    if Module.get_attribute(module, :favn_pipeline_block_open) do
      :ok
    else
      raise ArgumentError, "pipeline clause `#{label}` must be declared inside `pipeline ... do`"
    end
  end

  @doc false
  def validate_deps!(mode) do
    if mode in [:all, :none], do: :ok, else: raise(ArgumentError, "deps must be :all or :none")
  end

  @doc false
  def validate_atom_clause!(value, label) do
    if is_atom(value) do
      :ok
    else
      raise ArgumentError, "pipeline clause `#{label}` must be an atom"
    end
  end

  @doc false
  def validate_map_like_clause!(value, label) do
    if is_map(value) or Keyword.keyword?(value) do
      :ok
    else
      raise ArgumentError, "pipeline clause `#{label}` must be a map or keyword list"
    end
  end

  @doc false
  def validate_outputs!(value) do
    if is_list(value) and Enum.all?(value, &is_atom/1) do
      :ok
    else
      raise ArgumentError, "pipeline clause `outputs` must be a list of atoms"
    end
  end

  @doc false
  def validate_max_concurrency!(value) do
    if is_integer(value) and value > 0 do
      :ok
    else
      raise ArgumentError, "pipeline clause `max_concurrency` must be a positive integer"
    end
  end

  @doc false
  def validate_execution_pool!(value) do
    cond do
      is_atom(value) and not is_nil(value) ->
        :ok

      true ->
        raise ArgumentError, "pipeline clause `execution_pool` must be a non-nil atom"
    end
  end

  @doc false
  @spec normalize_schedule_clause!(term()) ::
          {:ref, Schedule.ref()} | {:inline, Schedule.unresolved_t()}
  def normalize_schedule_clause!({module, name})
      when is_atom(module) and is_atom(name) do
    {:ref, {module, name}}
  end

  def normalize_schedule_clause!(opts) when is_list(opts) do
    case Schedule.new_inline(opts) do
      {:ok, schedule} ->
        {:inline, schedule}

      {:error, reason} ->
        raise ArgumentError, "pipeline clause `schedule` is invalid: #{inspect(reason)}"
    end
  end

  def normalize_schedule_clause!(value) do
    raise ArgumentError,
          "pipeline clause `schedule` must be `{Module, :name}` or keyword options, got: #{inspect(value)}"
  end

  @doc false
  def normalize_window_clause!(name, opts) when is_list(opts) do
    case Policy.new(name, opts) do
      {:ok, policy} ->
        policy

      {:error, reason} ->
        raise ArgumentError, "pipeline clause `window` is invalid: #{inspect(reason)}"
    end
  end

  def normalize_window_clause!(_name, opts) do
    raise ArgumentError,
          "pipeline clause `window` options must be a keyword list, got: #{inspect(opts)}"
  end
end
