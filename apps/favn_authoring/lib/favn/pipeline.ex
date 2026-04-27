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
          asset MyApp.Gold.Sales.FctOrders
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
  - `config map_or_keyword`: runtime pipeline config exposed through pipeline context
  - `meta map_or_keyword`: descriptive metadata for operators and tooling
  - `schedule {Module, :name}`: reference a named schedule
  - `schedule cron: ..., ...`: declare an inline schedule
  - `window atom`: attach a named pipeline window
  - `source atom`: attach a named pipeline source
  - `outputs [atom, ...]`: attach named outputs

  ## Schedule Options

  Inline `schedule` supports:

  - `cron`: required 5-field cron expression, or a 6-field expression with a
  leading seconds field
  - `timezone`: optional IANA timezone string
  - `missed`: `:skip | :one | :all`, defaults to `:skip`
  - `overlap`: `:forbid | :allow | :queue_one`, defaults to `:forbid`
  - `active`: boolean, defaults to `true`

  ## Expanded Example

      defmodule MyApp.Warehouse do
        use Favn.Namespace, relation: [connection: :warehouse]
      end

      defmodule MyApp.Warehouse.Raw do
        use Favn.Namespace, relation: [catalog: "raw"]
      end

      defmodule MyApp.Warehouse.Raw.Sales do
        use Favn.Namespace, relation: [schema: "sales"]
      end

      defmodule MyApp.Warehouse.Raw.Sales.Orders do
        use Favn.Asset

        @meta owner: "data-platform", category: :sales, tags: [:raw, :daily]
        @relation true
        def asset(_ctx), do: :ok
      end

      defmodule MyApp.Warehouse.Raw.Sales.OrderLines do
        use Favn.Asset

        @meta owner: "data-platform", category: :sales, tags: [:raw, :daily]
        @relation [name: "order_line_items"]
        def asset(_ctx), do: :ok
      end

      defmodule MyApp.Warehouse.Gold do
        use Favn.Namespace, relation: [catalog: "gold"]
      end

      defmodule MyApp.Warehouse.Gold.Sales do
        use Favn.Namespace, relation: [schema: "sales"]
      end

      defmodule MyApp.Warehouse.Gold.Sales.OrderSummary do
        use Favn.SQLAsset

        @meta owner: "analytics", category: :sales, tags: [:gold, :daily]
        @materialized :view

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
            module MyApp.Warehouse.Gold.Sales
            tag :daily
            category :sales
          end

          deps :all
          config requested_by: "scheduler", priority: :normal
          meta owner: "analytics", purpose: :daily_refresh
          schedule cron: "0 2 * * *", timezone: "Europe/Oslo", missed: :one
          window :daily
          source :scheduler
          outputs [:warehouse, :metrics]
        end
      end

  Namespace defaults are inherited from parent modules, so leaf asset modules
  only need `use Favn.Namespace` when they want to add or override shared
  relation defaults. `@relation true` is the normal path when the module leaf
  should become the relation name, while `@relation [name: "..."]` is the
  normal way to override only the relation name. `@meta` stays module-local and
  is not inherited from namespace modules.

  ## Rules

  - declare exactly one `pipeline :name do ... end`
  - use either shorthand selection (`asset`, `assets`) or `select do ... end`
  - selector semantics are additive and deduplicated

  ## See also

  - `Favn`
  - `Favn.Window`
  - `Favn.Triggers.Schedules`
  """

  alias Favn.Pipeline.Definition
  alias Favn.Triggers.Schedule

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
      Module.register_attribute(__MODULE__, :favn_pipeline_config, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_meta, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_schedule, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_window, persist: false)
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
  Attaches pipeline-level config metadata as a map or keyword list.

  `config` is intended for runtime-facing values that assets or orchestration
  code may read from pipeline context.

  ## Examples

      config requested_by: "scheduler", priority: :high
      config %{requested_by: "operator", dry_run: true}
  """
  defmacro config(opts) do
    quote bind_quoted: [opts: opts] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "config")
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_config, "config")
      Favn.Pipeline.validate_map_like_clause!(opts, "config")
      @favn_pipeline_config Map.new(opts)
    end
  end

  @doc """
  Attaches pipeline metadata as a map or keyword list.

  `meta` is intended for descriptive or classification data rather than runtime
  control fields.

  ## Examples

      meta owner: "analytics", purpose: :daily_refresh
      meta %{team: "data-platform", tier: :gold}
  """
  defmacro meta(opts) do
    quote bind_quoted: [opts: opts] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "meta")
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_meta, "meta")
      Favn.Pipeline.validate_map_like_clause!(opts, "meta")
      @favn_pipeline_meta Map.new(opts)
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
  - `missed` optional, defaults to `:skip`
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
  Declares the named pipeline window to use at runtime.

  The value must be an atom understood by your runtime or surrounding app.

  ## Example

      window :daily
  """
  defmacro window(name) do
    quote bind_quoted: [name: name] do
      Favn.Pipeline.ensure_in_pipeline_block!(__MODULE__, "window")
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_window, "window")
      Favn.Pipeline.validate_atom_clause!(name, "window")
      @favn_pipeline_window name
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
      asset MyApp.Gold.Sales.FctOrders
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
        MyApp.Gold.Sales.FctOrders
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

  ## Example

      select do
        module MyApp.Gold.Sales
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
        config: Module.get_attribute(env.module, :favn_pipeline_config) || %{},
        meta: Module.get_attribute(env.module, :favn_pipeline_meta) || %{},
        schedule: Module.get_attribute(env.module, :favn_pipeline_schedule),
        window: Module.get_attribute(env.module, :favn_pipeline_window),
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
  rescue
    _error ->
      {:error, :pipeline_not_defined}
  end

  def fetch(_invalid), do: {:error, :not_pipeline_module}

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
end
