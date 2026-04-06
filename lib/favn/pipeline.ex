defmodule Favn.Pipeline do
  @moduledoc """
  Tiny code-defined DSL for pipeline composition.

  This DSL defines orchestration composition only. Dependency planning is still
  delegated to the existing asset dependency planner.

  Selector semantics in `select do ... end` are additive (union-based): each
  selector contributes refs, then refs are deduplicated and sorted.
  """

  alias Favn.Pipeline.Definition

  @type fetch_error :: :not_pipeline_module | :pipeline_not_defined

  defmacro __using__(_opts) do
    quote do
      import Favn.Pipeline

      Module.register_attribute(__MODULE__, :favn_pipeline_name, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_declared, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_selectors, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_pipeline_selection_mode, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_deps, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_config, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_meta, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_schedule, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_partition, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_source, persist: false)
      Module.register_attribute(__MODULE__, :favn_pipeline_outputs, persist: false)

      @before_compile Favn.Pipeline
    end
  end

  defmacro pipeline(name, do: block) when is_atom(name) do
    if Module.get_attribute(__CALLER__.module, :favn_pipeline_declared) do
      raise ArgumentError, "pipeline can only be declared once per module"
    end

    Module.put_attribute(__CALLER__.module, :favn_pipeline_declared, true)

    quote do
      @favn_pipeline_name unquote(name)
      unquote(block)
    end
  end

  defmacro deps(mode) do
    quote bind_quoted: [mode: mode] do
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_deps, "deps")
      Favn.Pipeline.validate_deps!(mode)
      @favn_pipeline_deps mode
    end
  end

  defmacro config(opts) do
    quote bind_quoted: [opts: opts] do
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_config, "config")
      Favn.Pipeline.validate_map_like_clause!(opts, "config")
      @favn_pipeline_config Map.new(opts)
    end
  end

  defmacro meta(opts) do
    quote bind_quoted: [opts: opts] do
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_meta, "meta")
      Favn.Pipeline.validate_map_like_clause!(opts, "meta")
      @favn_pipeline_meta Map.new(opts)
    end
  end

  defmacro schedule(name) do
    quote bind_quoted: [name: name] do
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_schedule, "schedule")
      Favn.Pipeline.validate_atom_clause!(name, "schedule")
      @favn_pipeline_schedule name
    end
  end

  defmacro partition(name) do
    quote bind_quoted: [name: name] do
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_partition, "partition")
      Favn.Pipeline.validate_atom_clause!(name, "partition")
      @favn_pipeline_partition name
    end
  end

  defmacro source(name) do
    quote bind_quoted: [name: name] do
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_source, "source")
      Favn.Pipeline.validate_atom_clause!(name, "source")
      @favn_pipeline_source name
    end
  end

  defmacro outputs(value) do
    quote bind_quoted: [value: value] do
      Favn.Pipeline.ensure_singleton_clause!(__MODULE__, :favn_pipeline_outputs, "outputs")
      Favn.Pipeline.validate_outputs!(value)
      @favn_pipeline_outputs value
    end
  end

  defmacro asset(ref) do
    quote bind_quoted: [ref: ref] do
      current_mode = Module.get_attribute(__MODULE__, :favn_pipeline_selection_mode)

      if current_mode in [nil, :shorthand] do
        Module.put_attribute(__MODULE__, :favn_pipeline_selection_mode, :shorthand)
        Module.put_attribute(__MODULE__, :favn_pipeline_selectors, {:asset, ref})
      else
        raise ArgumentError, "pipeline cannot mix shorthand selection with `select do ... end`"
      end
    end
  end

  defmacro assets(refs) do
    quote bind_quoted: [refs: refs] do
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

  defmacro select(do: block) do
    selectors = __extract_selectors__(block)

    quote bind_quoted: [selectors: selectors] do
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
        partition: Module.get_attribute(env.module, :favn_pipeline_partition),
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
    if function_exported?(module, :__favn_pipeline__, 0) do
      case module.__favn_pipeline__() do
        %Definition{} = definition -> {:ok, definition}
        _other -> {:error, :pipeline_not_defined}
      end
    else
      {:error, :not_pipeline_module}
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
end
