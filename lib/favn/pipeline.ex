defmodule Favn.Pipeline do
  @moduledoc """
  Tiny code-defined DSL for pipeline composition.

  This DSL defines orchestration composition only. Dependency planning is still
  delegated to the existing asset dependency planner.
  """

  alias Favn.Pipeline.Definition

  @type fetch_error :: :not_pipeline_module | :pipeline_not_defined

  defmacro __using__(_opts) do
    quote do
      import Favn.Pipeline

      Module.register_attribute(__MODULE__, :favn_pipeline_name, persist: false)
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
    quote do
      @favn_pipeline_name unquote(name)
      unquote(block)
    end
  end

  defmacro deps(mode) do
    quote do
      @favn_pipeline_deps unquote(mode)
    end
  end

  defmacro config(opts) do
    quote do
      @favn_pipeline_config Map.new(unquote(opts))
    end
  end

  defmacro meta(opts) do
    quote do
      @favn_pipeline_meta Map.new(unquote(opts))
    end
  end

  defmacro schedule(name) do
    quote do
      @favn_pipeline_schedule unquote(name)
    end
  end

  defmacro partition(name) do
    quote do
      @favn_pipeline_partition unquote(name)
    end
  end

  defmacro source(name) do
    quote do
      @favn_pipeline_source unquote(name)
    end
  end

  defmacro outputs(value) do
    quote do
      @favn_pipeline_outputs unquote(value)
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
end
