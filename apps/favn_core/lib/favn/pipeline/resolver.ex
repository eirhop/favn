defmodule Favn.Pipeline.Resolver do
  @moduledoc """
  Resolve a code-defined pipeline to deterministic planner/runtime inputs.

  Selector entries are additive (union-based). Each selector contributes refs,
  then refs are deduplicated and sorted.
  """

  alias Favn.Pipeline.Definition
  alias Favn.Pipeline.Resolution
  alias Favn.Pipeline.SelectorNormalizer
  alias Favn.Triggers.Schedule
  alias Favn.Window.{Anchor, Policy}

  @type schedule_lookup :: (module(), atom() -> {:ok, Schedule.unresolved_t()} | {:error, term()})

  @type resolve_opts :: [
          params: map(),
          trigger: map(),
          anchor_window: Anchor.t() | nil,
          assets: [map()],
          schedule_lookup: schedule_lookup() | nil
        ]

  @spec resolve(Definition.t(), resolve_opts()) :: {:ok, Resolution.t()} | {:error, term()}
  def resolve(%Definition{} = definition, opts \\ []) when is_list(opts) do
    trigger = Keyword.get(opts, :trigger, %{kind: :manual})
    params = Keyword.get(opts, :params, %{})
    anchor_window = Keyword.get(opts, :anchor_window)
    assets_input = Keyword.get(opts, :assets)
    schedule_lookup = Keyword.get(opts, :schedule_lookup)
    default_timezone = Schedule.default_timezone()

    with {:ok, selectors} <- SelectorNormalizer.normalize(definition.selectors),
         normalized_definition = %Definition{definition | selectors: selectors},
         :ok <- validate_definition(normalized_definition),
         :ok <- validate_params(params),
         :ok <- validate_trigger(trigger),
         :ok <- validate_anchor_window(anchor_window),
         :ok <- validate_schedule_lookup(schedule_lookup),
         :ok <- validate_assets_input(assets_input),
         {:ok, schedule} <-
           resolve_schedule(definition.schedule, default_timezone, schedule_lookup),
         {:ok, assets} <- resolve_assets(assets_input),
         {:ok, target_refs} <- resolve_selectors(selectors, assets) do
      pipeline_ctx =
        build_pipeline_ctx(definition, target_refs, trigger, params, anchor_window, schedule)

      {:ok,
       %Resolution{
         pipeline: normalized_definition,
         target_refs: target_refs,
         dependencies: normalized_definition.deps,
         pipeline_ctx: pipeline_ctx
       }}
    end
  end

  defp validate_definition(%Definition{} = definition) do
    with :ok <- validate_name(definition.name),
         :ok <- validate_deps(definition.deps),
         :ok <- validate_selectors(definition.selectors),
         :ok <- validate_window(definition.window),
         :ok <- validate_source(definition.source) do
      validate_outputs(definition.outputs)
    end
  end

  defp validate_name(name) when is_atom(name), do: :ok
  defp validate_name(_invalid), do: {:error, :pipeline_not_defined}

  defp validate_deps(:all), do: :ok
  defp validate_deps(:none), do: :ok
  defp validate_deps(other), do: {:error, {:invalid_dependencies_mode, other}}

  defp validate_selectors([]), do: {:error, :empty_pipeline_selection}

  defp validate_selectors(selectors) when is_list(selectors), do: :ok

  defp validate_window(nil), do: :ok

  defp validate_window(%Policy{} = policy) do
    case Policy.validate(policy) do
      {:ok, _policy} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_window(value), do: {:error, {:invalid_window, value}}

  defp validate_source(nil), do: :ok
  defp validate_source(value) when is_atom(value), do: :ok
  defp validate_source(value), do: {:error, {:invalid_source, value}}

  defp validate_outputs(outputs) when is_list(outputs) do
    if Enum.all?(outputs, &is_atom/1), do: :ok, else: {:error, {:invalid_outputs, outputs}}
  end

  defp validate_outputs(other), do: {:error, {:invalid_outputs, other}}

  defp validate_params(params) when is_map(params), do: :ok
  defp validate_params(_invalid), do: {:error, :invalid_run_params}

  defp validate_trigger(trigger) when is_map(trigger), do: :ok
  defp validate_trigger(_invalid), do: {:error, :invalid_pipeline_trigger}

  defp validate_anchor_window(nil), do: :ok

  defp validate_anchor_window(%Anchor{} = anchor_window),
    do: Anchor.validate(anchor_window)

  defp validate_anchor_window(other), do: {:error, {:invalid_anchor_window, other}}

  defp validate_schedule_lookup(nil), do: :ok
  defp validate_schedule_lookup(fun) when is_function(fun, 2), do: :ok
  defp validate_schedule_lookup(other), do: {:error, {:invalid_schedule_lookup, other}}

  defp validate_assets_input(nil), do: {:error, :missing_assets}
  defp validate_assets_input(values) when is_list(values), do: :ok
  defp validate_assets_input(other), do: {:error, {:invalid_assets_opt, other}}

  defp resolve_assets(nil), do: {:error, :missing_assets}
  defp resolve_assets(values), do: {:ok, values}

  defp resolve_schedule(nil, _default_timezone, _lookup), do: {:ok, nil}

  defp resolve_schedule({:inline, %Schedule{} = schedule}, default_timezone, _lookup) do
    Schedule.apply_default_timezone(schedule, default_timezone)
  end

  defp resolve_schedule({:ref, {module, name}}, _default_timezone, nil)
       when is_atom(module) and is_atom(name) do
    {:error, :missing_schedule_lookup}
  end

  defp resolve_schedule({:ref, {module, name}}, default_timezone, schedule_lookup)
       when is_atom(module) and is_atom(name) do
    with {:ok, schedule} <- schedule_lookup.(module, name) do
      Schedule.apply_default_timezone(schedule, default_timezone)
    end
  end

  defp resolve_schedule(value, _default_timezone, _lookup),
    do: {:error, {:invalid_schedule, value}}

  defp resolve_selectors(selectors, assets) when is_list(selectors) do
    assets_by_ref = Map.new(assets, &{&1.ref, &1})

    with {:ok, refs} <- collect_selector_refs(selectors, assets, assets_by_ref) do
      refs = refs |> Enum.uniq() |> Enum.sort()
      if refs == [], do: {:error, :pipeline_resolved_empty}, else: {:ok, refs}
    end
  end

  defp collect_selector_refs(selectors, assets, assets_by_ref) do
    Enum.reduce_while(selectors, {:ok, []}, fn selector, {:ok, acc} ->
      case selector_refs(selector, assets, assets_by_ref) do
        {:ok, refs} -> {:cont, {:ok, refs ++ acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp selector_refs({:asset, ref}, _assets, assets_by_ref) do
    if Map.has_key?(assets_by_ref, ref), do: {:ok, [ref]}, else: {:error, :asset_not_found}
  end

  defp selector_refs({:module, mod}, assets, _assets_by_ref) do
    refs =
      assets
      |> Enum.filter(&(&1.module == mod))
      |> Enum.map(& &1.ref)

    if refs == [] do
      {:error, :not_asset_module}
    else
      {:ok, refs}
    end
  end

  defp selector_refs({:tag, value}, assets, _assets_by_ref) do
    refs =
      assets
      |> Enum.filter(fn asset -> value in tags_from_meta(asset) end)
      |> Enum.map(& &1.ref)

    {:ok, refs}
  end

  defp selector_refs({:category, value}, assets, _assets_by_ref) do
    refs =
      assets
      |> Enum.filter(fn asset ->
        asset
        |> Map.get(:meta, %{})
        |> Map.get(:category)
        |> Kernel.==(value)
      end)
      |> Enum.map(& &1.ref)

    {:ok, refs}
  end

  defp tags_from_meta(asset) do
    case Map.get(Map.get(asset, :meta, %{}), :tags, []) do
      values when is_list(values) -> values
      _ -> []
    end
  end

  defp build_pipeline_ctx(definition, target_refs, trigger, params, anchor_window, schedule) do
    %{
      id: definition.name,
      name: definition.name,
      run_kind: :pipeline,
      resolved_refs: target_refs,
      deps: definition.deps,
      config: definition.config,
      meta: definition.meta,
      trigger: trigger,
      params: params,
      anchor_window: anchor_window,
      window: definition.window,
      schedule: schedule,
      source: definition.source,
      outputs: definition.outputs
    }
  end
end
