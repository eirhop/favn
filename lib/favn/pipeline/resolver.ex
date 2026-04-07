defmodule Favn.Pipeline.Resolver do
  @moduledoc """
  Resolve a code-defined pipeline to deterministic planner/runtime inputs.

  Selector entries are additive (union-based). Each selector contributes refs,
  then refs are deduplicated and sorted.
  """

  alias Favn.Pipeline.Definition
  alias Favn.Pipeline.Resolution
  alias Favn.Triggers.Schedule
  alias Favn.Triggers.Schedules

  @type resolve_opts :: [
          params: map(),
          trigger: map(),
          anchor_window: Favn.Window.Anchor.t() | nil
        ]

  @spec resolve(Definition.t(), resolve_opts()) :: {:ok, Resolution.t()} | {:error, term()}
  def resolve(%Definition{} = definition, opts \\ []) when is_list(opts) do
    trigger = Keyword.get(opts, :trigger, %{kind: :manual})
    params = Keyword.get(opts, :params, %{})
    anchor_window = Keyword.get(opts, :anchor_window)
    default_timezone = Schedule.default_timezone()

    with :ok <- validate_definition(definition),
         :ok <- validate_params(params),
         :ok <- validate_trigger(trigger),
         :ok <- validate_anchor_window(anchor_window),
         {:ok, schedule} <- resolve_schedule(definition.schedule, default_timezone),
         {:ok, assets} <- Favn.list_assets(),
         {:ok, target_refs} <- resolve_selectors(definition, assets) do
      pipeline_ctx = %{
        id: definition.name,
        name: definition.name,
        config: definition.config,
        meta: definition.meta,
        trigger: trigger,
        params: params,
        runtime_window: nil,
        anchor_window: anchor_window,
        window: definition.window,
        schedule: schedule,
        partition: definition.partition,
        source: definition.source,
        outputs: definition.outputs
      }

      {:ok,
       %Resolution{
         pipeline: definition,
         target_refs: target_refs,
         dependencies: definition.deps,
         pipeline_ctx: pipeline_ctx
       }}
    end
  end

  defp validate_definition(%Definition{} = definition) do
    with :ok <- validate_name(definition.name),
         :ok <- validate_deps(definition.deps),
         :ok <- validate_selectors(definition.selectors),
         :ok <- validate_window(definition.window),
         :ok <- validate_partition(definition.partition),
         :ok <- validate_source(definition.source),
         :ok <- validate_outputs(definition.outputs) do
      :ok
    end
  end

  defp validate_name(name) when is_atom(name), do: :ok
  defp validate_name(_invalid), do: {:error, :pipeline_not_defined}

  defp validate_deps(:all), do: :ok
  defp validate_deps(:none), do: :ok
  defp validate_deps(other), do: {:error, {:invalid_dependencies_mode, other}}

  defp validate_selectors([]), do: {:error, :empty_pipeline_selection}

  defp validate_selectors(selectors) when is_list(selectors) do
    if Enum.all?(selectors, &valid_selector?/1), do: :ok, else: {:error, :invalid_selector}
  end

  defp validate_selectors(_invalid), do: {:error, :invalid_selector}

  defp valid_selector?({:asset, {module, name}}) when is_atom(module) and is_atom(name), do: true
  defp valid_selector?({:module, module}) when is_atom(module), do: true
  defp valid_selector?({:tag, value}) when is_atom(value) or is_binary(value), do: true
  defp valid_selector?({:category, value}) when is_atom(value) or is_binary(value), do: true
  defp valid_selector?(_), do: false

  defp validate_partition(nil), do: :ok
  defp validate_partition(value) when is_atom(value), do: :ok
  defp validate_partition(value), do: {:error, {:invalid_partition, value}}

  defp validate_window(nil), do: :ok
  defp validate_window(value) when is_atom(value), do: :ok
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

  defp validate_anchor_window(%Favn.Window.Anchor{} = anchor_window),
    do: Favn.Window.Anchor.validate(anchor_window)

  defp validate_anchor_window(other), do: {:error, {:invalid_anchor_window, other}}

  defp resolve_schedule(nil, _default_timezone), do: {:ok, nil}

  defp resolve_schedule({:inline, %Schedule{} = schedule}, default_timezone) do
    with {:ok, resolved} <- Schedule.apply_default_timezone(schedule, default_timezone) do
      {:ok, resolved}
    end
  end

  defp resolve_schedule({:ref, {module, name}}, default_timezone)
       when is_atom(module) and is_atom(name) do
    with {:ok, schedule} <- Schedules.fetch(module, name),
         {:ok, resolved} <- Schedule.apply_default_timezone(schedule, default_timezone) do
      {:ok, resolved}
    end
  end

  defp resolve_schedule(value, _default_timezone), do: {:error, {:invalid_schedule, value}}

  defp resolve_selectors(%Definition{selectors: selectors}, assets) do
    assets_by_ref = Map.new(assets, &{&1.ref, &1})

    with {:ok, refs} <- do_resolve_selectors(selectors, assets, assets_by_ref) do
      refs = refs |> Enum.uniq() |> Enum.sort()
      if refs == [], do: {:error, :pipeline_resolved_empty}, else: {:ok, refs}
    end
  end

  defp do_resolve_selectors(selectors, assets, assets_by_ref) do
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
    if Favn.asset_module?(mod) do
      {:ok,
       assets
       |> Enum.filter(&(&1.module == mod))
       |> Enum.map(& &1.ref)}
    else
      {:error, :not_asset_module}
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
      |> Enum.filter(fn asset -> Map.get(asset.meta, :category) == value end)
      |> Enum.map(& &1.ref)

    {:ok, refs}
  end

  defp tags_from_meta(asset) do
    case Map.get(asset.meta, :tags, []) do
      values when is_list(values) -> values
      _ -> []
    end
  end
end
