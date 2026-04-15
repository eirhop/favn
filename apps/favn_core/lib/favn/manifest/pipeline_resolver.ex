defmodule Favn.Manifest.PipelineResolver do
  @moduledoc """
  Resolves persisted manifest pipeline descriptors to planner-ready inputs.
  """

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Window.Anchor
  alias Favn.Window.Validate

  @type dependencies_mode :: :all | :none

  @type resolution :: %{
          required(:pipeline) => Pipeline.t(),
          required(:target_refs) => [Favn.Ref.t()],
          required(:dependencies) => dependencies_mode(),
          required(:pipeline_ctx) => map()
        }

  @type resolve_opts :: [params: map(), trigger: map(), anchor_window: Anchor.t() | nil]

  @spec resolve(Index.t(), Pipeline.t() | {module(), atom()}, resolve_opts()) ::
          {:ok, resolution()} | {:error, term()}
  def resolve(%Index{} = index, %Pipeline{} = pipeline, opts) when is_list(opts) do
    trigger = Keyword.get(opts, :trigger, %{kind: :manual})
    params = Keyword.get(opts, :params, %{})
    anchor_window = Keyword.get(opts, :anchor_window)

    with :ok <- validate_opts(opts),
         :ok <- validate_pipeline(pipeline),
         :ok <- validate_trigger(trigger),
         :ok <- validate_params(params),
         :ok <- validate_anchor_window(anchor_window),
         {:ok, schedule} <- resolve_schedule(index, pipeline.schedule),
         {:ok, target_refs} <- resolve_selectors(index, pipeline.selectors) do
      {:ok,
       %{
         pipeline: pipeline,
         target_refs: target_refs,
         dependencies: pipeline.deps,
         pipeline_ctx: %{
           id: pipeline.name,
           name: pipeline.name,
           run_kind: :pipeline,
           resolved_refs: target_refs,
           deps: pipeline.deps,
           config: pipeline.config,
           meta: pipeline.metadata,
           trigger: trigger,
           params: params,
           anchor_window: anchor_window,
           window: pipeline.window,
           schedule: schedule,
           source: pipeline.source,
           outputs: pipeline.outputs
         }
       }}
    end
  end

  def resolve(%Index{} = index, {module, name}, opts)
      when is_atom(module) and is_atom(name) and is_list(opts) do
    with {:ok, pipeline} <- Index.fetch_pipeline(index, {module, name}) do
      resolve(index, pipeline, opts)
    end
  end

  def resolve(%Index{} = _index, _pipeline, _opts), do: {:error, :invalid_pipeline}

  defp validate_opts(opts),
    do: Validate.strict_keyword_opts(opts, [:params, :trigger, :anchor_window])

  defp validate_pipeline(%Pipeline{deps: deps, selectors: selectors}) do
    with :ok <- validate_deps(deps),
         :ok <- validate_selectors(selectors) do
      validate_outputs(%Pipeline{deps: deps, selectors: selectors})
    end
  end

  defp validate_pipeline(_other), do: {:error, :invalid_pipeline}

  defp validate_deps(:all), do: :ok
  defp validate_deps(:none), do: :ok
  defp validate_deps(other), do: {:error, {:invalid_dependencies_mode, other}}

  defp validate_selectors([]), do: {:error, :empty_pipeline_selection}

  defp validate_selectors(selectors) when is_list(selectors) do
    if Enum.all?(selectors, &valid_selector?/1), do: :ok, else: {:error, :invalid_selector}
  end

  defp validate_selectors(_other), do: {:error, :invalid_selector}

  defp valid_selector?({:asset, {module, name}}) when is_atom(module) and is_atom(name), do: true
  defp valid_selector?({:module, module}) when is_atom(module), do: true
  defp valid_selector?({:tag, value}) when is_atom(value) or is_binary(value), do: true
  defp valid_selector?({:category, value}) when is_atom(value) or is_binary(value), do: true
  defp valid_selector?(_other), do: false

  defp validate_outputs(%Pipeline{outputs: outputs}) do
    if Enum.all?(outputs, &is_atom/1), do: :ok, else: {:error, {:invalid_outputs, outputs}}
  end

  defp validate_params(params) when is_map(params), do: :ok
  defp validate_params(_other), do: {:error, :invalid_run_params}

  defp validate_trigger(trigger) when is_map(trigger), do: :ok
  defp validate_trigger(_other), do: {:error, :invalid_pipeline_trigger}

  defp validate_anchor_window(nil), do: :ok
  defp validate_anchor_window(%Anchor{} = anchor), do: Anchor.validate(anchor)
  defp validate_anchor_window(other), do: {:error, {:invalid_anchor_window, other}}

  defp resolve_schedule(_index, nil), do: {:ok, nil}

  defp resolve_schedule(index, {:ref, {module, name}}) when is_atom(module) and is_atom(name) do
    Index.fetch_schedule(index, {module, name})
  end

  defp resolve_schedule(_index, {:inline, %Schedule{} = schedule}), do: {:ok, schedule}
  defp resolve_schedule(_index, %Schedule{} = schedule), do: {:ok, schedule}
  defp resolve_schedule(_index, value), do: {:error, {:invalid_schedule, value}}

  defp resolve_selectors(index, selectors) do
    assets = Index.list_assets(index)
    assets_by_ref = Map.new(assets, &{&1.ref, &1})

    selectors
    |> Enum.reduce_while({:ok, []}, fn selector, {:ok, acc} ->
      case selector_refs(selector, assets, assets_by_ref) do
        {:ok, refs} -> {:cont, {:ok, refs ++ acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, refs} ->
        refs = refs |> Enum.uniq() |> Enum.sort()
        if refs == [], do: {:error, :pipeline_resolved_empty}, else: {:ok, refs}

      {:error, _reason} = error ->
        error
    end
  end

  defp selector_refs({:asset, ref}, _assets, assets_by_ref) do
    if Map.has_key?(assets_by_ref, ref), do: {:ok, [ref]}, else: {:error, :asset_not_found}
  end

  defp selector_refs({:module, module}, assets, _assets_by_ref) when is_atom(module) do
    refs =
      assets
      |> Enum.filter(&(&1.module == module))
      |> Enum.map(& &1.ref)

    if refs == [], do: {:error, :not_asset_module}, else: {:ok, refs}
  end

  defp selector_refs({:tag, value}, assets, _assets_by_ref) do
    refs =
      assets
      |> Enum.filter(fn asset -> value in tags_from_metadata(asset) end)
      |> Enum.map(& &1.ref)

    {:ok, refs}
  end

  defp selector_refs({:category, value}, assets, _assets_by_ref) do
    refs =
      assets
      |> Enum.filter(fn asset ->
        asset
        |> Map.get(:metadata, %{})
        |> Map.get(:category)
        |> Kernel.==(value)
      end)
      |> Enum.map(& &1.ref)

    {:ok, refs}
  end

  defp selector_refs(_selector, _assets, _assets_by_ref), do: {:error, :invalid_selector}

  defp tags_from_metadata(%Asset{metadata: metadata}) when is_map(metadata) do
    case Map.get(metadata, :tags, []) do
      values when is_list(values) -> values
      _other -> []
    end
  end

  defp tags_from_metadata(_asset), do: []
end
