defmodule FavnOrchestrator.Operator.Catalogue.RunHistory do
  @moduledoc """
  Selects and projects run history for operator catalogue details.

  Current pipeline identity is preferred, with a bounded legacy target-ref
  fallback for older persisted runs that predate `pipeline_submit_ref`.
  """

  alias Favn.Manifest.Pipeline
  alias FavnOrchestrator.Operator.Catalogue.Targets

  @epoch DateTime.from_unix!(0)

  @doc "Returns newest-first runs belonging to a pipeline target."
  @spec for_pipeline(Pipeline.t(), map(), [map()]) :: [map()]
  def for_pipeline(%Pipeline{} = pipeline, %{selected_assets: selected_assets}, runs)
      when is_list(runs) do
    selected_assets = Enum.sort(selected_assets)

    runs
    |> Enum.filter(fn run ->
      pipeline_submit_ref_matches?(run, pipeline) ||
        legacy_pipeline_targets_match?(run, selected_assets)
    end)
    |> Enum.sort_by(&DateTime.to_unix(time_key(&1), :microsecond), :desc)
  end

  @doc "Projects one run into a stable pipeline-history entry."
  @spec entry(map()) :: map()
  def entry(run) when is_map(run) do
    scope = scope(run)

    %{
      id: Map.fetch!(run, :id),
      status: Map.fetch!(run, :status),
      submit_kind: Map.get(run, :submit_kind),
      started_at: Map.get(run, :started_at),
      finished_at: Map.get(run, :finished_at),
      duration_ms: duration_ms(run),
      scope: scope,
      window: legacy_window(scope)
    }
  end

  @doc "Returns canonical asset-ref/run pairs represented by a run."
  @spec ref_entries(map()) :: [{String.t(), map()}]
  def ref_entries(run) when is_map(run) do
    refs =
      [Map.get(run, :asset_ref) | List.wrap(Map.get(run, :target_refs))] ++
        (run
         |> Map.get(:asset_results, %{})
         |> normalize_map()
         |> Map.keys())

    refs
    |> Enum.filter(&match?({_module, _name}, &1))
    |> Enum.uniq()
    |> Enum.map(&{Targets.ref_string(&1), run})
  end

  @doc "Returns the most recently updated run, or `nil` for an empty list."
  @spec latest([map()]) :: map() | nil
  def latest(runs) when is_list(runs) do
    Enum.max_by(runs, &DateTime.to_unix(time_key(&1), :microsecond), &>=/2, fn -> nil end)
  end

  @doc "Returns the best available persisted timestamp for a run."
  @spec time_key(map()) :: DateTime.t()
  def time_key(run) when is_map(run) do
    [:finished_at, :started_at, :updated_at, :inserted_at]
    |> Enum.find_value(@epoch, fn key ->
      case Map.get(run, key) do
        %DateTime{} = value -> value
        _value -> nil
      end
    end)
  end

  defp scope(run) do
    params = normalize_map(Map.get(run, :params))
    metadata = normalize_map(Map.get(run, :metadata))

    cond do
      backfill = Map.get(metadata, :backfill) || Map.get(metadata, "backfill") ->
        Map.put(normalize_scope(backfill), :type, :range)

      window =
          Map.get(params, :window) || Map.get(params, "window") ||
            Map.get(metadata, :selected_window) || Map.get(metadata, "selected_window") ||
            Map.get(metadata, :window) || Map.get(metadata, "window") ->
        Map.put(normalize_scope(window), :type, :window)

      true ->
        nil
    end
  end

  defp legacy_window(%{type: :window} = scope), do: Map.delete(scope, :type)
  defp legacy_window(_scope), do: nil

  defp normalize_scope(value) when is_map(value), do: Map.new(value, &normalize_scope_pair/1)
  defp normalize_scope(value) when is_binary(value), do: %{label: value}
  defp normalize_scope(value), do: %{label: inspect(value)}

  defp normalize_scope_pair({"type", value}), do: {:type, value}
  defp normalize_scope_pair({"kind", value}), do: {:kind, value}
  defp normalize_scope_pair({"timezone", value}), do: {:timezone, value}
  defp normalize_scope_pair({"range_start_at", value}), do: {:range_start_at, value}
  defp normalize_scope_pair({"range_end_at", value}), do: {:range_end_at, value}
  defp normalize_scope_pair({"requested_count", value}), do: {:requested_count, value}
  defp normalize_scope_pair({"window_keys", value}), do: {:window_keys, value}
  defp normalize_scope_pair({"id", value}), do: {:id, value}
  defp normalize_scope_pair({"key", value}), do: {:key, value}
  defp normalize_scope_pair({"label", value}), do: {:label, value}
  defp normalize_scope_pair(pair), do: pair

  defp pipeline_submit_ref_matches?(run, pipeline) do
    case pipeline_submit_ref(run) do
      nil -> false
      submit_ref -> same_pipeline_ref?(submit_ref, pipeline.module)
    end
  end

  defp legacy_pipeline_targets_match?(run, selected_assets) do
    is_nil(pipeline_submit_ref(run)) && pipeline_origin?(run) && selected_assets != [] &&
      run
      |> Map.get(:target_refs)
      |> List.wrap()
      |> Enum.map(&Targets.ref_string/1)
      |> Enum.sort()
      |> Kernel.==(selected_assets)
  end

  defp pipeline_origin?(run) do
    Map.get(run, :submit_kind) in [:pipeline, :backfill_pipeline] ||
      not is_nil(pipeline_metadata_value(run, :pipeline_submit_ref)) ||
      not is_nil(pipeline_metadata_value(run, :pipeline_target_refs))
  end

  defp pipeline_submit_ref(run) do
    pipeline_metadata_value(run, :pipeline_submit_ref) || direct_pipeline_submit_ref(run)
  end

  defp direct_pipeline_submit_ref(run) do
    if Map.get(run, :submit_kind) in [:pipeline, :backfill_pipeline],
      do: Map.get(run, :submit_ref)
  end

  defp pipeline_metadata_value(run, key) do
    metadata = normalize_map(Map.get(run, :metadata))
    Map.get(metadata, key) || Map.get(metadata, Atom.to_string(key))
  end

  defp same_pipeline_ref?(module, module) when is_atom(module), do: true

  defp same_pipeline_ref?(value, module) when is_atom(value) and is_atom(module),
    do: value == module

  defp same_pipeline_ref?(value, module) when is_binary(value) and is_atom(module),
    do: value == Atom.to_string(module)

  defp same_pipeline_ref?(_value, _module), do: false

  defp duration_ms(%{
         started_at: %DateTime{} = started_at,
         finished_at: %DateTime{} = finished_at
       }) do
    max(DateTime.diff(finished_at, started_at, :millisecond), 0)
  end

  defp duration_ms(_run), do: nil

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_value), do: %{}
end
