defmodule FavnOrchestrator.Backfill.Repair do
  @moduledoc """
  Rebuilds derived operational-backfill read models from authoritative run snapshots.

  Repair never appends run events and never rewrites `FavnOrchestrator.RunState`
  snapshots. Dry-run planning is the default; applying a repair replaces only the
  scoped derived read models.
  """

  alias Favn.Run.AssetResult
  alias Favn.Window.Anchor
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @terminal_statuses [:ok, :error, :cancelled, :timed_out]
  @required_coverage_keys [
    :source_key,
    :segment_key_hash,
    :coverage_until,
    :window_kind,
    :timezone
  ]
  @raw_source_keys [:segment_id, :source_id, :source_secret, :token, :secret]

  @type report :: map()

  @spec repair(keyword()) :: {:ok, report()} | {:error, term()}
  def repair(opts \\ []) when is_list(opts) do
    apply? = Keyword.get(opts, :apply, false)

    with {:ok, scope} <- normalize_scope(opts),
         {:ok, runs} <- Storage.list_runs(),
         {:ok, plan} <- plan(runs, scope),
         :ok <- maybe_apply(apply?, scope, plan) do
      {:ok, report(apply?, scope, plan)}
    end
  end

  @spec plan([RunState.t()], keyword()) :: {:ok, map()} | {:error, term()}
  def plan(runs, scope) when is_list(runs) and is_list(scope) do
    scoped_runs = Enum.filter(runs, &run_in_scope?(&1, scope))
    parent_by_id = parent_by_id(scoped_runs)

    {coverage_baselines, coverage_skips} = derive_coverage_baselines(scoped_runs)
    {windows, window_skips} = derive_backfill_windows(scoped_runs, parent_by_id)
    {asset_states, asset_skips} = derive_asset_window_states(scoped_runs, windows, scope)

    {:ok,
     %{
       coverage_baselines: coverage_baselines,
       backfill_windows: windows,
       asset_window_states: asset_states,
       skips: coverage_skips ++ window_skips ++ asset_skips
     }}
  end

  defp maybe_apply(false, _scope, _plan), do: :ok

  defp maybe_apply(true, scope, plan) do
    Storage.replace_backfill_read_models(
      scope,
      plan.coverage_baselines,
      plan.backfill_windows,
      plan.asset_window_states
    )
  end

  defp normalize_scope(opts) do
    scope =
      []
      |> maybe_put_scope(:backfill_run_id, Keyword.get(opts, :backfill_run_id))
      |> maybe_put_scope(:pipeline_module, Keyword.get(opts, :pipeline_module))

    if length(scope) <= 1, do: {:ok, scope}, else: {:error, :invalid_repair_scope}
  end

  defp maybe_put_scope(scope, _key, nil), do: scope
  defp maybe_put_scope(scope, _key, ""), do: scope

  defp maybe_put_scope(scope, :pipeline_module, value) when is_binary(value),
    do: [{:pipeline_module, Module.concat([value])} | scope]

  defp maybe_put_scope(scope, key, value), do: [{key, value} | scope]

  defp run_in_scope?(_run, []), do: true
  defp run_in_scope?(%RunState{id: id}, backfill_run_id: id), do: true
  defp run_in_scope?(%RunState{trigger: %{backfill_run_id: id}}, backfill_run_id: id), do: true

  defp run_in_scope?(%RunState{} = run, pipeline_module: pipeline_module),
    do: pipeline_module(run) == {:ok, pipeline_module}

  defp run_in_scope?(_run, _scope), do: false

  defp parent_by_id(runs) do
    runs
    |> Enum.filter(&(&1.submit_kind == :backfill_pipeline))
    |> Map.new(&{&1.id, &1})
  end

  defp derive_coverage_baselines(runs) do
    runs
    |> Enum.reduce({[], []}, fn run, {baselines, skips} ->
      case coverage_baseline(run) do
        {:ok, baseline} -> {[baseline | baselines], skips}
        {:skip, nil} -> {baselines, skips}
        {:skip, reason} -> {baselines, [skip(run.id, :coverage_baseline, reason) | skips]}
      end
    end)
    |> reverse_pair()
  end

  defp coverage_baseline(%RunState{status: :ok} = run) do
    with {:ok, coverage} <- coverage_metadata(run),
         :ok <- reject_raw_source_identity(coverage),
         {:ok, pipeline_module} <- pipeline_module(run),
         {:ok, attrs} <- normalize_coverage_attrs(coverage),
         timestamp <- run.updated_at || run.inserted_at || DateTime.utc_now(),
         {:ok, baseline} <-
           CoverageBaseline.new(%{
             baseline_id: baseline_id(run, pipeline_module, attrs),
             pipeline_module: pipeline_module,
             source_key: Map.fetch!(attrs, :source_key),
             segment_key_hash: Map.fetch!(attrs, :segment_key_hash),
             segment_key_redacted: Map.get(attrs, :segment_key_redacted),
             window_kind: Map.fetch!(attrs, :window_kind),
             timezone: Map.fetch!(attrs, :timezone),
             coverage_start_at: Map.get(attrs, :coverage_start_at),
             coverage_until: Map.fetch!(attrs, :coverage_until),
             created_by_run_id: run.id,
             manifest_version_id: run.manifest_version_id,
             status: Map.get(attrs, :status, :ok),
             metadata: Map.get(attrs, :metadata, %{}),
             created_at: timestamp,
             updated_at: timestamp
           }) do
      {:ok, baseline}
    else
      :ignore -> {:skip, nil}
      {:error, reason} -> {:skip, reason}
    end
  end

  defp coverage_baseline(_run), do: {:skip, nil}

  defp derive_backfill_windows(runs, parent_by_id) do
    runs
    |> Enum.reduce({[], []}, fn run, {windows, skips} ->
      case backfill_window(run, parent_by_id) do
        {:ok, window} -> {[window | windows], skips}
        {:skip, nil} -> {windows, skips}
        {:skip, reason} -> {windows, [skip(run.id, :backfill_window, reason) | skips]}
      end
    end)
    |> reverse_pair()
  end

  defp backfill_window(%RunState{} = run, parent_by_id) do
    with {:ok, context} <- child_context(run),
         {:ok, anchor} <- anchor_window(run),
         {:ok, parent} <- fetch_parent(parent_by_id, context.backfill_run_id),
         now <- run.updated_at || DateTime.utc_now(),
         {:ok, window} <-
           BackfillWindow.new(%{
             backfill_run_id: context.backfill_run_id,
             child_run_id: run.id,
             pipeline_module: context.pipeline_module,
             manifest_version_id: run.manifest_version_id,
             coverage_baseline_id: coverage_baseline_id(parent),
             window_kind: anchor.kind,
             window_start_at: anchor.start_at,
             window_end_at: anchor.end_at,
             timezone: anchor.timezone,
             window_key: context.window_key,
             status: window_status(run),
             attempt_count: 1,
             latest_attempt_run_id: run.id,
             last_success_run_id: if(run.status == :ok, do: run.id),
             last_error: if(run.status == :ok, do: nil, else: run.error),
             errors: if(run.status == :ok or is_nil(run.error), do: [], else: [run.error]),
             started_at: run.inserted_at,
             finished_at: if(run.status in @terminal_statuses, do: now),
             created_at: run.inserted_at || now,
             updated_at: now
           }) do
      {:ok, window}
    else
      :ignore -> {:skip, nil}
      {:error, reason} -> {:skip, reason}
    end
  end

  defp derive_asset_window_states(_runs, _windows, backfill_run_id: _id),
    do: {[], [skip(nil, :asset_window_state, :latest_state_requires_wider_scope)]}

  defp derive_asset_window_states(runs, windows, _scope) do
    window_by_run_id = Map.new(windows, &{&1.child_run_id, &1})

    runs
    |> Enum.reduce({[], []}, fn run, {states, skips} ->
      case Map.fetch(window_by_run_id, run.id) do
        {:ok, window} -> derive_asset_states_for_run(run, window, states, skips)
        :error -> {states, skips}
      end
    end)
    |> then(fn {states, skips} -> {latest_asset_states(states), Enum.reverse(skips)} end)
  end

  defp derive_asset_states_for_run(%RunState{} = run, %BackfillWindow{} = window, states, skips) do
    case asset_results(run) do
      [] -> {states, [skip(run.id, :asset_window_state, :missing_terminal_result) | skips]}
      results -> Enum.reduce(results, {states, skips}, &derive_asset_state(&1, run, window, &2))
    end
  end

  defp derive_asset_state(result, %RunState{} = run, %BackfillWindow{} = window, {states, skips}) do
    with {:ok, {asset_ref_module, asset_ref_name}} <- asset_result_ref(result),
         {:ok, status} <- asset_result_status(result),
         metadata <- asset_result_metadata(result),
         error <- asset_result_error(result),
         {:ok, state} <-
           AssetWindowState.new(%{
             asset_ref_module: asset_ref_module,
             asset_ref_name: asset_ref_name,
             pipeline_module: window.pipeline_module,
             manifest_version_id: run.manifest_version_id,
             window_kind: window.window_kind,
             window_start_at: window.window_start_at,
             window_end_at: window.window_end_at,
             timezone: window.timezone,
             window_key: window.window_key,
             status: status,
             latest_run_id: run.id,
             latest_parent_run_id: window.backfill_run_id,
             latest_success_run_id: if(status == :ok, do: run.id),
             latest_error: if(status == :ok, do: nil, else: error),
             errors: if(status == :ok or is_nil(error), do: [], else: [error]),
             rows_written: rows_written(metadata),
             metadata: metadata,
             updated_at: run.updated_at || DateTime.utc_now()
           }) do
      {[state | states], skips}
    else
      {:error, reason} -> {states, [skip(run.id, :asset_window_state, reason) | skips]}
    end
  end

  defp latest_asset_states(states) do
    states
    |> Enum.group_by(&{&1.asset_ref_module, &1.asset_ref_name, &1.window_key})
    |> Enum.map(fn {_key, grouped} ->
      Enum.max_by(grouped, &{DateTime.to_unix(&1.updated_at, :microsecond), &1.latest_run_id})
    end)
  end

  defp child_context(%RunState{trigger: trigger} = run) when is_map(trigger) do
    with :backfill <- field(trigger, :kind),
         backfill_run_id when is_binary(backfill_run_id) and backfill_run_id != "" <-
           field(trigger, :backfill_run_id),
         window_key when is_binary(window_key) and window_key != "" <- field(trigger, :window_key),
         {:ok, pipeline_module} <- pipeline_module(run) do
      {:ok,
       %{
         backfill_run_id: backfill_run_id,
         pipeline_module: pipeline_module,
         window_key: window_key
       }}
    else
      _ -> :ignore
    end
  end

  defp child_context(_run), do: :ignore

  defp fetch_parent(parent_by_id, backfill_run_id) do
    case Map.fetch(parent_by_id, backfill_run_id) do
      {:ok, parent} -> {:ok, parent}
      :error -> {:error, :missing_parent_backfill_run}
    end
  end

  defp anchor_window(%RunState{metadata: metadata}) do
    metadata
    |> field(:pipeline_context)
    |> field(:anchor_window)
    |> normalize_anchor()
  end

  defp normalize_anchor(%Anchor{} = anchor), do: {:ok, anchor}

  defp normalize_anchor(anchor) when is_map(anchor) do
    with kind when kind in [:hour, :day, :month, :year, "hour", "day", "month", "year"] <-
           field(anchor, :kind),
         {:ok, kind} <- normalize_kind(kind),
         %DateTime{} = start_at <- field(anchor, :start_at),
         %DateTime{} = end_at <- field(anchor, :end_at),
         timezone when is_binary(timezone) and timezone != "" <- field(anchor, :timezone),
         {:ok, anchor} <- Anchor.new(kind, start_at, end_at, timezone: timezone) do
      {:ok, anchor}
    else
      _ -> {:error, :missing_anchor_window_metadata}
    end
  end

  defp normalize_anchor(_anchor), do: {:error, :missing_anchor_window_metadata}

  defp normalize_kind(value) when value in [:hour, :day, :month, :year], do: {:ok, value}
  defp normalize_kind(value) when is_binary(value), do: {:ok, String.to_existing_atom(value)}

  defp window_status(%RunState{status: status}) when status in @terminal_statuses, do: status
  defp window_status(%RunState{}), do: :running

  defp coverage_baseline_id(%RunState{metadata: metadata}) when is_map(metadata) do
    metadata |> field(:backfill) |> field(:coverage_baseline_id)
  end

  defp coverage_metadata(%RunState{} = run) do
    [run.result |> field(:metadata) |> field(:coverage), field(run.metadata, :coverage)]
    |> Enum.find(&is_map/1)
    |> case do
      nil -> :ignore
      coverage -> require_coverage_keys(coverage)
    end
  end

  defp require_coverage_keys(coverage) do
    missing = Enum.filter(@required_coverage_keys, &(field(coverage, &1) in [nil, ""]))

    if missing == [],
      do: {:ok, coverage},
      else: {:error, {:missing_required_coverage_keys, missing}}
  end

  defp normalize_coverage_attrs(coverage) do
    attrs =
      [
        :source_key,
        :segment_key_hash,
        :segment_key_redacted,
        :coverage_until,
        :coverage_start_at,
        :window_kind,
        :timezone,
        :status,
        :metadata
      ]
      |> Enum.reduce(%{}, fn key, acc ->
        case field(coverage, key) do
          nil -> acc
          value -> Map.put(acc, key, value)
        end
      end)

    {:ok, attrs}
  end

  defp reject_raw_source_identity(value) do
    if raw_source_identity?(value), do: {:error, :raw_source_identity_not_allowed}, else: :ok
  end

  defp raw_source_identity?(%{__struct__: _}), do: false

  defp raw_source_identity?(value) when is_map(value),
    do:
      Enum.any?(value, fn {key, nested} ->
        raw_source_key?(key) or raw_source_identity?(nested)
      end)

  defp raw_source_identity?(value) when is_list(value),
    do: Enum.any?(value, &raw_source_identity?/1)

  defp raw_source_identity?(_value), do: false
  defp raw_source_key?(key) when is_atom(key), do: key in @raw_source_keys

  defp raw_source_key?(key) when is_binary(key),
    do: Enum.any?(@raw_source_keys, &(Atom.to_string(&1) == key))

  defp raw_source_key?(_key), do: false

  defp baseline_id(%RunState{} = run, pipeline_module, attrs) do
    hash_input =
      {:coverage_baseline, pipeline_module, attrs.source_key, attrs.segment_key_hash,
       attrs.window_kind, attrs.timezone, attrs.coverage_until, run.manifest_version_id}

    "baseline_" <>
      (:crypto.hash(:sha256, :erlang.term_to_binary(hash_input)) |> Base.encode16(case: :lower))
  end

  defp pipeline_module(%RunState{metadata: metadata}) when is_map(metadata) do
    cond do
      valid_module?(field(metadata, :pipeline_submit_ref)) ->
        {:ok, field(metadata, :pipeline_submit_ref)}

      valid_module?(field(field(metadata, :pipeline_context), :module)) ->
        {:ok, field(field(metadata, :pipeline_context), :module)}

      valid_module?(field(field(metadata, :pipeline_context), :pipeline_module)) ->
        {:ok, field(field(metadata, :pipeline_context), :pipeline_module)}

      true ->
        {:error, :missing_pipeline_module}
    end
  end

  defp valid_module?(module), do: is_atom(module) and not is_nil(module)

  defp asset_results(%RunState{status: status, result: %{asset_results: results}})
       when status in @terminal_statuses and is_list(results), do: results

  defp asset_results(%RunState{status: status, result: %{"asset_results" => results}})
       when status in @terminal_statuses and is_list(results), do: results

  defp asset_results(_run), do: []

  defp asset_result_ref(%AssetResult{ref: {module, name}}) when is_atom(module) and is_atom(name),
    do: {:ok, {module, name}}

  defp asset_result_ref(%{ref: {module, name}}) when is_atom(module) and is_atom(name),
    do: {:ok, {module, name}}

  defp asset_result_ref(_result), do: {:error, :invalid_asset_result_ref}

  defp asset_result_status(%AssetResult{status: status}), do: normalize_status(status)
  defp asset_result_status(%{status: status}), do: normalize_status(status)
  defp asset_result_status(_result), do: {:error, :invalid_asset_result_status}

  defp normalize_status(status) when status in @terminal_statuses, do: {:ok, status}

  defp normalize_status(status) when status in ["ok", "error", "cancelled", "timed_out"],
    do: {:ok, String.to_existing_atom(status)}

  defp normalize_status(_status), do: {:error, :invalid_asset_result_status}

  defp asset_result_metadata(%AssetResult{meta: metadata}) when is_map(metadata), do: metadata
  defp asset_result_metadata(%{meta: metadata}) when is_map(metadata), do: metadata
  defp asset_result_metadata(%{metadata: metadata}) when is_map(metadata), do: metadata
  defp asset_result_metadata(_result), do: %{}

  defp asset_result_error(%AssetResult{error: error}), do: error
  defp asset_result_error(%{error: error}), do: error
  defp asset_result_error(_result), do: nil

  defp rows_written(metadata) when is_map(metadata) do
    Enum.find_value([:rows_written, :row_count, :rows], fn key ->
      case Map.get(metadata, key) do
        value when is_integer(value) and value >= 0 -> value
        _ -> nil
      end
    end)
  end

  defp field(value, key) when is_map(value) and is_atom(key),
    do: Map.get(value, key) || Map.get(value, Atom.to_string(key))

  defp field(_value, _key), do: nil

  defp skip(nil, model, reason), do: %{model: model, reason: reason}
  defp skip(run_id, model, reason), do: %{run_id: run_id, model: model, reason: reason}

  defp reverse_pair({left, right}), do: {Enum.reverse(left), Enum.reverse(right)}

  defp report(apply?, scope, plan) do
    %{
      apply: apply?,
      scope: Map.new(scope),
      counts: %{
        coverage_baselines: length(plan.coverage_baselines),
        backfill_windows: length(plan.backfill_windows),
        asset_window_states: length(plan.asset_window_states),
        skips: length(plan.skips)
      },
      skips: plan.skips
    }
  end
end
