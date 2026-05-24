defmodule FavnOrchestrator.Storage.ExecutionGroupSummary do
  @moduledoc """
  Builds and serializes the persisted execution-group overview read model.

  The summary is storage-owned data used to page operator execution-group lists
  without repeatedly grouping all runs or hydrating every group one by one.
  """

  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.RunQuery

  @terminal_statuses [
    :ok,
    :partial,
    :error,
    :blocked,
    :cancelled,
    :timed_out,
    :skipped,
    :skipped_fresh
  ]
  @failed_statuses [:error, :timed_out, :cancelled, :blocked]
  @running_statuses [:running, :retrying]
  @queued_statuses [:pending, :queued, nil]

  @type t :: map()

  @doc "Builds an execution-group summary map from the current group runs and windows."
  @spec build([RunState.t()], [BackfillWindow.t()]) ::
          {:ok, t()} | {:error, :empty_execution_group}
  def build([], _windows), do: {:error, :empty_execution_group}

  def build([%RunState{} | _] = runs, windows) when is_list(windows) do
    group_id = runs |> List.first() |> RunQuery.root_execution_group_id()
    runs_by_id = Map.new(runs, &{&1.id, &1})
    root = Map.get(runs_by_id, group_id) || Enum.min_by(runs, &run_started_sort_key/1)

    children =
      runs
      |> Enum.reject(&(&1.id == root.id))
      |> Enum.sort_by(&run_started_sort_key/1)

    windows = Enum.filter(windows, &match?(%BackfillWindow{}, &1))
    attempt_counts = attempt_counts(root, children, windows)
    window_counts = window_counts(windows)
    public_root_status = public_status(root)
    active? = active_group?(runs, windows, attempt_counts)
    status = group_status(public_root_status, attempt_counts, window_counts, active?)
    failure_count = attempt_counts.failed + window_counts.failed
    timing = group_timing(runs, windows, active?)
    target_assets = target_assets(root)

    {:ok,
     %{
       id: root.id,
       root_execution_group_id: root.id,
       status: status,
       health: group_health(status, failure_count, active?),
       active?: active?,
       trigger_type: RunQuery.trigger_type(root),
       target_assets: target_assets,
       root_status: public_root_status,
       started_at: timing.started_at,
       finished_at: timing.finished_at,
       duration_ms: duration_ms(timing.started_at, timing.finished_at),
       total_windows: window_counts.total,
       completed_windows: window_counts.completed,
       failed_windows: window_counts.failed,
       total_asset_attempts: attempt_counts.total,
       completed_asset_attempts: attempt_counts.completed,
       failed_asset_attempts: attempt_counts.failed,
       running_asset_attempts: attempt_counts.running,
       queued_asset_attempts: attempt_counts.queued,
       failure_count: failure_count,
       progress: progress(attempt_counts),
       summary_totals: %{windows: window_counts, asset_attempts: attempt_counts},
       last_activity_at: latest_datetime(Enum.flat_map(runs, &[&1.updated_at, &1.inserted_at])),
       currently_running_asset_attempts: [],
       child_run_ids: Enum.map(children, & &1.id)
     }}
  end

  @doc "Encodes a summary for adapter storage."
  @spec encode(t()) :: binary()
  def encode(summary) when is_map(summary), do: :erlang.term_to_binary(summary)

  @doc "Decodes a stored summary payload."
  @spec decode(binary()) :: {:ok, t()} | {:error, :invalid_execution_group_summary}
  def decode(payload) when is_binary(payload) do
    {:ok, :erlang.binary_to_term(payload, [:safe])}
  rescue
    ArgumentError -> {:error, :invalid_execution_group_summary}
  end

  defp attempt_counts(root, children, windows) do
    windows_by_child = Map.new(windows, &{&1.latest_attempt_run_id || &1.child_run_id, &1})

    [root | children]
    |> Enum.reject(&backfill_parent?/1)
    |> Enum.flat_map(&run_attempt_statuses(&1, Map.get(windows_by_child, &1.id)))
    |> then(fn statuses ->
      %{
        total: length(statuses),
        completed: Enum.count(statuses, &terminal_status?/1),
        failed: Enum.count(statuses, &failed_status?/1),
        running: Enum.count(statuses, &running_status?/1),
        queued: Enum.count(statuses, &queued_status?/1)
      }
    end)
  end

  defp run_attempt_statuses(%RunState{} = run, window) do
    statuses = persisted_result_statuses(run)

    cond do
      statuses != [] -> statuses
      match?(%BackfillWindow{}, window) -> [status_name(window.status)]
      active_status?(run.status) -> List.duplicate(run.status, max(expected_step_count(run), 1))
      expected_step_count(run) > 0 -> List.duplicate(public_status(run), expected_step_count(run))
      true -> []
    end
  end

  defp persisted_result_statuses(%RunState{result: result}) when is_map(result) do
    statuses = result_statuses(result, :node_results)

    case statuses do
      [] -> result_statuses(result, :asset_results)
      [_ | _] -> statuses
    end
  end

  defp persisted_result_statuses(_run), do: []

  defp result_statuses(result, field) do
    result
    |> Map.get(field, Map.get(result, Atom.to_string(field), []))
    |> result_values()
    |> Enum.map(&(map_get(&1, :status) || map_get(&1, :state)))
    |> Enum.map(&status_name/1)
    |> Enum.reject(&is_nil/1)
  end

  defp result_values(value) when is_map(value), do: Map.values(value)
  defp result_values(value) when is_list(value), do: value
  defp result_values(_value), do: []

  defp expected_step_count(%RunState{plan: %Favn.Plan{nodes: nodes}})
       when is_map(nodes) and map_size(nodes) > 0,
       do: map_size(nodes)

  defp expected_step_count(%RunState{target_refs: refs}) when is_list(refs), do: length(refs)
  defp expected_step_count(_run), do: 0

  defp window_counts(windows) do
    statuses = Enum.map(windows, &status_name(&1.status))

    %{
      total: length(windows),
      completed: Enum.count(statuses, &terminal_status?/1),
      failed: Enum.count(statuses, &failed_status?/1)
    }
  end

  defp active_group?(runs, windows, attempt_counts) do
    attempt_counts.running > 0 or attempt_counts.queued > 0 or
      Enum.any?(runs, &(public_status(&1) in [:pending, :running])) or
      Enum.any?(windows, &(status_name(&1.status) in [:pending, :queued, :running]))
  end

  defp group_status(root_status, attempt_counts, window_counts, active?) do
    cond do
      attempt_counts.failed > 0 or window_counts.failed > 0 -> :error
      active? -> :running
      root_status -> root_status
      true -> :pending
    end
  end

  defp group_health(_status, failure_count, _active?) when failure_count > 0, do: :error
  defp group_health(_status, _failure_count, true), do: :active
  defp group_health(:partial, _failure_count, _active?), do: :warning
  defp group_health(_status, _failure_count, _active?), do: :ok

  defp progress(%{total: 0}), do: nil

  defp progress(counts) do
    %{
      unit: :assets,
      label: "#{counts.completed} / #{counts.total} asset attempts",
      counts: counts
    }
  end

  defp group_timing(runs, windows, active?) do
    started_at =
      runs
      |> Enum.map(& &1.inserted_at)
      |> Kernel.++(Enum.map(windows, & &1.started_at))
      |> earliest_datetime()

    finished_at =
      if active? do
        nil
      else
        runs
        |> Enum.map(&finished_at/1)
        |> Kernel.++(Enum.map(windows, & &1.finished_at))
        |> latest_datetime()
      end

    %{started_at: started_at, finished_at: finished_at}
  end

  defp target_assets(%RunState{} = run),
    do: Enum.map(RunQuery.target_refs(run), &RunQuery.public_ref/1)

  defp backfill_parent?(%RunState{submit_kind: kind})
       when kind in [:backfill_asset, :backfill_pipeline],
       do: true

  defp backfill_parent?(_run), do: false

  defp public_status(%RunState{status: status}), do: status_name(status)

  defp status_name(nil), do: nil
  defp status_name(status) when is_atom(status), do: status

  defp status_name(status) when is_binary(status) do
    try do
      String.to_existing_atom(status)
    rescue
      ArgumentError -> status
    end
  end

  defp terminal_status?(status), do: status_name(status) in @terminal_statuses
  defp failed_status?(status), do: status_name(status) in @failed_statuses
  defp running_status?(status), do: status_name(status) in @running_statuses
  defp queued_status?(status), do: status_name(status) in @queued_statuses
  defp active_status?(status), do: status_name(status) in [:pending, :running]

  defp finished_at(%RunState{status: status, updated_at: updated_at})
       when status in [:ok, :partial, :error, :cancelled, :timed_out],
       do: updated_at

  defp finished_at(_run), do: nil

  defp duration_ms(%DateTime{} = started_at, %DateTime{} = finished_at),
    do: DateTime.diff(finished_at, started_at, :millisecond)

  defp duration_ms(_started_at, _finished_at), do: nil

  defp earliest_datetime(values), do: datetime_extreme(values, &(DateTime.compare(&1, &2) == :lt))
  defp latest_datetime(values), do: datetime_extreme(values, &(DateTime.compare(&1, &2) == :gt))

  defp datetime_extreme(values, compare_fun) do
    values
    |> Enum.reject(&is_nil/1)
    |> Enum.reduce(nil, fn
      %DateTime{} = value, nil ->
        value

      %DateTime{} = value, %DateTime{} = current ->
        if(compare_fun.(value, current), do: value, else: current)

      _value, current ->
        current
    end)
  end

  defp run_started_sort_key(%RunState{inserted_at: %DateTime{} = inserted_at}),
    do: DateTime.to_unix(inserted_at, :microsecond)

  defp run_started_sort_key(_run), do: 0

  defp map_get(map, key) when is_map(map),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
end
