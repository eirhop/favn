defmodule FavnOrchestrator.Storage.Adapter.Memory.Backfills do
  @moduledoc """
  Backfill read models and progress transitions for the in-memory adapter.

  Window keys are indexed by backfill run because progress and execution-group
  summaries repeatedly read that exact scope.
  """

  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Backfill.Progress
  alias FavnOrchestrator.CursorPage
  alias FavnOrchestrator.Storage.Adapter.Memory.Query
  alias FavnOrchestrator.Storage.Adapter.Memory.State

  @baseline_filters [
    :baseline_id,
    :pipeline_module,
    :source_key,
    :segment_key_hash,
    :window_kind,
    :timezone,
    :created_by_run_id,
    :manifest_version_id,
    :status
  ]
  @window_filters [
    :backfill_run_id,
    :child_run_id,
    :pipeline_module,
    :manifest_version_id,
    :coverage_baseline_id,
    :window_kind,
    :timezone,
    :window_key,
    :status,
    :latest_attempt_run_id,
    :last_success_run_id
  ]
  @asset_window_filters [
    :asset_ref_module,
    :asset_ref_name,
    :pipeline_module,
    :manifest_version_id,
    :window_kind,
    :timezone,
    :window_key,
    :status,
    :latest_run_id,
    :latest_parent_run_id,
    :latest_success_run_id
  ]

  @doc false
  @spec put_baseline(State.t(), CoverageBaseline.t()) :: State.t()
  def put_baseline(%State{} = state, %CoverageBaseline{} = baseline) do
    baselines = Map.put(state.coverage_baselines, baseline.baseline_id, baseline)
    %{state | coverage_baselines: baselines}
  end

  @doc false
  def get_baseline(%State{} = state, baseline_id),
    do: Query.fetch(state.coverage_baselines, baseline_id)

  @doc false
  def list_baselines(%State{} = state, filters) do
    with :ok <- Query.validate_filters(filters, @baseline_filters) do
      Query.page(state.coverage_baselines |> Map.values(), filters, fn baseline ->
        {-DateTime.to_unix(baseline.updated_at, :microsecond), baseline.baseline_id}
      end)
    end
  end

  @doc false
  @spec put_window(State.t(), BackfillWindow.t()) :: State.t()
  def put_window(%State{} = state, %BackfillWindow{} = window) do
    key = window_key(window)

    %{
      state
      | backfill_windows: Map.put(state.backfill_windows, key, window),
        backfill_window_keys_by_run: put_window_index(state.backfill_window_keys_by_run, window)
    }
  end

  @doc false
  @spec put_windows(State.t(), [BackfillWindow.t()]) :: State.t()
  def put_windows(%State{} = state, windows), do: Enum.reduce(windows, state, &put_window(&2, &1))

  @doc false
  @spec put_windows_with_progress(State.t(), [BackfillWindow.t()]) ::
          {:ok, State.t()} | {:error, term()}
  def put_windows_with_progress(%State{} = state, windows) when is_list(windows) do
    next_state = put_windows(state, windows)

    windows
    |> Enum.map(& &1.backfill_run_id)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, next_state}, fn backfill_run_id, {:ok, current_state} ->
      case rebuild_progress(current_state, backfill_run_id) do
        {{:ok, _progress}, updated_state} -> {:cont, {:ok, updated_state}}
        {{:error, reason}, _unchanged_state} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc false
  def get_window(%State{} = state, key), do: Query.fetch(state.backfill_windows, key)

  @doc false
  def list_windows(%State{} = state, filters) do
    with :ok <- Query.validate_filters(filters, @window_filters) do
      Query.page(Map.values(state.backfill_windows), filters, &window_sort_key/1)
    end
  end

  @doc false
  def scan_windows(%State{} = state, filters, opts) do
    with :ok <- Query.validate_filters(filters, @window_filters),
         {:ok, after_key} <- window_cursor(Keyword.get(opts, :after)) do
      rows =
        state.backfill_windows
        |> Map.values()
        |> Query.filter(filters)
        |> Enum.sort_by(&window_sort_key/1)
        |> Query.drop_after(after_key, &window_sort_key/1)
        |> Enum.take(Keyword.fetch!(opts, :limit) + 1)

      {:ok, CursorPage.from_fetched(rows, opts, &window_cursor!/1)}
    end
  end

  @doc false
  @spec apply_child_projection(State.t(), BackfillWindow.t(), [AssetWindowState.t()]) ::
          {{:ok, Progress.t()} | {:error, term()}, State.t()}
  def apply_child_projection(%State{} = state, %BackfillWindow{} = window, asset_states) do
    old_status =
      state.backfill_windows[window_key(window)] &&
        state.backfill_windows[window_key(window)].status

    next_state =
      state
      |> put_window(window)
      |> put_asset_window_states(asset_states)

    case next_progress(next_state, window.backfill_run_id, old_status, window.status) do
      {:ok, progress} ->
        progress_by_run = Map.put(next_state.backfill_progress, window.backfill_run_id, progress)
        {{:ok, progress}, %{next_state | backfill_progress: progress_by_run}}

      {:error, reason} ->
        {{:error, reason}, state}
    end
  end

  @doc false
  @spec get_progress(State.t(), String.t()) ::
          {{:ok, Progress.t()} | {:error, term()}, State.t()}
  def get_progress(%State{} = state, backfill_run_id) do
    case Map.fetch(state.backfill_progress, backfill_run_id) do
      {:ok, %Progress{} = progress} -> {{:ok, progress}, state}
      :error -> rebuild_progress(state, backfill_run_id)
    end
  end

  @doc false
  @spec rebuild_progress(State.t(), String.t()) ::
          {{:ok, Progress.t()} | {:error, term()}, State.t()}
  def rebuild_progress(%State{} = state, backfill_run_id) do
    case progress_from_windows(state, backfill_run_id) do
      {:ok, progress} ->
        progress_by_run = Map.put(state.backfill_progress, backfill_run_id, progress)
        {{:ok, progress}, %{state | backfill_progress: progress_by_run}}

      {:error, reason} ->
        {{:error, reason}, state}
    end
  end

  @doc false
  @spec put_asset_window_state(State.t(), AssetWindowState.t()) :: State.t()
  def put_asset_window_state(%State{} = state, %AssetWindowState{} = window_state) do
    key = {window_state.asset_ref_module, window_state.asset_ref_name, window_state.window_key}
    %{state | asset_window_states: Map.put(state.asset_window_states, key, window_state)}
  end

  @doc false
  def put_asset_window_states(%State{} = state, window_states) do
    Enum.reduce(window_states, state, &put_asset_window_state(&2, &1))
  end

  @doc false
  def get_asset_window_state(%State{} = state, key),
    do: Query.fetch(state.asset_window_states, key)

  @doc false
  def list_asset_window_states(%State{} = state, filters) do
    with :ok <- Query.validate_filters(filters, @asset_window_filters) do
      Query.page(Map.values(state.asset_window_states), filters, fn window_state ->
        {-DateTime.to_unix(window_state.updated_at, :microsecond),
         Atom.to_string(window_state.asset_ref_module),
         Atom.to_string(window_state.asset_ref_name), window_state.window_key}
      end)
    end
  end

  @doc false
  @spec windows_for_run(State.t(), String.t()) :: [BackfillWindow.t()]
  def windows_for_run(%State{} = state, backfill_run_id) do
    windows_for_run(state.backfill_windows, state.backfill_window_keys_by_run, backfill_run_id)
  end

  defp windows_for_run(windows, window_index, backfill_run_id) do
    window_index
    |> Map.get(backfill_run_id, MapSet.new())
    |> Enum.flat_map(fn key ->
      case Map.fetch(windows, key) do
        {:ok, window} -> [window]
        :error -> []
      end
    end)
  end

  @doc false
  @spec replace(State.t(), term(), [CoverageBaseline.t()], [BackfillWindow.t()], [
          AssetWindowState.t()
        ]) ::
          {:ok, [String.t()], State.t()} | {:error, term()}
  def replace(%State{} = state, requested_scope, baselines, windows, asset_states) do
    with {:ok, scope} <- replacement_scope(requested_scope) do
      affected_ids = affected_backfill_ids(state.backfill_windows, scope, windows)

      next_windows =
        state.backfill_windows
        |> reject_scope(scope)
        |> put_window_values(windows)

      next_window_index = window_index(next_windows)

      next_state = %{
        state
        | coverage_baselines:
            state.coverage_baselines |> reject_scope(scope) |> put_baseline_values(baselines),
          backfill_windows: next_windows,
          backfill_window_keys_by_run: next_window_index,
          backfill_progress:
            state.backfill_progress
            |> reject_progress(scope)
            |> rebuild_progress_for_ids(next_windows, next_window_index, affected_ids),
          asset_window_states:
            state.asset_window_states
            |> reject_scope(scope)
            |> put_asset_state_values(asset_states)
      }

      {:ok, affected_ids, next_state}
    end
  end

  defp next_progress(state, backfill_run_id, old_status, new_status) do
    case Map.fetch(state.backfill_progress, backfill_run_id) do
      {:ok, %Progress{} = progress} ->
        case Progress.apply_status_change(progress, old_status, new_status, DateTime.utc_now()) do
          {:ok, %Progress{} = next} ->
            {:ok, next}

          {:error, {:stale_backfill_progress, _old, _new, _counts}} ->
            progress_from_windows(state, backfill_run_id)

          {:error, reason} ->
            {:error, reason}
        end

      :error ->
        progress_from_windows(state, backfill_run_id)
    end
  end

  defp progress_from_windows(state, backfill_run_id) do
    progress_from_windows(
      state.backfill_windows,
      state.backfill_window_keys_by_run,
      backfill_run_id
    )
  end

  defp progress_from_windows(windows, window_index, backfill_run_id) do
    case windows_for_run(windows, window_index, backfill_run_id) do
      [] -> {:error, :not_found}
      windows -> Progress.from_windows(backfill_run_id, windows, DateTime.utc_now())
    end
  end

  defp rebuild_progress_for_ids(progress, windows, window_index, ids) do
    Enum.reduce(ids, progress, fn id, acc ->
      case progress_from_windows(windows, window_index, id) do
        {:ok, rebuilt} -> Map.put(acc, id, rebuilt)
        {:error, :not_found} -> Map.delete(acc, id)
        {:error, _reason} -> acc
      end
    end)
  end

  defp replacement_scope(:all), do: {:ok, :all}
  defp replacement_scope({:backfill_run, id}) when is_binary(id), do: {:ok, {:backfill_run, id}}
  defp replacement_scope({:pipeline, module}) when is_atom(module), do: {:ok, {:pipeline, module}}
  defp replacement_scope(scope), do: {:error, {:unsupported_replacement_scope, scope}}

  defp reject_scope(_values, :all), do: %{}

  defp reject_scope(values, scope) do
    values |> Enum.reject(fn {_key, value} -> in_scope?(value, scope) end) |> Map.new()
  end

  defp reject_progress(_values, :all), do: %{}
  defp reject_progress(values, {:backfill_run, id}), do: Map.delete(values, id)
  defp reject_progress(values, {:pipeline, _module}), do: values

  defp in_scope?(%CoverageBaseline{created_by_run_id: id}, {:backfill_run, id}), do: true
  defp in_scope?(%BackfillWindow{backfill_run_id: id}, {:backfill_run, id}), do: true
  defp in_scope?(%AssetWindowState{latest_parent_run_id: id}, {:backfill_run, id}), do: true
  defp in_scope?(%{pipeline_module: module}, {:pipeline, module}), do: true
  defp in_scope?(_value, _scope), do: false

  defp affected_backfill_ids(backfill_windows, :all, replacements) do
    (Enum.map(Map.values(backfill_windows), & &1.backfill_run_id) ++
       Enum.map(replacements, & &1.backfill_run_id))
    |> Enum.uniq()
  end

  defp affected_backfill_ids(_windows, {:backfill_run, id}, replacements) do
    Enum.uniq([id | Enum.map(replacements, & &1.backfill_run_id)])
  end

  defp affected_backfill_ids(windows, {:pipeline, module}, replacements) do
    deleted_ids =
      windows
      |> Map.values()
      |> Enum.filter(&(&1.pipeline_module == module))
      |> Enum.map(& &1.backfill_run_id)

    Enum.uniq(deleted_ids ++ Enum.map(replacements, & &1.backfill_run_id))
  end

  defp put_baseline_values(values, baselines) do
    Enum.reduce(baselines, values, &Map.put(&2, &1.baseline_id, &1))
  end

  defp put_window_values(values, windows) do
    Enum.reduce(windows, values, &Map.put(&2, window_key(&1), &1))
  end

  defp put_asset_state_values(values, states) do
    Enum.reduce(states, values, fn state, acc ->
      Map.put(acc, {state.asset_ref_module, state.asset_ref_name, state.window_key}, state)
    end)
  end

  defp put_window_index(index, window) do
    key = window_key(window)

    Map.update(
      index,
      window.backfill_run_id,
      MapSet.new([key]),
      &MapSet.put(&1, key)
    )
  end

  defp window_index(windows) do
    Enum.reduce(windows, %{}, fn {_key, window}, index -> put_window_index(index, window) end)
  end

  defp window_key(window), do: {window.backfill_run_id, window.pipeline_module, window.window_key}

  defp window_cursor(nil), do: {:ok, nil}

  defp window_cursor(%{
         kind: :backfill_window,
         window_start_at: %DateTime{} = started_at,
         backfill_run_id: run_id,
         pipeline_module: pipeline_module,
         window_key: window_key
       })
       when is_binary(run_id) and is_atom(pipeline_module) and is_binary(window_key),
       do: {:ok, window_sort_key(started_at, run_id, pipeline_module, window_key)}

  defp window_cursor(_cursor), do: {:error, :invalid_cursor_pagination}

  defp window_cursor!(window) do
    %{
      kind: :backfill_window,
      window_start_at: window.window_start_at,
      backfill_run_id: window.backfill_run_id,
      pipeline_module: window.pipeline_module,
      window_key: window.window_key
    }
  end

  defp window_sort_key(window) do
    window_sort_key(
      window.window_start_at,
      window.backfill_run_id,
      window.pipeline_module,
      window.window_key
    )
  end

  defp window_sort_key(started_at, run_id, pipeline_module, window_key) do
    {DateTime.to_unix(started_at, :microsecond), run_id, Atom.to_string(pipeline_module),
     window_key}
  end
end
