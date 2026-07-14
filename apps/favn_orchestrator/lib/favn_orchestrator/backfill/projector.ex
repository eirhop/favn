defmodule FavnOrchestrator.Backfill.Projector do
  @moduledoc """
  Projects terminal child backfill run transitions into normalized backfill state.

  Run snapshots and run events remain the authoritative write path. This module
  only maintains derived ledger and asset/window state after a run transition has
  already been persisted.
  """

  alias Favn.Run.AssetResult
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.Progress, as: BackfillProgress
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TransitionWriter

  @running_events [:run_created, :run_started]
  @terminal_events [:run_finished, :run_failed, :run_cancelled, :run_timed_out]
  @terminal_run_statuses [:ok, :partial, :error, :cancelled, :timed_out]

  @doc "Projects a durable child-run transition into the backfill read models."
  @spec project_transition(RunState.t(), atom(), map()) :: :ok | {:error, term()}
  def project_transition(%RunState{} = run_state, event_type, data \\ %{})
      when is_atom(event_type) and is_map(data) do
    case backfill_child_context(run_state) do
      {:ok, context} ->
        cond do
          event_type in @running_events ->
            project_running(context, run_state)

          event_type in @terminal_events ->
            project_terminal(context, run_state, event_type, data)

          true ->
            :ok
        end

      :ignore ->
        :ok
    end
  end

  defp backfill_child_context(%RunState{trigger: trigger} = run_state) when is_map(trigger) do
    with kind when kind in [:backfill, "backfill"] <- trigger_field(trigger, :kind),
         backfill_run_id when is_binary(backfill_run_id) and backfill_run_id != "" <-
           trigger_field(trigger, :backfill_run_id),
         window_key when is_binary(window_key) and window_key != "" <-
           trigger_field(trigger, :window_key),
         {:ok, pipeline_module} <- pipeline_module(run_state) do
      {:ok,
       %{
         backfill_run_id: backfill_run_id,
         pipeline_module: pipeline_module,
         window_key: window_key
       }}
    else
      _other -> :ignore
    end
  end

  defp trigger_field(trigger, key) when is_map(trigger) do
    Map.get(trigger, key) || Map.get(trigger, Atom.to_string(key))
  end

  defp project_running(context, %RunState{} = run_state) do
    now = run_state.updated_at || DateTime.utc_now()

    with {:ok, window} <- fetch_window(context) do
      if terminal_projection_for_same_attempt?(window, run_state.id) do
        :ok
      else
        updated = %{
          window
          | status: :running,
            child_run_id: window.child_run_id || run_state.id,
            latest_attempt_run_id: run_state.id,
            attempt_count: next_attempt_count(window, run_state.id),
            started_at: window.started_at || now,
            finished_at: nil,
            updated_at: now
        }

        with {:ok, progress} <- Storage.apply_backfill_child_projection(updated, []) do
          maybe_project_parent(progress)
        end
      end
    end
  end

  defp project_terminal(context, %RunState{} = run_state, event_type, data) do
    now = run_state.updated_at || DateTime.utc_now()
    status = terminal_window_status(event_type)
    error = terminal_error(status, run_state, data)

    with {:ok, window} <- fetch_window(context),
         updated <- %{
           window
           | status: status,
             child_run_id: window.child_run_id || run_state.id,
             latest_attempt_run_id: run_state.id,
             attempt_count: next_attempt_count(window, run_state.id),
             last_success_run_id:
               if(status == :ok, do: run_state.id, else: window.last_success_run_id),
             last_error: if(status == :ok, do: nil, else: error),
             errors: next_errors(window.errors, status, error),
             finished_at: now,
             updated_at: now
         },
         {:ok, asset_window_states} <- build_asset_window_states(updated, run_state),
         {:ok, progress} <-
           Storage.apply_backfill_child_projection(updated, asset_window_states) do
      maybe_project_parent(progress)
    end
  end

  defp fetch_window(%{
         backfill_run_id: backfill_run_id,
         pipeline_module: pipeline_module,
         window_key: window_key
       }) do
    Storage.get_backfill_window(backfill_run_id, pipeline_module, window_key)
  end

  defp maybe_project_parent(%BackfillProgress{} = progress) do
    project_parent_from_progress(progress)
  end

  @doc "Reprojects a terminal child backfill run into its persisted window."
  @spec reproject_child_window(RunState.t()) :: :ok | :ignore | {:error, term()}
  def reproject_child_window(%RunState{status: status} = run_state)
      when status in @terminal_run_statuses do
    case backfill_child_context(run_state) do
      {:ok, context} ->
        project_terminal(context, run_state, terminal_event_type(status), %{
          status: status,
          error: run_state.error
        })

      :ignore ->
        :ignore
    end
  end

  def reproject_child_window(%RunState{}), do: :ignore

  @doc "Reprojects a backfill parent run status from its persisted windows."
  @spec reproject_parent(String.t()) :: :ok | {:error, term()}
  def reproject_parent(backfill_run_id) when is_binary(backfill_run_id) do
    with {:ok, progress} <- Storage.rebuild_backfill_progress(backfill_run_id) do
      project_parent_from_progress(progress)
    end
  end

  defp project_parent_from_progress(%BackfillProgress{} = progress) do
    with {:ok, parent} <- Storage.get_run(progress.backfill_run_id),
         status <- projected_parent_status(progress.status, parent.status),
         error <- parent_error(status, parent),
         true <- status != parent.status or error != parent.error do
      event_type = parent_event_type(status)

      parent
      |> RunState.transition(
        status: status,
        error: error,
        result: %{status: status, backfill_windows: progress.total_count}
      )
      |> TransitionWriter.persist_transition(event_type, %{
        status: status,
        window_counts: BackfillProgress.window_counts(progress)
      })
    else
      false -> :ok
      {:error, :not_found} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec list_all_backfill_windows(keyword()) :: {:ok, [BackfillWindow.t()]} | {:error, term()}
  def list_all_backfill_windows(filters) when is_list(filters) do
    fetch_backfill_window_pages(filters, nil, [])
  end

  defp fetch_backfill_window_pages(filters, cursor, acc) do
    with {:ok, page} <-
           Storage.scan_backfill_windows(filters, [{:limit, 500}, {:after, cursor}]) do
      acc = prepend_page_items(page.items, acc)

      if page.has_more? do
        fetch_backfill_window_pages(filters, page.next_cursor, acc)
      else
        {:ok, Enum.reverse(acc)}
      end
    end
  end

  defp prepend_page_items(items, acc), do: Enum.reduce(items, acc, &[&1 | &2])

  @doc false
  @spec parent_status([BackfillWindow.t()]) ::
          :running | :ok | :partial | :cancelled | :timed_out | :error
  def parent_status([]), do: :running

  def parent_status(windows) do
    statuses = Enum.map(windows, & &1.status)

    cond do
      Enum.all?(statuses, &(&1 == :ok)) -> :ok
      Enum.all?(statuses, &(&1 == :cancelled)) -> :cancelled
      Enum.all?(statuses, &(&1 == :timed_out)) -> :timed_out
      Enum.any?(statuses, &(&1 == :ok)) -> :partial
      mixed_terminal_failure?(statuses) -> :partial
      Enum.any?(statuses, &(&1 in [:pending, :running])) -> :running
      true -> :error
    end
  end

  defp mixed_terminal_failure?(statuses) do
    Enum.any?(statuses, &(&1 in [:error, :cancelled, :timed_out])) and
      Enum.any?(statuses, &(&1 in [:pending, :running]))
  end

  defp parent_event_type(:running), do: :backfill_progressed
  defp parent_event_type(:ok), do: :backfill_finished
  defp parent_event_type(:partial), do: :backfill_partial
  defp parent_event_type(:cancelled), do: :backfill_cancelled
  defp parent_event_type(:timed_out), do: :backfill_timed_out
  defp parent_event_type(:error), do: :backfill_failed

  defp parent_error(:ok, _parent), do: nil
  defp parent_error(_status, %RunState{} = parent), do: parent.error

  defp projected_parent_status(:running, status)
       when status in [:partial, :error, :cancelled, :timed_out],
       do: status

  defp projected_parent_status(status, _parent_status), do: status

  defp build_asset_window_states(%BackfillWindow{} = window, %RunState{} = run_state) do
    run_state
    |> asset_results()
    |> Enum.reduce_while({:ok, []}, fn result, {:ok, acc} ->
      case asset_window_state(window, run_state, result) do
        {:ok, state} ->
          {:cont, {:ok, [state | acc]}}

        :ignore ->
          {:cont, {:ok, acc}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, states} -> {:ok, Enum.reverse(states)}
      {:error, _reason} = error -> error
    end
  end

  defp asset_window_state(%BackfillWindow{} = window, %RunState{} = run_state, result) do
    with {:ok, {asset_ref_module, asset_ref_name}} <- asset_result_ref(result),
         status <- asset_result_status(result),
         metadata <- asset_result_metadata(result),
         error <- asset_result_error(result),
         {:ok, existing} <-
           existing_asset_window_state(asset_ref_module, asset_ref_name, window.window_key),
         {:ok, state} <-
           AssetWindowState.new(%{
             asset_ref_module: asset_ref_module,
             asset_ref_name: asset_ref_name,
             pipeline_module: window.pipeline_module,
             manifest_version_id: run_state.manifest_version_id,
             window_kind: window.window_kind,
             window_start_at: window.window_start_at,
             window_end_at: window.window_end_at,
             timezone: window.timezone,
             window_key: window.window_key,
             status: status,
             latest_run_id: run_state.id,
             latest_parent_run_id: window.backfill_run_id,
             latest_success_run_id:
               if(status == :ok, do: run_state.id, else: existing_latest_success_run_id(existing)),
             latest_error: if(status == :ok, do: nil, else: error),
             errors:
               if(status == :ok,
                 do: existing_errors(existing),
                 else: append_error(existing_errors(existing), error)
               ),
             rows_written: rows_written(metadata),
             metadata: metadata,
             updated_at: run_state.updated_at || DateTime.utc_now()
           }) do
      {:ok, state}
    else
      :error -> :ignore
      {:error, _reason} = error -> error
    end
  end

  defp existing_asset_window_state(asset_ref_module, asset_ref_name, window_key) do
    case Storage.get_asset_window_state(asset_ref_module, asset_ref_name, window_key) do
      {:ok, %AssetWindowState{} = state} -> {:ok, state}
      {:error, :not_found} -> {:ok, nil}
      {:error, _reason} = error -> error
    end
  end

  defp existing_latest_success_run_id(%AssetWindowState{latest_success_run_id: run_id}),
    do: run_id

  defp existing_latest_success_run_id(_state), do: nil

  defp existing_errors(%AssetWindowState{errors: errors}) when is_list(errors), do: errors
  defp existing_errors(_state), do: []

  defp append_error(errors, nil), do: errors
  defp append_error(errors, error), do: errors ++ [error]

  defp asset_results(%RunState{result: result}) when is_map(result) do
    case field(result, :asset_results) do
      results when is_list(results) -> results
      results when is_map(results) -> Map.values(results)
      _other -> []
    end
  end

  defp asset_results(_run_state), do: []

  defp asset_result_ref(%AssetResult{ref: {module, name}}) when is_atom(module) and is_atom(name),
    do: {:ok, {module, name}}

  defp asset_result_ref(%{ref: {module, name}}) when is_atom(module) and is_atom(name),
    do: {:ok, {module, name}}

  defp asset_result_ref(result) when is_map(result) do
    case field(result, :ref) do
      {module, name} when is_atom(module) and is_atom(name) -> {:ok, {module, name}}
      _other -> :error
    end
  end

  defp asset_result_ref(_result), do: :error

  defp asset_result_status(%AssetResult{status: status}), do: normalize_asset_status(status)
  defp asset_result_status(%{status: status}), do: normalize_asset_status(status)

  defp asset_result_status(result) when is_map(result),
    do: normalize_asset_status(field(result, :status))

  defp normalize_asset_status(status) when status in [:ok, :error, :cancelled, :timed_out],
    do: status

  defp normalize_asset_status(status) when status in ["ok", "error", "cancelled", "timed_out"],
    do: String.to_existing_atom(status)

  defp normalize_asset_status(_status), do: :error

  defp asset_result_metadata(%AssetResult{meta: metadata}) when is_map(metadata), do: metadata
  defp asset_result_metadata(%{meta: metadata}) when is_map(metadata), do: metadata
  defp asset_result_metadata(%{metadata: metadata}) when is_map(metadata), do: metadata

  defp asset_result_metadata(result) when is_map(result) do
    case field(result, :meta) || field(result, :metadata) do
      metadata when is_map(metadata) -> metadata
      _other -> %{}
    end
  end

  defp asset_result_error(%AssetResult{error: error}), do: error
  defp asset_result_error(%{error: error}), do: error
  defp asset_result_error(result) when is_map(result), do: field(result, :error)

  defp rows_written(metadata) when is_map(metadata) do
    Enum.find_value([:rows_written, :row_count, :rows], fn key ->
      case field(metadata, key) do
        value when is_integer(value) and value >= 0 -> value
        _other -> nil
      end
    end)
  end

  defp terminal_window_status(:run_finished), do: :ok
  defp terminal_window_status(:run_failed), do: :error
  defp terminal_window_status(:run_cancelled), do: :cancelled
  defp terminal_window_status(:run_timed_out), do: :timed_out

  defp terminal_event_type(:ok), do: :run_finished
  defp terminal_event_type(:cancelled), do: :run_cancelled
  defp terminal_event_type(:timed_out), do: :run_timed_out
  defp terminal_event_type(_status), do: :run_failed

  defp terminal_error(:ok, _run_state, _data), do: nil
  defp terminal_error(_status, %RunState{error: nil}, data), do: field(data, :error)
  defp terminal_error(_status, %RunState{error: error}, _data), do: error

  defp next_errors(errors, :ok, _error) when is_list(errors), do: errors
  defp next_errors(errors, _status, nil) when is_list(errors), do: errors

  defp next_errors(errors, _status, error) when is_list(errors) do
    if List.last(errors) == error, do: errors, else: errors ++ [error]
  end

  defp terminal_projection_for_same_attempt?(
         %BackfillWindow{latest_attempt_run_id: run_id, status: status},
         run_id
       ),
       do: status in @terminal_run_statuses

  defp terminal_projection_for_same_attempt?(_window, _run_id), do: false

  defp next_attempt_count(
         %BackfillWindow{latest_attempt_run_id: run_id, attempt_count: count},
         run_id
       ),
       do: max(count, 1)

  defp next_attempt_count(%BackfillWindow{attempt_count: count}, _run_id), do: count + 1

  defp pipeline_module(%RunState{metadata: metadata}) when is_map(metadata) do
    case field(metadata, :pipeline_submit_ref) ||
           asset_module(field(metadata, :asset_submit_ref)) do
      module when is_atom(module) and not is_nil(module) -> {:ok, module}
      _other -> {:error, :missing_backfill_pipeline_module}
    end
  end

  defp asset_module({module, _name}) when is_atom(module), do: module
  defp asset_module(_asset_ref), do: nil

  defp field(map, key) when is_map(map) and is_atom(key),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
end
