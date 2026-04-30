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
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TransitionWriter

  @running_events [:run_created, :run_started]
  @terminal_events [:run_finished, :run_failed, :run_cancelled, :run_timed_out]

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
    with :backfill <- Map.get(trigger, :kind),
         backfill_run_id when is_binary(backfill_run_id) and backfill_run_id != "" <-
           Map.get(trigger, :backfill_run_id),
         window_key when is_binary(window_key) and window_key != "" <-
           Map.get(trigger, :window_key),
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

  defp project_running(context, %RunState{} = run_state) do
    now = run_state.updated_at || DateTime.utc_now()

    with {:ok, window} <- fetch_window(context),
         updated <- %{
           window
           | status: :running,
             child_run_id: window.child_run_id || run_state.id,
             latest_attempt_run_id: run_state.id,
             attempt_count: next_attempt_count(window, run_state.id),
             started_at: window.started_at || now,
             updated_at: now
         },
         :ok <- Storage.put_backfill_window(updated) do
      maybe_project_parent(context.backfill_run_id)
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
         :ok <- Storage.put_backfill_window(updated),
         :ok <- project_asset_window_states(updated, run_state) do
      maybe_project_parent(context.backfill_run_id)
    end
  end

  defp fetch_window(%{
         backfill_run_id: backfill_run_id,
         pipeline_module: pipeline_module,
         window_key: window_key
       }) do
    Storage.get_backfill_window(backfill_run_id, pipeline_module, window_key)
  end

  defp maybe_project_parent(backfill_run_id) do
    with {:ok, windows} <- Storage.list_backfill_windows(backfill_run_id: backfill_run_id),
         {:ok, parent} <- Storage.get_run(backfill_run_id),
         status <- parent_status(windows),
         true <- status != parent.status do
      event_type = parent_event_type(status)

      parent
      |> RunState.transition(
        status: status,
        result: %{status: status, backfill_windows: length(windows)}
      )
      |> TransitionWriter.persist_transition(event_type, %{
        status: status,
        window_counts: window_counts(windows)
      })
    else
      false -> :ok
      {:error, :not_found} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp parent_status([]), do: :running

  defp parent_status(windows) do
    statuses = Enum.map(windows, & &1.status)

    cond do
      Enum.any?(statuses, &(&1 in [:pending, :running])) -> :running
      Enum.all?(statuses, &(&1 == :ok)) -> :ok
      Enum.all?(statuses, &(&1 == :cancelled)) -> :cancelled
      Enum.all?(statuses, &(&1 == :timed_out)) -> :timed_out
      Enum.any?(statuses, &(&1 == :ok)) -> :partial
      true -> :error
    end
  end

  defp parent_event_type(:running), do: :backfill_progressed
  defp parent_event_type(:ok), do: :backfill_finished
  defp parent_event_type(:partial), do: :backfill_partial
  defp parent_event_type(:cancelled), do: :backfill_cancelled
  defp parent_event_type(:timed_out), do: :backfill_timed_out
  defp parent_event_type(:error), do: :backfill_failed

  defp window_counts(windows) do
    Enum.reduce(windows, %{}, fn %BackfillWindow{status: status}, acc ->
      Map.update(acc, status, 1, &(&1 + 1))
    end)
  end

  defp project_asset_window_states(%BackfillWindow{} = window, %RunState{} = run_state) do
    run_state
    |> asset_results()
    |> Enum.reduce_while(:ok, fn result, :ok ->
      case asset_window_state(window, run_state, result) do
        {:ok, state} ->
          case Storage.put_asset_window_state(state) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end

        :ignore ->
          {:cont, :ok}
      end
    end)
  end

  defp asset_window_state(%BackfillWindow{} = window, %RunState{} = run_state, result) do
    with {:ok, {asset_ref_module, asset_ref_name}} <- asset_result_ref(result),
         status <- asset_result_status(result),
         metadata <- asset_result_metadata(result),
         error <- asset_result_error(result),
         existing <-
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
      {:ok, %AssetWindowState{} = state} -> state
      {:error, :not_found} -> nil
      {:error, _reason} -> nil
    end
  end

  defp existing_latest_success_run_id(%AssetWindowState{latest_success_run_id: run_id}),
    do: run_id

  defp existing_latest_success_run_id(_state), do: nil

  defp existing_errors(%AssetWindowState{errors: errors}) when is_list(errors), do: errors
  defp existing_errors(_state), do: []

  defp append_error(errors, nil), do: errors
  defp append_error(errors, error), do: errors ++ [error]

  defp asset_results(%RunState{result: %{asset_results: results}}) when is_list(results),
    do: results

  defp asset_results(_run_state), do: []

  defp asset_result_ref(%AssetResult{ref: {module, name}}) when is_atom(module) and is_atom(name),
    do: {:ok, {module, name}}

  defp asset_result_ref(%{ref: {module, name}}) when is_atom(module) and is_atom(name),
    do: {:ok, {module, name}}

  defp asset_result_ref(_result), do: :error

  defp asset_result_status(%AssetResult{status: status}), do: normalize_asset_status(status)
  defp asset_result_status(%{status: status}), do: normalize_asset_status(status)
  defp asset_result_status(_result), do: :error

  defp normalize_asset_status(status) when status in [:ok, :error, :cancelled, :timed_out],
    do: status

  defp normalize_asset_status(status) when status in ["ok", "error", "cancelled", "timed_out"],
    do: String.to_existing_atom(status)

  defp normalize_asset_status(_status), do: :error

  defp asset_result_metadata(%AssetResult{meta: metadata}) when is_map(metadata), do: metadata
  defp asset_result_metadata(%{meta: metadata}) when is_map(metadata), do: metadata
  defp asset_result_metadata(%{metadata: metadata}) when is_map(metadata), do: metadata
  defp asset_result_metadata(_result), do: %{}

  defp asset_result_error(%AssetResult{error: error}), do: error
  defp asset_result_error(%{error: error}), do: error
  defp asset_result_error(_result), do: :asset_failed

  defp rows_written(metadata) when is_map(metadata) do
    Enum.find_value([:rows_written, :row_count, :rows], fn key ->
      case Map.get(metadata, key) do
        value when is_integer(value) and value >= 0 -> value
        _other -> nil
      end
    end)
  end

  defp terminal_window_status(:run_finished), do: :ok
  defp terminal_window_status(:run_failed), do: :error
  defp terminal_window_status(:run_cancelled), do: :cancelled
  defp terminal_window_status(:run_timed_out), do: :timed_out

  defp terminal_error(:ok, _run_state, _data), do: nil
  defp terminal_error(_status, %RunState{error: nil}, data), do: Map.get(data, :error)
  defp terminal_error(_status, %RunState{error: error}, _data), do: error

  defp next_errors(errors, :ok, _error) when is_list(errors), do: errors
  defp next_errors(errors, _status, nil) when is_list(errors), do: errors
  defp next_errors(errors, _status, error) when is_list(errors), do: errors ++ [error]

  defp next_attempt_count(
         %BackfillWindow{latest_attempt_run_id: run_id, attempt_count: count},
         run_id
       ),
       do: max(count, 1)

  defp next_attempt_count(%BackfillWindow{attempt_count: count}, _run_id), do: count + 1

  defp pipeline_module(%RunState{metadata: metadata}) when is_map(metadata) do
    case Map.get(metadata, :pipeline_submit_ref) do
      module when is_atom(module) and not is_nil(module) -> {:ok, module}
      _other -> {:error, :missing_backfill_pipeline_module}
    end
  end
end
