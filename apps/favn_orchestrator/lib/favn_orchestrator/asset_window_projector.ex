defmodule FavnOrchestrator.AssetWindowProjector do
  @moduledoc """
  Projects terminal windowed asset executions into the asset/window read model.

  The read model is keyed by asset reference and concrete window key, independent
  of whether the run came from an asset command, pipeline command, or backfill
  child command.
  """

  alias Favn.Run.NodeResult
  alias Favn.Window.Key
  alias Favn.Window.Runtime
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @terminal_events [:run_finished, :run_failed, :run_cancelled, :run_timed_out]

  @spec project_transition(RunState.t(), atom(), map()) :: :ok | {:error, term()}
  def project_transition(run_state, event_type, data \\ %{})

  def project_transition(%RunState{} = run_state, event_type, _data)
      when event_type in @terminal_events do
    run_state
    |> node_results()
    |> Enum.reduce_while({:ok, []}, fn result, {:ok, acc} ->
      case asset_window_state(run_state, result) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, state} -> {:cont, {:ok, [state | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, states} -> Storage.put_asset_window_states(Enum.reverse(states))
      {:error, reason} -> {:error, reason}
    end
  end

  def project_transition(%RunState{}, _event_type, _data), do: :ok

  defp node_results(%RunState{result: %{node_results: results}}) when is_list(results),
    do: results

  defp node_results(_run_state), do: []

  defp asset_window_state(
         %RunState{} = run_state,
         %NodeResult{window: %Runtime{} = window} = result
       ) do
    asset_window_state(run_state, Map.from_struct(result), window)
  end

  defp asset_window_state(%RunState{} = run_state, %{window: %Runtime{} = window} = result) do
    asset_window_state(run_state, result, window)
  end

  defp asset_window_state(_run_state, _result), do: {:ok, nil}

  defp asset_window_state(%RunState{} = run_state, result, %Runtime{} = window) do
    with {:ok, {module, name}} <- result_ref(result),
         {:ok, status} <- result_status(result),
         {:ok, existing} <- existing_state(module, name, Key.encode(window.key)),
         {:ok, state} <- build_state(run_state, result, window, module, name, status, existing) do
      {:ok, state}
    end
  end

  defp build_state(
         %RunState{} = run_state,
         result,
         %Runtime{} = window,
         module,
         name,
         status,
         existing
       ) do
    window_key = Key.encode(window.key)
    now = run_state.updated_at || DateTime.utc_now()

    AssetWindowState.new(%{
      asset_ref_module: module,
      asset_ref_name: name,
      pipeline_module: Map.get(run_state.metadata, :pipeline_submit_ref),
      manifest_version_id: run_state.manifest_version_id,
      window_kind: window.kind,
      window_start_at: window.start_at,
      window_end_at: window.end_at,
      timezone: window.timezone,
      window_key: window_key,
      status: status,
      latest_run_id: run_state.id,
      latest_parent_run_id: run_state.parent_run_id,
      latest_success_run_id: latest_success_run_id(existing, status, run_state.id),
      latest_error: if(status == :ok, do: nil, else: result_error(result)),
      errors: next_errors(existing, status, result_error(result)),
      metadata: result_metadata(result),
      updated_at: now
    })
  end

  defp result_ref(%{ref: {module, name}}) when is_atom(module) and is_atom(name),
    do: {:ok, {module, name}}

  defp result_ref(_result), do: :error

  defp result_status(%{status: status}) when status in [:ok, :error, :cancelled, :timed_out],
    do: {:ok, status}

  defp result_status(%{status: :blocked}), do: {:ok, :error}
  defp result_status(%{status: :skipped_fresh}), do: {:ok, :ok}
  defp result_status(_result), do: {:ok, :error}

  defp existing_state(module, name, window_key) do
    case Storage.get_asset_window_state(module, name, window_key) do
      {:ok, %AssetWindowState{} = state} -> {:ok, state}
      {:error, :not_found} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp latest_success_run_id(_existing, :ok, run_id), do: run_id

  defp latest_success_run_id(%AssetWindowState{latest_success_run_id: run_id}, _status, _run_id),
    do: run_id

  defp latest_success_run_id(_existing, _status, _run_id), do: nil

  defp next_errors(existing, :ok, _error), do: existing_errors(existing)
  defp next_errors(existing, _status, nil), do: existing_errors(existing)
  defp next_errors(existing, _status, error), do: existing_errors(existing) ++ [error]

  defp existing_errors(%AssetWindowState{errors: errors}) when is_list(errors), do: errors
  defp existing_errors(_existing), do: []

  defp result_error(%{error: error}), do: error
  defp result_error(%{reason: reason}), do: reason
  defp result_error(_result), do: nil

  defp result_metadata(%{meta: metadata}) when is_map(metadata), do: metadata
  defp result_metadata(%{metadata: metadata}) when is_map(metadata), do: metadata
  defp result_metadata(_result), do: %{}
end
