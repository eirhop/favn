defmodule FavnView.RunStepViewModel do
  @moduledoc false

  alias FavnView.LogsViewModel

  def from_run(run) do
    run_id = Map.get(run, :id)
    node_results = node_results(Map.get(run, :node_results, %{}), run_id)

    if node_results == [] do
      asset_results(Map.get(run, :asset_results, %{}), run_id)
    else
      node_results
    end
  end

  def node_results(results, run_id) when is_map(results) do
    results
    |> Enum.map(fn {key, result} -> node_result(result, run_id, key) end)
    |> Enum.sort_by(&{Map.get(&1, :secondary) || "", &1.asset_ref})
  end

  def node_results(results, run_id) when is_list(results) do
    results
    |> Enum.map(&node_result(&1, run_id, node_key(&1)))
    |> Enum.sort_by(&{Map.get(&1, :secondary) || "", &1.asset_ref})
  end

  def node_results(_results, _run_id), do: []

  def asset_results(results, run_id) when is_map(results) do
    results
    |> Enum.map(fn {key, result} -> asset_result(result, run_id, key) end)
    |> Enum.sort_by(&{Map.get(&1, :secondary) || "", &1.asset_ref})
  end

  def asset_results(results, run_id) when is_list(results) do
    results
    |> Enum.map(&asset_result(&1, run_id, nil))
    |> Enum.sort_by(&{Map.get(&1, :secondary) || "", &1.asset_ref})
  end

  def asset_results(_results, _run_id), do: []

  defp node_result(result, run_id, node_key) do
    asset_ref = LogsViewModel.ref_label(Map.get(result, :ref) || node_asset_ref(node_key))

    %{
      id: step_id(result, run_id, node_key, asset_ref),
      asset_ref: asset_ref,
      display_name: LogsViewModel.display_name(asset_ref),
      secondary: node_secondary(result),
      status: LogsViewModel.status_label(Map.get(result, :status)),
      status_tone: LogsViewModel.status_tone(Map.get(result, :status)),
      duration: LogsViewModel.duration_ms_label(Map.get(result, :duration_ms)),
      started_at: LogsViewModel.timestamp_label(Map.get(result, :started_at)),
      attempt: Map.get(result, :attempt),
      error: error_summary(Map.get(result, :error) || Map.get(result, :reason)),
      output: output_metadata(result),
      inspectable?: true
    }
  end

  defp asset_result(result, run_id, key) do
    asset_ref = LogsViewModel.ref_label(Map.get(result, :ref) || key)

    %{
      id: step_id(result, run_id, key, asset_ref),
      asset_ref: asset_ref,
      display_name: LogsViewModel.display_name(asset_ref),
      secondary: asset_secondary(result),
      status: LogsViewModel.status_label(Map.get(result, :status)),
      status_tone: LogsViewModel.status_tone(Map.get(result, :status)),
      duration: LogsViewModel.duration_ms_label(Map.get(result, :duration_ms)),
      started_at: LogsViewModel.timestamp_label(Map.get(result, :started_at)),
      attempt: Map.get(result, :attempt),
      error: error_summary(Map.get(result, :error)),
      output: output_metadata(result),
      inspectable?: true
    }
  end

  defp step_id(result, run_id, key, asset_ref) do
    Map.get(result, :id) || Map.get(result, "id") || Map.get(result, :step_id) ||
      Map.get(result, "step_id") || persisted_key_id(key, asset_ref) ||
      LogsViewModel.deterministic_step_id(run_id, asset_ref)
  end

  defp persisted_key_id(nil, _asset_ref), do: nil

  defp persisted_key_id(key, asset_ref) when is_binary(key) and key != asset_ref,
    do: LogsViewModel.safe_id(key)

  defp persisted_key_id(key, _asset_ref) when is_tuple(key),
    do:
      key
      |> :erlang.term_to_binary()
      |> Base.url_encode64(padding: false)
      |> LogsViewModel.safe_id()

  defp persisted_key_id(_key, _asset_ref), do: nil

  defp asset_secondary(result) do
    result
    |> Map.get(:stage)
    |> case do
      nil -> nil
      stage -> "Stage #{stage}"
    end
  end

  defp node_secondary(result) do
    [
      window_secondary(Map.get(result, :window)),
      freshness_secondary(Map.get(result, :freshness_key)),
      reason_secondary(Map.get(result, :reason)),
      asset_secondary(result)
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" · ")
    |> case do
      "" -> nil
      secondary -> secondary
    end
  end

  defp window_secondary(nil), do: nil
  defp window_secondary(%{label: label}) when is_binary(label), do: label
  defp window_secondary(%{"label" => label}) when is_binary(label), do: label
  defp window_secondary(%{id: id}) when is_binary(id), do: id
  defp window_secondary(%{"id" => id}) when is_binary(id), do: id
  defp window_secondary(window), do: "Window #{inspect(window, limit: 5)}"

  defp freshness_secondary(nil), do: nil
  defp freshness_secondary(key), do: "Freshness #{key}"

  defp reason_secondary(nil), do: nil
  defp reason_secondary(reason) when reason in [:fresh, "fresh"], do: "Fresh"
  defp reason_secondary(reason), do: humanize(reason)

  defp node_key(result), do: Map.get(result, :node_key) || Map.get(result, "node_key")
  defp node_asset_ref({ref, _window}) when is_tuple(ref), do: ref
  defp node_asset_ref(_node_key), do: nil

  defp output_metadata(result) do
    meta = Map.get(result, :meta, %{}) || %{}

    Map.get(meta, :output) || Map.get(meta, "output") || Map.get(meta, :outputs) ||
      Map.get(meta, "outputs") || Map.get(meta, :materialization) ||
      Map.get(meta, "materialization")
  end

  defp error_summary(nil), do: nil
  defp error_summary(%{message: message}) when is_binary(message), do: message
  defp error_summary(%{"message" => message}) when is_binary(message), do: message
  defp error_summary(%{reason: reason}), do: error_reason_summary(reason)
  defp error_summary(%{"reason" => reason}), do: error_reason_summary(reason)
  defp error_summary(reason) when is_binary(reason), do: reason
  defp error_summary(reason) when is_atom(reason), do: humanize(reason)
  defp error_summary(_error), do: "Execution error"

  defp error_reason_summary(reason) when is_binary(reason), do: reason
  defp error_reason_summary(reason) when is_atom(reason), do: humanize(reason)
  defp error_reason_summary(reason), do: inspect(reason, limit: 5, printable_limit: 200)

  defp humanize(value) when is_atom(value), do: value |> Atom.to_string() |> humanize()

  defp humanize(value) when is_binary(value) do
    value
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  defp humanize(value), do: inspect(value)
end
