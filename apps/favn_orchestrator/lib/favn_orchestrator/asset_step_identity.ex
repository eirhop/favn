defmodule FavnOrchestrator.AssetStepIdentity do
  @moduledoc """
  Canonical asset-step identity helpers owned by the orchestrator.

  Asset step IDs identify one planned asset execution within a run. They are
  stable across persisted results, run events, logs, and UI links.
  """

  @doc """
  Returns the canonical asset step ID for a planned run node.

  The planned `node_key` is preferred because it distinguishes repeated
  executions of the same asset reference, such as windowed nodes. Legacy callers
  without a node key fall back to the historical run/ref-derived ID.
  """
  @spec asset_step_id(String.t(), term(), term()) :: String.t()
  def asset_step_id(run_id, node_key, asset_ref) when is_binary(run_id) do
    persisted_key_id(node_key, asset_ref) || safe_id("#{run_id}:#{inspect(asset_ref)}")
  end

  defp persisted_key_id(nil, _asset_ref), do: nil
  defp persisted_key_id(key, asset_ref) when is_binary(key) and key != asset_ref, do: safe_id(key)

  defp persisted_key_id(key, _asset_ref) when is_tuple(key),
    do: key |> :erlang.term_to_binary() |> Base.url_encode64(padding: false) |> safe_id()

  defp persisted_key_id(_key, _asset_ref), do: nil

  defp safe_id(value), do: value |> to_string() |> String.replace(~r/[^a-zA-Z0-9_-]+/, "-")
end
